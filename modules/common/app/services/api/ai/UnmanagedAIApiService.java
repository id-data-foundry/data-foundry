package services.api.ai;

import java.io.File;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.javadsl.FileIO;
import org.apache.pekko.stream.javadsl.Framing;
import org.apache.pekko.stream.javadsl.FramingTruncation;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import play.Logger;
import play.cache.SyncCacheApi;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.libs.ws.WSBodyReadables;
import play.libs.ws.WSClient;
import play.libs.ws.WSRequest;
import play.libs.ws.WSResponse;
import play.mvc.Http;
import play.mvc.Http.MultipartFormData;
import play.mvc.Http.MultipartFormData.DataPart;
import play.mvc.Http.MultipartFormData.FilePart;
import services.api.ApiServiceConstants;
import services.api.ai.LocalModelMetadata.ModelMetadata;
import services.api.remoting.RemoteApiRequest;
import services.api.remoting.RemoteApiRequest.Outcome;
import services.inlets.ScheduledService;
import services.notifications.NotificationLevel;
import services.notifications.NotificationMessage;
import services.notifications.SystemNotificationService;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;
import utils.conf.ConfigurationUtils;

@Singleton
public class UnmanagedAIApiService extends AbstractAIApiService implements ApiServiceConstants, ScheduledService {

	private static final Logger.ALogger logger = Logger.of(UnmanagedAIApiService.class);
	private static final java.util.regex.Pattern TOTAL_TOKENS_PATTERN = java.util.regex.Pattern
			.compile("\"total_tokens\"\\s*:\\s*(\\d+)");
	private static final ByteString SSE_SEP = ByteString.fromString("\n\n");
	private static final int MAX_SSE_FRAME = 1 << 20; // 1 MiB per event

	private final Config configuration;
	private final AiLaneLimiter laneLimiter;
	private final WSClient wsClient;
	private final SyncCacheApi cache;
	private final Materializer materializer;
	private final SystemNotificationService notificationService;

	private final Duration streamIdleTimeout;
	private final Duration streamMaxDuration;

	private final AtomicBoolean isAiOnline = new AtomicBoolean(true);
	private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
	private final int failureThreshold;
	private final boolean alertOnOffline;
	private final boolean alertOnRecovery;

	// generate an internal API key on every start
	private final String internalDocumentationAPIKey = "df-internal-"
			+ UUID.randomUUID().toString().replace("-", "").substring(0, 16);

	// dedicated single-threaded daemon executor to offload usage logging from the request completion path
	private final ExecutorService dbLogExecutor = Executors.newSingleThreadExecutor(r -> {
		Thread t = new Thread(r, "df-ai-usage-logger");
		t.setDaemon(true);
		return t;
	});

	@Inject
	protected UnmanagedAIApiService(Config configuration, SyncCacheApi cache, AdminUtils adminUtils,
			DatasetConnector datasetConnector, TokenResolverUtil tokenResolver, AiLaneLimiter laneLimiter,
			WSClient wsClient, Materializer materializer, LocalModelMetadata lmmd,
			SystemNotificationService notificationService) {
		super(configuration, adminUtils, datasetConnector, tokenResolver, lmmd);
		this.configuration = configuration;
		this.cache = cache;
		this.laneLimiter = laneLimiter;
		this.wsClient = wsClient;
		this.materializer = materializer;
		this.notificationService = notificationService;

		this.failureThreshold = configuration.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_AI_THRESHOLD)
				? configuration.getInt(ConfigurationUtils.DF_NOTIFICATIONS_AI_THRESHOLD)
				: 2;
		this.alertOnOffline = !configuration.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_AI_OFFLINE)
				|| configuration.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_AI_OFFLINE);
		this.alertOnRecovery = !configuration.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_AI_RECOVERY)
				|| configuration.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_AI_RECOVERY);

		this.streamIdleTimeout = configuration.hasPath(ConfigurationUtils.DF_AI_STREAM_IDLE_TIMEOUT)
				? configuration.getDuration(ConfigurationUtils.DF_AI_STREAM_IDLE_TIMEOUT)
				: Duration.ofSeconds(90);
		this.streamMaxDuration = configuration.hasPath(ConfigurationUtils.DF_AI_STREAM_MAX_DURATION)
				? configuration.getDuration(ConfigurationUtils.DF_AI_STREAM_MAX_DURATION)
				: Duration.ofMinutes(30);

		// check whether local AI is defined
		if (ConfigurationUtils.checkConfiguration(configuration, ConfigurationUtils.DF_AI_BASEURL)) {
			logger.info("AI service is defined: " + configuration.getString(ConfigurationUtils.DF_AI_BASEURL));
		} else {
			logger.info("AI service is not defined.");
		}
	}

	public boolean isOnline() {
		return isAiOnline.get();
	}

	public int getConsecutiveFailures() {
		return consecutiveFailures.get();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void refresh() {
		if (!ConfigurationUtils.checkConfiguration(configuration, ConfigurationUtils.DF_AI_BASEURL)) {
			return;
		}

		// start request to discover available models
		try {
			RemoteApiRequest internalAPIRequest = new RemoteApiRequest(REQUEST_TASK_MODELS,
					ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, "", "", -1L);
			internalAPIRequest.setUserApiKey(getInternalDocumentationAPIKey());

			// submit and wait for timeout
			submitApiRequest(internalAPIRequest).get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS,
					TimeUnit.MILLISECONDS);

			// ok, parse and make a mapping data structure
			String modelJson = internalAPIRequest.getResult();
			if (modelJson == null || modelJson.trim().isEmpty()) {
				throw new IllegalStateException("Empty response from AI backend");
			}

			String errorMsg = extractErrorMessageIfPresent(modelJson);
			if (errorMsg != null) {
				throw new IllegalStateException(errorMsg);
			}

			boolean modelsLoaded = localModelMetadata.updateModels(modelJson);
			if (!modelsLoaded) {
				throw new IllegalStateException("No models returned by AI backend");
			}

			// additionally ping endpoints for capabilities
			pingEndpoint("/chat/completions").thenAccept(localModelMetadata::setTextToTextAvailable);
			pingEndpoint("/images/generations").thenAccept(localModelMetadata::setTextToImageAvailable);
			pingEndpoint("/audio/transcriptions").thenAccept(localModelMetadata::setSpeechToTextAvailable);
			pingEndpoint("/audio/speech").thenAccept(localModelMetadata::setTextToSpeechAvailable);

			// Success! Reset consecutive failure counter and check for recovery transition
			consecutiveFailures.set(0);
			if (isAiOnline.compareAndSet(false, true)) {
				logger.info("✅ AI backend at " + aiBaseUrl + " is back ONLINE");
				if (alertOnRecovery && notificationService != null) {
					notificationService.send(NotificationMessage.builder().title("AI Service Online")
							.message("AI service at " + aiBaseUrl + " has recovered and is now reachable.")
							.level(NotificationLevel.INFO).tag("robot").tag("white_check_mark").build());
				}
			}

		} catch (Exception e) {
			logger.error("❌ Failed to fetch models from AI backend: " + e.getMessage());
			localModelMetadata.clearModels();

			int failures = consecutiveFailures.incrementAndGet();
			if (failures >= failureThreshold && isAiOnline.compareAndSet(true, false)) {
				logger.warn("🚨 AI backend at " + aiBaseUrl + " marked OFFLINE after " + failures + " failures");
				if (alertOnOffline && notificationService != null) {
					notificationService.send(NotificationMessage.builder().title("AI Service Offline")
							.message("AI service at " + aiBaseUrl + " is unreachable (failures: " + failures + "): "
									+ e.getMessage())
							.level(NotificationLevel.CRITICAL).tag("robot").tag("warning").tag("plug").build());
				}
			}
		}
	}

	/**
	 * ping the given endpoint path and check for availability (anything but 404 or connection error)
	 *
	 * @param path
	 * @return
	 */
	private CompletableFuture<Boolean> pingEndpoint(String path) {
		if (wsClient == null) {
			return CompletableFuture.completedFuture(false);
		}
		return wsClient.url(aiBaseUrl + path).setRequestTimeout(Duration.ofSeconds(2)).get().thenApply(res -> {
			return res.getStatus() != 404;
		}).exceptionally(e -> {
			return false;
		}).toCompletableFuture();
	}

	private String extractErrorMessageIfPresent(String json) {
		if (json == null || json.trim().isEmpty()) {
			return null;
		}
		try {
			JsonNode node = Json.parse(json);
			if (node.has("error")) {
				JsonNode errorNode = node.get("error");
				if (errorNode.isTextual()) {
					return errorNode.asText();
				} else if (errorNode.has("message") && errorNode.get("message").isTextual()) {
					return errorNode.get("message").asText();
				}
				return errorNode.toString();
			}
		} catch (Exception e) {
			// not valid JSON or parsing error
		}
		return null;
	}

	@Override
	public void stop() {
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletableFuture<Void> submitApiRequest(RemoteApiRequest request) {

		// 1. check if we have an API token set and whether this token is valid
		switch (request.getInternalApiKey()) {
		case "":
			request.setOutcome(Outcome.UNAUTHORIZED);
			request.setResult(Optional.of(Json.newObject().put(RESPONSE_ERROR, "No api-key available.").toString()));
			logger.info("ai.request id={} lane={} model={} user={} queueMs=0 upstreamMs=0 totalMs={} outcome={}",
					request.getId(), request.getLane(), request.getModel(), request.getUsername(),
					request.getTotalDurationMs(), request.getOutcome());
			return CompletableFuture.completedFuture(null);
		default:
			break;
		}

		// 2.1 fast track credit requests
		if (request.isCreditRequest()) {
			request.setOutcome(Outcome.OK);
			request.setResult(checkCredits(request.getUserApiKey()));
			logger.info("ai.request id={} lane={} model={} user={} queueMs=0 upstreamMs=0 totalMs={} outcome={}",
					request.getId(), request.getLane(), request.getModel(), request.getUsername(),
					request.getTotalDurationMs(), request.getOutcome());
			return CompletableFuture.completedFuture(null);
		}

		// 2.2 fast track models request
		if (request.isModelsRequest()) {
			return dispatchWithPermit(request);
		}

		// 3. check and map requested model
		preProcessRequest(request);

		// 4. fast track local documentation API requests
		if (request.getUserApiKey().equals(getInternalDocumentationAPIKey()) && request.getRequestedTokens() <= 1) {
			return dispatchWithPermit(request);
		}

		// 5. check authorization from DB: check available credits for this request, if sufficient update credits
		Optional<String> errorResponse = checkAndUpdateCredits(request.getUserApiKey(), request.getRequestedTokens());
		// if an error response is returned, abort and return it directly
		if (errorResponse.isPresent()) {
			if (errorResponse.get().contains("No valid API key")) {
				request.setOutcome(Outcome.UNAUTHORIZED);
			} else {
				request.setOutcome(Outcome.NO_CREDITS);
			}
			request.setResult(errorResponse);
			request.cancel();

			logger.info("ai.request id={} lane={} model={} user={} queueMs=0 upstreamMs=0 totalMs={} outcome={}",
					request.getId(), request.getLane(), request.getModel(), request.getUsername(),
					request.getTotalDurationMs(), request.getOutcome());

			return CompletableFuture.completedFuture(null);
		}

		// 6. submit and wait for timeout
		return dispatchWithPermit(request);
	}

	private CompletableFuture<Void> dispatchWithPermit(RemoteApiRequest request) {
		return laneLimiter.acquire(request.getLane()).thenCompose(permit -> {
			request.markDispatched();
			return processRequest(request).whenComplete((v, e) -> {
				try {
					permit.close();
				} catch (Exception ex) {
					// ignore
				} finally {
					logger.info(
							"ai.request id={} lane={} model={} user={} queueMs={} upstreamMs={} totalMs={} outcome={}",
							request.getId(), request.getLane(), request.getModel(), request.getUsername(),
							request.getQueueDurationMs(), request.getUpstreamDurationMs(), request.getTotalDurationMs(),
							request.getOutcome());
				}
			});
		}).toCompletableFuture();
	}

	/**
	 * run a remote API request for max. msTimeout millisecond; abort on timeout (synchronous wrapper)
	 *
	 * @param request
	 * @return
	 */
	public String submitApiRequestSync(RemoteApiRequest request) {
		try {
			this.submitApiRequest(request).toCompletableFuture().get(request.getMsTimeout() + 1000,
					TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			// ignore
		}
		return request.getResult();
	}

	public CompletionStage<Source<ByteString, ?>> openStream(RemoteApiRequest request) {
		// 1. must have an internal key
		if (request.getInternalApiKey() == null || request.getInternalApiKey().isEmpty()) {
			request.setOutcome(Outcome.UNAUTHORIZED);
			logger.info("ai.request id={} lane={} model={} user={} queueMs=0 upstreamMs=0 totalMs={} outcome={}",
					request.getId(), request.getLane(), request.getModel(), request.getUsername(),
					request.getTotalDurationMs(), request.getOutcome());
			return CompletableFuture.completedFuture(Source.single(sseError("No api-key available.")));
		}
		// 2. map model, apply defaults (same as the buffered path)
		preProcessRequest(request);

		// 3. credits + throttle, unless this is the internal documentation key
		boolean internalDocs = request.getUserApiKey().equals(getInternalDocumentationAPIKey())
				&& request.getRequestedTokens() <= 1;
		if (!internalDocs) {
			Optional<String> error = checkAndUpdateCredits(request.getUserApiKey(), request.getRequestedTokens());
			if (error.isPresent()) {
				if (error.get().contains("No valid API key")) {
					request.setOutcome(Outcome.UNAUTHORIZED);
				} else {
					request.setOutcome(Outcome.NO_CREDITS);
				}
				logger.info("ai.request id={} lane={} model={} user={} queueMs=0 upstreamMs=0 totalMs={} outcome={}",
						request.getId(), request.getLane(), request.getModel(), request.getUsername(),
						request.getTotalDurationMs(), request.getOutcome());
				return CompletableFuture.completedFuture(Source.single(sseRaw(error.get())));
			}
		}
		return laneLimiter.acquire(request.getLane()).thenCompose(permit -> {
			request.markDispatched();
			return doOpenStream(request, permit);
		});
	}

	private CompletionStage<Source<ByteString, ?>> doOpenStream(RemoteApiRequest request, AutoCloseable permit) {
		final long start = System.currentTimeMillis();
		final AtomicInteger tokens = new AtomicInteger(0);

		ObjectNode params = request.getParams();
		if (params != null && !params.has("stream_options")) {
			params.set("stream_options", Json.newObject().put("include_usage", true));
		}

		return wsClient.url(aiBaseUrl + request.getPath()).setRequestTimeout(streamMaxDuration)
				.setMethod(REQUEST_METHOD_POST).setBody(params)
				.addHeader(ApiServiceConstants.X_API_MODEL, nss(request.getModel())).stream().thenApply(res -> {
					if (res.getStatus() != Http.Status.OK) {
						try {
							permit.close();
						} catch (Exception ex) {
							// ignore
						}
						request.setOutcome(Outcome.UPSTREAM_ERROR);
						logModelInvocation(request, request.getModel(), mapTaskToType(request.getType()),
								request.getRequestedTokens(), false, "upstream status " + res.getStatus(),
								System.currentTimeMillis() - start);
						logger.info(
								"ai.request id={} lane={} model={} user={} queueMs={} upstreamMs={} totalMs={} outcome={}",
								request.getId(), request.getLane(), request.getModel(), request.getUsername(),
								request.getQueueDurationMs(), request.getUpstreamDurationMs(),
								request.getTotalDurationMs(), request.getOutcome());
						return Source.single(sseError("upstream returned status " + res.getStatus()));
					}
					return res.getBody(WSBodyReadables.instance.source())
							// liveness measured on RAW bytes, before framing
							.idleTimeout(streamIdleTimeout)
							// whole SSE events, so a sentinel can never split or be faked by content
							.via(Framing.delimiter(SSE_SEP, MAX_SSE_FRAME, FramingTruncation.ALLOW)).map(frame -> {
								String text = frame.utf8String();
								int total = extractTokensFromChunk(text);
								if (total > 0) {
									tokens.set(total);
								} else if (text.contains("\"content\"")) {
									tokens.incrementAndGet();
								}
								return frame.concat(SSE_SEP);
							})
							// log the REAL outcome (must sit above recover)
							.watchTermination((mat, done) -> {
								done.whenComplete((d, e) -> {
									try {
										permit.close();
									} catch (Exception ex) {
										// ignore
									}
									if (e != null) {
										request.setOutcome(Outcome.UPSTREAM_ERROR);
									}
									logModelInvocation(request, request.getModel(), mapTaskToType(request.getType()),
											tokens.get() > 0 ? tokens.get() : request.getRequestedTokens(), e == null,
											e == null ? null : e.getLocalizedMessage(),
											System.currentTimeMillis() - start);
									logger.info(
											"ai.request id={} lane={} model={} user={} queueMs={} upstreamMs={} totalMs={} outcome={}",
											request.getId(), request.getLane(), request.getModel(),
											request.getUsername(), request.getQueueDurationMs(),
											request.getUpstreamDurationMs(), request.getTotalDurationMs(),
											request.getOutcome());
								});
								return mat;
							})
							// client always gets a clean terminator instead of a silent cut
							.recover(Throwable.class, () -> sseError("stream ended early"));
				}).exceptionally(e -> {
					try {
						permit.close();
					} catch (Exception ex) {
						// ignore
					}
					request.setOutcome(Outcome.UPSTREAM_ERROR);
					logModelInvocation(request, request.getModel(), mapTaskToType(request.getType()),
							request.getRequestedTokens(), false, e.getLocalizedMessage(),
							System.currentTimeMillis() - start);
					logger.info(
							"ai.request id={} lane={} model={} user={} queueMs={} upstreamMs={} totalMs={} outcome={}",
							request.getId(), request.getLane(), request.getModel(), request.getUsername(),
							request.getQueueDurationMs(), request.getUpstreamDurationMs(), request.getTotalDurationMs(),
							request.getOutcome());
					return Source.single(sseError(e.getLocalizedMessage()));
				});
	}

	/** one SSE error event followed by a terminator, so clients always close cleanly */
	private ByteString sseError(String message) {
		return sseRaw(Json.newObject()
				.set("error", Json.newObject().put("message", message).put("type", "upstream_error")).toString());
	}

	private ByteString sseRaw(String jsonPayload) {
		return ByteString.fromString("data: " + jsonPayload + "\n\ndata: [DONE]\n\n");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * retrieve a sorted list of models as list of modelmetadata instances
	 * 
	 * @return
	 */
	public List<ModelMetadata> getModels() {
		return localModelMetadata.getModels();
	}

	/**
	 * Check if the given API key is valid (either internal documentation key or valid in the datastore)
	 *
	 * @param apiKey
	 * @return true if valid, false otherwise
	 */
	public boolean isValidApiKey(String apiKey) {
		if (apiKey == null) {
			return false;
		}
		if (apiKey.equals(getInternalDocumentationAPIKey())) {
			return true;
		}
		Optional<String> check = checkCredits(apiKey);
		return check.isEmpty() || !check.get().contains(RESPONSE_ERROR);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private CompletionStage<Void> processRequest(RemoteApiRequest request) {
		if (request.isCanceled()) {
			request.setOutcome(Outcome.TIMEOUT);
			return CompletableFuture.completedFuture(null);
		}
		return runApiRequest(request);
	}

	private CompletionStage<Void> runApiRequest(RemoteApiRequest request) {
		// check if we have enough time to submit the request
		long start = System.currentTimeMillis();
		CompletionStage<WSResponse> requestCompletionStage;

		// check request method
		if (request.getMethod().equals(REQUEST_METHOD_POST)) {
			// POST request
			// if a file was posted, we just repost the file
			MultipartFormData<TemporaryFile> formData = request.getMultipartFormData();
			if (formData != null) {
				List<Http.MultipartFormData.FilePart<Source<ByteString, ?>>> fileParts = new LinkedList<>();
				formData.getFiles().forEach(tf -> {
					Source<ByteString, ?> file = FileIO.fromPath(tf.getRef().path());
					FilePart<Source<ByteString, ?>> fp = new FilePart<>(tf.getKey(), tf.getFilename(),
							tf.getContentType(), file, tf.getFileSize());
					fileParts.add(fp);
				});
				List<DataPart> dataParts = new LinkedList<>();
				formData.asFormUrlEncoded().entrySet().forEach(e -> {
					if (e.getValue().length == 1) {
						dataParts.add(new DataPart(e.getKey(), e.getValue()[0]));
					}
				});

				// check if all properties are present
				if (fileParts.size() < 1) {
					request.setOutcome(Outcome.BAD_REQUEST);
					request.setResult(Optional.of(Json.newObject()
							.put(RESPONSE_ERROR, "Audio file property missing from request.").toString()));
					return CompletableFuture.completedFuture(null);
				} else if (dataParts.size() < 1 || dataParts.stream().noneMatch(dp -> dp.getKey().equals("model"))) {
					request.setOutcome(Outcome.BAD_REQUEST);
					request.setResult(Optional.of(
							Json.newObject().put(RESPONSE_ERROR, "Model property missing from request.").toString()));
					return CompletableFuture.completedFuture(null);
				}

				// ensure that the model is set on outgoing requests
				request.setModel(dataParts.stream().filter(dp -> dp.getKey().equals("model")).map(dp -> dp.getValue())
						.findAny().orElse(""));

				// prepare all items for the request
				List<Object> ll = new LinkedList<>();
				ll.addAll(fileParts);
				ll.addAll(dataParts);
				requestCompletionStage = prepareWSRemoteAPIRequest(request).post(Source.from(ll));
			}
			// otherwise we post the request params
			else {
				requestCompletionStage = prepareWSRemoteAPIRequest(request).post(request.getParams());
			}
		} else {
			// GET request
			requestCompletionStage = prepareWSRemoteAPIRequest(request).get();
		}

		// run request asynchronously without blocking threads
		return requestCompletionStage.thenCompose(res -> {
			logger.trace("AI API request: " + aiBaseUrl + request.getPath() + " -> " + request.getModel() + " ["
					+ (System.currentTimeMillis() - start) + "ms]");

			// 1. Check for non-200 status codes
			if (res.getStatus() != Http.Status.OK) {
				String errorMsg = res.getBody();
				request.setOutcome(Outcome.UPSTREAM_ERROR);
				request.setResult(Optional.of(
						request.errorMessage("API returned status " + res.getStatus() + ": " + errorMsg).toString()));
				if (!request.isModelsRequest()) {
					logModelInvocation(request, request.getModel(), mapTaskToType(request.getType()),
							request.getRequestedTokens(), false, "API returned status " + res.getStatus(),
							System.currentTimeMillis() - start);
				}
				return CompletableFuture.completedFuture(null);
			}

			// 2. Handle binary responses with streaming
			if (request.getType().equals(REQUEST_TASK_IMAGE_GENERATION)) {
				String token = UUID.randomUUID().toString();
				TemporaryFile tif = play.libs.Files.singletonTemporaryFileCreator().create("generatedImage", ".png");
				File tempImageFile = tif.path().toFile();

				return res.getBody(WSBodyReadables.instance.source())
						.runWith(FileIO.toPath(tempImageFile.toPath()), materializer).thenAccept(ioResult -> {
							// cache for 1 minute
							cache.set(token, tempImageFile.getAbsolutePath(), (int) Duration.ofMinutes(1).toSeconds());
							request.setOutcome(Outcome.OK);
							request.setResult(Optional.of(Json.newObject().put("image_id", token)
									.put("prompt", request.getParams().path(REQUEST_PROMPT).asText("")).toString()));
							if (!request.isModelsRequest()) {
								logModelInvocation(request, request.getModel(), mapTaskToType(request.getType()),
										request.getRequestedTokens(), true, null, System.currentTimeMillis() - start);
							}
						});
			} else if (request.getType().equals(REQUEST_TASK_SPEECH_GENERATION)) {
				TemporaryFile tif = play.libs.Files.singletonTemporaryFileCreator().create("generatedSpeech", ".mp3");
				File tempSpeechFile = tif.path().toFile();

				return res.getBody(WSBodyReadables.instance.source())
						.runWith(FileIO.toPath(tempSpeechFile.toPath()), materializer).thenAccept(ioResult -> {
							request.setOutcome(Outcome.OK);
							request.setResult(Optional.of(tempSpeechFile.getAbsolutePath()));
							if (!request.isModelsRequest()) {
								logModelInvocation(request, request.getModel(), mapTaskToType(request.getType()),
										request.getRequestedTokens(), true, null, System.currentTimeMillis() - start);
							}
						});
			}
			// Handle textual responses directly in memory
			else {
				String responseBody = res.getBody();
				request.setOutcome(Outcome.OK);
				request.setResult(Optional.of(responseBody));
				if (!request.isModelsRequest()) {
					int actualTokens = 1;
					try {
						JsonNode responseJson = Json.parse(responseBody);
						if (responseJson.has("usage") && responseJson.get("usage").has("total_tokens")) {
							actualTokens = responseJson.get("usage").path("total_tokens").asInt(1);
						} else {
							actualTokens = estimatePromptTokens(request.getParams())
									+ estimateResponseTokens(responseJson);
						}
					} catch (Exception e) {
						actualTokens = request.getRequestedTokens();
					}
					logModelInvocation(request, request.getModel(), mapTaskToType(request.getType()), actualTokens,
							true, null, System.currentTimeMillis() - start);
				}
				return CompletableFuture.completedFuture(null);
			}
		}).exceptionally((e) -> {
			request.setOutcome(Outcome.UPSTREAM_ERROR);
			request.setResult(Optional
					.of(request.errorMessage("API request execution problem: " + e.getLocalizedMessage()).toString()));
			logger.error("AI API request: " + aiBaseUrl + request.getPath() + " ["
					+ (System.currentTimeMillis() - start) + "ms]: " + e.getLocalizedMessage());
			if (!request.isModelsRequest()) {
				logModelInvocation(request, request.getModel(), mapTaskToType(request.getType()),
						request.getRequestedTokens(), false, e.getLocalizedMessage(),
						System.currentTimeMillis() - start);
			}
			return null;
		});
	}

	public AiLaneLimiter getLaneLimiter() {
		return laneLimiter;
	}

	private WSRequest prepareWSRemoteAPIRequest(RemoteApiRequest request) {
		return wsClient.url(aiBaseUrl + request.getPath()).setRequestTimeout(Duration.ofMillis(request.getMsTimeout()))
				.addHeader(ApiServiceConstants.X_API_MODEL, nss(request.getModel()));
	}

	public List<List<Double>> dispatchEmbeddingRequest(String username, List<String> contentToEmbed) {
		long start = System.currentTimeMillis();
		// create request
		ObjectNode jsonPayload = Json.newObject();
		jsonPayload.put("model", LOCALAI_EMBEDDING_MODEL_DEFAULT);
		jsonPayload.set("input", Json.toJson(contentToEmbed));

		try {
			WSResponse res = wsClient.url(aiBaseUrl + "/embeddings")
					.setRequestTimeout(Duration.ofMillis(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS))
					.post(jsonPayload).toCompletableFuture()
					.get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS + 1000, TimeUnit.MILLISECONDS);

			if (res.getStatus() == Http.Status.OK) {
				JsonNode responseJson = res.asJson();
				if (responseJson.has("data") && responseJson.get("data").isArray()) {
					int embeddingTokens = 1;
					if (responseJson.has("usage") && responseJson.get("usage").has("total_tokens")) {
						embeddingTokens = responseJson.get("usage").path("total_tokens").asInt(1);
					} else {
						int charCount = contentToEmbed.stream().mapToInt(String::length).sum();
						embeddingTokens = Math.max(1, charCount / 4);
					}
					logModelInvocation("SYSTEM", username, LOCALAI_EMBEDDING_MODEL_DEFAULT, "embeddings",
							embeddingTokens, true, null, System.currentTimeMillis() - start);
					return StreamSupport.stream(responseJson.get("data").spliterator(), false).map(item -> {
						JsonNode embeddingNode = item.get("embedding");
						return StreamSupport.stream(embeddingNode.spliterator(), false).map(JsonNode::asDouble)
								.collect(Collectors.toList());
					}).collect(Collectors.toList());
				}
			}
			logModelInvocation("SYSTEM", username, LOCALAI_EMBEDDING_MODEL_DEFAULT, "embeddings", 1, false,
					"Status code: " + res.getStatus(), System.currentTimeMillis() - start);
		} catch (Exception e) {
			logger.error("❌ Failed to fetch embeddings from AI backend: " + e.getMessage());
			logModelInvocation("SYSTEM", username, LOCALAI_EMBEDDING_MODEL_DEFAULT, "embeddings", 1, false,
					e.getMessage(), System.currentTimeMillis() - start);
		}

		return new LinkedList<>();
	}

	public String getInternalDocumentationAPIKey() {
		return internalDocumentationAPIKey;
	}

	/**
	 * parse a chat completion response from the AI backend
	 *
	 * @param jsonResponse
	 * @return
	 */
	public ObjectNode parseChatCompletionResponse(String jsonResponse) {
		ObjectNode result = Json.newObject();
		try {
			JsonNode jn = Json.parse(jsonResponse);

			// check for error
			if (jn.has(RESPONSE_ERROR)) {
				result.set(RESPONSE_ERROR, jn.get(RESPONSE_ERROR));
				return result;
			}

			// check for choices
			if (jn.has("choices") && jn.get("choices").isArray() && jn.get("choices").size() > 0) {
				JsonNode choice = jn.get("choices").get(0);
				if (choice.has("message")) {
					JsonNode message = choice.get("message");
					result.put(RESPONSE_ROLE, message.path(RESPONSE_ROLE).asText("assistant"));
					result.put(RESPONSE_CONTENT, message.path(RESPONSE_CONTENT).asText(""));
				}
				result.put(RESPONSE_FINISH_REASON, choice.path("finish_reason").asText(""));
			} else if (jn.has(RESPONSE_CONTENT)) {
				// fallback for non-OpenAI responses that might already be flat
				result.put(RESPONSE_ROLE, jn.path(RESPONSE_ROLE).asText("assistant"));
				result.put(RESPONSE_CONTENT, jn.path(RESPONSE_CONTENT).asText(""));
			}

			// extract usage/cost
			if (jn.has("usage")) {
				result.put(RESPONSE_COST, jn.get("usage").path("total_tokens").asInt(0));
			}

		} catch (Exception e) {
			result.put(RESPONSE_ERROR, "Failed to parse AI response: " + e.getMessage());
		}
		return result;
	}

	private String mapTaskToType(String task) {
		if (task == null) {
			return "unknown";
		}
		switch (task) {
		case REQUEST_TASK_CHAT_COMPLETION:
		case REQUEST_TASK_COMPLETION:
			return "t2t";
		case REQUEST_TASK_IMAGE_GENERATION:
			return "tti";
		case REQUEST_TASK_SPEECH_GENERATION:
			return "tts";
		case REQUEST_TASK_AUDIO_TRANSCRIPTION:
			return "stt";
		case REQUEST_TASK_EMBEDDING:
			return "embeddings";
		default:
			return task;
		}
	}

	public void logModelInvocation(RemoteApiRequest request, String model, String modelType, int requestedTokens,
			boolean success, String errorMessage, long durationMs) {
		String explicitUser = request != null ? request.getUsername() : null;
		String apiKey = request != null ? request.getUserApiKey() : null;
		logModelInvocation(apiKey, explicitUser, model, modelType, requestedTokens, success, errorMessage, durationMs);
	}

	public void logModelInvocation(String apiKey, String model, String modelType, int requestedTokens, boolean success,
			String errorMessage, long durationMs) {
		logModelInvocation(apiKey, null, model, modelType, requestedTokens, success, errorMessage, durationMs);
	}

	public void logModelInvocation(String apiKey, String explicitUsername, String model, String modelType,
			int requestedTokens, boolean success, String errorMessage, long durationMs) {
		dbLogExecutor.submit(() -> {
			try {
				initDatastoreIfNeeded();
				if (localAiUsageStore == null) {
					return;
				}

				String username = "SYSTEM";
				String email = "system@df";
				long projectId = -1L;

				if (apiKey != null && !apiKey.isEmpty() && !apiKey.equals("SYSTEM")) {
					ApiKeyDetails details = getApiKeyDetails(apiKey);
					if (details != null) {
						username = details.username();
						email = details.email();
						projectId = details.projectId();
					} else if (apiKey.equals(getInternalDocumentationAPIKey())) {
						if (explicitUsername != null && !explicitUsername.trim().isEmpty()
								&& !"SYSTEM".equalsIgnoreCase(explicitUsername)) {
							username = explicitUsername;
							email = explicitUsername.contains("@") ? explicitUsername : explicitUsername + "@df";
						} else {
							username = "SYSTEM";
							email = "system@df";
						}
					} else {
						username = explicitUsername != null && !explicitUsername.trim().isEmpty() ? explicitUsername
								: "UNKNOWN";
						email = apiKey;
					}
				} else if ("SYSTEM".equals(apiKey)) {
					if (explicitUsername != null && !explicitUsername.trim().isEmpty()
							&& !"SYSTEM".equalsIgnoreCase(explicitUsername)) {
						username = explicitUsername;
						email = explicitUsername.contains("@") ? explicitUsername : explicitUsername + "@df";
					} else {
						username = "SYSTEM";
						email = "system@df";
					}
				}

				ObjectNode dataNode = Json.newObject();
				dataNode.put("success", success);
				dataNode.put("tokens", requestedTokens);
				dataNode.put("duration", durationMs);
				dataNode.put("projectId", projectId);
				dataNode.put("email", email);
				if (errorMessage != null && !errorMessage.isEmpty()) {
					dataNode.put("error", errorMessage);
				}

				String pp3Value = success ? "success" : "error";

				localAiUsageStore.internalAddRecord("local_ai_service", username, modelType, pp3Value,
						new java.util.Date(), model != null ? model : "unknown", dataNode);
			} catch (Exception e) {
				logger.error("Failed to log model invocation: ", e);
			}
		});
	}

	/**
	 * Extracts total_tokens from an SSE stream chunk using pattern matching.
	 *
	 * @param chunk the Server-Sent Event stream chunk
	 * @return the total tokens value if present, otherwise 0
	 */
	private int extractTokensFromChunk(String chunk) {
		if (chunk == null) {
			return 0;
		}
		try {
			java.util.regex.Matcher m = TOTAL_TOKENS_PATTERN.matcher(chunk);
			if (m.find()) {
				return Integer.parseInt(m.group(1));
			}
		} catch (Exception e) {
			// ignore
		}
		return 0;
	}

	/**
	 * Estimates the number of prompt tokens from request parameters based on character count as a fallback for legacy
	 * or non-compliant backends (Option A fallback).
	 *
	 * @param params the JSON parameters of the request
	 * @return estimated prompt token count (approx. 1 token per 4 characters)
	 */
	private int estimatePromptTokens(JsonNode params) {
		if (params == null) {
			return 0;
		}
		int charCount = 0;
		if (params.has("messages") && params.get("messages").isArray()) {
			for (JsonNode msg : params.get("messages")) {
				charCount += msg.path("content").asText("").length();
			}
		} else if (params.has("prompt")) {
			charCount += params.get("prompt").asText("").length();
		}
		return Math.max(1, charCount / 4);
	}

	/**
	 * Estimates the number of response tokens from a JSON response payload based on character count as a fallback for
	 * legacy or non-compliant backends (Option A fallback).
	 *
	 * @param responseJson the JSON response returned by the backend
	 * @return estimated response token count (approx. 1 token per 4 characters)
	 */
	private int estimateResponseTokens(JsonNode responseJson) {
		if (responseJson == null) {
			return 0;
		}
		int charCount = 0;
		if (responseJson.has("choices") && responseJson.get("choices").isArray()
				&& responseJson.get("choices").size() > 0) {
			JsonNode choice = responseJson.get("choices").get(0);
			if (choice.has("message")) {
				charCount += choice.get("message").path("content").asText("").length();
			} else if (choice.has("text")) {
				charCount += choice.get("text").asText("").length();
			}
		}
		return Math.max(1, charCount / 4);
	}

}

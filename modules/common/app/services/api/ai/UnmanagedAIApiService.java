package services.api.ai;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.FileUtils;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.javadsl.FileIO;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

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
import services.api.remoting.RemoteRequestsExecutionService;
import services.api.remoting.StreamingRemoteApiRequest;
import services.inlets.ScheduledService;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;
import utils.conf.ConfigurationUtils;

@Singleton
public class UnmanagedAIApiService extends AbstractAIApiService implements ApiServiceConstants, ScheduledService {

	private static final Logger.ALogger logger = Logger.of(UnmanagedAIApiService.class);

	private final Config configuration;
	private final RemoteRequestsExecutionService executionService;
	private final WSClient wsClient;
	private final SyncCacheApi cache;
	private final Materializer materializer;

	// generate an internal API key on every start
	private final String internalDocumentationAPIKey = "df-internal-"
			+ UUID.randomUUID().toString().replace("-", "").substring(0, 16);

	@Inject
	protected UnmanagedAIApiService(Config configuration, SyncCacheApi cache, AdminUtils adminUtils,
			DatasetConnector datasetConnector, TokenResolverUtil tokenResolver,
			RemoteRequestsExecutionService executionService, WSClient wsClient, Materializer materializer,
			LocalModelMetadata lmmd) {
		super(configuration, adminUtils, datasetConnector, tokenResolver, lmmd);
		this.configuration = configuration;
		this.cache = cache;
		this.executionService = executionService;
		this.wsClient = wsClient;
		this.materializer = materializer;

		// check whether local AI is defined
		if (ConfigurationUtils.checkConfiguration(configuration, ConfigurationUtils.DF_LOCALAI_HOST)) {
			logger.info("Local AI service is defined: " + configuration.getString(ConfigurationUtils.DF_LOCALAI_HOST));
		} else {
			logger.info("Local AI service is not defined.");
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void refresh() {
		if (!ConfigurationUtils.checkConfiguration(configuration, ConfigurationUtils.DF_LOCALAI_HOST)) {
			return;
		}

		// start request to discover available models
		logger.info("Requesting model JSON to update models.");

		try {
			RemoteApiRequest internalAPIRequest = new RemoteApiRequest(REQUEST_TASK_MODELS,
					ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, "", "", -1L);
//			internalAPIRequest.setPath("/v1/models");
			internalAPIRequest.setPath("/models");
			internalAPIRequest.setUserApiKey(getInternalDocumentationAPIKey());

			// submit and wait for timeout
			submitApiRequest(internalAPIRequest).get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS,
					TimeUnit.MILLISECONDS);

			// ok, parse and make a mapping data structure
			String modelJson = internalAPIRequest.getResult();
			localModelMetadata.updateModels(modelJson);
		} catch (Exception e) {
			// do nothing
		}
	}

	@Override
	public void stop() {
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Future<Void> submitApiRequest(RemoteApiRequest request) {

		// 1. check if we have an API token set and whether this token is valid
		switch (request.getInternalApiKey()) {
		case "":
			request.setResult(Optional.of(Json.newObject().put(RESPONSE_ERROR, "No api-key available.").toString()));
			return CompletableFuture.completedFuture(null);
		case OPENAI_APIKEY:
			request.setInternalApiKey(openAIAPIKey);
			break;
		default:
			break;
		}

		// 2.1 fast track credit requests
		if (request.isCreditRequest()) {
			request.setResult(checkCredits(request.getUserApiKey()));
			return CompletableFuture.completedFuture(null);
		}

		// 2.2 fast track models request
		if (request.isModelsRequest()) {
			return executionService.submitRequest(request, (r) -> processRequest(r), request.getMsTimeout());
		}

		// 3. check and map requested model
		preProcessRequest(request);

		// 4. fast track local documentation API requests
		if (request.getUserApiKey().equals(getInternalDocumentationAPIKey()) && request.getRequestedTokens() <= 1) {
			// submit and wait for timeout
			return executionService.submitRequest(request, (r) -> processRequest(r), request.getMsTimeout());
		}

		// 5. check authorization from DB: check available credits for this request, if sufficient update credits
		Optional<String> errorResponse = checkAndUpdateCredits(request.getUserApiKey(), request.getRequestedTokens());
		// if an error response is returned, abort and return it directly
		if (errorResponse.isPresent()) {
			request.setResult(errorResponse);
			request.cancel();

			return CompletableFuture.completedFuture(null);
		}

		// 6. submit and wait for timeout
		return executionService.submitRequest(request, (r) -> processRequest(r), request.getMsTimeout());
	}

	/**
	 * run a remote API request for max. msTimeout millisecond; abort on timeout
	 * 
	 * @param request
	 * @param msTimeout
	 * @return
	 */
	public void submitApiRequest(StreamingRemoteApiRequest request) {

		// fast track credit requests
		if (request.isCreditRequest()) {
			request.appendResult(checkCredits(request.getUserApiKey()).get());
			request.finish();
		}

		// compute requested credits
		if (request.isDirectOpenAIApiRequest()) {
			// this can be sent directly, no need to check for DB: submit and return immediately
			executionService.submitRequest(request, (r) -> processRequest(r), request.getMsTimeout());
			return;
		}

		// check authorization from DB and check available credits for this request, if sufficient update credits
		Optional<String> errorResponse = checkAndUpdateCredits(request.getUserApiKey(), request.getRequestedTokens());
		// if an error response is returned, abort and return it directly
		if (errorResponse.isPresent()) {
			request.cancel();
			return;
		}

		// submit and return immediately
		executionService.submitRequest(request, (r) -> processRequest(r), request.getMsTimeout());
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

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private void processRequest(RemoteApiRequest request) {
		if (!request.isCanceled()) {
			try {
				if (request instanceof StreamingRemoteApiRequest) {
					runApiRequest((StreamingRemoteApiRequest) request);
				} else {
					runApiRequest(request);
				}
			} catch (Exception e) {
				logger.error("API request execution problem", e);
			}
		} else {
			if (request instanceof StreamingRemoteApiRequest) {
				((StreamingRemoteApiRequest) request).finish();
			} else {
				request.setResult(Optional.of(request.errorMessage("API timeout").toString()));
			}
		}
	}

	private void runApiRequest(StreamingRemoteApiRequest request) {
		wsClient.url(localAIHost + request.getPath()).setRequestTimeout(Duration.ofMillis(request.getMsTimeout()))
				.setMethod("POST").setBody(request.getParams()).addHeader("X-API-Model", nss(request.getModel()))
				.stream().thenAccept((res) -> {
					res.getBody(WSBodyReadables.instance.source()).map(bs -> {
						String decodeString = bs.decodeString(StandardCharsets.UTF_8);
						if (decodeString.contains("[DONE]")) {
							request.appendResult(decodeString);
							request.finish();
						} else {
							request.appendResult(decodeString);
						}
						return bs;
					}).runWith(Sink.ignore(), materializer);
				}).exceptionally((e) -> {
					request.setResult(Optional.empty());
					return null;
				});
	}

	private void runApiRequest(RemoteApiRequest request) {
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
					request.setResult(Optional.of(Json.newObject()
							.put(RESPONSE_ERROR, "Audio file property missing from request.").toString()));
					return;
				} else if (dataParts.size() < 1 || dataParts.stream().noneMatch(dp -> dp.getKey().equals("model"))) {
					request.setResult(Optional.of(
							Json.newObject().put(RESPONSE_ERROR, "Model property missing from request.").toString()));
					return;
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

		// run request
		try {
			requestCompletionStage.thenAccept(res -> {
				logger.trace("AI API request: " + localAIHost + request.getPath() + " ["
						+ (System.currentTimeMillis() - start) + "ms]");
				if (request.getType().equals(REQUEST_TASK_IMAGE_GENERATION)) {
					try {
						String token = UUID.randomUUID().toString();
						TemporaryFile tif = play.libs.Files.singletonTemporaryFileCreator().create("generatedImage",
								".png");
						File tempImageFile = tif.path().toFile();
						FileUtils.writeByteArrayToFile(tempImageFile, res.getBodyAsBytes().toArray());
						// cache for 1 day
						cache.set(token, tempImageFile.getAbsolutePath(), (int) Duration.ofDays(1).toSeconds());
						request.setResult(
								Optional.of(Json.newObject().put("image_id", token).put("prompt", "").toString()));
					} catch (IOException e) {
						request.setResult(Optional.of(Json.newObject()
								.put(RESPONSE_ERROR, "Image generation result problem (storage)").toString()));
						logger.error("Problem in storing image generation result as temp file and in cache.", e);
					}
				} else if (request.getType().equals(REQUEST_TASK_SPEECH_GENERATION)) {
					try {
						TemporaryFile tif = play.libs.Files.singletonTemporaryFileCreator().create("generatedSpeech",
								".mp3");
						File tempImageFile = tif.path().toFile();
						FileUtils.writeByteArrayToFile(tempImageFile, res.getBodyAsBytes().toArray());
						request.setResult(Optional.of(tempImageFile.getAbsolutePath()));
					} catch (IOException e) {
						request.setResult(Optional.of(Json.newObject()
								.put(RESPONSE_ERROR, "Image generation result problem (storage)").toString()));
						logger.error("Problem in storing image generation result as temp file and in cache.", e);
					}
				} else {
					request.setResult(Optional.of(res.getBody()));
				}
			}).exceptionally((e) -> {
				request.setResult(Optional.empty());
				logger.error("AI API request: " + localAIHost + request.getPath() + " ["
						+ (System.currentTimeMillis() - start) + "ms]: " + e.getLocalizedMessage());
				// don't issue an exception
				return null;
			}).toCompletableFuture().get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("AI API request: " + localAIHost + request.getPath() + " ["
					+ (System.currentTimeMillis() - start) + "ms]: " + e.getLocalizedMessage());
		}
	}

	private WSRequest prepareWSRemoteAPIRequest(RemoteApiRequest request) {
		return wsClient.url(localAIHost + request.getPath())
				.setRequestTimeout(Duration.ofMillis(request.getMsTimeout()))
				.addHeader("X-API-Model", nss(request.getModel()));
	}

	public String getInternalDocumentationAPIKey() {
		return internalDocumentationAPIKey;
	}

}

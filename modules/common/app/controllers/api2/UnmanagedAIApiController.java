package controllers.api2;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.auth.UserAuth;
import play.Environment;
import play.cache.SyncCacheApi;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http.MultipartFormData;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.api.ApiServiceConstants;
import services.api.ai.AiLane;
import services.api.ai.AiLaneLimiter;
import services.api.ai.UnmanagedAIApiService;
import services.api.remoting.RemoteApiRequest;
import services.api.remoting.RemoteApiRequest.Outcome;

public class UnmanagedAIApiController extends Controller implements ApiServiceConstants {

	@Inject
	UnmanagedAIApiService aiApiService;

	@Inject
	SyncCacheApi cache;

	@Inject
	Environment environment;

	@Inject
	services.processing.MediaProcessingService mediaProcessingService;

	public record ApiCall(String username, String apiKey) {
	}

	Optional<ApiCall> authorize(Request request) {
		String authHeader = request.header("Authorization").orElse("");
		String authorization = "";
		if (authHeader.startsWith("Bearer ")) {
			authorization = authHeader.substring(7).trim();
		}
		String apiKey = checkDocumentationAPIKey(request, authorization);
		if (apiKey == null || apiKey.isEmpty()) {
			return Optional.empty();
		}
		String username = request.header(ApiServiceConstants.X_API_USER).orElse("");
		return Optional.of(new ApiCall(username, apiKey));
	}

	private ObjectNode err(String message, String type) {
		ObjectNode result = Json.newObject();
		result.putObject(RESPONSE_ERROR).put(RESPONSE_MESSAGE, message).put("type", type).putNull("param").put("code",
				"");
		return result;
	}

	private Result respond(RemoteApiRequest req, Throwable err, String contentType) {
		Throwable c = (err instanceof java.util.concurrent.CompletionException && err.getCause() != null)
				? err.getCause()
				: err;

		if (c instanceof TimeoutException) {
			return status(GATEWAY_TIMEOUT, err("Deadline of " + req.getMsTimeout() + " ms exceeded.", "timeout"))
					.as(contentType).withHeader("X-Request-Id", req.getId());
		}
		if (c instanceof AiLaneLimiter.LaneFull) {
			AiLaneLimiter.LaneFull full = (AiLaneLimiter.LaneFull) c;
			return status(TOO_MANY_REQUESTS, err("The " + full.lane + " queue is full.", "lane_full")).as(contentType)
					.withHeader("Retry-After", "5").withHeader("X-Request-Id", req.getId());
		}
		if (c != null) {
			return status(BAD_GATEWAY, err("Upstream failure: " + c.getMessage(), "upstream_error")).as(contentType)
					.withHeader("X-Request-Id", req.getId());
		}
		return switch (req.getOutcome()) {
		case OK -> ok(req.getResult()).as(contentType).withHeader("X-Request-Id", req.getId());
		case TIMEOUT ->
			status(GATEWAY_TIMEOUT, req.getResult()).as(contentType).withHeader("X-Request-Id", req.getId());
		case UPSTREAM_ERROR ->
			status(BAD_GATEWAY, req.getResult()).as(contentType).withHeader("X-Request-Id", req.getId());
		case NO_CREDITS ->
			status(PAYMENT_REQUIRED, req.getResult()).as(contentType).withHeader("X-Request-Id", req.getId());
		case UNAUTHORIZED -> unauthorized(req.getResult()).as(contentType).withHeader("X-Request-Id", req.getId());
		case BAD_REQUEST -> badRequest(req.getResult()).as(contentType).withHeader("X-Request-Id", req.getId());
		};
	}

	public CompletionStage<Result> chatCompletion(Request request) {
		Optional<ApiCall> callOpt = authorize(request);
		if (callOpt.isEmpty()) {
			return CompletableFuture
					.completedFuture(badRequest(err("Authorization header missing or invalid", "unauthorized")));
		}
		JsonNode json = request.body().asJson();
		if (json == null || !json.isObject() || !json.has(REQUEST_MODEL)) {
			return CompletableFuture
					.completedFuture(badRequest(err("Expecting a JSON object with a model", "bad_request")));
		}
		ApiCall call = callOpt.get();
		((ObjectNode) json).put(REQUEST_API_TOKEN, call.apiKey());

		// ---- streaming: piped straight through, no actor, no buffer ----
		if (json.path(REQUEST_STREAM).asBoolean(false)) {
			RemoteApiRequest streamRequest = new RemoteApiRequest(REQUEST_TASK_CHAT_COMPLETION, AiLane.LLM.timeoutMs(),
					call.username(), call.apiKey(), -1L, (ObjectNode) json);
			return aiApiService.openStream(streamRequest)
					.thenApply(body -> ok().chunked(body).as("text/event-stream")
							.withHeader("Cache-Control", "no-cache").withHeader("X-Accel-Buffering", "no")
							.withHeader("X-Request-Id", streamRequest.getId()))
					.exceptionally(err -> respond(streamRequest, err, "application/json"));
		}

		// ---- buffered request ----
		RemoteApiRequest req = new RemoteApiRequest(REQUEST_TASK_CHAT_COMPLETION, AiLane.LLM.timeoutMs(),
				call.username(), call.apiKey(), -1L, (ObjectNode) json);
		return aiApiService.submitApiRequest(req).orTimeout(AiLane.LLM.timeoutMs(), TimeUnit.MILLISECONDS)
				.handle((v, err) -> respond(req, err, "application/json"));
	}

	public CompletionStage<Result> audioTranscription(Request request) {
		Optional<ApiCall> callOpt = authorize(request);
		if (callOpt.isEmpty()) {
			return CompletableFuture
					.completedFuture(badRequest(err("Authorization header missing or invalid", "unauthorized")));
		}
		MultipartFormData<TemporaryFile> mpfd = request.body().asMultipartFormData();
		if (mpfd == null || mpfd.isEmpty()) {
			return CompletableFuture.completedFuture(badRequest(err("File to transcribe missing.", "bad_request")));
		}
		ApiCall call = callOpt.get();
		RemoteApiRequest req = new RemoteApiRequest(REQUEST_TASK_AUDIO_TRANSCRIPTION, AiLane.STT.timeoutMs(),
				call.username(), call.apiKey(), -1L, Json.newObject());
		req.setMultipartFormData(mpfd);

		return aiApiService.submitApiRequest(req).orTimeout(AiLane.STT.timeoutMs(), TimeUnit.MILLISECONDS)
				.handle((v, err) -> respond(req, err, "application/json"));
	}

	public CompletionStage<Result> imageGeneration(Request request) {
		Optional<ApiCall> callOpt = authorize(request);
		if (callOpt.isEmpty()) {
			return CompletableFuture
					.completedFuture(badRequest(err("Authorization header missing or invalid", "unauthorized")));
		}
		JsonNode json = request.body().asJson();
		if (json == null || !json.isObject()) {
			return CompletableFuture.completedFuture(badRequest(err("Expecting JSON body", "bad_request")));
		}
		ApiCall call = callOpt.get();
		ObjectNode requestParams = (ObjectNode) json;
		RemoteApiRequest req = new RemoteApiRequest(REQUEST_TASK_IMAGE_GENERATION, AiLane.IMAGE.timeoutMs(),
				call.username(), call.apiKey(), -1L, requestParams);

		return aiApiService.submitApiRequest(req).orTimeout(AiLane.IMAGE.timeoutMs(), TimeUnit.MILLISECONDS)
				.handle((v, e) -> {
					if (e != null || req.getOutcome() != Outcome.OK) {
						return respond(req, e, "application/json");
					}
					try {
						JsonNode result = Json.parse(req.getResult());
						JsonNode imageId = result.get("image_id");
						if (imageId == null) {
							return status(BAD_GATEWAY, err("Image backend returned no image id.", "upstream_error"))
									.withHeader("X-Request-Id", req.getId());
						}
						return ok(Json.newObject()
								.put("image_url",
										routes.UnmanagedAIApiController.image(imageId.asText()).absoluteURL(request,
												environment.isProd()))
								.set("prompt", requestParams.get("prompt")).toString()).as("application/json")
								.withHeader("X-Request-Id", req.getId());
					} catch (Exception parseEx) {
						return status(BAD_GATEWAY, err("Invalid JSON response from image backend.", "upstream_error"))
								.withHeader("X-Request-Id", req.getId());
					}
				});
	}

	public Result image(Request request, String token) {
		String filePath = (String) cache.get(token).orElse("");
		File tempFile = new File(filePath);

		// check file
		if (!tempFile.exists() || !tempFile.isFile() || !tempFile.canRead()) {
			return internalServerError();
		}

		return ok(tempFile).as("image/png");
	}

	public CompletionStage<Result> speechGeneration(Request request) {
		Optional<ApiCall> callOpt = authorize(request);
		if (callOpt.isEmpty()) {
			return CompletableFuture
					.completedFuture(badRequest(err("Authorization header missing or invalid", "unauthorized")));
		}
		JsonNode json = request.body().asJson();
		if (json == null || !json.isObject()) {
			return CompletableFuture.completedFuture(badRequest(err("Expecting JSON body", "bad_request")));
		}
		ApiCall call = callOpt.get();
		ObjectNode requestParams = (ObjectNode) json;
		RemoteApiRequest req = new RemoteApiRequest(REQUEST_TASK_SPEECH_GENERATION, AiLane.TTS.timeoutMs(),
				call.username(), call.apiKey(), -1L, requestParams);

		return aiApiService.submitApiRequest(req).orTimeout(AiLane.TTS.timeoutMs(), TimeUnit.MILLISECONDS)
				.handle((v, e) -> {
					if (e != null || req.getOutcome() != Outcome.OK) {
						return respond(req, e, "application/json");
					}
					File generatedSpeech = new File(req.getResult());
					if (!generatedSpeech.exists() || !generatedSpeech.isFile() || !generatedSpeech.canRead()
							|| generatedSpeech.length() < 1000) {
						return status(BAD_GATEWAY, err("Speech generation produced invalid file.", "upstream_error"))
								.withHeader("X-Request-Id", req.getId());
					}
					return ok(generatedSpeech).as("audio/mpeg").withHeader("X-Request-Id", req.getId());
				});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletionStage<Result> pdfToText(Request request) {
		Optional<ApiCall> callOpt = authorize(request);
		if (callOpt.isEmpty()) {
			return CompletableFuture
					.completedFuture(badRequest(err("Authorization header missing or invalid", "unauthorized")));
		}
		ApiCall call = callOpt.get();
		if (!aiApiService.isValidApiKey(call.apiKey())) {
			return CompletableFuture
					.completedFuture(unauthorized(err("Invalid API key or unauthorized.", "unauthorized")));
		}

		MultipartFormData<TemporaryFile> mpfd = request.body().asMultipartFormData();
		if (mpfd == null || mpfd.getFile("file") == null) {
			return CompletableFuture.completedFuture(
					badRequest(err("PDF file missing (use multipart field name 'file')", "bad_request")));
		}

		File pdfFile = mpfd.getFile("file").getRef().path().toFile();
		String internalToken = "api2_pdf_" + java.util.UUID.randomUUID().toString();

		return mediaProcessingService
				.scheduleMediaToTextProcess(pdfFile, "", "application/pdf", call.apiKey(), internalToken)
				.toCompletableFuture().orTimeout(300, TimeUnit.SECONDS).handle((result, err) -> {
					if (err != null) {
						return internalServerError(err("PDF processing timed out or failed.", "processing_error"));
					}
					String cleaned = result.replace(" [END]", "");
					return ok(Json.newObject().put("text", cleaned)).as("application/json");
				});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletionStage<Result> models(Request request) {
		Optional<ApiCall> callOpt = authorize(request);
		if (callOpt.isEmpty()) {
			return CompletableFuture
					.completedFuture(badRequest(err("Authorization header missing or invalid", "unauthorized")));
		}
		ApiCall call = callOpt.get();
		RemoteApiRequest req = new RemoteApiRequest(REQUEST_TASK_MODELS, AiLane.MODELS.timeoutMs(), "", call.apiKey(),
				-1L);

		return aiApiService.submitApiRequest(req).orTimeout(AiLane.MODELS.timeoutMs(), TimeUnit.MILLISECONDS)
				.handle((v, err) -> respond(req, err, "application/json"));
	}

	@Authenticated(UserAuth.class)
	public Result modelsPage(Request request) {
		return ok(views.html.tools.ai.index.render(aiApiService.getModels()));
	}

	@Authenticated(UserAuth.class)
	public Result laneStats() {
		return ok(aiApiService.getLaneLimiter().snapshot()).as("application/json");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	String checkDocumentationAPIKey(Request request, String authorization) {
		String referrer = request.header(REFERER).orElse("");
		if (referrer.isEmpty()) {
			return authorization;
		}

		try {
			URL refUrl = new URL(referrer);
			// Resolve effective host (supporting WAF / reverse proxy X-Forwarded-Host)
			String forwardedHost = request.header("X-Forwarded-Host").orElse("");
			String rawHost = !forwardedHost.isEmpty() ? forwardedHost.split(",")[0].trim() : request.host();
			String requestHost = rawHost.contains(":") ? rawHost.substring(0, rawHost.indexOf(':')) : rawHost;

			// Ensure the referer host matches our host, and the path is under /documentation
			if (refUrl.getHost().equalsIgnoreCase(requestHost)
					&& (refUrl.getPath().equals("/documentation") || refUrl.getPath().startsWith("/documentation/"))) {
				return aiApiService.getInternalDocumentationAPIKey();
			}
		} catch (MalformedURLException e) {
			// do nothing
		}

		return authorization;
	}

}

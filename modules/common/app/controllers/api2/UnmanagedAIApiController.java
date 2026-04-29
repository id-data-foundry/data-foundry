package controllers.api2;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Status;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

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
import services.api.ai.UnmanagedAIApiService;
import services.api.remoting.RemoteApiRequest;
import services.api.remoting.StreamingRemoteApiRequest;

public class UnmanagedAIApiController extends Controller implements ApiServiceConstants {

	@Inject
	UnmanagedAIApiService aiApiService;

	@Inject
	SyncCacheApi cache;

	@Inject
	Environment environment;

	@Inject
	services.processing.MediaProcessingService mediaProcessingService;

	public CompletionStage<Result> chatCompletion(Request request) {
		return CompletableFuture.supplyAsync(() -> {

			// check request
			JsonNode json = request.body().asJson();
			if (json == null || !json.isObject()) {
				return badRequest("Expecting Json data");
			}

			String authHeader = request.header("Authorization").orElse("");
			if (authHeader.isEmpty() || !authHeader.startsWith("Bearer ")) {
				return badRequest("Authorization header missing or invalid");
			}

			String authorization = authHeader.replace("Bearer ", "").trim();
			if (authorization.isEmpty()) {
				return badRequest("Authorization header missing or invalid");
			}

			// check whether we have a documentation API key
			final String apiKey = checkDocumentationAPIKey(request, authorization);

			// add authorization to request parameters
			if (json.isObject()) {
				((ObjectNode) json).put(REQUEST_API_TOKEN, apiKey);
			}

			// check model for where to dispatch
			if (!json.has(REQUEST_MODEL)) {
				return badRequest("Model missing from request");
			}

			// create request
			if (json.has(REQUEST_STREAM) && json.get(REQUEST_STREAM).asBoolean()) {
				// streaming request
				@SuppressWarnings("deprecation")
				Source<ByteString, ?> source = Source.<ByteString>actorRef(16, OverflowStrategy.dropTail())
						.mapMaterializedValue(sourceActor -> {
							ChunkedWriter writer = new ChunkedWriter(sourceActor);
							StreamingRemoteApiRequest internalAPIRequest = new StreamingRemoteApiRequest(writer,
									ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, REQUEST_TASK_CHAT_COMPLETION,
									"", apiKey, -1L, (ObjectNode) json);
							internalAPIRequest.setPath("/v1/chat/completions");
							aiApiService.submitApiRequest(internalAPIRequest);
							return NotUsed.getInstance();
						});
				return ok().chunked(source).as("application/json");
			} else {
				// buffered request
				RemoteApiRequest internalAPIRequest = new RemoteApiRequest(REQUEST_TASK_CHAT_COMPLETION,
						ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, "", apiKey, -1L, (ObjectNode) json);
				internalAPIRequest.setPath("/v1/chat/completions");

				try {
					// submit and wait for timeout
					aiApiService.submitApiRequest(internalAPIRequest)
							.get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
				} catch (InterruptedException | ExecutionException | TimeoutException e) {
					// do nothing
				}

				return ok(internalAPIRequest.getResult()).as("application/json");
			}
		});
	}

	public CompletionStage<Result> audioTranscription(Request request) {
		return CompletableFuture.supplyAsync(() -> {

			// check request
			String authHeader = request.header("Authorization").orElse("");
			if (authHeader.isEmpty() || !authHeader.startsWith("Bearer ")) {
				return badRequest("Authorization header missing or invalid");
			}

			String authorization = authHeader.replace("Bearer ", "").trim();
			if (authorization.isEmpty()) {
				return badRequest("Authorization header missing or invalid");
			}

			MultipartFormData<TemporaryFile> mpfd = request.body().asMultipartFormData();
			if (mpfd.isEmpty()) {
				return badRequest("File to transcribe missing.");
			}

			// check whether we have a documentation API key
			final String apiKey = checkDocumentationAPIKey(request, authorization);

			// create request
			RemoteApiRequest internalAPIRequest = new RemoteApiRequest(REQUEST_TASK_MODELS,
					ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, "", apiKey, -1L, Json.newObject());
			internalAPIRequest.setPath("/v1/audio/transcriptions");

			// set file in request
			internalAPIRequest.setMultipartFormData(mpfd);

			try {
				// submit and wait for timeout
				aiApiService.submitApiRequest(internalAPIRequest)
						.get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				// do nothing
			}
			return ok(internalAPIRequest.getResult()).as("application/json");
		});
	}

	public CompletionStage<Result> imageGeneration(Request request) {
		return CompletableFuture.<Result>supplyAsync(() -> {

			// check request
			String authHeader = request.header("Authorization").orElse("");
			if (authHeader.isEmpty() || !authHeader.startsWith("Bearer ")) {
				return badRequest("Authorization header missing or invalid");
			}

			String authorization = authHeader.replace("Bearer ", "").trim();
			if (authorization.isEmpty()) {
				return badRequest("Authorization header missing or invalid");
			}

			// check whether we have a documentation API key
			final String apiKey = checkDocumentationAPIKey(request, authorization);

			// create request
			ObjectNode requestParams = (ObjectNode) request.body().asJson();
			RemoteApiRequest internalAPIRequest = new RemoteApiRequest(REQUEST_TASK_IMAGE_GENERATION,
					ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS * 5, "", apiKey, -1L, requestParams);
			internalAPIRequest.setPath("/v1/images/generations");

			try {
				// submit and wait for timeout
				aiApiService.submitApiRequest(internalAPIRequest)
						.get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS * 5, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				// do nothing
			}

			JsonNode result = Json.parse(internalAPIRequest.getResult());
			return ok(Json.newObject()
					.put("image_url", routes.UnmanagedAIApiController.image(result.get("image_id").asText())
							.absoluteURL(request, environment.isProd()))
					.set("prompt", requestParams.get("prompt")).toString());
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
		return CompletableFuture.<Result>supplyAsync(() -> {

			// check request
			String authHeader = request.header("Authorization").orElse("");
			if (authHeader.isEmpty() || !authHeader.startsWith("Bearer ")) {
				return badRequest("Authorization header missing or invalid");
			}

			String authorization = authHeader.replace("Bearer ", "").trim();
			if (authorization.isEmpty()) {
				return badRequest("Authorization header missing or invalid");
			}

			// check whether we have a documentation API key
			final String apiKey = checkDocumentationAPIKey(request, authorization);

			// create request
			ObjectNode requestParams = (ObjectNode) request.body().asJson();
			RemoteApiRequest internalAPIRequest = new RemoteApiRequest(REQUEST_TASK_SPEECH_GENERATION,
					ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS / 2, "", apiKey, -1L, requestParams);
			internalAPIRequest.setPath("/v1/audio/speech");

			try {
				// submit and wait for timeout
				aiApiService.submitApiRequest(internalAPIRequest)
						.get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS / 3, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				// do nothing
			}

			// check generated file
			File generatedSpeech = new File(internalAPIRequest.getResult());
			if (!generatedSpeech.exists() || !generatedSpeech.isFile() || !generatedSpeech.canRead()
					|| generatedSpeech.length() < 1000) {
				return internalServerError();
			}

			return ok(generatedSpeech).as("audio/mpeg");
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletionStage<Result> pdfToText(Request request) {
		return CompletableFuture.supplyAsync(() -> {

			// check request
			String authHeader = request.header("Authorization").orElse("");
			if (authHeader.isEmpty() || !authHeader.startsWith("Bearer ")) {
				return badRequest(Json.newObject().put("error", "Authorization header missing or invalid"));
			}

			String authorization = authHeader.replace("Bearer ", "").trim();
			if (authorization.isEmpty()) {
				return badRequest(Json.newObject().put("error", "Authorization header missing or invalid"));
			}

			// check whether we have a documentation API key
			final String apiKey = checkDocumentationAPIKey(request, authorization);

			// check API key
			if (!aiApiService.isValidApiKey(apiKey)) {
				return unauthorized(Json.newObject().put("error", "Invalid API key or unauthorized."));
			}

			MultipartFormData<TemporaryFile> mpfd = request.body().asMultipartFormData();
			if (mpfd == null || mpfd.getFile("file") == null) {
				return badRequest(Json.newObject().put("error", "PDF file missing (use multipart field name 'file')"));
			}

			// extract file
			MultipartFormData.FilePart<TemporaryFile> pdfPart = mpfd.getFile("file");
			File pdfFile = pdfPart.getRef().path().toFile();
			String internalToken = "api2_pdf_" + java.util.UUID.randomUUID().toString();

			try {
				// submit and wait for timeout (30 seconds)
				String result = mediaProcessingService
						.scheduleMediaToTextProcess(pdfFile, "", "application/pdf", apiKey, internalToken)
						.toCompletableFuture().get(300, TimeUnit.SECONDS);

				// clean up result
				result = result.replace(" [END]", "");

				return ok(Json.newObject().put("text", result)).as("application/json");
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				return internalServerError(Json.newObject().put("error", "PDF processing timed out or failed."));
			}
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletionStage<Result> models(Request request) {
		return CompletableFuture.supplyAsync(() -> {

			// check request
			String authHeader = request.header("Authorization").orElse("");
			if (authHeader.isEmpty() || !authHeader.startsWith("Bearer ")) {
				return badRequest("Authorization header missing or invalid");
			}

			String authorization = authHeader.replace("Bearer ", "").trim();
			if (authorization.isEmpty()) {
				return badRequest("Authorization header missing or invalid");
			}

			// create request
			RemoteApiRequest internalAPIRequest = new RemoteApiRequest(REQUEST_TASK_MODELS,
					ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, "", authorization, -1L);
//			internalAPIRequest.setPath("/v1/models");
			internalAPIRequest.setPath("/models");

			try {
				// submit and wait for timeout
				aiApiService.submitApiRequest(internalAPIRequest)
						.get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				// do nothing
			}
			return ok(internalAPIRequest.getResult()).as("application/json");
		});
	}

	@Authenticated(UserAuth.class)
	public Result modelsPage(Request request) {
		return ok(views.html.tools.ai.index.render(aiApiService.getModels()));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private String checkDocumentationAPIKey(Request request, String authorization) {
		String referrer = request.header(REFERER).orElse("");
		String host = request.host();

		try {
			URL refUrl = new URL(referrer);
			// ensure same host, /documentation path
			if (referrer.contains(host) && refUrl.getPath().startsWith("/documentation")) {
				return aiApiService.getInternalDocumentationAPIKey();
			}
		} catch (MalformedURLException e) {
			// do nothing
		}

		return authorization;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	static public class ChunkedWriter {
		private final ActorRef output;

		public ChunkedWriter(ActorRef output) {
			this.output = output;
		}

		public void append(String str) {
			output.tell(ByteString.fromString(str), null);
		}

		public void close() {
			output.tell(new Status.Success(NotUsed.getInstance()), null);
		}
	}

}

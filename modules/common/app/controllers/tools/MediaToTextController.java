package controllers.tools;

import java.io.File;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pekko.actor.ActorSystem;

import com.google.inject.Inject;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import models.Person;
import play.cache.SyncCacheApi;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http.MultipartFormData;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.api.ApiServiceConstants;
import services.api.ai.UnmanagedAIApiService;
import services.api.remoting.RemoteApiRequest;
import services.processing.MediaProcessingService;
import utils.auth.TokenResolverUtil;

@Authenticated(UserAuth.class)
public class MediaToTextController extends AbstractAsyncController implements ApiServiceConstants {
	@Inject
	ActorSystem actorSystem;
	@Inject
	MediaProcessingService mediaProcessingService;
	@Inject
	SyncCacheApi cache;
	@Inject
	TokenResolverUtil tokenResolverUtil;
	@Inject
	UnmanagedAIApiService aiApiService;

	public Result index() {
		return ok(views.html.tools.transcribe.index.render());
	}

	public CompletionStage<Result> submit(Request request, String lang, String type) {
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		// create user access token and set cache to empty string
		final String publicToken = UUID.randomUUID().toString();
		final String internalToken = internalToken(publicToken);
		cache.set(internalToken, "", MediaProcessingService.EXPIRATION_TIMEOUT_SECS);

		if (type.startsWith("application/pdf")) {
			// process PDF file
			return processPDF(request, lang, type, user, publicToken);
		} else {
			// process image or anything else
			return processAV(request, lang, type, user, publicToken);
		}
	}

	/**
	 * submit the processing request and waits until the result is available
	 * 
	 * @param request
	 * @param type
	 * @return
	 */
	public CompletionStage<Result> submitAndProcess(Request request, String type) {
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		// create user access token and set cache to empty string
		final String publicToken = UUID.randomUUID().toString();
		final String internalToken = internalToken(publicToken);
		cache.set(internalToken, "", MediaProcessingService.EXPIRATION_TIMEOUT_SECS);

		// get the uploaded file
		File file = request.body().asRaw().asFile();

		// check the uploaded file
		if (!file.exists()) {
			return CompletableFuture.completedStage(badRequest("[ERROR] File missing."));
		}

		// schedule the job and return data as String when the processing is done
		return mediaProcessingService.scheduleMediaToTextProcess(file, "", nss(type), user.getName(), internalToken)
				.thenApply(s -> {
					if (s.startsWith("[ERROR]")) {
						return badRequest(s);
					} else {
						return ok(s);
					}
				});
	}

	public Result result(Request request, String publicToken) {
		getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		Optional<String> content = cache.get(internalToken(publicToken));
		if (!content.isPresent()) {
			return notFound("No content found.");
		}

		return ok(content.get());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private CompletionStage<Result> processPDF(Request request, String lang, String type, Person user,
			String publicToken) {
		// get the uploaded file
		MultipartFormData<TemporaryFile> mpfd = request.body().asMultipartFormData();
		if (mpfd == null || mpfd.isEmpty()) {
			return CompletableFuture.completedStage(badRequest("File to transcribe missing."));
		}

		File file = mpfd.getFile("file").getRef().path().toFile();
		if (!file.exists()) {
			return CompletableFuture.completedStage(badRequest("[ERROR] File missing."));
		}

		try {
			mediaProcessingService
					.scheduleMediaToTextProcess(file, nss(lang), nss(type), user.getName(), internalToken(publicToken))
					.toCompletableFuture().get();
		} catch (InterruptedException e) {
		} catch (ExecutionException e) {
		}

		// return token immediately
		return CompletableFuture.completedStage(ok(publicToken));
	}

	private CompletionStage<Result> processAV(Request request, String lang, String type, Person user,
			String publicToken) {

		// create request with empty parameters; they will be set differently later on
		RemoteApiRequest internalAPIRequest = new RemoteApiRequest(REQUEST_TASK_MODELS,
				ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, "", aiApiService.getInternalDocumentationAPIKey(),
				-1L, Json.newObject());
		internalAPIRequest.setPath("/v1/audio/transcriptions");

		// set file and params in request via multi-part form data
		MultipartFormData<TemporaryFile> mpfd = request.body().asMultipartFormData();
		if (mpfd == null || mpfd.isEmpty()) {
			return CompletableFuture.completedStage(badRequest("File to transcribe missing."));
		}
		internalAPIRequest.setMultipartFormData(mpfd);

		// schedule the job and return data as String
		actorSystem.dispatcher().execute(() -> {
			try {
				// submit and wait for timeout
				aiApiService.submitApiRequest(internalAPIRequest)
						.get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
				String resultText = Json.parse(internalAPIRequest.getResult()).get("text").asText();
				resultText = resultText.isEmpty() ? "Transcription error." : resultText;
				cache.set(internalToken(publicToken), resultText, MediaProcessingService.EXPIRATION_TIMEOUT_SECS);
			} catch (/* InterruptedException | ExecutionException | TimeoutException | */ Exception e) {
				// do nothing, cancel output
				cache.set(internalToken(publicToken), "[DONE]", MediaProcessingService.EXPIRATION_TIMEOUT_SECS);
			}
		});

		// return token immediately
		return CompletableFuture.completedStage(ok(publicToken));
	}

	private String internalToken(final String publicToken) {
		return "SpeechToTextController_" + publicToken;
	}
}

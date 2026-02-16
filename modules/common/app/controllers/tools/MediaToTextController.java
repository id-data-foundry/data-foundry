package controllers.tools;

import java.io.File;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.apache.pekko.actor.ActorSystem;

import com.google.inject.Inject;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import models.Person;
import play.cache.SyncCacheApi;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.processing.MediaProcessingService;
import utils.auth.TokenResolverUtil;

public class MediaToTextController extends AbstractAsyncController {

	@Inject
	ActorSystem actorSystem;
	@Inject
	MediaProcessingService mediaProcessingService;
	@Inject
	SyncCacheApi cache;
	@Inject
	TokenResolverUtil tokenResolverUtil;

	@Authenticated(UserAuth.class)
	public Result index() {
		int jobs = mediaProcessingService.getEnqueuedJobs();
		final String busyness;
		if (jobs < 3) {
			busyness = "ready for you 🏄‍♂️";
		} else if (jobs < 8) {
			busyness = "a little busy 💪";
		} else if (jobs < 16) {
			busyness = "pretty busy 🤨";
		} else {
			busyness = "come back later, perhaps? 🥴";
		}
		return ok(views.html.tools.processing.stt.index.render(busyness));
	}

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> submit(Request request, String lang, String type) {
		Person user = getAuthenticatedUserOrReturn(request,
		        redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		// create user access token and set cache to empty string
		final String publicToken = generateToken();
		final String internalToken = user.getId() + publicToken;
		cache.set(internalToken, "", MediaProcessingService.EXPIRATION_TIMEOUT_SECS);

		// get the uploaded file
		File file = request.body().asRaw().asFile();
		if (!file.exists()) {
			return CompletableFuture.completedStage(badRequest("[ERROR] File missing."));
		}

		// schedule the job and return data as String
		actorSystem.dispatcher().execute(() -> {
			try {
				mediaProcessingService
				        .scheduleMediaToTextProcess(file, nss(lang), nss(type), user.getName(), internalToken)
				        .toCompletableFuture().get();
			} catch (InterruptedException e) {
			} catch (ExecutionException e) {
			}
		});

		// return token immediately
		return CompletableFuture.completedStage(ok(publicToken));
	}

	/**
	 * submit the processing request and waits until the result is available
	 * 
	 * @param request
	 * @param type
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> submitAndProcess(Request request, String type) {
		Person user = getAuthenticatedUserOrReturn(request,
		        redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		// create user access token and set cache to empty string
		final String publicToken = generateToken();
		final String internalToken = user.getId() + publicToken;
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

	@Authenticated(UserAuth.class)
	public Result result(Request request, String publicToken) {
		Person user = getAuthenticatedUserOrReturn(request,
		        redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		final String internalToken = user.getId() + publicToken;
		Optional<String> content = cache.get(internalToken);
		if (!content.isPresent()) {
			return notFound("No content found.");
		}

		return ok(content.get());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private String generateToken() {
		return "SpeechToTextController_" + UUID.randomUUID().toString();
	}
}

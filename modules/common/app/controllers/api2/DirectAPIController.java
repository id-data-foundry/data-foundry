package controllers.api2;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.node.ObjectNode;

import models.Project;
import play.cache.SyncCacheApi;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http.Request;
import play.mvc.Result;
import services.api.ApiServiceConstants;
import services.api.GenericApiService;
import services.api.ai.ManagedAIApiService;
import services.api.processing.AudioProcessingApiService;
import services.api.remoting.RemoteApiRequest;

@Singleton
public class DirectAPIController extends Controller {

	private SyncCacheApi cache;
	private final ManagedAIApiService managedAIService;
	private final AudioProcessingApiService audioService;

	@Inject
	public DirectAPIController(SyncCacheApi cache, ManagedAIApiService managedAIService,
	        AudioProcessingApiService audioService) {
		this.cache = cache;
		this.managedAIService = managedAIService;
		this.audioService = audioService;
	}

	/**
	 * send an API request to the OpenAI API
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public CompletionStage<Result> submitOpenAIRequest(Request request, long id) {
		return CompletableFuture.supplyAsync(() -> {
			Project project = Project.find.byId(id);
			if (project == null || project.getOwner() == null) {
				return noContent().as("application/json");
			}
			String username = project.getOwner().getName();
			final RemoteApiRequest apiRequest = new RemoteApiRequest("",
			        ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, username, "", id,
			        (ObjectNode) request.body().asJson());
			return ok(managedAIService.submitApiRequest(apiRequest)).as("application/json");
		});
	}

	/**
	 * send an API request to the LocalAI API
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public CompletionStage<Result> submitLocalAIRequest(Request request, long id) {
		return CompletableFuture.supplyAsync(() -> {
			Project project = Project.find.byId(id);
			if (project == null || project.getOwner() == null) {
				return noContent().as("application/json");
			}
			String username = project.getOwner().getName();
			final RemoteApiRequest apiRequest = new RemoteApiRequest("",
			        ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, username, "", id,
			        (ObjectNode) request.body().asJson());
			return ok(managedAIService.submitApiRequest(apiRequest)).as("application/json");
		});
	}

	/**
	 * send an API request to the STT engine (speech to text)
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public CompletionStage<Result> submitS2TRequest(Request request, long id) {
		return CompletableFuture.supplyAsync(() -> {
			Project project = Project.find.byId(id);
			if (project == null || project.getOwner() == null) {
				return noContent().as("application/json");
			}
			String username = project.getOwner().getName();
			final RemoteApiRequest apiRequest = new RemoteApiRequest("s2t",
			        ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, username, "", id,
			        (ObjectNode) request.body().asJson());
			try {
				audioService.submitApiRequest(apiRequest).get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS,
				        TimeUnit.MILLISECONDS);
				return ok(apiRequest.getResult());
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				return ok(Json.newObject().put("error", "No results, API timed out."));
			}
		});
	}

	/**
	 * send an API request to the TTS engine (text to speech)
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public CompletionStage<Result> submitT2SRequest(Request request, long id) {
		return CompletableFuture.supplyAsync(() -> {
			Project project = Project.find.byId(id);
			if (project == null || project.getOwner() == null) {
				return noContent().as("application/json");
			}
			String username = project.getOwner().getName();
			final RemoteApiRequest apiRequest = new RemoteApiRequest("t2s",
			        ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, username, "", id,
			        (ObjectNode) request.body().asJson());
			try {
				audioService.submitApiRequest(apiRequest).get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS,
				        TimeUnit.MILLISECONDS);
				return ok(apiRequest.getResult());
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				return ok(Json.newObject().put("error", "No results, API timed out."));
			}
		});
	}

	/**
	 * send an API request to the TTS engine (text to speech) and download the generated file directly
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public CompletionStage<Result> submitAndDownloadT2SRequest(Request request, long id) {
		return CompletableFuture.supplyAsync(() -> {
			Project project = Project.find.byId(id);
			if (project == null || project.getOwner() == null) {
				return noContent().as("application/json");
			}
			String username = project.getOwner().getName();
			final RemoteApiRequest apiRequest = new RemoteApiRequest("t2s",
			        ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, username, "", id,
			        (ObjectNode) request.body().asJson());
			try {
				audioService.submitApiRequest(apiRequest).get(ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS,
				        TimeUnit.MILLISECONDS);
				ObjectNode on = (ObjectNode) Json.parse(apiRequest.getResult());
				String token = on.get(GenericApiService.RESPONSE_TEXT).asText();
				return downloadCachedT2SFile(request, token);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				return noContent();
			}
		});
	}

	/**
	 * download a generated file with the given token
	 * 
	 * @param request
	 * @param token
	 * @return
	 */
	public Result downloadCachedT2SFile(Request request, String token) {
		Optional<String> filePathOpt = cache.get(token);
		if (filePathOpt.isEmpty()) {
			return notFound();
		}

		String filePath = filePathOpt.get();
		if (!filePath.endsWith(".wav")) {
			return notFound();
		}

		File downloadFile = new File(filePath);
		if (!downloadFile.exists()) {
			return notFound();
		}

		return ok(downloadFile);
	}

}

package services.api.processing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pekko.actor.ActorSystem;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import play.Logger;
import play.cache.SyncCacheApi;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import services.api.GenericApiService;
import services.api.remoting.RemoteApiRequest;
import services.api.remoting.RemoteRequestsExecutionService;
import services.api.requests.ApiRequest;
import services.processing.MediaProcessingService;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;

@Singleton
public class AudioProcessingApiService extends GenericApiService {

	private static final Logger.ALogger logger = Logger.of(AudioProcessingApiService.class);

	private static final int DEFAULT_AUDIO_PROCESSING_COST = 5;
	private static final String REQUEST_AUDIO = "audio";
	private static final String REQUEST_TEXT = "text";
	private static final String REQUEST_LANGUAGE = "lang";
	private static final String SANITIZE_PATTERN = "[^a-zA-Z_0-9,;:.\\s]";

	private final SyncCacheApi cache;
	private final MediaProcessingService mediaProcessor;
	private final ActorSystem actorSystem;

	private final RemoteRequestsExecutionService executionService;

	@Inject
	protected AudioProcessingApiService(Config configuration, AdminUtils adminUtils, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolver, SyncCacheApi cache, ActorSystem actorSystem,
	        RemoteRequestsExecutionService executionService, MediaProcessingService speechToText) {
		super(configuration, adminUtils, datasetConnector, tokenResolver);

		this.cache = cache;
		this.mediaProcessor = speechToText;
		this.actorSystem = actorSystem;
		this.executionService = executionService;
	}

	private void processRequest(RemoteApiRequest request) {
		if (request != null && request.isValid() && !request.isCanceled()) {
			try {
				if (request.getType().equals("s2t")) {
					String result = runS2TRequest(request);
					request.setResult(Optional.of(result));
				} else if (request.getType().equals("t2s")) {
					String result = runT2SRequest(request);
					request.setResult(Optional.of(result));
				} else {
					request.setResult(Optional.empty());
				}
			} catch (Exception e) {
			}
		}
	}

	/**
	 * runs a speech-to-text request via a Python process
	 * 
	 * @param r
	 * @return
	 */
	private String runS2TRequest(ApiRequest<String> r) {

		// check request
		if (!r.isValid()) {
			return Json.newObject().put(RESPONSE_ERROR, "Parameters are not provided in a JSON object {}.").toString();
		}

		String username = r.getUsername();
		long projectId = r.getProjectId();
		ObjectNode request = r.getParams();

		// check if we have a token set and whether this token is valid
		if (!request.has(REQUEST_API_TOKEN)) {
			// don't dispatch without api-key
			return Json.newObject().put(RESPONSE_ERROR, "No api-key given.").toString();
		}

		// check profile existence
		String apiToken = request.get(REQUEST_API_TOKEN).asText();

		// check if the apiKey is used in the right project
		long id = tokenResolver.getProjectIdFromParticipationToken(apiToken.replace("df-", ""));
		if (projectId != id) {
			return Json.newObject().put(RESPONSE_ERROR, "API key does not match this project.").toString();
		}

		// early abort in case the request does not contain audio
		if (!request.has(REQUEST_AUDIO)) {
			return Json.newObject().put(RESPONSE_ERROR, "Request does not contain audio parameter.").toString();
		}

		// check available credits for this request, if sufficient update credits
		int requestedTokens = DEFAULT_AUDIO_PROCESSING_COST;
		if (requestedTokens > 0) {
			Optional<String> errorResponse = checkAndUpdateCredits(apiToken, requestedTokens);
			// if an error response is returned, abort and return it directly
			if (errorResponse.isPresent()) {
				return errorResponse.get();
			}
		}

		String audioData = request.get(REQUEST_AUDIO).asText();
		// check basics of data URL format for audioData
		if (audioData.contains("data:") && audioData.contains(",")) {
			// extract data from data URL
			audioData = audioData.split(",")[1];
		}

		// save a temporary file
		File file;
		try {
			TemporaryFile tf = play.libs.Files.singletonTemporaryFileCreator().create("audioStreamProcessing", ".wav");
			file = tf.path().toFile();
			Files.write(tf.path(), Base64.getDecoder().decode(audioData));

			// delete file after 10 mins
			actorSystem.scheduler().scheduleOnce(Duration.ofMinutes(10), () -> {
				file.delete();
			}, actorSystem.dispatcher());

		} catch (IOException e) {
			logger.error("Server error processing audio data.", e);
			return Json.newObject().put(RESPONSE_ERROR, "Server error processing audio data.").toString();
		}

		logger.info("Starting audio processing for " + file.getAbsolutePath() + " for user " + username);

		// dispatch the request to processing
		String processingResult = "";
		try {
			processingResult = mediaProcessor
			        .scheduleMediaToTextProcess(file, "en_sm", "audio", r.getUsername(), UUID.randomUUID().toString())
			        .thenApply(token -> {
				        String textFromAudio;
				        do {
					        textFromAudio = (String) cache.get(token).orElse("");
					        try {
						        Thread.sleep(500);
					        } catch (InterruptedException e) {
					        }
				        } while (!textFromAudio.contains("[ERROR]") && !textFromAudio.contains("[END]"));

				        // post-process the text output
				        textFromAudio = textFromAudio.replace("[END]", "");
				        textFromAudio = textFromAudio.replace("\n", " - ");

				        return textFromAudio;
			        }).toCompletableFuture().get(60, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			return Json.newObject().put(RESPONSE_ERROR, "Server error processing audio data.").toString();
		}
		return Json.newObject().put(RESPONSE_TEXT, processingResult).toString();
	}

	/**
	 * runs a text-to-speech request via espeak
	 * 
	 * @param r
	 * @return
	 */
	private String runT2SRequest(ApiRequest<String> r) {
		// check request
		if (!r.isValid()) {
			return Json.newObject().put(RESPONSE_ERROR, "Parameters are not provided in a JSON object {}.").toString();
		}

		String username = r.getUsername();
		long projectId = r.getProjectId();
		ObjectNode request = r.getParams();

		// check if we have a token set and whether this token is valid
		if (!request.has(REQUEST_API_TOKEN)) {
			// don't dispatch without api-key
			return Json.newObject().put(RESPONSE_ERROR, "No api-key given.").toString();
		}

		// check profile existence
		String apiToken = request.get(REQUEST_API_TOKEN).asText();

		// check if the apiKey is used in the right project
		long id = tokenResolver.getProjectIdFromParticipationToken(apiToken.replace("df-", ""));
		if (projectId != id) {
			return Json.newObject().put(RESPONSE_ERROR, "API key does not match this project.").toString();
		}

		// early abort in case the request does not contain text
		if (!request.has(REQUEST_TEXT)) {
			return Json.newObject().put(RESPONSE_ERROR, "Request does not contain text input.").toString();
		}

		// check available credits for this request, if sufficient update credits
		int requestedTokens = DEFAULT_AUDIO_PROCESSING_COST;
		if (requestedTokens > 0) {
			Optional<String> errorResponse = checkAndUpdateCredits(apiToken, requestedTokens);
			// if an error response is returned, abort and return it directly
			if (errorResponse.isPresent()) {
				return errorResponse.get();
			}
		}

		// retrieve and sanitize the input text
		String textData = request.get(REQUEST_TEXT).asText();
		textData = textData.trim().replaceAll(SANITIZE_PATTERN, "");
		if (textData.isEmpty()) {
			return Json.newObject().put(RESPONSE_ERROR, "Request does not contain text input.").toString();
		}

		// retrieve and sanitize language
		String language = request.get(REQUEST_LANGUAGE).asText("en");
		language = language.replaceAll(SANITIZE_PATTERN, "");
		if (language.length() > 2) {
			language = "en";
		}
		logger.info("Starting text processing (" + language + ") for '" + textData + "' for user " + username);

		// dispatch the request to processing
		try {
			String token = UUID.randomUUID().toString();
			TemporaryFile tf = play.libs.Files.singletonTemporaryFileCreator().create("generatedWavFile", ".wav");
			File tempFile = tf.path().toFile();
			mediaProcessor.processText(textData, tempFile, language, username);
			cache.set(token, tempFile.getAbsolutePath());

			// delete cache entry and file after 10 mins
			actorSystem.scheduler().scheduleOnce(Duration.ofMinutes(10), () -> {
				cache.remove(token);
				tempFile.delete();
			}, actorSystem.dispatcher());

			// the processing result should be a download link to a generated file
			return Json.newObject().put(RESPONSE_TEXT, token).toString();
		} catch (IOException e) {
			return Json.newObject().put(RESPONSE_ERROR, "Speech generation request failed due to I/O.").toString();
		} catch (Exception e) {
			return Json.newObject().put(RESPONSE_ERROR, "Speech generation request failed.").toString();
		}
	}

	public Future<Void> submitApiRequest(RemoteApiRequest request) {
		return executionService.submitRequest(request, (r) -> processRequest(r), request.getMsTimeout());
	}
}

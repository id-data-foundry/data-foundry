package services.api.ai;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.theokanning.openai.completion.CompletionRequest;
import com.theokanning.openai.completion.CompletionRequest.CompletionRequestBuilder;
import com.theokanning.openai.completion.CompletionResult;
import com.theokanning.openai.completion.chat.ChatCompletionRequest;
import com.theokanning.openai.completion.chat.ChatCompletionRequest.ChatCompletionRequestBuilder;
import com.theokanning.openai.completion.chat.ChatCompletionResult;
import com.theokanning.openai.completion.chat.ChatMessage;
import com.theokanning.openai.embedding.EmbeddingRequest;
import com.theokanning.openai.embedding.EmbeddingResult;
import com.theokanning.openai.moderation.Moderation;
import com.theokanning.openai.moderation.ModerationRequest;
import com.theokanning.openai.moderation.ModerationRequest.ModerationRequestBuilder;
import com.theokanning.openai.moderation.ModerationResult;
import com.theokanning.openai.service.OpenAiService;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import play.Logger;
import play.libs.Json;
import services.api.ApiServiceConstants;
import services.api.remoting.RemoteApiRequest;
import services.api.remoting.RemoteRequestsExecutionService;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;

@Singleton
public class ManagedAIApiService extends AbstractAIApiService implements ApiServiceConstants {

	private static final Logger.ALogger logger = Logger.of(ManagedAIApiService.class);

	private final RemoteRequestsExecutionService executionService;

	@Inject
	public ManagedAIApiService(Config configuration, AdminUtils adminUtils, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolver, RemoteRequestsExecutionService executorService, LocalModelMetadata lmmd) {
		super(configuration, adminUtils, datasetConnector, tokenResolver, lmmd);
		this.executionService = executorService;

	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * run a remote API request for max. msTimeout millisecond; abort on timeout
	 * 
	 * @param request
	 * @param timeoutMS
	 * @return
	 */
	public String submitApiRequest(RemoteApiRequest request) {

		// 1. check if we have an API token set and whether this token is valid
		switch (request.getInternalApiKey()) {
		case "":
			return Json.newObject().put(RESPONSE_ERROR, "No api-key available.").toString();
		case OPENAI_APIKEY:
			// only check project match if the local key is given
			if (!request.getUserApiKey().startsWith(OPENAI_API_KEY_PREFIX)) {
				// check if the apiKey is used in the right project
				long id = tokenResolver.getProjectIdFromParticipationToken(request.getUserApiKey().replace("df-", ""));
				if (request.getProjectId() != id) {
					request.cancel();
					return Json.newObject().put(RESPONSE_ERROR, "API key does not match this project.").toString();
				}
			}
			request.setInternalApiKey(openAIAPIKey);
			break;
		case LOCALAI_APIKEY: {
			// check if the apiKey is used in the right project
			long id = tokenResolver.getProjectIdFromParticipationToken(request.getUserApiKey().replace("df-", ""));
			if (request.getProjectId() != id) {
				request.cancel();
				return Json.newObject().put(RESPONSE_ERROR, "API key does not match this project.").toString();
			}
		}
		}

		// 2. fast track credit requests
		if (request.isCreditRequest()) {
			return checkCredits(request.getUserApiKey()).get();
		}

		// 3. check and map requested model
		preProcessRequest(request);

		// 4. check authorization from DB: check available credits for this request, if sufficient update credits
		Optional<String> errorResponse = checkAndUpdateCredits(request.getUserApiKey(), request.getRequestedTokens());
		// if an error response is returned, abort and return it directly
		if (errorResponse.isPresent()) {
			return errorResponse.get();
		}

		// 5. submit and wait for timeout
		try {
			executionService.submitRequest(request, (r) -> processRequest(r), request.getMsTimeout())
			        .get(request.getMsTimeout() + 1000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			// do nothing on failure
		}

		return request.getResult();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * run an OpenAI API request
	 * 
	 * @return
	 */
	private void processRequest(RemoteApiRequest r) {

		// 1. check time-based request validity
		if (!r.isValid()) {
			r.cancel();
			r.setResult(Optional.of(Json.newObject()
			        .put(RESPONSE_ERROR, "Parameters are not provided in a JSON object {}.").toString()));
			return;
		}

		// 2. check available credits for this request, if sufficient update credits
		if (r.getRequestedTokens() > 0) {
			Optional<String> errorResponse = checkAndUpdateCredits(r.getUserApiKey(), r.getRequestedTokens());
			// if an error response is returned, abort and return it directly
			if (errorResponse.isPresent()) {
				r.cancel();
				r.setResult(Optional.of(errorResponse.get()));
				return;
			}
		}

		// preparation is done, now we can start the request
		r.setStarted(true);

		// determine task
		switch (r.getTask()) {
		case "":
			// empty task -> opt for the default completion request
			r.setResult(Optional.of(
			        dispatchCompletionRequest(r.getUsername(), r.getInternalApiKey(), r.getModel(), r.getParams())));
			break;
		case REQUEST_TASK_COMPLETION:
			r.setResult(Optional.of(
			        dispatchCompletionRequest(r.getUsername(), r.getInternalApiKey(), r.getModel(), r.getParams())));
			break;
		case REQUEST_TASK_CHAT_COMPLETION:
			r.setResult(Optional.of(dispatchChatCompletionRequest(r)));
			break;
		case REQUEST_TASK_MODERATION:
			r.setResult(Optional.of(dispatchModerationRequest(r.getUsername(), r.getInternalApiKey(), r.getParams())));
			break;
		case REQUEST_TASK_CREDITS:
			r.setResult(Optional.of(getProjectAPIUsage(r.getInternalApiKey()).toString()));
			break;
		default:
			r.setResult(
			        Optional.of(Json.newObject().put(RESPONSE_ERROR, "Specified task is not available.").toString()));
		}
	}

	/**
	 * dispatch a completion request
	 * 
	 * @param username
	 * @param apiKey
	 * @param model
	 * @param prompt
	 * @param requestParams
	 * @return
	 */
	private String dispatchCompletionRequest(String username, final String apiKey, String model,
	        ObjectNode requestParams) {

		final String prompt;
		if (requestParams.has(REQUEST_PROMPT)) {
			String param = requestParams.get(REQUEST_PROMPT).asText();
			prompt = param;
		} else {
			// don't dispatch without prompt
			return Json.newObject().put(RESPONSE_ERROR, "No prompt given.").toString();
		}

		// compose request parameters, starting with the prompt
		CompletionRequestBuilder crb = CompletionRequest.builder().prompt(prompt);

		// add temperature
		if (requestParams.has(REQUEST_TEMPERATURE)) {
			Double pp = requestParams.get(REQUEST_TEMPERATURE).asDouble();
			crb = crb.temperature(pp);
		}

		// add presence penalty
		if (requestParams.has(REQUEST_PRESENCE_PENALTY)) {
			Double pp = requestParams.get(REQUEST_PRESENCE_PENALTY).asDouble();
			crb = crb.presencePenalty(pp);
		}

		// add frequency penalty
		if (requestParams.has(REQUEST_FREQUENCY_PENALTY)) {
			Double pp = requestParams.get(REQUEST_FREQUENCY_PENALTY).asDouble();
			crb = crb.frequencyPenalty(pp);
		}

		// set standard params
		crb = crb.model(model);

		// check max tokens
		if (requestParams.has(REQUEST_MAX_TOKENS)) {
			int pp = requestParams.get(REQUEST_MAX_TOKENS).asInt();
			crb = crb.maxTokens(pp);
		} else {
			crb = crb.maxTokens(250);
		}

		// build request
		CompletionRequest completionRequest = crb.build();

		// create service with 28 secs timeout for moderation requests
		OpenAiService service = new OpenAiService(
		        createService(apiKey, ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS));

		ObjectNode result = Json.newObject();
		try {
			// dispatch the completion request and handle the outcome
			CompletionResult completion = service.createCompletion(completionRequest);
			if (!apiKey.equals(LOCALAI_APIKEY)) {
				logger.info("Dispatched OpenAI API completion request by " + username + ": " + prompt);
			}

			// log the result and finish reason in the response
			if (!completion.getChoices().isEmpty()) {
				completion.getChoices().forEach(choice -> {
					result.put(RESPONSE_TEXT, choice.getText());
					result.put(RESPONSE_FINISH_REASON, choice.getFinish_reason());
				});

				// log the cost in the response
				result.put(RESPONSE_COST, completion.getUsage().getTotalTokens());
			} else {
				result.put(RESPONSE_ERROR,
				        "Unknown API problem (check API key or API status --> https://status.openai.com).");
			}
		} catch (Exception e) {
			result.put(RESPONSE_ERROR,
			        "Unknown API problem (check API key or API status --> https://status.openai.com).");
			logger.error("Problem", e);
		}

		return result.toString();
	}

	/**
	 * dispatch a chat completion request
	 * 
	 * @param username
	 * @param apiKey
	 * @param prompt
	 * @param request
	 * @return
	 */
	private String dispatchChatCompletionRequest(RemoteApiRequest request) {

		ObjectNode requestParams = request.getParams();

		// extract messages from request
		final List<ChatMessage> messages = new LinkedList<>();
		if (requestParams.has(REQUEST_MESSAGES) && requestParams.get(REQUEST_MESSAGES).isArray()) {
			ArrayNode an = (ArrayNode) requestParams.get(REQUEST_MESSAGES);
			an.forEach(jn -> {
				if (jn.has("role") && jn.has("content")) {
					messages.add(new ChatMessage(jn.get("role").asText(), jn.get("content").asText()));
				}
			});
		} else {
			// don't dispatch without messages
			return Json.newObject().put(RESPONSE_ERROR, "No messages given.").toString();
		}

		if (messages.isEmpty()) {
			// don't dispatch without messages
			return Json.newObject().put(RESPONSE_ERROR, "No messages given.").toString();
		}

		// compose request parameters, starting with the prompt
		ChatCompletionRequestBuilder ccrb = ChatCompletionRequest.builder().messages(messages);

		// add temperature
		if (requestParams.has(REQUEST_TEMPERATURE)) {
			Double pp = requestParams.get(REQUEST_TEMPERATURE).asDouble();
			ccrb = ccrb.temperature(pp);
		}

		// add presence penalty
		if (requestParams.has(REQUEST_PRESENCE_PENALTY)) {
			Double pp = requestParams.get(REQUEST_PRESENCE_PENALTY).asDouble();
			ccrb = ccrb.presencePenalty(pp);
		}

		// add frequency penalty
		if (requestParams.has(REQUEST_FREQUENCY_PENALTY)) {
			Double pp = requestParams.get(REQUEST_FREQUENCY_PENALTY).asDouble();
			ccrb = ccrb.frequencyPenalty(pp);
		}

		// set standard params
		if (request.getModel().isEmpty()) {
			ccrb = ccrb.model(MODEL_GPT_4O_MINI).maxTokens(250);
		} else {
			ccrb = ccrb.model(request.getModel());

			// check max tokens
			if (requestParams.has(REQUEST_MAX_TOKENS)) {
				int pp = requestParams.get(REQUEST_MAX_TOKENS).asInt();
				ccrb = ccrb.maxTokens(pp);
			} else {
				ccrb = ccrb.maxTokens(250);
			}
		}

		// build request
		ChatCompletionRequest completionRequest = ccrb.build();

		// create service with 60 secs timeout
		OpenAiService service = new OpenAiService(
		        createService(request.getInternalApiKey(), ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS));

		ObjectNode result = Json.newObject();

		// dispatch the completion request and handle the outcome
		try {
			ChatCompletionResult completion = service.createChatCompletion(completionRequest);

			// only log if this is a paid request (= OpenAI)
			if (request.getRequestedTokens() > 1) {
				logger.info("Dispatched OpenAI API chat completion request by " + request.getUsername() + ": "
				        + messages.stream().map(m -> m.getRole() + ":" + m.getContent())
				                .collect(Collectors.joining("\n")));
			}

			// add the result and finish reason to the response
			if (!completion.getChoices().isEmpty()) {
				completion.getChoices().forEach(choice -> {
					result.put(RESPONSE_ROLE, choice.getMessage().getRole());
					result.put(RESPONSE_CONTENT, choice.getMessage().getContent());
					result.put(RESPONSE_FINISH_REASON, choice.getFinishReason());
				});

				// log the cost in the response
				result.put(RESPONSE_COST, completion.getUsage().getTotalTokens());
			} else {
				result.put(RESPONSE_ERROR,
				        "Unknown API problem (check API key or API status --> https://status.openai.com).");
			}
		} catch (Exception e) {
			result.put(RESPONSE_ERROR,
			        "Unknown API problem (check API key or API status --> https://status.openai.com).");
			logger.error("Problem", e);
		}

		return result.toString();
	}

	/**
	 * dispatch a moderation request
	 * 
	 * @param username
	 * @param apiKey
	 * @param prompt
	 * @return
	 */
	private String dispatchModerationRequest(String username, final String apiKey, ObjectNode requestParams) {
		final String prompt;
		if (requestParams.has(REQUEST_PROMPT)) {
			String param = requestParams.get(REQUEST_PROMPT).asText();
			prompt = param;
		} else {
			// don't dispatch without prompt
			return Json.newObject().put(RESPONSE_ERROR, "No prompt given.").toString();
		}

		ModerationRequestBuilder mrb = ModerationRequest.builder().input(prompt);
		ModerationRequest mr = mrb.model("text-moderation-latest").build();

		// create service
		OpenAiService service = new OpenAiService(
		        createService(apiKey, ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS));

		ObjectNode result = Json.newObject();
		try {
			// dispatch the completion request and handle the outcome
			ModerationResult moderation = service.createModeration(mr);
			if (!moderation.getResults().isEmpty()) {
				Moderation m = moderation.getResults().get(0);
				result.put("flagged", m.flagged);
				ObjectNode categories = result.putObject("categories");
				categories.put("hate", m.categories.hate);
				categories.put("hate/threatening", m.categories.hateThreatening);
				categories.put("self-harm", m.categories.selfHarm);
				categories.put("sexual", m.categories.sexual);
				categories.put("sexual/minors", m.categories.sexualMinors);
				categories.put("violence", m.categories.violence);
				categories.put("violence/graphic", m.categories.violenceGraphic);
				ObjectNode scores = result.putObject("category_scores");
				scores.put("hate", m.categoryScores.hate);
				scores.put("hate/threatening", m.categoryScores.hateThreatening);
				scores.put("self-harm", m.categoryScores.selfHarm);
				scores.put("sexual", m.categoryScores.sexual);
				scores.put("sexual/minors", m.categoryScores.sexualMinors);
				scores.put("violence", m.categoryScores.violence);
				scores.put("violence/graphic", m.categoryScores.violenceGraphic);
				return result.toString();
			}
		} catch (Exception e) {
			result.put(RESPONSE_ERROR,
			        "Unknown API problem (check API key or API status --> https://status.openai.com).");
			logger.error("Problem", e);
		}

		return Json.newObject().toString();
	}

	public List<List<Double>> dispatchEmbeddingRequest(String usernam, List<String> contentToEmbed) {

		// create request
		EmbeddingRequest er = EmbeddingRequest.builder().input(contentToEmbed)
		        .model("text-embedding-nomic-embed-text-v2-moe").build();

		// create service
		OpenAiService service = new OpenAiService(
		        createService(LOCALAI_APIKEY, ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS));

		// run service with request
		EmbeddingResult ers = service.createEmbeddings(er);

		// return listed results
		return ers.getData().stream().map(d -> d.getEmbedding()).collect(Collectors.toList());
	}

}

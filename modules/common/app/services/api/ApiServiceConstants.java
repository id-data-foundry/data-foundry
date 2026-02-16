package services.api;

import static java.util.Map.entry;

import java.util.Map;

public interface ApiServiceConstants {

	String DF_API_KEY_PREFIX = "df-";

	String LOCALAI_APIKEY = "LocalAI";
	String LOCALAI_MODEL_PREFIX = "local-";

	String OPENAI_APIKEY = "OpenAI";
	String OPENAI_BASE_URL = "https://api.openai.com/";
	String OPENAI_API_KEY_PREFIX = "sk-";

	String REQUEST_METHOD_POST = "POST";
	String REQUEST_METHOD_GET = "GET";

	String REQUEST_API_TOKEN = "api_token";
	String REQUEST_MODEL = "model";
	String REQUEST_TASK = "task";
	String REQUEST_PROMPT = "prompt";
	String REQUEST_MESSAGES = "messages";
	String REQUEST_MAX_TOKENS = "max_tokens";
	String REQUEST_TEMPERATURE = "temperature";
	String REQUEST_FREQUENCY_PENALTY = "frequency_penalty";
	String REQUEST_PRESENCE_PENALTY = "presence_penalty";
	String REQUEST_STREAM = "stream";

	String REQUEST_TASK_CREDITS = "credits";
	String REQUEST_TASK_COMPLETION = "completion";
	String REQUEST_TASK_CHAT_COMPLETION = "chat";
	String REQUEST_TASK_MODERATION = "moderation";
	String REQUEST_TASK_MODELS = "models";
	String REQUEST_TASK_EMBEDDING = "embedding";
	String REQUEST_TASK_IMAGE_GENERATION = "image_generation";
	String REQUEST_TASK_SPEECH_GENERATION = "speech_generation";

	// default LocalAI model
	String MODEL_LOCALAI_DEFAULT = "hermes-2-pro-llama-3-8b";

	// available OpenAI models
	String MODEL_GPT_4 = "gpt-4";
	String MODEL_GPT_4O_MINI = "gpt-4o-mini";
	String MODEL_GPT_3_5_TURBO = "gpt-3.5-turbo";
	String MODEL_ADA_DEFAULT = "ada";
	String MODEL_BABBAGE = "babbage";
	String MODEL_CURIE = "curie";
	String MODEL_DAVINCI = "davinci";

	// list of all available OpenAI models
	String[] OPENAI_MODELS = { MODEL_GPT_4O_MINI, MODEL_GPT_4, MODEL_GPT_3_5_TURBO, MODEL_BABBAGE, MODEL_CURIE,
	        MODEL_DAVINCI, MODEL_ADA_DEFAULT };

	// map user-defined model to actual OpenAI model
	Map<String, String> OPENAI_MODEL_FULLNAMES = Map.ofEntries( //
	        entry(MODEL_DAVINCI, "text-davinci-003"), //
	        entry(MODEL_CURIE, "babbage-002"), //
	        entry(MODEL_BABBAGE, "babbage-002"), //
	        entry(MODEL_ADA_DEFAULT, "babbage-002") //
	);

	// map user-defined model to actual OpenAI model
	Map<String, Integer> OPENAI_MODEL_COSTS = Map.ofEntries( //
	        entry(MODEL_GPT_4, 200), //
	        entry(MODEL_GPT_4O_MINI, 20), //
	        entry(MODEL_GPT_3_5_TURBO, 20), //
	        entry(MODEL_DAVINCI, 40), //
	        entry(MODEL_CURIE, 10), //
	        entry(MODEL_BABBAGE, 10), //
	        entry(MODEL_ADA_DEFAULT, 10) //
	);

	String TOKENS_USED = "tokensUsed";
	String TOKENS_MAX = "tokensMax";

	String RESPONSE_TEXT = "text";
	String RESPONSE_AUDIO = "audio";
	String RESPONSE_CONTENT = "content";
	String RESPONSE_ROLE = "role";
	String RESPONSE_FINISH_REASON = "finishReason";
	String RESPONSE_COST = "cost";
	String RESPONSE_ERROR = "error";
	String RESPONSE_MESSAGE = "message";

	int API_REQUEST_DEFAULT_TIMEOUT_MS = 2 * 60 * 1000;
	int API_REQUEST_JS_TIMEOUT_MS = 5 * 1000;

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	default int getModelCosts(String modelName) {
		return OPENAI_MODEL_COSTS.getOrDefault(modelName, -1);
	}

	default int getModelCostsFuzzy(String modelName) {
		// special check for gpt-4o
		if (modelName.contains(MODEL_GPT_4O_MINI)) {
			return getModelCosts(MODEL_GPT_4O_MINI);
		} else {
			return OPENAI_MODEL_COSTS.entrySet().stream().filter(e -> modelName.contains(e.getKey())).findFirst()
			        .map(e -> e.getValue()).orElse(-1);
		}
	}

	default String getModelFullName(String modelName) {
		return OPENAI_MODEL_FULLNAMES.getOrDefault(modelName, modelName);
	}

}

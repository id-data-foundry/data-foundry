package services.api;

public interface ApiServiceConstants {

	String DF_API_KEY_PREFIX = "df-";

	// local AI configuration
	String LOCALAI_APIKEY = "LocalAI";
	String LOCALAI_MODEL_PREFIX = "local-";
	String LOCALAI_MODEL_DEFAULT = "hermes-2-pro-llama-3-8b";

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
	String REQUEST_TASK_AUDIO_TRANSCRIPTION = "audio_transcription";

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

	public String X_API_MODEL = "X-API-Model";

}

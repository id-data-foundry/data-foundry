package services.api.remoting;

import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.node.ObjectNode;

import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http.MultipartFormData;
import services.api.ApiServiceConstants;
import services.api.requests.ApiRequest;

/**
 * Configures a remote API request that has three checks, first validity, then whether it is canceled (before or during
 * run) and finally whether it is completed. For this request type, the presence of a result (!= null) means that the
 * request is complete.
 * 
 * @param <T>
 */
public class RemoteApiRequest extends ApiRequest<String> implements ApiServiceConstants {

	String id = UUID.randomUUID().toString();
	private String state = "initialized";
	private boolean isCanceled = false;
	private Optional<String> result;
	private final int msTimeout;
	private long validUntil;

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// user API key, coming from the initial request; this is used to check whether the request can be sent directly to
	// OpenAI or whether it needs to be handled internally first
	private String userApiKey;

	// system API key that is set by the system and used to make the call to the API service
	private String internalApiKey;

	// apart from the model info, this is used to check which type of request we are talking about, either an OpenAI
	// request when an OpenAI model is clearly mentioned, or a LocaLAI request otherwise (or model has a 'local-'
	// prefix)
	private String model;

	// the task might be available in an unmanaged request
	private String task;

	// the number of requested tokens is important for OpenAI requests, but also for lcal AI requests to rate-limit them
	private int requestedTokens;

	// the API path to call, for the unmanaged side of the API services
	private String path = "";

	// the API method to call, for the unmanaged side of the API services: GET, or POST
	private String method = "";

	// the file that was posted to the action to create this request
	private MultipartFormData<TemporaryFile> multipartFormData;

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public RemoteApiRequest(String type, int msTimeout, String username, String userAuthorization, long projectId) {
		super(type, username, projectId, Json.newObject());
		this.msTimeout = msTimeout;
		setUserApiKey(userAuthorization);
		setMethod(REQUEST_METHOD_GET);
		configureRequest();
	}

	public RemoteApiRequest(String type, int msTimeout, String username, String userAuthorization, long projectId,
			String params) {
		super(type, username, projectId, params);
		this.msTimeout = msTimeout;
		setUserApiKey(userAuthorization);
		setMethod(REQUEST_METHOD_POST);
		configureRequest();
	}

	public RemoteApiRequest(String type, int msTimeout, String username, String userAuthorization, long projectId,
			ObjectNode params) {
		super(type, username, projectId, params);
		this.msTimeout = msTimeout;
		setUserApiKey(userAuthorization);
		setMethod(REQUEST_METHOD_POST);
		configureRequest();
	}

	@Override
	public void cancel() {
		isCanceled = true;
	}

	public boolean isCanceled() {
		return isCanceled;
	}

	@Override
	public boolean isValid() {
		return super.isValid();
	}

	public long getValidUntil() {
		return validUntil;
	}

	public void setValidUntil(long validUntil) {
		this.validUntil = validUntil;
	}

	/**
	 * check whether the API request is complete and results can be retrieved
	 * 
	 * @return
	 */
	@Override
	public boolean isCompleted() {
		return result != null;
	}

	/**
	 * request results, returns error message in case of timeout
	 * 
	 * @return
	 */
	@Override
	public String getResult() {
		return isCompleted() && result.isPresent() ? result.get() : errorMessage("API timeout").toString();
	}

	/**
	 * set the result, so it can be retrieved by a different thread
	 * 
	 * @param result
	 */
	public void setResult(Optional<String> result) {
		this.result = result;
	}

	/**
	 * is triggered when the supervisor timeout runs out; cancels and sets a result if none is set
	 * 
	 */
	public void timeout() {
		cancel();
		this.result = this.result == null ? Optional.of(errorMessage("API timeout").toString()) : this.result;
	}

	/**
	 * generate JSON error message
	 * 
	 * @param message
	 * @return
	 */
	public ObjectNode errorMessage(String message) {
		ObjectNode result = Json.newObject();
		result.putObject(RESPONSE_ERROR) //
				.put(RESPONSE_MESSAGE, message) //
				.put("type", "invalid_request_error") //
				.putNull("params") //
				.put("code", "");
		return result;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * configures the request by extracting information from a few fields in the request parameters; note that this
	 * method will NOT take any action in terms of responses based on missing or invalid data
	 * 
	 */
	private void configureRequest() {
		ObjectNode requestParams = getParams();

		// decide which type of request we have here: OpenAI, LocalAI or credits; for this we need the API token (either
		// from header as set in constructor, or as a JSON property) and the selected model, and the task in case
		// of a credit request
		final String apiToken = !getUserApiKey().isEmpty() ? getUserApiKey()
				: requestParams.path(REQUEST_API_TOKEN).asText("");
		final String modelName = requestParams.path(REQUEST_MODEL).asText("");
		final String task = requestParams.path(REQUEST_TASK).asText("");

		// we can set the user-provided API token already here for all requests
		setUserApiKey(apiToken);
		// same for the task
		this.task = task;

		// ------------------------------------------------------------------------------------------------------------
		// OpenAI requests are indicated by...
		// (1) an OpenAI API key
		if (apiToken.startsWith(OPENAI_API_KEY_PREFIX)) {
			// if someone uses their own API key, bypass all db stuff, assign model and go
			setInternalApiKey(apiToken);
			this.model = modelName;
			this.requestedTokens = 0;
		}
		// (2) a specific OpenAI model name
		else if (getModelCostsFuzzy(modelName) > 0) {
			setInternalApiKey(OPENAI_APIKEY);
			this.model = getModelFullName(modelName);
			if (modelName.contains("-")) {
				this.requestedTokens = getModelCostsFuzzy(modelName);
			} else {
				this.requestedTokens = getModelCosts(modelName);
			}
		}
		// ------------------------------------------------------------------------------------------------------------
		// Or is this a credit request perhaps?
		else if (task.equals(REQUEST_TASK_CREDITS)) {
			setInternalApiKey(LOCALAI_APIKEY);
			this.model = "";
			this.requestedTokens = 0;
		}
		// ------------------------------------------------------------------------------------------------------------
		// Or is the model empty? --> map to default LocalAI model
		else if (modelName.isEmpty()) {
			setInternalApiKey(LOCALAI_APIKEY);
			this.model = MODEL_LOCALAI_DEFAULT;
			this.requestedTokens = 1;
		}
		// ------------------------------------------------------------------------------------------------------------
		// Or last option: a LocalAI request?
		else {
			setInternalApiKey(LOCALAI_APIKEY);
			// explicitly local model specified, just replace "local-", go with LocalAI and 1 requested token
			if (modelName.contains(LOCALAI_MODEL_PREFIX) && modelName.length() > LOCALAI_MODEL_PREFIX.length()) {
				this.model = modelName.replaceFirst(LOCALAI_MODEL_PREFIX, "");
			}
			// if there is a concrete model specified
			else {
				this.model = modelName;
			}
			this.requestedTokens = 1;
		}
	}

	public String getUserApiKey() {
		return userApiKey != null ? userApiKey : "";
	}

	public void setUserApiKey(String apiKey) {
		this.userApiKey = apiKey;
	}

	public String getInternalApiKey() {
		return internalApiKey;
	}

	public void setInternalApiKey(String internalApiKey) {
		this.internalApiKey = internalApiKey;
	}

	public String getTask() {
		return task != null ? task : "";
	}

	/**
	 * check whether this request is a credit request, which can be handled very fast internally
	 * 
	 * @return
	 */
	public boolean isCreditRequest() {
		return getTask().equals(REQUEST_TASK_CREDITS) || getType().equals(REQUEST_TASK_CREDITS);
	}

	/**
	 * check whether this request is a models request, which can be handled very fast internally
	 * 
	 * @return
	 */
	public boolean isModelsRequest() {
		return getTask().equals(REQUEST_TASK_MODELS) || getType().equals(REQUEST_TASK_MODELS);
	}

	/**
	 * check whether the api token has an OpenAI prefix
	 * 
	 * @return
	 */
	public boolean isDirectOpenAIApiRequest() {
		return getUserApiKey().startsWith(OPENAI_API_KEY_PREFIX);
	}

	public int getRequestedTokens() {
		return requestedTokens;
	}

	public String getModel() {
		return model;
	}

	public void setModel(String model) {
		this.model = model;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public void setMultipartFormData(MultipartFormData<TemporaryFile> mpfd) {
		this.multipartFormData = mpfd;
	}

	public MultipartFormData<TemporaryFile> getMultipartFormData() {
		return this.multipartFormData;
	}

	public int getMsTimeout() {
		return msTimeout;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

}

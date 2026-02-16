package services.api.ai;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.theokanning.openai.OpenAiApi;
import com.theokanning.openai.service.OpenAiService;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import okhttp3.ConnectionPool;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import play.Logger;
import retrofit2.Retrofit;
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory;
import retrofit2.converter.jackson.JacksonConverterFactory;
import services.api.GenericApiService;
import services.api.remoting.RemoteApiRequest;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;
import utils.conf.ConfigurationUtils;

public class AbstractAIApiService extends GenericApiService {

	protected final String openAIAPIKey;
	protected final String localAIHost;
	protected final LocalModelMetadata localModelMetadata;

	private static final Logger.ALogger logger = Logger.of(UnmanagedAIApiService.class);

	protected AbstractAIApiService(Config configuration, AdminUtils adminUtils, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolver, LocalModelMetadata lmmd) {
		super(configuration, adminUtils, datasetConnector, tokenResolver);

		// retrieve the Open AI API key from configuration
		if (configuration.hasPath(ConfigurationUtils.DF_VENDOR_OPENAI)) {
			openAIAPIKey = configuration.getString(ConfigurationUtils.DF_VENDOR_OPENAI);
		} else {
			openAIAPIKey = "";
		}

		// retrieve Local AI host from configuration
		final String tempLocalAIHost;
		if (configuration.hasPath(ConfigurationUtils.DF_LOCALAI_HOST)) {
			tempLocalAIHost = configuration.getString(ConfigurationUtils.DF_LOCALAI_HOST);
		} else {
			tempLocalAIHost = "";
		}

		if (tempLocalAIHost.isEmpty()) {
			// empty, then stop
			localAIHost = tempLocalAIHost;
		} else if (!tempLocalAIHost.startsWith("http")) {
			// add protocol, if not given; default is HTTPS
			localAIHost = "https://" + tempLocalAIHost;
		} else {
			// take over the configured value; this should be the usual case
			localAIHost = tempLocalAIHost;
		}

		localModelMetadata = lmmd;
	}

	/**
	 * create service for the API call; this method is API key aware, that is, it will check whether the request
	 * contains an OpenAI key or a local AI "key" and dispatch the API request accordingly to the right service
	 * 
	 * @param apiKey
	 * @param timeoutMS
	 * @return
	 */
	protected OpenAiApi createService(String apiKey, int timeoutMS) {
		ObjectMapper mapper = OpenAiService.defaultObjectMapper();
		if (apiKey.equals(LOCALAI_APIKEY)) {
			OkHttpClient client = new OkHttpClient.Builder().connectionPool(new ConnectionPool(2, 30, TimeUnit.SECONDS))
			        .readTimeout(timeoutMS, TimeUnit.MILLISECONDS).build();
			Retrofit retrofit = new Retrofit.Builder().baseUrl(localAIHost + "/").client(client)
			        .addConverterFactory(JacksonConverterFactory.create(mapper))
			        .addCallAdapterFactory(RxJava2CallAdapterFactory.create()).build();
			return retrofit.create(OpenAiApi.class);
		} else {
			OkHttpClient client = new OkHttpClient.Builder().addInterceptor(new AuthenticationInterceptor(apiKey))
			        .connectionPool(new ConnectionPool(2, 30, TimeUnit.SECONDS))
			        .readTimeout(timeoutMS, TimeUnit.MILLISECONDS).build();
			Retrofit retrofit = new Retrofit.Builder().baseUrl(OPENAI_BASE_URL).client(client)
			        .addConverterFactory(JacksonConverterFactory.create(mapper))
			        .addCallAdapterFactory(RxJava2CallAdapterFactory.create()).build();
			return retrofit.create(OpenAiApi.class);
		}
	}

	/**
	 * parse the request JSON and check for important properties; this method has side-effects
	 * 
	 * @param request
	 */
	protected void preProcessRequest(RemoteApiRequest request) {
		ObjectNode json = request.getParams();

		// check and map requested model
		if (json.has("model")) {
			String requestedModel = json.get("model").asText("");
			if (!requestedModel.isEmpty()) {
				// map the model and replace it in the request json
				String mappedModelId = this.localModelMetadata.mapModelId(requestedModel);
				json.put("model", mappedModelId);

				// log if a mapping took place
				if (!requestedModel.equals(mappedModelId)) {
					logger.info("Model mapped: " + requestedModel + " -> " + mappedModelId);
				}
			}
		}

		// also set the max_tokens to default if not set
		if (!json.has("max_tokens")) {
			json.put("max_tokens", 500);
		}
	}

	/**
	 * patched from Open AI Api library
	 *
	 */
	public class AuthenticationInterceptor implements Interceptor {
		private final String token;

		AuthenticationInterceptor(String token) {
			this.token = token;
		}

		@Override
		public Response intercept(Chain chain) throws IOException {
			Request request = chain.request().newBuilder().header("Authorization", "Bearer " + token).build();
			return chain.proceed(request);
		}
	}

}

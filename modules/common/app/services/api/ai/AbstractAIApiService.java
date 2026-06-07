package services.api.ai;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import play.Logger;
import services.api.GenericApiService;
import services.api.remoting.RemoteApiRequest;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;
import utils.conf.ConfigurationUtils;

public class AbstractAIApiService extends GenericApiService {

	protected final String localAIAPIKey;
	protected final String aiBaseUrl;

	protected final LocalModelMetadata localModelMetadata;

	private static final Logger.ALogger logger = Logger.of(UnmanagedAIApiService.class);

	protected AbstractAIApiService(Config configuration, AdminUtils adminUtils, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolver, LocalModelMetadata lmmd) {
		super(configuration, adminUtils, datasetConnector, tokenResolver);

		// retrieve DF AI host from configuration
		String tempAIBaseUrl;
		if (configuration.hasPath(ConfigurationUtils.DF_AI_BASEURL)) {
			tempAIBaseUrl = configuration.getString(ConfigurationUtils.DF_AI_BASEURL);
		} else {
			tempAIBaseUrl = "";
		}

		// retrieve the DF AI API key from configuration
		if (configuration.hasPath(ConfigurationUtils.DF_AI_API_KEY)) {
			localAIAPIKey = configuration.getString(ConfigurationUtils.DF_AI_API_KEY);
		} else {
			localAIAPIKey = "";
		}

		if (tempAIBaseUrl.isEmpty()) {
			// empty, then stop
			aiBaseUrl = tempAIBaseUrl;
		} else {
			if (!tempAIBaseUrl.startsWith("http")) {
				// add protocol, if not given; default is HTTPS
				tempAIBaseUrl = "https://" + tempAIBaseUrl;
			}

			// remove trailing slash, if given
			if (tempAIBaseUrl.endsWith("/")) {
				tempAIBaseUrl = tempAIBaseUrl.substring(0, tempAIBaseUrl.length() - 1);
			}

			aiBaseUrl = tempAIBaseUrl;
		}

		localModelMetadata = lmmd;
	}

	/**
	 * parse the request JSON and check for important properties; this method has side-effects
	 * 
	 * @param request
	 */
	protected void preProcessRequest(RemoteApiRequest request) {
		ObjectNode json = request.getParams();

		// check and map requested model
		if (json.has(REQUEST_MODEL)) {
			String requestedModel = json.get(REQUEST_MODEL).asText("");
			if (!requestedModel.isEmpty()) {
				// map the model and replace it in the request json
				String mappedModelId = this.localModelMetadata.mapModelId(requestedModel);
				json.put(REQUEST_MODEL, mappedModelId);

				// log if a mapping took place
				if (!requestedModel.equals(mappedModelId)) {
					logger.info("Model mapped: " + requestedModel + " -> " + mappedModelId);
				}
			}
		}

		// also set the max_tokens to default if not set
		if (!json.has(REQUEST_MAX_TOKENS)) {
			json.put(REQUEST_MAX_TOKENS, 500);
		}
	}

}

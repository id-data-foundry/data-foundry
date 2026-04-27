package controllers.tools;

import com.typesafe.config.Config;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import jakarta.inject.Inject;
import play.filters.csrf.AddCSRFToken;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.api.ai.UnmanagedAIApiService;
import utils.conf.ConfigurationUtils;

@Authenticated(UserAuth.class)
public class TranslateController extends AbstractAsyncController {

	private final UnmanagedAIApiService aiService;
	private final String AI_SERVICE_BASE_URL;

	@Inject
	public TranslateController(Config config, UnmanagedAIApiService aiService) {
		this.aiService = aiService;

		// base URL for AI services
		if (ConfigurationUtils.checkConfiguration(config, ConfigurationUtils.DF_BASEURL)) {
			AI_SERVICE_BASE_URL = config.getString(ConfigurationUtils.DF_BASEURL);
		} else {
			AI_SERVICE_BASE_URL = "";
		}
	}

	@AddCSRFToken
	public Result index(Request request) {
		String documentationAPIKey = aiService.getInternalDocumentationAPIKey();
		return ok(
				views.html.tools.translate.index.render(AI_SERVICE_BASE_URL, documentationAPIKey, csrfToken(request)));
	}
}

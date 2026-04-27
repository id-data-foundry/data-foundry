package controllers.tools;

import com.typesafe.config.Config;
import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import jakarta.inject.Inject;
import models.Person;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.api.ai.UnmanagedAIApiService;
import utils.conf.ConfigurationUtils;
import play.filters.csrf.AddCSRFToken;

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

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result index(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(HOME));
		
		String documentationAPIKey = aiService.getInternalDocumentationAPIKey();
		return ok(views.html.tools.translate.index.render(AI_SERVICE_BASE_URL, documentationAPIKey, csrfToken(request)));
	}
}

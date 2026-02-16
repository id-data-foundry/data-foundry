package controllers.api2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.auth.UserAuth;
import controllers.auth.V2UserApiAuth;
import controllers.swagger.AbstractApiController;
import datasets.DatasetConnector;
import models.Person;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.auth.TokenResolverUtil;

public class AuthApiController extends AbstractApiController {

	@Inject
	public AuthApiController(FormFactory formFactory, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolverUtil) {
		super(formFactory, datasetConnector, tokenResolverUtil);
	}

	@AddCSRFToken
	public Result authenticate(Request request, String service) {
		return ok(views.html.tools.api.login.render(csrfToken(request), "", service));
	}

	@RequireCSRFCheck
	public Result authenticateMe(Request request, String service) {

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest("Expecting some data");
		}

		String password = df.get("password");
		String username = df.get("username");

		// try to find user and check password
		Person user = Person.findByEmail(username).get();
		if (user == null || !user.checkPassword(password)) {
			return redirect(controllers.routes.HomeController.login(username, "")).addingToSession(request, "error",
			        "Could not find you or your password does not match.");
		}

		user.touch();

		// retrieve token validity
		Long time = tokenResolverUtil.retrieveTimeoutFromUserAccessToken(user.getAccesscode());
		String validity = new SimpleDateFormat("MMM d, yyyy").format(new Date(time));

		return ok(views.html.tools.api.token.render(user.getAccesscode(), validity, service)).addingToSession(request,
		        "username", user.getEmail());
	}

	@Authenticated(UserAuth.class)
	public Result generate(Request request, String service) {
		// check person
		Optional<Person> userOpt = getAuthenticatedUser(request);
		if (!userOpt.isPresent()) {
			return notFound();
		}

		// generate token for one day
		Person user = userOpt.get();
		user.setAccesscode(tokenResolverUtil.createUserAccessToken(user.getId(),
		        System.currentTimeMillis() + V2UserApiAuth.API_TOKEN_VALIDITY));
		user.update();

		// create response
		ObjectNode response = okJSONResponseObject();
		response.put("token", user.getAccesscode());
		response.put("service", service);

		// kill session by replacing it with a new one
		return ok(response).withNewSession();
	}
}
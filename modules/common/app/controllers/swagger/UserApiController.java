package controllers.swagger;

import java.util.Optional;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.auth.V2MultiUserApiAuth;
import controllers.auth.V2UserApiAuth;
import datasets.DatasetConnector;
import jakarta.inject.Singleton;
import models.Person;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.data.validation.Constraints.EmailValidator;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.auth.TokenResolverUtil;

@Singleton
public class UserApiController extends AbstractApiController {

	@Inject
	public UserApiController(FormFactory formFactory, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolverUtil) {
		super(formFactory, datasetConnector, tokenResolverUtil);
	}

	@Authenticated(V2MultiUserApiAuth.class)
	public Result registerMe(Request request) {

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("Expecting some data."));
		}

		// try to find existing
		String email = nss(df.get("email"));
		if (Person.findByEmailCount(email) > 0) {
			return notAcceptable(errorJSONResponseObject(
					"We have found you already in the system. Contact support for a password reset."));
		}

		if (!new EmailValidator().isValid(email)) {
			return notAcceptable(errorJSONResponseObject("The email seems not correct, please check."));
		}

		// check password
		String password = nss(df.get("password"));
		if (password.length() < 8) {
			return badRequest(errorJSONResponseObject("Password is too short. We need at least 8 characters."));
		}
		if (!password.equals(df.get("re_password"))) {
			return badRequest(errorJSONResponseObject("Password does not match."));
		}

		// check essential fields
		String firstName = nss(df.get("first_name"));
		String lastName = nss(df.get("last_name"));
		if (firstName.length() < 3 || lastName.length() < 3) {
			return badRequest(
					errorJSONResponseObject("First or last name is too short. We need at least 3 characters."));
		}

		String access_code = df.get("access_code");
		if (access_code == null || !tokenResolverUtil.checkRegistrationAccessKey(access_code)) {
			return forbidden(errorJSONResponseObject("Wrong access code"));
		}

		// register user
		String website = df.get("website");
		Person user = Person.register(df.get("user_id"), firstName, lastName, email, website, password, access_code);
		user.save();
		// set accesscode
		user.setAccesscode(tokenResolverUtil.createUserAccessToken(user.getId(),
				System.currentTimeMillis() + V2UserApiAuth.API_TOKEN_VALIDITY));
		user.update();

		// create default container project
		// createPublicProjectForUser(user, website);
		// createPrivateProjectForUser(user, website);

		ObjectNode on = okJSONResponseObject();
		on.put("id", user.getId());
		return ok(on);
	}

	@Authenticated(V2MultiUserApiAuth.class)
	public Result loginUser(Request request) {

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("Expecting some data."));
		}

		String password = df.get("password");
		String username = df.get("username");

		// try to find user
		Optional<Person> userOpt = Person.findByEmail(username);
		if (!userOpt.isPresent()) {
			return forbidden(errorJSONResponseObject("Could not find you."));
		}

		// check password
		Person user = userOpt.get();
		if (!user.checkPassword(password)) {
			return forbidden(errorJSONResponseObject("Could not find you or your password does not match."));
		}
		user.touch();

		ObjectNode okJSONResponseObject = okJSONResponseObject();
		okJSONResponseObject.put("id", user.getId());
		okJSONResponseObject.put("api-token", user.getAccesscode());
		okJSONResponseObject.put("first_name", user.getFirstname());
		okJSONResponseObject.put("last_name", user.getLastname());
		okJSONResponseObject.put("email", user.getEmail());

		// ok and set the session
		return ok(okJSONResponseObject).addingToSession(request, "username", user.getEmail());
	}

	@Authenticated(V2MultiUserApiAuth.class)
	public Result logout() {
		// clear session, including the username field
		// redirect home with new session
		return ok(okJSONResponseObject("See you next time.")).withNewSession();
	}

	@Authenticated(V2UserApiAuth.class)
	public Result editUser(Request request) {

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return forbidden(errorJSONResponseObject("No user account given or user is not logged in."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("Expecting some data."));
		}

		// fill in provided information
		boolean dirty = false;
		if (df.get("first_name") != null) {
			user.setFirstname(df.get("first_name"));
			dirty = true;
		}
		if (df.get("last_name") != null) {
			user.setLastname(df.get("last_name"));
			dirty = true;
		}
		// email is not editable as it is the username
		// if (df.get("email") != null) {
		// user.email = df.get("email");
		// dirty = true;
		// }
		if (df.get("password") != null) {
			user.setPassword(df.get("password"));
			dirty = true;
		}
		if (df.get("website") != null) {
			user.setWebsite(df.get("website"));
			dirty = true;
		}

		// update user if changes have been made
		if (dirty) {
			user.update();
		}

		return ok(okJSONResponseObject("Information updated."));
	}

}
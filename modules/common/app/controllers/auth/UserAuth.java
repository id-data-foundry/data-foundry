package controllers.auth;

import java.util.Optional;

import javax.inject.Inject;

import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.play.PlayWebContext;

import controllers.AbstractAsyncController;
import models.Person;
import play.mvc.Controller;
import play.mvc.Http.Request;
import play.mvc.Result;

/**
 * Main user authentication for any researcher using the DF
 * 
 * @author mathias
 *
 */
public class UserAuth extends play.mvc.Security.Authenticator {

	@Inject
	private SessionStore sessionStore;

	@Override
	public Optional<String> getUsername(Request request) {

		// check referrer for CSRF, note "referer" is correct!
		Optional<String> ref = request.header(Controller.REFERER);
		if (ref.isPresent() && ref.get().contains("/web/")) {
			// do not authenticate if the referrer is a hosted web page
			// why? because this could be used for an attack via JavaScript
			return Optional.empty();
		}

		final PlayWebContext context = new PlayWebContext(request);
		final ProfileManager profileManager = new ProfileManager(context, sessionStore);
		Optional<UserProfile> proOpt = profileManager.getProfile();
		if (!proOpt.isPresent()) {
			return Optional.empty();
		}

		UserProfile userProfile = proOpt.get();

		// ensure that the username is present in profile
		// this is needed for AzureAD2 logins
		if (!userProfile.containsAttribute(Person.USER_NAME)) {
			if (userProfile instanceof CommonProfile) {
				CommonProfile cp = (CommonProfile) userProfile;
				cp.addAttribute(Person.USER_NAME, cp.getEmail().toLowerCase());
			}
		}

		// retrieve username and go
		String username = userProfile.getAttribute(Person.USER_NAME).toString();
		return Optional.ofNullable(username);
	}

	@Override
	public Result onUnauthorized(Request request) {

		// no redirect on htmx requests
		if (request.header("HX-Request").isPresent()) {
			return noContent();
		}

		// POST requests that are unauthorized should receive a proper 4xx response
		if (request.method().equalsIgnoreCase("POST")) {
			return forbidden();
		}

		// in case we have a deep link, let's make it a redirect to be followed after login
		String requestPath = request.path();
		if (requestPath.replaceAll("[^/]", "").length() > 1) {
			if (!requestPath.contains("download") && !requestPath.contains("/components/")) {
				return redirect(controllers.routes.HomeController.login("", ""))
				        .addingToSession(request, "error",
				                "Please log in first, we will take you to your destination then.")
				        .addingToSession(request, AbstractAsyncController.REDIRECT_URL, requestPath);
			}

			return redirect(controllers.routes.HomeController.login("", "")).addingToSession(request, "error",
			        "Please log in first, we will take you to your destination then.");
		} else {
			return redirect("/").addingToSession(request, "error", "Not authorized, sorry.");
		}
	}
}

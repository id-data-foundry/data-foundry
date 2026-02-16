package controllers;

import java.io.File;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.play.PlayWebContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;

import controllers.auth.V2UserApiAuth;
import models.Dataset;
import models.Person;
import models.Project;
import play.Logger;
import play.data.DynamicForm;
import play.filters.csrf.CSRF;
import play.mvc.Call;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Http.Request;
import play.mvc.Result;
import services.maintenance.RealTimeNotificationService;
import utils.DateUtils;
import utils.StringUtils;
import utils.auth.TokenResolverUtil;

public class AbstractAsyncController extends Controller {

	protected static final Call LANDING = controllers.routes.HomeController.index();

	protected static final Call HOME = controllers.routes.ProjectsController.index();

	protected static final Call PROJECT(long id) {
		return controllers.routes.ProjectsController.view(id);
	}

	protected static final Call DS(long id) {
		return controllers.routes.DatasetsController.view(id);
	}

	public static final String REDIRECT_URL = "redirectUrl";
	protected static final String CALLBACK_SSO = "callbackSSO";

	@Inject
	private SessionStore sessionStore;

	@Inject
	private TokenResolverUtil tokenResolverUtil;

	@Inject
	RealTimeNotificationService realtimeNotifications;

	private static final Logger.ALogger logger = Logger.of(AbstractAsyncController.class);

	/**
	 * retrieve session item with given key, never null
	 * 
	 * @param request
	 * @param key
	 * @return
	 */
	public String session(Request request, String key) {

		// temporary solution: reroute the user name request to new security system
		if (key.equals("username")) {
			final PlayWebContext context = new PlayWebContext(request);
			final ProfileManager profileManager = new ProfileManager(context, sessionStore);
			Optional<UserProfile> proOpt = profileManager.getProfile();
			if (proOpt.isPresent()) {
				return nss(proOpt.get().getAttribute(Person.USER_NAME).toString());
			}
		}

		// not resolved yet? answer in traditional way
		return nss(request.session().get(key).orElse(""));
	}

	/**
	 * retrieve the username (email) of the logged-in user
	 * 
	 * @param request
	 * @return
	 */
	public Optional<String> getAuthenticatedUserName(Request request) {
		final PlayWebContext context = new PlayWebContext(request);
		final ProfileManager profileManager = new ProfileManager(context, sessionStore);
		Optional<UserProfile> proOpt = profileManager.getProfile();
		if (proOpt.isPresent()) {
			Object attributeObj = proOpt.get().getAttribute(Person.USER_NAME);
			if (attributeObj == null) {
				return Optional.empty();
			}

			String username = attributeObj.toString();
			return Optional.of(username);
		}

		return Optional.empty();
	}

	/**
	 * retrieve the logged-in user
	 * 
	 * @param request
	 * @return
	 */
	public Optional<Person> getAuthenticatedUser(Request request) {
		final PlayWebContext context = new PlayWebContext(request);
		final ProfileManager profileManager = new ProfileManager(context, sessionStore);
		Optional<UserProfile> proOpt = profileManager.getProfile();
		if (proOpt.isPresent()) {
			UserProfile profile = proOpt.get();
			if (profile instanceof CommonProfile) {
				CommonProfile cp = (CommonProfile) profile;

				// ensure that the username is present in profile
				// this is needed for AzureAD2 logins
				if (!profile.containsAttribute(Person.USER_NAME)) {
					logger.info("User without username in profile, adding that...");
					cp.addAttribute(Person.USER_NAME, cp.getEmail().toLowerCase());
				}

				String email = profile.getAttribute(Person.USER_NAME).toString();

				// we can check whether the user is initialized or not by checking whether the user id is recorded in
				// the profile
				if (!profile.containsAttribute(Person.USER_ID)) {
					String firstName = cp.getFirstName();
					String lastName = cp.getFamilyName();
					String website = "";

					Optional<Person> userOpt = Person.findByEmail(email);
					if (userOpt.isPresent()) {
						Person user = userOpt.get();

						// user exists, store user id in profile, then return user
						profile.addAttribute(Person.USER_ID, user.getId());
						logger.info("User logged in: " + email);

						// update first name if needed
						if (!user.getFirstname().equals(firstName)) {
							user.setFirstname(firstName);
							user.update();
							logger.info("User first name updated: " + firstName);
						}

						// update last name if needed
						if (!user.getLastname().equals(lastName)) {
							user.setLastname(lastName);
							user.update();
							logger.info("User last name updated: " + lastName);
						}

						realtimeNotifications.notifyUser(userOpt.get().getInitials());
						return userOpt;
					}

					// if the user is not existing, create the user in the database
					// register user
					Person user = Person.register(email, firstName, lastName, email, website, "", "");
					user.save();

					// set accesscode
					user.setAccesscode(tokenResolverUtil.createUserAccessToken(user.getId(),
					        System.currentTimeMillis() + V2UserApiAuth.API_TOKEN_VALIDITY));
					user.update();

					// create default project for user
					Project p = Project.create(firstName + "'s 1st project", user,
					        "This is your first project, it's private and not shareable, please edit information with proper content.",
					        false, false);
					p.save();

					// refresh to get the user id
					profile.addAttribute(Person.USER_ID, user.getId());
					logger.info("New user registered: " + email);
					realtimeNotifications.notifyUser(user.getInitials());
					return Optional.of(user);
				}

				// user id is set, use that because the query will be faster
				if (profile.containsAttribute(Person.USER_ID)) {
					Long profileUserId = (Long) profile.getAttribute(Person.USER_ID);
					Person user = Person.find.byId(profileUserId);
					if (user != null) {
						realtimeNotifications.notifyUser(user.getInitials());
					}
					return Optional.of(user);
				}

				// else use email to find the user
				Optional<Person> userOpt = Person.findByEmail(email);
				if (userOpt.isPresent()) {
					realtimeNotifications.notifyUser(userOpt.get().getInitials());
				}
				return userOpt;
			}
		}

		return Optional.empty();
	}

	/**
	 * retrieve the username of the logged-in user or (if not found) throw an exception to redirect
	 * 
	 * @param request
	 * @param result
	 * @return
	 */
	public String getAuthenticatedUserNameOrReturn(Request request, Result result) {
		Optional<String> usernameOpt = getAuthenticatedUserName(request);
		if (usernameOpt.isPresent()) {
			return usernameOpt.get();
		}

		// do a full refresh for HTMX requests
		Result rrd = request.header("HX-Request").isPresent() ? result.withHeader("HX-Refresh", "true") : result;
		throw new ResponseException(
		        rrd.addingToSession(request, "error", "Please log in first, we will take you to your destination then.")
		                .addingToSession(request, AbstractAsyncController.REDIRECT_URL, request.path()));
	}

	/**
	 * retrieve the logged-in user or (if not found) throw an exception to redirect
	 * 
	 * @param request
	 * @param result
	 * @return
	 */
	public Person getAuthenticatedUserOrReturn(Request request, Result result) {
		Optional<Person> personOpt = getAuthenticatedUser(request);
		if (personOpt.isPresent()) {
			return personOpt.get();
		}

		// do a full refresh for HTMX requests
		Result rrd = request.header("HX-Request").isPresent() ? result.withHeader("HX-Refresh", "true") : result;
		throw new ResponseException(
		        rrd.addingToSession(request, "error", "Please log in first, we will take you to your destination then.")
		                .addingToSession(request, AbstractAsyncController.REDIRECT_URL, request.path()));
	}

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * returns a completionStage with redirect
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> cs(Supplier<Result> rs) {
		return CompletableFuture.supplyAsync(rs).exceptionally(e -> {
			// handle redirect exceptions
			if (e instanceof ResponseException) {
				return ((ResponseException) e).getResponse();
			}
			return noContent();
		});
	}

	/**
	 * returns a completionStage with redirect
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> redirectCS(Call destination) {
		return CompletableFuture.completedFuture(redirect(destination));
	}

	/**
	 * returns a completionStage with ok, for file downloads
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> okCS(File data) {
		// response().setHeader("Content-disposition", "attachment; filename=" + fileName);
		return CompletableFuture.completedFuture(ok(data).as("application/x-download"));
	}

	/**
	 * returns a completionStage with badRequest
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> badRequestCS(String data) {
		return CompletableFuture.completedFuture(badRequest(data));
	}

	/**
	 * returns a completionStage with badRequest
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> badRequestCS(JsonNode data) {
		return CompletableFuture.completedFuture(badRequest(data));
	}

	/**
	 * returns a completionStage with forbidden
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> forbiddenCS(String data) {
		return CompletableFuture.completedFuture(forbidden(data).as(Http.MimeTypes.CSS));
	}

	/**
	 * returns a completionStage with forbidden
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> forbiddenCS(JsonNode data) {
		return CompletableFuture.completedFuture(forbidden(data));
	}

	/**
	 * returns a completionStage with forbidden with CSV mime type
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> notFoundCS(String data) {
		return CompletableFuture.completedFuture(notFound(data).as("text/csv"));
	}

	/**
	 * returns a completionStage with forbidden
	 * 
	 * @return
	 */
	protected CompletableFuture<Result> notFoundCS(JsonNode data) {
		return CompletableFuture.completedFuture(notFound(data));
	}

	protected Source<ByteString, SourceQueueWithComplete<ByteString>> createStream() {
		return Source.<ByteString>queue(1024, OverflowStrategy.backpressure());
	}

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * create a CSRF token and return it as a String
	 * 
	 * @return
	 */
	protected String csrfToken(Request request) {
		return CSRF.getToken(request).get().value();
	}

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * null safe JsonNode --> String
	 */
	protected final String nss(JsonNode s) {
		return s != null ? StringUtils.nss(s.asText("")) : "";
	}

	/**
	 * null safe string
	 */
	protected final String nss(String s) {
		return StringUtils.nss(s);
	}

	/**
	 * null safe string with max length
	 */
	protected final String nss(String s, int maxLength) {
		return StringUtils.nss(s, maxLength);
	}

	/**
	 * escape HTML meta characters in String
	 * 
	 * @param s
	 * @return
	 */
	protected final String htmlEscape(String s) {
		return StringUtils.htmlEscape(s);
	}

	/**
	 * escape _some_ HTML meta characters in String
	 * 
	 * @param s
	 * @return
	 */
	protected final String htmlTagEscape(String s) {
		return s.replaceAll("[<>\":]", "");
	}

	/**
	 * valid text = Not Null and Not Empty
	 */
	protected static boolean nnne(String text) {
		return StringUtils.nnne(text);
	}

	/**
	 * format / escape string for CSV output
	 */
	protected final String cf(String s) {
		return StringUtils.cf(s);
	}

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * extract dates from dynamic form and stores them after processing in dataset configuration
	 * 
	 * @param ds
	 * @param df
	 */
	protected void storeDates(Dataset ds, DynamicForm df) {
		Date[] dates = DateUtils.getDates(df.get("start-date"), df.get("end-date"), ds);
		ds.setStart(dates[0]);
		ds.setEnd(dates[1]);
	}

	/**
	 * extracts metadata from dynamic form and stores it in dataset configuration
	 * 
	 * @param ds
	 * @param df
	 */
	protected void storeMetadata(Dataset ds, DynamicForm df) {
		ds.setKeywords(nss(df.get("keywords")));
		ds.setDoi(nss(df.get("doi")));
		ds.setRelation(nss(df.get("relation")));
		ds.setOrganization(nss(df.get("organization")));
		ds.setRemarks(nss(df.get("remarks")));
	}

	// ----------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	public static class ResponseException extends RuntimeException {
		Result response;

		public ResponseException(Result result) {
			this.response = result;
		}

		public Result getResponse() {
			return response;
		}
	}

}

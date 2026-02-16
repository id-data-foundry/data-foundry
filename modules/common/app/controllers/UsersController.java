package controllers;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import controllers.auth.UserAuth;
import controllers.auth.V2UserApiAuth;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.sr.TelegramSession;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.data.validation.Constraints.EmailValidator;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import play.twirl.api.Html;
import services.email.NotificationService;
import services.search.SearchService;
import utils.DateUtils;
import utils.auth.Roles;
import utils.auth.TokenResolverUtil;
import utils.components.OnboardingSupport;
import utils.conf.ConfigurationUtils;
import utils.telegrambot.TelegramBotUtils;

/**
 * This controller contains an action to handle HTTP requests to the application's home page.
 */
public class UsersController extends AbstractAsyncController {

	private final NotificationService notificationService;
	private final SearchService searchService;
	private final FormFactory formFactory;
	private final SyncCacheApi cache;
	private final TokenResolverUtil tokenResolverUtil;
	private final OnboardingSupport onboardingSupport;
	private final boolean ssoEnabled;

	private static final Logger.ALogger logger = Logger.of(UsersController.class);

	@Inject
	public UsersController(Config configuration, NotificationService notificationService, SearchService searchService,
	        FormFactory formFactory, SyncCacheApi cache, TokenResolverUtil tokenResolverUtil,
	        OnboardingSupport onboardingSupport) {
		this.notificationService = notificationService;
		this.searchService = searchService;
		this.formFactory = formFactory;
		this.cache = cache;
		this.tokenResolverUtil = tokenResolverUtil;
		this.onboardingSupport = onboardingSupport;

		// only check SSO configuration once
		ssoEnabled = ConfigurationUtils.isSSO(configuration);
	}

	/**
	 * public user page. shows only projects visible to logged-in user or public ones
	 * 
	 * @param id
	 * @return
	 */
	public Result view(Request request, Long id) {
		// check session first
		Person visitor = getAuthenticatedUserOrReturn(request, redirect(controllers.routes.HomeController.login("", ""))
		        .addingToSession(request, AbstractAsyncController.REDIRECT_URL, request.path()));

		// find user in question
		Person user = Person.find.byId(id);
		if (user == null) {
			return redirect(LANDING).addingToSession(request, "error", "User not found.");
		}

		// list project that are public or at least visible for the user
		List<Project> projects = user.projects().stream().filter(p -> p.visibleFor(visitor))
		        .collect(Collectors.toList());

		return ok(views.html.users.index.render(user, projects));
	}

	public Result search(Request request, String query) {

		// check user session and that the search query has some content
		getAuthenticatedUserNameOrReturn(request, noContent());

		if (query.length() < 2) {
			return ok("search query is too short");
		}

		List<Person> users = Person.find.query().where().or() //
		        .icontains("firstname", query) //
		        .icontains("lastname", query) //
		        .endOr().findList();
		if (users.isEmpty()) {
			return ok("no users found");
		}

		return ok(views.html.elements.user.searchList.render(users));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Result invite(Request request) {
		// disable the registration if SSO is configured
		if (ssoEnabled) {
			return redirect(LANDING).addingToSession(request, "error",
			        "No registration possible, use SSO to sign in directly.");
		}

		// check user session and that the search query has some content
		getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// generate qr code link with token for the register action
		String token = tokenResolverUtil
		        .createUserRegistrationToken(DateUtils.endOfDay(DateUtils.moveDays(new Date(), 7)).getTime());
		String url = routes.UsersController.register(token).absoluteURL(request, true);
		String qrCode = routes.HomeController.qrCode("invite_url_", url).absoluteURL(request);
		return ok(views.html.users.invite.render(url, qrCode));
	}

	@AddCSRFToken
	public Result register(Request request, String token) {
		// disable the registration if SSO is configured
		if (ssoEnabled) {
			return redirect(LANDING).addingToSession(request, "error",
			        "No registration possible, use SSO to sign in directly.");
		}

		return ok(views.html.users.register.render(token, csrfToken(request)));
	}

	@RequireCSRFCheck
	public Result registerMe(Request request) {
		// disable the registration if SSO is configured
		if (ssoEnabled) {
			return redirect(LANDING).addingToSession(request, "error",
			        "No registration possible, use SSO to sign in directly.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(LANDING).addingToSession(request, "error", "No data in registration form.");
		}

		// try to find existing
		String emailAddress = nss(df.get("email"));
		if (Person.findByEmailCount(emailAddress) > 0) {
			return redirect(routes.HomeController.login("", "")).addingToSession(request, "message",
			        "We have found you already in the system. Contact support for a password reset.");
		}

		// check access code
		String access_code = df.get("access_inline");
		if (access_code == null || (!tokenResolverUtil.checkRegistrationAccessKey(access_code)
		        && !tokenResolverUtil.checkTimeoutFromUserRegistrationToken(access_code))) {
			return redirect(routes.UsersController.register("")).addingToSession(request, "error", "Wrong access code");
		}

		// check if email address is valid
		if (!new EmailValidator().isValid(emailAddress)) {
			return redirect(routes.UsersController.register(access_code)).addingToSession(request, "message",
			        "The email address you entered seems to be wrong.");
		}

		// check password
		String password = nss(df.get("password"));
		if (password.length() < 8) {
			return redirect(routes.UsersController.register(access_code)).addingToSession(request, "error",
			        "Password is too short. We need at least 8 characters.");
		}
		if (!password.equals(df.get("re_password"))) {
			return redirect(routes.UsersController.register(access_code)).addingToSession(request, "error",
			        "Password does not match");
		}

		String firstName = nss(df.get("first_name"));
		String lastName = nss(df.get("last_name"));
		if (firstName.length() < 2 || lastName.length() < 2) {
			return redirect(routes.UsersController.register(access_code)).addingToSession(request, "error",
			        "First or last name is too short. We need at least 2 characters.");
		}

		// register user
		Person user = Person.register(nss(df.get("user_id")), firstName, lastName, emailAddress, nss(df.get("website")),
		        password, access_code);
		user.save();

		// set accesscode
		user.setAccesscode(tokenResolverUtil.createUserAccessToken(user.getId(),
		        System.currentTimeMillis() + V2UserApiAuth.API_TOKEN_VALIDITY));
		user.update();

		// create default project
		Project p = Project.create(firstName + "'s 1st project", user,
		        "This is your first project, it's private and not shareable, please edit information with proper content.",
		        false, false);
		p.save();

		return redirect(routes.HomeController.login("", ""));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		// set Telegram PIN in cache (valid for one hour)
		final Optional<TelegramSession> session = TelegramSession.find.query().setMaxRows(1).where()
		        .eq("email", user.getEmail()).eq("state", "RESEARCHER").findOneOrEmpty();
		final String telegramToken;
		if (!session.isPresent()) {
			telegramToken = TelegramBotUtils.generateTelegramPersonalPIN();
			cache.set(TelegramBotUtils.TG_USER_CACHE_PREFIX + user.getEmail(), telegramToken, 3600);
		} else {
			telegramToken = "";
		}

		// get the LabNoteEntry info of this week
		Date lastUpdateDate = DateUtils.startOfDay(new Date());
		int updates = LabNotesEntry.countWeekUpdates(user, lastUpdateDate);

		return ok(views.html.users.edit.render(csrfToken(request), telegramToken, user,
		        onboardingSupport.isActive(user), updates, new SimpleDateFormat("MMM d, yyyy").format(lastUpdateDate)));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		DynamicForm df = formFactory.form().bindFromRequest(request);
		user.setFirstname(htmlTagEscape(nss(df.get("first_name"), 64)));
		user.setLastname(htmlTagEscape(nss(df.get("last_name"), 64)));
		user.setWebsite(htmlTagEscape(nss(df.get("user_website"), 120)));
		user.update();

		// redirect to any project
		return redirect(HOME);
	}

	public Result generateToken(Request request, String service) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		// if there is no access code yet, install the auth token for API access with a two-month timeout
		user.setAccesscode(tokenResolverUtil.createUserAccessToken(user.getId(),
		        System.currentTimeMillis() + V2UserApiAuth.API_TOKEN_VALIDITY));
		user.update();

		return redirect(routes.UsersController.edit()).addingToSession(request, "message",
		        "New API token generated and ready.");
	}

	@AddCSRFToken
	@RequireCSRFCheck
	public Result resetTelegramSession(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		// find Telegram session and delete it
		TelegramSession.find.query().where().ieq("email", user.getEmail()).delete();

		// return to the profile page
		return edit(request).addingToSession(request, "message", "Telegram session reset successfully.");
	}

	@AddCSRFToken
	public Result resetPW(Request request, String token) {

		// check username from request
		String username = tokenResolverUtil.retrieveUsernameFromEmailResetToken(token);
		if (username == null) {
			return redirect(LANDING).addingToSession(request, "error", "Your reset token is broken.");
		}

		// check token correctness from cache
		Optional<String> cachedToken = cache.get("email_reset_" + username);
		if (!cachedToken.isPresent() || !token.equals(cachedToken.get())) {
			logger.info("Reset token for " + username + " is invalid or expired.");
			return redirect(LANDING).addingToSession(request, "error", "Your reset token is invalid or expired.");
		}

		Optional<Person> userOpt = Person.findByEmail(username);
		if (!userOpt.isPresent()) {
			logger.info("Person " + username + " not found in database.");
			return redirect(LANDING).addingToSession(request, "error", "We could not find you.");
		}

		return ok(views.html.users.resetPW.render(csrfToken(request), token));
	}

	@RequireCSRFCheck
	public Result resetPWMe(Request request, String token) throws InterruptedException, ExecutionException {

		// check username from request
		String username = tokenResolverUtil.retrieveUsernameFromEmailResetToken(token);
		if (username == null) {
			return redirect(LANDING).addingToSession(request, "error", "Password reset token problem.");
		}

		// check token correctness
		Optional<String> cachedToken = cache.get("email_reset_" + username);
		if (!cachedToken.isPresent() || !token.equals(cachedToken.get())) {
			logger.info("Reset token for " + username + " is invalid or expired.");
			return redirect(LANDING).addingToSession(request, "error", "Password reset token is invalid or expired.");
		}

		// check user
		Optional<Person> userOpt = Person.findByEmail(username);
		if (!userOpt.isPresent()) {
			logger.info("Person " + username + " not found in database.");
			return redirect(LANDING).addingToSession(request, "error", "We could not find you.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		String passwd1 = df.get("password");
		String passwd2 = df.get("re_password");
		if (passwd1.length() < 8) {
			// redirect to reset form
			return redirect(routes.UsersController.resetPW(token)).addingToSession(request, "error",
			        "New password is either too short, at least 8 characters are needed.");
		}
		if (!passwd1.equals(passwd2)) {
			// redirect to reset form
			return redirect(routes.UsersController.resetPW(token)).addingToSession(request, "error",
			        "The first password is not matching the second copy.");
		}

		// store new password
		Person user = userOpt.get();
		user.setPassword(passwd1);
		user.update();

		// log because it is security relevant
		logger.info("Password successfully changed for " + username);

		// reset cache
		cache.remove("email_reset_" + username);

		// redirect to any project
		return redirect(routes.HomeController.login(username, "")).addingToSession(request, "message",
		        "Password reset successful.");
	}

	/**
	 * set main / current state of user (user.identity)
	 * 
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result setState(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		DynamicForm df = formFactory.form().bindFromRequest(request);
		String catalog = nss(df.get("catalog"));
		String newState = nss(df.get("state"));

		if (nnne(catalog) && nnne(newState)) {
			if (catalog.equals("main")) {
				if (onboardingSupport.switchState(user, catalog, newState)) {
					logger.trace("user: " + user.getName() + "\tswitch " + catalog + " state to " + newState);
					return ok();
				} else {
					logger.error("[Onboarding] wrong catalog or state for main");
				}
			} else if (onboardingSupport.hasScene(catalog)) {
				if (onboardingSupport.updateScene(user, catalog, newState)) {
					// if updateScene is done, then updateCurrent
					onboardingSupport.updateCurrent(user, catalog, "ongoing");
					logger.trace("user: " + user.getName() + "\tchange " + catalog + "");
					return ok();
				} else {
					logger.error("[Onboarding] wrong catalog");
				}
			}
		}

		return badRequest("Something wrong with onboarding parameters.");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result bulkTeams(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		if (!user.isRole(Roles.STAFF_ROLE)) {
			return redirect(LANDING);
		}

		return ok(views.html.users.bulkTeams.render(csrfToken(request)));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	@RequireCSRFCheck
	public CompletionStage<Result> bulkTeamsMe(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		return CompletableFuture.supplyAsync(() -> {
			if (!user.isRole(Roles.STAFF_ROLE)) {
				return redirect(LANDING);
			}

			DynamicForm df = formFactory.form().bindFromRequest(request);
			String teams = ((String) df.value("teams").orElse("")).trim();
			String prefix = ((String) df.value("prefix").orElse("Team ")).trim();
			// ensure that we have a team set for the project name
			if (prefix.isEmpty()) {
				prefix = "Team ";
			}
			String description = ((String) df.value("description").orElse("")).trim();
			String validated = ((String) df.value("validated").orElse("")).trim();

//			String text = "alice@example.com; bob@example.com\ncharlie@example.org;dave@example.net\neve@example.com";
			final String lineRegex = "^([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}\\s*;\\s*)*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,})\\s*;?\\s*$";
			final Pattern pattern = Pattern.compile(lineRegex);

			StringBuilder sb = new StringBuilder();
			if (validated.isEmpty() || !validated.equals("validated")) {
				// check team emails
				boolean hasProblem = false;

				int lineCount = 1;
				for (String line : teams.split("\\R")) {
					Matcher matcher = pattern.matcher(line);
					if (matcher.matches()) {
						sb.append("<p>✅ " + prefix + " " + lineCount++ + ": ");
						Arrays.stream(line.split(";")).filter(s -> s != null && !s.isEmpty()).map(String::trim)
						        .forEach(email -> {
							        if (Person.findByEmail(email).isPresent()) {
								        sb.append("<span class='email-ok'>" + email + " 🙂</span>");
							        } else {
								        sb.append("<span class='email-missing'>" + email + "</span>");
							        }
						        });
						sb.append("</p>");
					} else {
						hasProblem |= true;
						sb.append("""
						        <p style="color: red;">❌ Team %d: %s </p>
						        """.formatted(lineCount++, line));
					}
				}

				if (!hasProblem) {
					sb.append("""
					         <form
					         hx-post="%s"
					         hx-target="#result"
					         hx-swap="innerHTML"
					         hx-disabled-elt="button,#result">
					         <input type="hidden" name="prefix" value="%s">
					         <input type="hidden" name="description" value="%s">
					         <input type="hidden" name="teams" value="%s">
					         	<input type="hidden" name="csrfToken" value="%s">
					         <input type="hidden" name="validated" value="validated">
					         <p>
					         Please check the teams carefully above.
					         If you click on "Create projects", we create the teams with you as the team owner,
					         then send collaboration invites to the respective emails per team.
					         </p>
					        	<div class="progress htmx-indicator">
					          <div class="indeterminate"></div>
					        </div>
					         <button class="btn">Create projects</button>
					         </form>
					         """.formatted(routes.UsersController.bulkTeamsMe(), prefix, description,
					        teams.replaceAll("\\R", "|"), csrfToken(request)));
				}

				return ok(sb.toString()).as(Http.MimeTypes.HTML);
			}

			// create projects and send invitations to collaborate

			int lineCount = 1;
			AtomicInteger invitesSent = new AtomicInteger();
			for (String line : teams.split("\\|")) {
				Matcher matcher = pattern.matcher(line);
				if (matcher.matches()) {
					// create project
					Project project = Project.create(prefix + " " + lineCount++, user, description, false, false);
					project.setLicense("MIT");
					project.save();
					project.refresh();

					// log that the project has been created
					LabNotesEntry.log(Project.class, LabNotesEntryType.CREATE, "Project created", project);

					// invite email as a collaborator to the project
					Arrays.stream(line.split(";")).filter(s -> s != null && !s.isEmpty()).map(String::trim)
					        .forEach(email -> {
						        // prepare email components
						        String actionLink = routes.ProjectsController
						                .addCollaboration(project.getId(),
						                        tokenResolverUtil.getCollaborationToken(project.getId(), email))
						                .absoluteURL(request, true);
						        Html htmlBody = views.html.emails.invite.render("Invitation to collaborate",
						                String.format("%s would like to invite you to collaborate on the project %s. "
						                        + "If you are interested, just click the button below. "
						                        + "If not, please ignore this message (trash it to self-destruct).",
						                        user.getName(), project.getName()),
						                actionLink);
						        String textBody = String.format(
						                "Hello! \n\n%s would like to invite you to collaborate on the project %s. "
						                        + "If you are interested, just click the link below. "
						                        + "If not, please ignore this message (trash it to self-destruct). \n\n(%s) \n\n",
						                user.getName(), project.getName(), actionLink);

						        // send email
						        notificationService.sendMail(email, textBody, htmlBody, actionLink,
						                "[ID Data Foundry] Invitation to collaborate", user.getEmail(), user.getName(),
						                "Invitation to collaborate: " + actionLink);

						        // delay in sending out emails
						        try {
							        Thread.sleep(500);
						        } catch (InterruptedException e) {
						        }

						        // ping search service
						        searchService.ping();
						        invitesSent.incrementAndGet();
					        });
					sb.append("</p>");
				}
			}

			// here we do the real thing!
			return ok(
			        """
			                <p>
			                	🎉 We have created %d projects and sent %d collaboration invites to the respective email addresses.
			                </p>
			                """
			                .formatted(lineCount - 1, invitesSent.get())).as(Http.MimeTypes.HTML);
		});
	}

//	private String getAnnouncement() {
//		// find announcement folder
//		Optional<File> envFolder = environment.isDev() ? environment.getExistingFile("dist/announcements/")
//		        : environment.getExistingFile("announcements/");
//		if (!envFolder.isPresent()) {
//			return "";
//		}
//
//		final MarkdownRenderer mdr = new MarkdownRenderer();
//		final File folder = envFolder.get();
//		String result = Arrays
//		        .stream(folder.listFiles(
//		                f -> f.exists() && f.length() > 0 && f.getName().matches("([0-9]{8})-([0-9]{8})[.]md")))
//		        .map(f -> {
//			        try {
//				        List<String> lines = com.google.common.io.Files.readLines(f, Charset.defaultCharset());
//				        return mdr.render(lines.stream().collect(Collectors.joining("\n")));
//			        } catch (IOException e) {
//				        logger.error("Error in preparing the announcement.", e);
//				        return null;
//			        }
//		        }).filter(s -> s != null && !s.isEmpty()).collect(Collectors.joining("</p><p>"));
//
//		return result.isEmpty() ? "" : ("<p>" + result + "</p>");
//	}
}
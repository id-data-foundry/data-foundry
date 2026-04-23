package controllers;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.pekko.actor.Cancellable;
import org.apache.pekko.stream.javadsl.Source;
import org.pac4j.core.exception.TechnicalException;
import org.pac4j.play.java.Secure;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Files;
import com.typesafe.config.Config;

import controllers.Assets.Asset;
import controllers.auth.UserAuth;
import models.Dataset;
import models.Person;
import models.Project;
import play.Environment;
import play.Logger;
import play.cache.SyncCacheApi;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.EventSource;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.MimeTypes;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import play.twirl.api.Html;
import services.RefreshTask;
import services.api.ai.LocalModelMetadata;
import services.email.NotificationService;
import services.maintenance.ProjectLifecycleService;
import services.maintenance.RealTimeNotificationService;
import utils.auth.TokenResolverUtil;
import utils.components.TourManager;
import utils.components.TourManager.Tour;
import utils.conf.ConfigurationUtils;
import utils.rendering.FileUtil;
import utils.rendering.MarkdownRenderer;
import utils.validators.FileTypeUtils;

public class HomeController extends AbstractAsyncController {

	private final Config configuration;
	private final Environment environment;
	private final SyncCacheApi cache;
	private final NotificationService notificationService;
	private final TokenResolverUtil tokenResolverUtil;
	private final MarkdownRenderer mdRenderer;
	private final TourManager tourManager;
	private final RealTimeNotificationService realtimeNotifications;
	private final LocalModelMetadata localModelMetadata;

	private final boolean ssoEnabled;
	private final boolean ssoClientOIDC;

	private static final Logger.ALogger logger = Logger.of(HomeController.class);

	@Inject
	public HomeController(RefreshTask rt, Config configuration, Environment environment, SyncCacheApi sca,
			NotificationService ns, ProjectLifecycleService tbs, TokenResolverUtil tru, MarkdownRenderer mdRenderer,
			TourManager tourManager, RealTimeNotificationService realtimeNotifications, LocalModelMetadata lmmd) {
		this.configuration = configuration;
		this.environment = environment;
		this.cache = sca;
		this.notificationService = ns;
		this.tokenResolverUtil = tru;
		this.mdRenderer = mdRenderer;
		this.tourManager = tourManager;
		this.realtimeNotifications = realtimeNotifications;
		this.localModelMetadata = lmmd;

		// only check SSO configuration once
		ssoEnabled = ConfigurationUtils.isSSO(configuration);
		// plain OIDC is enabled if tenant is not defined and SSO is enabled
		ssoClientOIDC = !ConfigurationUtils.checkConfiguration(configuration, ConfigurationUtils.DF_SSO_TENANT)
				&& ssoEnabled;
	}

	public Result index(Request request) {

		// check if we are in DEV mode and the database is uninitialized
		if (environment.isDev() && Project.find.all().isEmpty()) {
			// redirect to dev init screen
			return redirect(routes.DBController.index());
		}

		// check session first
		if (getAuthenticatedUser(request).isPresent()) {
			// redirect to projects and refresh the feedback link when landing plain
			if (!request.session().get("feedback_link").isPresent()
					&& configuration.hasPath(ConfigurationUtils.DF_FEEDBACK_LINK)) {
				return redirect(HOME).addingToSession(request, "feedback_link",
						configuration.getString(ConfigurationUtils.DF_FEEDBACK_LINK));
			} else {
				return redirect(HOME);
			}
		}

		// GUEST users...

		// retrieve simple project / dataset metrics that are cached for 120 seconds
		long projects = cache.getOrElseUpdate("df_total_project_count", () -> Project.find.query().findCount(), 120);
		long publicProjects = cache.getOrElseUpdate("df_total_public_project_count",
				() -> Project.find.query().where().eq("publicProject", true).findCount(), 120);
		long datasets = cache.getOrElseUpdate("df_total_dataset_count", () -> Dataset.find.query().findCount(), 120);

		// inject announcement into the session
		String announcement = getAnnouncement();
		if (announcement != null && !announcement.isEmpty()) {
			return ok(views.html.home.index.render(projects, publicProjects, datasets, request))
					.addingToSession(request, "announcement", announcement);
		} else {
			return ok(views.html.home.index.render(projects, publicProjects, datasets, request))
					.removingFromSession(request, "announcement");
		}
	}

	@AddCSRFToken
	public Result login(Request request, String username, String redirectUrl) throws TechnicalException {

		// ensure all emails in the system are lowercase
		username = username.toLowerCase();

		// if the redirect URL is set, then put it into the session for later redirect after login
		if (!redirectUrl.isEmpty()) {
			return ok(views.html.home.login.render(ssoEnabled, ssoClientOIDC, environment.isDev(), username,
					csrfToken(request))).addingToSession(request, REDIRECT_URL, redirectUrl);
		}

		return ok(views.html.home.login.render(ssoEnabled, ssoClientOIDC, environment.isDev(), username,
				csrfToken(request)));
	}

	@Secure(clients = "AzureAd2Client")
	public Result ssoLogin() {
		return redirect(HOME);
	}

	@Secure(clients = "OidcClient")
	public Result ssoLoginOIDC() {
		return redirect(HOME);
	}

	@AddCSRFToken
	public Result uiLogin(Request request) throws TechnicalException {
		String referer = request.header(REFERER).orElse("");
		return redirect(routes.HomeController.login("", referer));
	}

	@RequireCSRFCheck
	public Result requestResetPW(Request request, String username) {

		if (username == null || username.length() == 0) {
			return redirect(LANDING);
		}

		String userEmail = username.trim().toLowerCase();

		// try to find person
		Optional<Person> opt = Person.findByEmail(username);
		if (!opt.isPresent()) {
			// note: we don't add a error message here to prevent that user accounts are discovered through this
			// functionality
			return redirect(routes.HomeController.login(userEmail, ""));
		}

		Person user = opt.get();

		// cache for 25 hours to allow for support to check stuff
		String token = tokenResolverUtil.createEmailResetToken(user.getEmail());
		cache.set("email_reset_" + user.getEmail(), token, 25 * 60 * 60);

		// switch to link directly (because admin permissions)
		String actionLink = routes.UsersController.resetPW(token).absoluteURL(request, true);

		// send email to user about password reset
		Html htmlBody = views.html.emails.invite.render("Password reset",
				String.format("We send you an email because you requested a password reset for Data Foundry. "
						+ "If that's the case, just click the button below to be forwarded to a form where you can reset your password. "
						+ "Otherwise please ignore this message (trash it to self-destruct)."),
				actionLink);
		String textBody = "Hello! \n\nWe send you an email because you requested a password reset for Data Foundry. "
				+ "If that's the case, just click the link below to be forwarded to a form where you can reset your password. "
				+ "Otherwise please ignore this message (trash it to self-destruct). \n\n" + actionLink + "\n\n";

		notificationService.sendMail(user.getEmail(), textBody, htmlBody, actionLink,
				"[ID Data Foundry] Password reset", "Password reset request: " + actionLink);
		// note: we don't add a success message here to prevent that user accounts are discovered through this
		// functionality
		return redirect(routes.HomeController.login(userEmail, ""));
	}

	/**
	 * check whether the caller has a valid user session right now
	 *
	 * @param request
	 * @return
	 */
	public Result amILoggedInRightNow(Request request) {
		return getAuthenticatedUserName(request).isPresent() ? ok() : forbidden();
	}

	/**
	 * logout the current user; redirect to PAC4J
	 *
	 * @return
	 */
	public Result logout(Request request) {
		return redirect(org.pac4j.play.routes.LogoutController.logout());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * display an overview of licenses used in data foundry
	 *
	 * @return
	 */
	public Result licenses() {
		return ok(views.html.home.licenses.render());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result tools() {
		return ok(views.html.tools.index.render());
	}

	@Authenticated(UserAuth.class)
	public Result support(Request request) {
		Optional<Person> user = getAuthenticatedUser(request);

		// check whether we have a valid registration key
		boolean registration = configuration.hasPath(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS)
				&& configuration.getStringList(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS).stream()
						.anyMatch(s -> !s.trim().isEmpty());

		// check whether link to teams community is defined
		Optional<String> techCommunity = configuration.hasPath(ConfigurationUtils.DF_LINKS_TEAMS_COMMUNITY)
				? Optional.of(configuration.getString(ConfigurationUtils.DF_LINKS_TEAMS_COMMUNITY))
				: Optional.empty();

		return ok(views.html.home.support.render(user, ssoEnabled && registration, techCommunity));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result documentation() {
		return ok(views.html.home.documentation.render());
	}

	@Authenticated(UserAuth.class)
	public Result docusite(Request request, String filename) {

		// attempt to retrieve file
		Optional<File> fileOpt = getEnvironmentFile("documentation", filename);
		if (!fileOpt.isPresent()) {
			return notFound();
		}

		File file = fileOpt.get();

		// check if directory
		if (file.isDirectory()) {
			return redirect(routes.HomeController.docusite(file.getName() + File.separator + "index.html"));
		}

		// check if file other than HTML, then immediate return file
		if (!filename.endsWith(".html")) {
			return ok(file).withHeader("Cache-Control", "max-age=3600");
		}

		// if HTML, read and replace variables, then return as HTML
		try {
			// read file
			String docString = java.nio.file.Files.readString(fileOpt.get().toPath());

			// replace documentation placeholder for configured Telegram bot name
			if (ConfigurationUtils.hasKeyConfiguration(configuration, ConfigurationUtils.DF_TELEGRAM_BOTNAME)) {
				docString = docString.replace("DATAFOUNDRYBOTNAME",
						configuration.getString(ConfigurationUtils.DF_TELEGRAM_BOTNAME));
			}

			// also replace DATAFOUNDRYBASEURL placeholder
			docString = docString.replace("DATAFOUNDRYBASEURL",
					(environment.isProd() ? "https://" : "http://") + request.host());
			docString = docString.replace("DATAFOUNDRYDOMAIN", request.host());

			// finally replace configuration variables (using the direct configuration key as placeholders)
			docString = ConfigurationUtils.replaceConfigurationVars(configuration, docString);

			// insert in template and render
			return ok(docString).as(MimeTypes.HTML).withHeader("Cache-Control", "max-age=3600");
		} catch (Exception e) {
			return internalServerError();
		}
	}

	/**
	 * provide access to files in /dist/content (dev) /content (prod). note: this includes all files, so do NOT host
	 * private content there. an example for appropriate content is an organizational privacy policy or data protection
	 * statement, or a logo
	 *
	 * @param filename
	 * @return
	 */
	public Result publicContent(Request request, String filename) {

		// attempt to retrieve file
		Optional<File> fileOpt = getEnvironmentFile("content", filename);
		if (!fileOpt.isPresent()) {

			// file without extension? --> try with extensions
			if (!filename.contains(".")) {
				// try Markdown
				return publicContent(request, filename + ".md");
			} else if (!filename.contains(".md")) {
				// try HTML
				return publicContent(request, filename.replace(".md", ".html"));
			}

			// we give up
			return notFound();
		}

		File file = fileOpt.get();

		// check if directory
		if (file.isDirectory()) {
			return redirect(routes.HomeController.publicContent(file.getName() + File.separator + "index.html"));
		}

		// if suitable extension and file size, attempt a Markdown rendering
		if (filename.endsWith(".md") && file.length() < 1024 * 1024) {
			// read file contents
			try {
				String contents = new String(java.nio.file.Files.readAllBytes(Paths.get(file.getAbsolutePath())));
				return ok(views.html.home.documentationPage.render(mdRenderer.render(contents)))
						.as("text/html; charset=utf-8").withHeader("Cache-Control", "max-age=3600");
			} catch (IOException e) {
				// log and return the file
				logger.error("Markdown transformation failed: " + file.getAbsolutePath());
				return ok(file).as("text/html; charset=utf-8").withHeader("Cache-Control", "max-age=3600");
			}
		} else if (filename.endsWith(".html")) {
			return ok(file).as("text/html; charset=utf-8");
		} else if (filename.endsWith(".min.js") || filename.endsWith(".min.css")) {
			return ok(file).withHeader("Cache-Control", "max-age=3600");
		} else if (filename.endsWith(".js") || filename.endsWith(".css")) {
			return ok(file).withHeader("Cache-Control", "max-age=60");
		} else if (FileTypeUtils.looksLikeImageFile(file)) {
			return ok(file).withHeader("Cache-Control", "max-age=3600");
		} else {
			return ok(file).withHeader("Cache-Control", "max-age=3600");
		}
	}

	/**
	 * safe retrieval of files in dist/ folders
	 *
	 * @param folderName
	 * @param fileName
	 * @return
	 */
	private Optional<File> getEnvironmentFile(String folderName, String fileName) {

		// check folder name
		if (!folderName.endsWith("/")) {
			folderName = folderName + "/";
		}

		// sanitize filename (first unwanted characters, then directory traversal checks)
		fileName = fileName.replaceAll("[^A-Za-z0-9_./%-]", "");
		fileName = fileName.replaceAll("[.][.]", "");

		// check for URL encoded spaces etc.
		if (fileName.contains("%")) {
			fileName = URLDecoder.decode(fileName, StandardCharsets.UTF_8);
		}

		// first check folder
		String relativePath = (environment.isDev() ? "dist/" : "") + folderName;
		Optional<File> existingFolder = environment.getExistingFile(relativePath);
		if (existingFolder.isEmpty()) {
			logger.error("Folder not found: " + relativePath + fileName);
			return Optional.empty();
		}

		// check file in folder
		Optional<File> fileOpt = FileUtil.getSafeFileInFolder(existingFolder.orElseThrow(), fileName);
		if (!fileOpt.isPresent()) {
			fileOpt = FileUtil.getSafeFileInFolder(existingFolder.orElseThrow(), fileName + ".html");
			if (!fileOpt.isPresent()) {
				logger.error("Not found: " + relativePath + fileName);
				return Optional.empty();
			}
		}

		return fileOpt;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result startTour(Request request, String tourName) {

		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// prepare tour in tour manager
		Optional<Tour> tourOpt = tourManager.prepare(username, tourName);
		if (tourOpt.isEmpty()) {
			return redirect(LANDING);
		}

		return redirect(tourOpt.get().landingPage);
	}

	@Authenticated(UserAuth.class)
	public Result stopTour(Request request) {

		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// discard current tour in tour manager
		tourManager.discard(username);

		return noContent();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	public Result configurableLink(Request request, String link) {
		switch (link) {
		// internal pages ------------------
		case "about":
			return redirect(
					ConfigurationUtils.configure(configuration, ConfigurationUtils.DF_LINKS_ABOUT, "/documentation"));
		case "contact":
			return redirect(ConfigurationUtils.configure(configuration, ConfigurationUtils.DF_LINKS_CONTACT,
					"/content/contact"));
		case "data-protection":
			return redirect(ConfigurationUtils.configure(configuration, ConfigurationUtils.DF_LINKS_DATA_PROTECTION,
					"/documentation/Learning/DataFoundry/DataProtection.html"));
		// external pages ------------------
		case "privacy":
			return redirect(ConfigurationUtils.configure(configuration, ConfigurationUtils.DF_LINKS_PRIVACY,
					"https://www.tue.nl/en/storage/privacy/"));
		case "scientific-integrity":
			return redirect(ConfigurationUtils.configure(configuration,
					ConfigurationUtils.DF_LINKS_SCIENTIFIC_INTEGRITY,
					"https://www.tue.nl/en/our-university/about-the-university/integrity/scientific-integrity/"));
		// images ------------------
		case "organization-logo":
			return ConfigurationUtils.hasKeyConfiguration(configuration, ConfigurationUtils.DF_LINKS_ORG_LOGO)
					? redirect(ConfigurationUtils.configure(configuration, ConfigurationUtils.DF_LINKS_ORG_LOGO, ""))
					: redirect(routes.Assets.versioned(new Asset("images/tue-logo-stack-S.png")));
		// default -----------------
		default:
			return redirect(LANDING);
		}
	}

	public Result securityTxt(Request request) {
		String canonical = "";
		if (configuration.hasPath("df.base_url")) {
			canonical = configuration.getString("df.base_url") + "/.well-known/security.txt";
		}
		return ok(views.html.home.securitytxt.render(
				ConfigurationUtils.configure(configuration, ConfigurationUtils.DF_SECURITYTXT_CONTACT,
						"security@data-foundry.net"),
				ConfigurationUtils.configure(configuration, ConfigurationUtils.DF_SECURITYTXT_EXPIRES, "Jan 1, 2027"),
				canonical,
				ConfigurationUtils.configure(configuration, ConfigurationUtils.DF_SECURITYTXT_LANG, "English")))
				.as(MimeTypes.TEXT);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result getDFVibe() {
		Source<EventSource.Event, Cancellable> eventSource = Source.tick(Duration.ZERO, Duration.ofSeconds(2), "")
				.map(tick -> {
					// compile the requests
					ObjectNode jo = Json.newObject();
					jo.put("incomingData", realtimeNotifications.getIncomingRequestRate());
					jo.set("activeUsers", realtimeNotifications.activeUsers().stream().collect(() -> Json.newArray(),
							ArrayNode::add, ArrayNode::addAll));
					jo.set("api_requests", realtimeNotifications.runningRequests().stream().map(r -> {
						return Json.newObject().put("task", r.getTask())
								.put("model", this.localModelMetadata.getModelName(r.getModel()))
								.put("state", r.getState()).put("valid", r.getValidUntil());
					}).collect(() -> Json.newArray(), ArrayNode::add, ArrayNode::addAll));
					return EventSource.Event.event(jo);
				});

		// return response with flow management
		return ok().chunked(eventSource.via(EventSource.flow())).as(Http.MimeTypes.EVENT_STREAM);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result getFlashes(Request request) {
		Optional<String> message = request.session().get("message");
		Optional<String> error = request.session().get("error");

		// check tour available
		String username = getAuthenticatedUserName(request).orElse("");
		Optional<Tour> tourOpt = tourManager.retrieve(username);

		// careful with deleting stuff from a session!
		Result result = ok(views.html.elements.flashSupport.render(request, message, error, tourOpt))
				.as(MimeTypes.JAVASCRIPT);
		if (message.isPresent()) {
			result = result.removingFromSession(request, "message");
		}
		if (error.isPresent()) {
			result = result.removingFromSession(request, "error");
		}
		return result;
	}

	public Result getExtFlashes(Request request) {
		Optional<String> message = request.session().get("message");
		Optional<String> error = request.session().get("error");

		// don't even send anything if there are no messages
		if (message.isEmpty() && error.isEmpty()) {
			return noContent().as(MimeTypes.JAVASCRIPT);
		}

		// careful with deleting stuff from a session!
		Result result = ok(views.html.elements.flashSupport.render(request, message, error, Optional.empty()))
				.as(MimeTypes.JAVASCRIPT);
		if (message.isPresent()) {
			result = result.removingFromSession(request, "message");
		}
		if (error.isPresent()) {
			result = result.removingFromSession(request, "error");
		}
		return result;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	private String getAnnouncement() {
		// find announcement folder
		Optional<File> envFolder = environment.isDev() ? environment.getExistingFile("dist/announcements/")
				: environment.getExistingFile("announcements/");
		if (!envFolder.isPresent()) {
			return "";
		}

		final MarkdownRenderer mdr = new MarkdownRenderer();
		final File folder = envFolder.get();
		String result = Arrays
				.stream(folder.listFiles(
						f -> f.exists() && f.length() > 0 && f.getName().matches("([0-9]{8})-([0-9]{8})[.]md")))
				.map(f -> {
					try {
						List<String> lines = Files.readLines(f, Charset.defaultCharset());
						return mdr.render(lines.stream().collect(Collectors.joining("\n")));
					} catch (IOException e) {
						logger.error("", e);
						return null;
					}
				}).filter(s -> s != null && !s.isEmpty()).collect(Collectors.joining("</p><p>"));

		return result.isEmpty() ? "" : ("<p>" + result + "</p>");
	}

}

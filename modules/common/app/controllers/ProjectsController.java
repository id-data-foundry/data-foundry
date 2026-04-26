package controllers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.pac4j.core.util.Pac4jConstants;

import com.typesafe.config.Config;

import controllers.api.CompleteDSController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import datasets.ModelTemplates;
import models.Collaboration;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.Subscription;
import models.ds.ClusterDS;
import models.ds.CompleteDS;
import models.sr.TelegramSession;
import models.sr.TelegramSession.TelegramSessionState;
import models.sr.Wearable;
import models.vm.TimedMedia;
import play.Environment;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.data.validation.Constraints.EmailValidator;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Files.TemporaryFile;
import play.mvc.Http;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import play.twirl.api.Html;
import services.api.GenericApiService.ProjectAPIInfo;
import services.api.ai.ManagedAIApiService;
import services.email.NotificationService;
import services.maintenance.ProjectLifecycleService;
import services.search.SearchService;
import services.slack.Slack;
import services.telegrambot.TelegramBotService;
import utils.DataUtils;
import utils.DateUtils;
import utils.auth.TokenResolverUtil;
import utils.components.OnboardingMessage;
import utils.components.OnboardingSupport;
import utils.conf.ConfigurationUtils;
import utils.conf.Configurator;
import utils.rendering.MarkdownRenderer;
import utils.telegrambot.TelegramBotUtils;
import utils.validators.FileTypeUtils;

public class ProjectsController extends AbstractAsyncController {

	private final Config config;
	private final Configurator configurator;
	private final Environment environment;
	private final SyncCacheApi cache;
	private final FormFactory formFactory;
	private final CompleteDSController completeDSController;
	private final DatasetConnector datasetConnector;
	private final NotificationService notificationService;
	private final TokenResolverUtil tokenResolverUtil;
	private final OnboardingSupport onboardingSupport;
	private final TelegramBotService telegramBotUtils;
	private final SearchService searchService;
	private final ManagedAIApiService managedAIAPIService;
	private final ProjectLifecycleService lifeCycleService;
	private static final Logger.ALogger logger = Logger.of(ProjectsController.class);
	private final int MAX_ACTIVE_PROJECTS;

	@Inject
	public ProjectsController(Config config, Configurator configurator, Environment environment, SyncCacheApi cache,
			FormFactory formFactory, CompleteDSController cdsc, DatasetConnector datasetConnector,
			NotificationService notificationService, TokenResolverUtil tokenResolverUtil,
			OnboardingSupport onboardingSupport, TelegramBotService telegramBotService, SearchService searchService,
			ManagedAIApiService managedAiAPIService, ProjectLifecycleService lifeCycleService) {
		this.config = config;
		this.configurator = configurator;
		this.environment = environment;
		this.cache = cache;
		this.formFactory = formFactory;
		this.completeDSController = cdsc;
		this.datasetConnector = datasetConnector;
		this.notificationService = notificationService;
		this.tokenResolverUtil = tokenResolverUtil;
		this.onboardingSupport = onboardingSupport;
		this.telegramBotUtils = telegramBotService;
		this.searchService = searchService;
		this.managedAIAPIService = managedAiAPIService;
		this.lifeCycleService = lifeCycleService;

		this.MAX_ACTIVE_PROJECTS = ConfigurationUtils.configureInt(config, ConfigurationUtils.DF_MAX_ACTIVE_PROJECTS,
				20);
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result index(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		// if there is a redirect set, follow it and remove redirect
		String redirectUrl = session(request, REDIRECT_URL);
		if (redirectUrl != null && !redirectUrl.isEmpty()) {

			// if this is an internal callback SSO operation
			String uuid = session(request, CALLBACK_SSO);
			String pac4jSessionId = session(request, Pac4jConstants.SESSION_ID);
			if (!uuid.isEmpty() && !pac4jSessionId.isEmpty()) {
				// store the session id for PAC4J in the cache
				cache.set(uuid, pac4jSessionId, 300);
			}

			return redirect(redirectUrl) //
					.removingFromSession(request, REDIRECT_URL) //
					.removingFromSession(request, "error");
		}

		// find all owned projects and all collaborations, and combine, filter out archived projects, and sort by last
		// modification time
		List<Project> openProjects = new LinkedList<>();
		openProjects.addAll(user.projects());
		openProjects.addAll(user.collaborations());
		openProjects = openProjects.stream().filter(p -> !p.isArchivedProject())
				.sorted((a, b) -> -Long.compare(a.getLastUpdated(), b.getLastUpdated())).collect(Collectors.toList());

		// find all subscription projects
		List<Project> subscriptions = user.subscriptions();

		// find all archived projects across own and collab projects
		List<Project> archivedProjects = new LinkedList<>();
		archivedProjects.addAll(user.archivedProjects());
		archivedProjects
				.addAll(user.collaborations().stream().filter(p -> p.isArchivedProject()).collect(Collectors.toList()));

		// record last action
		user.touch();

		// check condition and set onboarding for home page
		final Optional<OnboardingMessage> onboardStateForPage;
		if (user.projects().size() == 0) {
			onboardStateForPage = onboardingSupport.getStateForPage(user, "home");
		} else {
			onboardStateForPage = Optional.empty();
		}

		// return with or without announcement
		String announcement = getAnnouncement();
		if (announcement != null && !announcement.isEmpty()) {
			return ok(views.html.projects.index.render(user, openProjects, subscriptions, archivedProjects,
					onboardStateForPage, request)).addingToSession(request, "announcement", announcement)
					.removingFromSession(request, "error");
		} else {
			return ok(views.html.projects.index.render(user, openProjects, subscriptions, archivedProjects,
					onboardStateForPage, request)).removingFromSession(request, "announcement")
					.removingFromSession(request, "error");
		}
	}

	public Result web(Request request, String projectname, String filename) {

		// check hosted files first (first layer)
		final Optional<File> file = environment.isDev() ? environment.getExistingFile("dist/domain/" + projectname)
				: environment.getExistingFile("domain/" + projectname);
		if (file.isPresent() && file.get().exists()) {
			return ok(file.get());
		}

		// check hosted files first (second layer)
		final Optional<File> fileOnPath = environment.isDev()
				? environment.getExistingFile("dist/domain/" + projectname + "/" + filename)
				: environment.getExistingFile("domain/" + projectname + "/" + filename);
		if (fileOnPath.isPresent() && fileOnPath.get().exists()) {
			return ok(fileOnPath.get());
		}

		// ----------------------------------------------------------------------

		// check if project exists
		Optional<Project> optProject = Project.find.query().setMaxRows(1).where().like("name", projectname).orderBy()
				.asc("id").findOneOrEmpty();
		if (!optProject.isPresent()) {
			if (!nnne(projectname) || projectname.equals("tools")) {
				return redirect(LANDING);
			} else {
				return redirect(LANDING).addingToSession(request, "error", "The project was not found.");
			}
		}

		Project project = optProject.get();
		if (!project.isPublicProject()) {
			return redirect(LANDING).addingToSession(request, "error",
					"The project is not public, so its website is not accessible at this moment.");
		}

		// check project datasets for COMPLETE and "www"
		Dataset ds = project.getProjectWebsiteDataset();

		// no dataset matches in this project
		if (ds == null) {
			return redirect(routes.HomeController.index()).addingToSession(request, "error",
					"The project does not have an applicable website dataset.");
		}

		return redirect(routes.DatasetsController.web(ds.getId(), filename));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@AddCSRFToken
	public Result view(Request request, long id) {
		Validator validator = validateProjectVisible(request, id);
		if (!validator.isValid()) {
			return validator.result;
		}
		Person user = validator.user;
		Project project = validator.project;

		// only ever show DF native projects
		if (!project.isDFNativeProject()) {
			return redirect(HOME);
		}

		// if project is frozen, abort and return frozen view
		if (project.isFrozen()) {
			// find frozen export dataset first, then hand to view
			Dataset projectDataExport = project.getDatasets().stream()
					.filter(ds -> ds.getName().equals(Dataset.PROJECT_DATA_EXPORT)).findAny().get();
			if (projectDataExport == null) {
				return redirect(HOME);
			}

			CompleteDS cds = datasetConnector.getTypedDatasetDS(projectDataExport);
			List<TimedMedia> files = cds.getFiles();
			return ok(views.html.projects.viewFrozen.render(project, projectDataExport, files.get(0), user))
					.removingFromSession(request, REDIRECT_URL);
		}

		// create sign-up link
		String recruitmentToken = tokenResolverUtil.getParticipationToken(project.getId(), -1l);
		String viewLink = routes.ParticipationController.recruit(recruitmentToken).absoluteURL(request, true);

		// check condition and set onboarding for page "project_view"
		if (project.getDatasets().size() == 0) {
			return ok(views.html.projects.view.render(project, user, csrfToken(request), viewLink,
					onboardingSupport.getStateForPage(user, "project_view"), configurator))
					.removingFromSession(request, REDIRECT_URL);
		}

		// ok and clear redirect from session
		return ok(views.html.projects.view.render(project, user, csrfToken(request), viewLink, Optional.empty(),
				configurator)).removingFromSession(request, REDIRECT_URL);
	}

	/**
	 * redirect to latest project, including collabs
	 * 
	 * @param request
	 * @return
	 */
	public Result latestProject(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		Optional<Project> projectOpt = user.getOwnAndCollabProjects().stream()
				.sorted((a, b) -> -a.getId().compareTo(b.getId())).findFirst();
		return projectOpt.isEmpty() ? redirect(LANDING)
				: redirect(routes.ProjectsController.view(projectOpt.get().getId()));
	}

	@AddCSRFToken
	public Result viewResources(Request request, long id) {
		Validator validator = validateProjectEditable(request, id);
		if (!validator.isValid()) {
			return validator.result;
		}
		Person user = validator.user;
		Project project = validator.project;

		List<TelegramSession> sessions = TelegramSession.find.query().where().eq("activeProjectId", project.getId())
				.eq("state", TelegramSessionState.PARTICIPANT.name()).findList();
		// initialize and sort list of sessions
		sessions = sessions.stream().map(ts -> {
			ts.initialize();
			return ts;
		}).sorted((a, b) -> a.getUserScreenName().compareToIgnoreCase(b.getUserScreenName()))
				.collect(Collectors.toList());

		return ok(views.html.projects.perspectives.resources.render(project, user, sessions, csrfToken(request),
				configurator));
	}

	@AddCSRFToken
	public Result viewStudyManagement(Request request, long id) {
		Validator validator = validateProjectEditable(request, id);
		if (!validator.isValid()) {
			return validator.result;
		}
		Person user = validator.user;
		Project project = validator.project;

		return ok(views.html.projects.perspectives.studymanagement.render(project, user, csrfToken(request)));
	}

	@AddCSRFToken
	public Result viewTimeline(Request request, long id) {
		Validator validator = validateProjectEditable(request, id);
		if (!validator.isValid()) {
			return validator.result;
		}
		Person user = validator.user;
		Project project = validator.project;

		return ok(views.html.projects.perspectives.timeline.render(project, user, csrfToken(request)));
	}

	@AddCSRFToken
	public Result visualize(Request request, long id, Long clusterId, Long participantId, Long deviceId,
			Long datasetId) {
		Validator validator = validateProjectVisible(request, id);
		if (!validator.isValid())
			return validator.result;

		return ok(views.html.projects.perspectives.visualize.render(validator.project, validator.user,
				csrfToken(request), configurator, clusterId, participantId, deviceId, datasetId));
	}

	@AddCSRFToken
	public Result viewNarrativeSurvey(Request request, long id) {
		Validator validator = validateProjectEditable(request, id);
		if (!validator.isValid()) {
			return validator.result;
		}
		Person user = validator.user;
		Project project = validator.project;

		Optional<Dataset> studyManagement = project.getStudyManagementDataset();
		if (studyManagement.isEmpty()) {
			return noContent();
		}

		Dataset ds = studyManagement.get();
		long surveyDSId = DataUtils
				.parseLong(ds.getConfiguration().getOrDefault(Dataset.NS_NARRATIVE_SURVEY_DATASETS, ""), -1L);
		Dataset cdsWeb = Dataset.find.byId(surveyDSId);
		if (cdsWeb == null) {
			return noContent();
		}

		// compile the files
		CompleteDS cdsWebDS = datasetConnector.getTypedDatasetDS(cdsWeb);
		List<TimedMedia> files = cdsWebDS.getFiles();
		List<TimedMedia> tweeFiles = files.stream().filter(f -> f.link.endsWith(".twee")).collect(Collectors.toList());
		List<TimedMedia> imageFiles = files.stream().filter(f -> !f.link.endsWith(".twee") && !f.link.endsWith(".html"))
				.collect(Collectors.toList());

		return ok(views.html.projects.perspectives.narrativeSurveysView.render(project, user, cdsWeb, tweeFiles,
				imageFiles, csrfToken(request), request));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Validator validateProjectVisible(Request request, long projectId) {

		final Validator result = new Validator();

		// quick check if user is given, check project access later
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(controllers.routes.HomeController.login("", ""))
						.addingToSession(request, "error",
								"Please log in first, we will take you to your destination then.")
						.addingToSession(request, AbstractAsyncController.REDIRECT_URL, request.path()));

		// check if project exists
		result.project = Project.find.byId(projectId);
		if (result.project == null) {
			result.result = redirect(controllers.routes.HomeController.login("", ""))
					.addingToSession(request, "error",
							"Please log in first, we will take you to your destination then.")
					.addingToSession(request, AbstractAsyncController.REDIRECT_URL, request.path());
			return result;
		}

		// record last action
		result.user = user;
		result.user.touch();

		// check ownership, collaboration, subscription
		if (!result.project.visibleFor(result.user)) {
			// redirect and also clear the redirect from session
			result.result = redirect(LANDING).addingToSession(request, "error", "Project not accessible to you.")
					.removingFromSession(request, REDIRECT_URL);
			return result;
		}

		// ensure frozen projects are also archived
		if (result.project.isFrozen() && !result.project.isArchivedProject()) {
			result.project.setArchivedProject(true);
			result.project.update();
		}

		result.result = null;
		return result;
	}

	public Validator validateProjectEditable(Request request, long projectId) {

		final Validator result = new Validator();

		// check if user is given
		Optional<Person> userOpt = getAuthenticatedUser(request);
		if (!userOpt.isPresent()) {
			result.result = redirect(controllers.routes.HomeController.login("", ""))
					.addingToSession(request, "error",
							"Please log in first, we will take you to your destination then.")
					.addingToSession(request, AbstractAsyncController.REDIRECT_URL, request.path());
			return result;
		}

		// check if project exists
		result.project = Project.find.byId(projectId);
		if (result.project == null) {
			result.result = redirect(LANDING).addingToSession(request, "error", "Project not found.");
			return result;
		}

		// check whether it's DF native
		if (!result.project.isDFNativeProject()) {
			result.result = redirect(LANDING).addingToSession(request, "error",
					"Project not accessible as Data Foundry project.");
			return result;
		}

		result.user = userOpt.get();

		// record last action
		result.user.touch();

		// check ownership, collaboration, subscription
		if (!result.project.editableBy(result.user)) {
			// redirect and also clear the redirect from session
			result.result = redirect(PROJECT(projectId))
					.addingToSession(request, "error", "Project not accessible to you.")
					.removingFromSession(request, REDIRECT_URL);
			return result;
		}

		result.result = null;
		return result;
	}

	class Validator {
		Person user = null;
		Project project = null;
		Result result = null;

		public boolean isValid() {
			return result == null && user != null && project != null;
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	private List<String> getFileTemplates() {
		final String localDevPrefix = environment.isDev() ? "dist/" : "";
		Optional<File> envFolder = environment.getExistingFile(localDevPrefix + "templates/");
		if (!envFolder.isPresent()) {
			return new LinkedList<>();
		}

		File folder = envFolder.get();
		if (folder.exists() && folder.isDirectory()) {
			return Arrays.stream(folder.listFiles(File::isDirectory)).map(File::getName).sorted()
					.collect(Collectors.toList());
		}
		return new LinkedList<>();
	}

	@AddCSRFToken
	public Result add(Request request, String template) {

		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check how many active projects a person has already
		if (user.projects().stream().filter(p -> !p.isArchivedProject()).count() >= MAX_ACTIVE_PROJECTS) {
			return redirect(HOME).addingToSession(request, "error", "You cannot have more than " + MAX_ACTIVE_PROJECTS
					+ " active projects at a time.<br>Consider archiving inactive projects first before creating a new project.");
		}

		// display the add form page
		return ok(views.html.projects.add.render(csrfToken(request), user, nss(template), getFileTemplates()));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request) {

		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data");
		}

		// check if data protection agreement is ticked
		boolean isDataProtectionAgreed = df.get("isDataProtectionAgreed") != null ? true : false;
		if (!isDataProtectionAgreed) {
			return redirect(HOME).addingToSession(request, "error",
					"You need to accept the data protection agreement before you can create this project.");
		}

		// check how many active projects a person has already
		if (user.projects().stream().filter(p -> !p.isArchivedProject()).count() >= 20) {
			return redirect(HOME).addingToSession(request, "error",
					"You cannot have more than 20 active projects at a time.<br>Consider archiving inactive projects.");
		}

		// create(String name, Person owner, String intro, boolean publicProject,
		// boolean shareableProject, String license)
		Project project = Project.create(nss(nss(df.get("project_name"))), user, nss(df.get("intro")), false, false);
		project.setLicense(nss(df.get("license")));
		project.save();

		// first log that the project has been created
		LabNotesEntry.log(Project.class, LabNotesEntryType.CREATE, "Project created", project);

		// add to project if this is using a template project
		String template = nss(df.get("project_template"));
		project.refresh();
		final String localDevPrefix = environment.isDev() ? "dist/" : "";
		Optional<File> templatesDir = environment.getExistingFile(localDevPrefix + "templates/");
		ModelTemplates.addProjectElements(project, datasetConnector, tokenResolverUtil, template,
				templatesDir.orElse(null));

		onboardingSupport.updateAfterDone(user, "new_project");

		// ping search service
		searchService.ping();

		return redirect(PROJECT(project.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// don't allow editing when archived
		if (project.isArchivedProject()) {
			return redirect(PROJECT(project.getId())).addingToSession(request, "error",
					"Project is archived, cannot edit that.");
		}

		return ok(views.html.projects.edit.render(csrfToken(request), project));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {

		// check authorization
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(username)) {
			return redirect(HOME);
		}

		// don't allow editing when archived
		if (project.isArchivedProject()) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Project is archived, cannot edit that.");
		}

		// check the form data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Expecting some data.");
		}

		// escape wrong or malicious data in submitted fields where needed
		project.setName(htmlTagEscape(nss(df.get("project_name"), 120)));
		project.setIntro(htmlTagEscape(nss(df.get("intro"))));
		project.setLicense(nss(df.get("license"), 64));
		project.setKeywords(htmlTagEscape(nss(df.get("keywords"))));
		project.setDoi(nss(df.get("doi")));
		project.setRelation(nss(df.get("relation")));
		project.setOrganization(htmlTagEscape(nss(df.get("organization"))));
		project.setRemarks(htmlTagEscape(nss(df.get("remarks"))));
		project.setPublicProject(df.get("isPublic") == null ? false : true);
		project.update();

		// ping search service
		searchService.ping();

		return redirect(PROJECT(id));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result addDataset(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not found or not editable.");
		}

		// don't allow editing when archived
		if (project.isArchivedProject()) {
			return redirect(PROJECT(project.getId())).addingToSession(request, "error",
					"Project is archived, cannot edit that.");
		}

		return ok(views.html.datasets.datasetSelection.render(project, configurator, csrfToken(request)));
	}

	/**
	 * redirect to latest dataset in the given project
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public Result latestDataset(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || !project.visibleFor(user)) {
			return redirect(HOME).addingToSession(request, "error", "Project not found or not editable.");
		}

		// find latest dataset for given project
		Optional<Dataset> datasetOpt = project.getOperationalDatasets().stream()
				.sorted((a, b) -> -a.getId().compareTo(b.getId())).findFirst();
		return datasetOpt.isEmpty() ? redirect(PROJECT(id))
				: redirect(routes.DatasetsController.view(datasetOpt.get().getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result addScript(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not found or not editable.");
		}

		// don't allow editing when archived
		if (project.isArchivedProject()) {
			return redirect(PROJECT(project.getId())).addingToSession(request, "error",
					"Project is archived, cannot edit that.");
		}

		return ok(views.html.projects.addScript.render(project, csrfToken(request)));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result manageResources(Request request, Long id) {

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(LANDING).addingToSession(request, "error", "Project not found.");
		}

		// check for open project if user is empty
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		if (username.isEmpty()) {
			if (!project.isPublicProject()) {
				return redirect(LANDING).addingToSession(request, "error", "Project not accessible.");
			}

			// render public project view
			return redirect(routes.ProjectsController.view(id));
		}

		// check if user exists
		Person user = Person.findByEmail(username).get();
		if (user == null) {
			return redirect(LANDING).addingToSession(request, "error", "User not found.");
		}

		// check ownership
		if (!project.editableBy(username)) {
			return redirect(LANDING).addingToSession(request, "error", "Project not accessible.");
		}

		// create sign-up link
		String recruitmentToken = tokenResolverUtil.getParticipationToken(project.getId(), -1l);
		String viewLink = routes.ParticipationController.recruit(recruitmentToken).absoluteURL(request, true);

		// render project view for owner
		return ok(
				views.html.projects.manageResources.render(project, user, csrfToken(request), viewLink, configurator));

	}

	/**
	 * add the collaboration from the owner's side ("invite")
	 * 
	 * @param id
	 * @param email
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result addCollaborator(Request request, Long id, String email) {

		// first normalize the email address
		email = email.toLowerCase();

		// check email address
		if (!new EmailValidator().isValid(email)) {
			return redirect(PROJECT(id));
		}

		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null || !project.belongsTo(user)) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// send email, redirect to view
		String actionLink = routes.ProjectsController
				.addCollaboration(id, tokenResolverUtil.getCollaborationToken(project.getId(), email))
				.absoluteURL(request, true);
		Html htmlBody = views.html.emails.invite.render("Invitation to collaborate",
				String.format(
						"%s would like to invite you to collaborate on the project %s. "
								+ "If you are interested, just click the button below. "
								+ "If not, please ignore this message (trash it to self-destruct).",
						user.getName(), project.getName()),
				actionLink);
		String textBody = String.format(
				"Hello! \n\n%s would like to invite you to collaborate on the project %s. "
						+ "If you are interested, just click the link below. "
						+ "If not, please ignore this message (trash it to self-destruct). \n\n(%s) \n\n",
				user.getName(), project.getName(), actionLink);

		notificationService.sendMail(email, textBody, htmlBody, actionLink,
				"[ID Data Foundry] Invitation to collaborate", user.getEmail(), user.getName(),
				"Invitation to collaborate: " + actionLink);

		// ping search service
		searchService.ping();

		return redirect(PROJECT(id)).addingToSession(request, "message", "Ok, we sent an invite to " + email);
	}

	/**
	 * remove the collaboration from the owner's side ("kick out")
	 * 
	 * @param id
	 * @param collaboratorId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result removeCollaborator(Request request, Long id, Long collaboratorId) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null || !project.belongsTo(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not found or not accessible.");
		}

		// check if collaborator exists
		Person collaborator = Person.find.byId(collaboratorId);
		if (collaborator == null) {
			return redirect(HOME).addingToSession(request, "error", "Collaborator not found.");
		}

		// check for existing collaboration (that can be removed)
		Optional<Collaboration> col = project.getCollaborators().stream()
				.filter(c -> c.getCollaborator().equals(collaborator)).findFirst();
		if (!col.isPresent()) {
			return redirect(PROJECT(id));
		}

		// remove collaboration
		Collaboration s = col.get();
		s.delete();
		project.getCollaborators().remove(s);
		project.update();

		// ping search service
		searchService.ping();

		return redirect(PROJECT(id)).addingToSession(request, "message", "Removed collaboration.");
	}

	/**
	 * add the collaboration from the collaborator's side ("confirmation")
	 * 
	 * @param id
	 * @param collaboratorId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result addCollaboration(Request request, Long id, String token) {

		// check if user exists
		Person collaborator = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		String collaboratorEmail = tokenResolverUtil.getCollaboratorEmailFromCollaborationToken(token);
		if (!collaborator.getEmail().equals(collaboratorEmail)) {
			logger.warn("Collab-Token problem: " + tokenResolverUtil.getRawCollabToken(token));
			return redirect(HOME).addingToSession(request, "error", "Something went wrong with your invite token.");
		}

		// check if project exists or already is owned or in collaboration
		Project project = Project.find.byId(id);
		long id_new = tokenResolverUtil.getProjectIdFromCollaborationToken(token);
		if (project == null || project.belongsTo(collaborator) || project.collaboratesWith(collaborator)
				|| project.getId() != id_new) {
			return redirect(HOME).addingToSession(request, "error", "Something went wrong with your invite.");
		}

		// create collaboration
		Collaboration s = new Collaboration(collaborator, project);
		s.save();
		project.getCollaborators().add(s);
		project.update();

		// send email to project owner about accepted collaboration request
		String subject = "Collaboration accepted",
				description = "your collaborator," + collaborator.getName() + ", for project:" + project.getName()
						+ " has accepted the invitation.",
				redirectLink = routes.ProjectsController.view(id).absoluteURL(request, true);
		sendConfirmMail(project, project.getOwner(), subject, description, redirectLink);

		// inform project owner via Telegram
		telegramBotUtils.sendMessageToProjectOwner(project.getId(),
				"A new collaborator, " + collaborator.getName() + ", has joined your project: " + project.getName(),
				Executors.newSingleThreadExecutor());

		// ping search service
		searchService.ping();

		return redirect(PROJECT(id)).addingToSession(request, "message", "Added collaboration.");
	}

	/**
	 * remove the collaboration from the collaborator's side ("leave project")
	 * 
	 * @param id
	 * @param collaboratorId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result removeCollaboration(Request request, Long id) {

		// check if user exists
		Person collaborator = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null || !project.collaboratesWith(collaborator)) {
			return redirect(HOME);
		}

		// check for existing collaboration (that can be removed)
		Optional<Collaboration> col = project.getCollaborators().stream()
				.filter(c -> c.getCollaborator().equals(collaborator)).findFirst();
		if (!col.isPresent()) {
			return redirect(PROJECT(id));
		}

		// remove collaboration
		Collaboration s = col.get();
		s.delete();
		project.getCollaborators().remove(s);
		project.update();

		// inform project owner via Telegram
		telegramBotUtils.sendMessageToProjectOwner(project.getId(), "We would like to inform you, your collaborator, "
				+ collaborator.getName() + ", has left your project: " + project.getName(),
				Executors.newSingleThreadExecutor());

		// ping search service
		searchService.ping();

		return redirect(HOME).addingToSession(request, "message", "Left collaboration.");
	}

	/**
	 * add subscription ("owner confirms")
	 * 
	 * @param id
	 * @param token
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result addSubscription(Request request, Long id, String token) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if subscriber exists
		Long subscriberId = tokenResolverUtil.getSubscriberIdFromSubscriptionToken(token);
		Person subscriber = Person.find.byId(subscriberId);
		if (subscriber == null) {
			logger.warn("Subscription-Token problem: " + tokenResolverUtil.getRawSubscriptionToken(token));
			return redirect(HOME).addingToSession(request, "error",
					"Something went wrong with the confirmation token.");
		}

		// check if project exists
		Project project = Project.find.byId(id);
		long id_new = tokenResolverUtil.getProjectIdFromSubscriptionToken(token);
		if (project == null || !project.belongsTo(username) || project.getId() != id_new) {
			return redirect(HOME).addingToSession(request, "error", "Something went wrong with the confirmation.");
		}

		// check for existing collaboration
		if (project.collaboratesWith(subscriber)) {
			return redirect(HOME).addingToSession(request, "error",
					"Subscriber already collaborates with you in this project.");
		}

		// check for existing subscription
		if (project.subscribedBy(subscriber)) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Subscriber is already subscribed.");
		}

		// create subscription
		Subscription s = new Subscription(subscriber, project);
		s.save();
		project.getSubscribers().add(s);
		project.update();

		// send email to potential subscriber about accepted subscription request (project owner accepted)
		String subject = "Subscription accepted",
				description = "your subscription for project:" + project.getName() + " has been accepted.",
				redirectLink = routes.ProjectsController.view(id).absoluteURL(request, true);
		sendConfirmMail(project, subscriber, subject, description, redirectLink);

		return redirect(PROJECT(id)).addingToSession(request, "message", "Added subscription.");
	}

	/**
	 * remove subscription ("owner kicks out")
	 * 
	 * @param id
	 * @param subscriberId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result removeSubscription(Request request, Long id, Long subscriberId) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null || !project.belongsTo(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not found or not accessible.");
		}

		// check if subscriber exists
		Person subscriber = Person.find.byId(subscriberId);
		if (subscriber == null) {
			return redirect(HOME).addingToSession(request, "error", "Subscriber not found.");
		}

		// check for existing subscription (that can be removed)
		Optional<Subscription> sub = project.getSubscribers().stream().filter(c -> c.getSubscriber().equals(subscriber))
				.findFirst();
		if (!sub.isPresent()) {
			return redirect(PROJECT(id));
		}

		// remove collaboration
		Subscription s = sub.get();
		s.delete();
		project.getSubscribers().remove(s);
		project.update();

		return redirect(HOME).addingToSession(request, "message", "Subscription removed.");
	}

	/**
	 * subscribe this user to the project ("send subscription request")
	 * 
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result subscribe(Request request, Long id) {

		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// check whether subscription is allowed
		if (!user.canSubscribe(project)) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "You cannot subscribe to this project.");
		}

		// if project is shareable, create subscription directly
		if (project.isShareableProject()) {
			Subscription s = new Subscription(user, project);
			s.save();
			project.getSubscribers().add(s);
			project.update();

			String subject = "Subscription accepted",
					description = "Your subscription for project " + project.getName() + " has been accepted.",
					redirectLink = routes.ProjectsController.view(id).absoluteURL(request, true);
			sendConfirmMail(project, user, subject, description, redirectLink);
			return redirect(PROJECT(id)).addingToSession(request, "message", "Ok, done!");
		} else {
			// otherwise send email
			String email = project.getOwner().getEmail();
			String actionLink = routes.ProjectsController
					.addSubscription(id, tokenResolverUtil.getSubscriptionToken(project.getId(), user.getId()))
					.absoluteURL(request, true);
			Html htmlBody = views.html.emails.invite.render("Subscription request",
					String.format(
							"%s would like to request a subscription to the project %s. "
									+ "If that's ok, just click the button below. "
									+ "If not, please ignore this message (trash it to self-destruct).",
							user.getName(), project.getName()),
					actionLink);
			String textBody = String.format(
					"Hello! \n\n%s would like to request a subscription to the project %s. "
							+ "If that's ok, just click the link below. "
							+ "If not, please ignore this message (trash it to self-destruct). \n\n(%s) \n\n",
					user.getName(), project.getName(), actionLink);

			notificationService.sendMail(email, textBody, htmlBody, actionLink,
					"[ID Data Foundry] Subscription request", user.getEmail(), user.getName(),
					"Subscription request: " + actionLink);

			return redirect(PROJECT(id)).addingToSession(request, "message", "Ok, we sent an invite to " + email);
		}
	}

	@Authenticated(UserAuth.class)
	public Result unsubscribe(Request request, Long id) {

		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// check if the subscription exist
		if (project.subscribedBy(user)) {
			for (int i = 0; i < project.getSubscribers().size(); i++) {
				Subscription sub = project.getSubscribers().get(i);

				if (sub.isSubscribed(user, project)) {
					sub.delete();
					return redirect(HOME).addingToSession(request, "message", "Ok, done!");
				}
			}
		}

		return redirect(PROJECT(id)).addingToSession(request, "message", "Unsubscribed from project.");
	}

	@Authenticated(UserAuth.class)
	public Result archive(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// archive could be done only if user is owner
		if (!project.belongsTo(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"This action is not allowed because you are not the project owner.");
		}

		// archive could be done only if project is not archived
		if (project.isArchivedProject()) {
			return redirect(HOME).addingToSession(request, "error", "This project has been archived already.");
		}

		// archive the project
		project.setArchivedProject(true);
		project.update();

		// anonymize participants
		project.deidentifyParticipants();
		logger.info("Participants in project " + project.getName() + "( " + project.getId() + " ) are deidentified");

		// anonymize participants
		project.deidentifyDevices();
		logger.info("Devices in project " + project.getName() + "( " + project.getId() + " ) are deidentified");

		// unlink wearables
		project.deidentifyWearables();
		logger.info("Wearables in project " + project.getName() + "( " + project.getId() + " ) are deidentified");

		// ping search service
		searchService.ping();

		return redirect(HOME).addingToSession(request, "message", "Project archived successfully.");
	}

	@Authenticated(UserAuth.class)
	public Result reopen(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// reopen could be done only if user is owner
		if (!project.belongsTo(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"This action is not allowed because you are not the project owner.");
		}

		// reopen could be done only if project is archived
		if (!project.isArchivedProject()) {
			return redirect(HOME).addingToSession(request, "error", "This project is on-going now.");
		}

		// reopen the project
		project.reopen();

		// ping search service
		searchService.ping();

		return redirect(HOME).addingToSession(request, "message", "Project reopened successfully.");
	}

	/**
	 * extend project for up to 3 months by updating the end date of all the datasets in the project
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result extendProjectDuration(Request request, Long id) {
		// check authorization
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));
		Project project = Project.find.byId(id);
		if (project == null || !project.belongsTo(username)) {
			return redirect(HOME);
		}

		// check the form data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Expecting some data");
		}

		// retrieve how many months to extend by
		int extendMonths = 0;
		try {
			extendMonths = df.get("months") == null ? 0 : DataUtils.parseInt(df.get("months"), 0);
			if (extendMonths > 3 || extendMonths <= 0) {
				logger.error("Extend month is only available for 1, 2, or 3");
				return redirect(PROJECT(id)).addingToSession(request, "error", "Extending months for maximum 3 months");
			}
		} catch (Exception e) {
			logger.error("Extend project parsing error:" + e);
			return redirect(PROJECT(id)).addingToSession(request, "error",
					"Expecting an integer for extending months.");
		}

		// extend all dataset end dates
		for (Dataset ds : project.getDatasets()) {
			ds.setEnd(DateUtils.moveMonths(DateUtils.endOfDay(new Date()), extendMonths));
			ds.update();
		}

		logger.info("Extended project " + project.getId() + " by " + extendMonths + " months.");
		return redirect(routes.ProjectsController.viewTimeline(id)).addingToSession(request, "msg",
				"Project extended for " + extendMonths + " months successfully");
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@AddCSRFToken
	public Result studyManagement(Request request, Long id) {
		return studyManagement(request, id, "");
	}

	@AddCSRFToken
	public Result studyManagement(Request request, Long id, String message) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request, noContent());

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return noContent();
		}

		// study setup stuff is only for owner or collaborator
		if (!project.editableBy(username)) {
			return noContent();
		}

		// if the SM dataset does not exist in the project, create it
		if (!project.getStudyManagementDataset().isPresent()) {
			Dataset smd = datasetConnector.create("Study Management", DatasetType.COMPLETE, project, "", "", null,
					null);
			smd.setKeywords("study setup");
			smd.setCollectorType(Dataset.STUDY_MANAGEMENT);
			smd.save();
			project.refresh();
		}

		Dataset studyManagementDS = project.getStudyManagementDataset().get();
		CompleteDS cds = (CompleteDS) datasetConnector.getDatasetDS(studyManagementDS);
		List<TimedMedia> files = cds.getFiles();
		// fix uploaded files
		files.stream().forEach(f -> {
			if (f.caption.isEmpty()) {
				f.caption = "other";
			}
		});

		// create sign-up link
		String recruitmentToken = tokenResolverUtil.getParticipationToken(project.getId(), -1l);
		String viewLink = routes.ParticipationController.recruit(recruitmentToken).absoluteURL(request, true);
		String anonViewLink = routes.ParticipationController.recruitAnon(recruitmentToken).absoluteURL(request, true);

		// create telegram signup link
		final String telegramLink;
		if (!telegramBotUtils.getBotUsername().isEmpty()) {
			telegramLink = "https://t.me/" + telegramBotUtils.getBotUsername();
		} else {
			telegramLink = "";
		}

		return ok(views.html.elements.project.studyManagement.render(project, files, viewLink, anonViewLink,
				telegramLink, message, csrfToken(request)));
	}

	@AddCSRFToken
	public Result openSignup(Request request, Long id, boolean signupOpen) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// signup could be done only if user is owner or collaborator
		if (!project.editableBy(username)) {
			return redirect(PROJECT(id)).addingToSession(request, "error",
					"You need to be project owner or collaborator.");
		}

		// reopen the project
		project.setSignupOpen(signupOpen);
		project.update();

		// (de-)install Telegram project token
		if (project.getStudyManagementDataset().isPresent()) {
			Dataset studyManagementDS = project.getStudyManagementDataset().get();
			if (project.isSignupOpen()) {
				studyManagementDS.getConfiguration().put(Dataset.TELEGRAM_PROJECT_TOKEN,
						TelegramBotUtils.generateTelegramProjectPIN(id));
			} else {
				studyManagementDS.getConfiguration().remove(Dataset.TELEGRAM_PROJECT_TOKEN);
			}
			studyManagementDS.update();
		}

		return studyManagement(request, id, "Project sign-up " + (project.isSignupOpen() ? "open now" : "closed now"));
	}

	@AddCSRFToken
	public Result selectParticipantDashboard(Request request, Long id, Long ds_id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(routes.HomeController.logout()).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// reopen could be done only if user is owner
		if (!project.editableBy(username)) {
			return redirect(PROJECT(id)).addingToSession(request, "error",
					"You need to be project owner or collaborator.");
		}

		Optional<Dataset> optDS = project.getDatasets().stream()
				.filter(ds -> ds.getId().equals(ds_id) && ds.getDsType() == DatasetType.COMPLETE && ds.isActive())
				.findFirst();
		if (optDS.isPresent()) {
			// first uncheck the previous one
			project.getDatasets().stream().filter(ds -> Dataset.PARTICIPANT_DASHBOARD_PAGE.equals(ds.getTargetObject())
					&& ds.getDsType() == DatasetType.COMPLETE).forEach(cd -> {
						cd.setTargetObject("");
						cd.update();
					});

			// then set new
			Dataset ds = optDS.get();
			ds.setTargetObject(Dataset.PARTICIPANT_DASHBOARD_PAGE);
			ds.update();
			return studyManagement(request, id, "Participant dashboard configured.");
		} else {
			if (ds_id == 0) {
				// first uncheck the previous one
				project.getDatasets().stream()
						.filter(ds -> Dataset.PARTICIPANT_DASHBOARD_PAGE.equals(ds.getTargetObject())
								&& ds.getDsType() == DatasetType.COMPLETE)
						.forEach(cd -> {
							cd.setTargetObject("");
							cd.update();
						});
				return studyManagement(request, id, "Participant dashboard unlinked.");
			} else {
				return notFound("Participant dashboard dataset not found.");
			}
		}
	}

	@AddCSRFToken
	public Result selectParticipantStudy(Request request, Long id, Long ds_id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(routes.HomeController.logout()).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// reopen could be done only if user is owner
		if (!project.editableBy(username)) {
			return redirect(PROJECT(id)).addingToSession(request, "error",
					"You need to be project owner or collaborator.");
		}

		Optional<Dataset> optDS = project.getDatasets().stream()
				.filter(ds -> ds.getId().equals(ds_id) && ds.getDsType() == DatasetType.COMPLETE && ds.isActive())
				.findFirst();
		if (optDS.isPresent()) {
			// first uncheck the previous one
			project.getDatasets().stream().filter(ds -> Dataset.PARTICIPANT_STUDY_PAGE.equals(ds.getTargetObject())
					&& ds.getDsType() == DatasetType.COMPLETE).forEach(cd -> {
						cd.setTargetObject("");
						cd.update();
					});

			// then set new
			Dataset ds = optDS.get();
			ds.setTargetObject(Dataset.PARTICIPANT_STUDY_PAGE);
			ds.update();
			return studyManagement(request, id, "Participant study page configured.");
		} else {
			if (ds_id == 0) {
				// uncheck the previous one
				project.getDatasets().stream().filter(ds -> Dataset.PARTICIPANT_STUDY_PAGE.equals(ds.getTargetObject())
						&& ds.getDsType() == DatasetType.COMPLETE).forEach(cd -> {
							cd.setTargetObject("");
							cd.update();
						});
				return studyManagement(request, id, "Participant study dataset unlinked.");
			} else {
				return notFound("Participant study dataset not found.");
			}
		}

	}

	@AddCSRFToken
	public Result studyManagementAddFile(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(routes.HomeController.logout()).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// reopen could be done only if user is owner
		if (!project.editableBy(username)) {
			return redirect(PROJECT(id)).addingToSession(request, "error",
					"You need to be project owner or collaborator.");
		}

		Optional<Dataset> studyManagementDataset = project.getStudyManagementDataset();
		if (studyManagementDataset.isPresent()) {
			try {

				Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
				if (body == null) {
					return redirect(HOME).addingToSession(request, "error", "Bad request.");
				}

				DynamicForm df = formFactory.form().bindFromRequest(request);
				if (df == null) {
					return redirect(HOME).addingToSession(request, "error", "Expecting some data.");
				}

				List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
				if (!fileParts.isEmpty()) {
					final Dataset dataset = studyManagementDataset.get();
					final CompleteDS cpds = datasetConnector.getTypedDatasetDS(dataset);

					for (int i = 0; i < fileParts.size(); i++) {
						TemporaryFile file = fileParts.get(i).getRef();
						String fileName = nss(fileParts.get(i).getFilename());

						// restrict file type
						if (!FileTypeUtils.looksLikeDocumentFile(fileName)) {
							logger.error("No other file than document files allowed here.");
							continue;
						}

						// store file, add record
						Optional<String> storeFile = cpds.storeFile(file.path().toFile(), fileName);
						if (storeFile.isPresent()) {
							String description = nss(df.get("description"));
							cpds.addRecord(storeFile.get(), description, new Date());
							if (description.equals("informed_consent_form")) {
								dataset.getConfiguration().put(Dataset.INFORMED_CONSENT, storeFile.get());
								dataset.update();
							}
						}
					}

					LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.DATA,
							"Files uploaded to dataset: " + dataset.getName(), project);
				}
			} catch (NullPointerException e) {
				logger.error("Error in adding a study management file.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		}

		return studyManagement(request, id);
	}

	@AddCSRFToken
	public Result studyManagementRemoveFile(Request request, Long id, Long fileId, String fileName) {
		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(routes.HomeController.logout()).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// reopen could be done only if user is owner
		if (!project.editableBy(username)) {
			return redirect(PROJECT(id)).addingToSession(request, "error",
					"You need to be project owner or collaborator.");
		}

		Optional<Dataset> studyManagementDataset = project.getStudyManagementDataset();
		if (studyManagementDataset.isPresent()) {
			Dataset dataset = studyManagementDataset.get();
			completeDSController.delete(request, dataset.getId(), fileId);

			// if the informed consent is deleted, store this information in the dataset configuration
			if (dataset.configuration(Dataset.INFORMED_CONSENT, "").equals(fileName)) {
				// TODO find other informed consent in dataset and link that or remove:
				dataset.getConfiguration().remove(Dataset.INFORMED_CONSENT);
			}
		}

		return studyManagement(request, id);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result requestReview(Request request, Long id) {

		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(routes.HomeController.logout()).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		// reopen could be done only if user is owner
		if (!project.belongsTo(user)) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "You need to be project owner.");
		}

		// random selection of reviewer
		final List<String> reviewUsers = config.getStringList(ConfigurationUtils.DF_USERS_REVIEWERS);
		if (reviewUsers == null || reviewUsers.isEmpty()) {
			return redirect(PROJECT(id));
		}

		final String username = user.getEmail();

		// pick random reviewer
		String email = username;
		for (int i = 0; i < 5 && email.equals(username); i++) {
			email = reviewUsers.get((int) Math.round(Math.random() * (reviewUsers.size() - 1)));
		}

		// find reviewer
		Person reviewer = Person.findByEmail(email).get();
		if (reviewer == null) {
			return redirect(PROJECT(id));
		}

		// send email to reviewer, redirect to view
		String actionLink = routes.ReviewController
				.view(tokenResolverUtil.getReviewToken(project.getId(), reviewer.getId())).absoluteURL(request, true);
		Html htmlBody = views.html.emails.invite.render("Review request",
				String.format(
						"%s would like to invite you to review the project %s. "
								+ "If you are interested, just click the button below. "
								+ "If not, please ignore this message (trash it to self-destruct).",
						user.getName(), project.getName()),
				actionLink);
		String textBody = String.format(
				"Hello! \n\n%s would like to invite you to collaborate on the project %s. "
						+ "If you are interested, just click the link below. "
						+ "If not, please ignore this message (trash it to self-destruct). \n\n%s \n\n",
				user.getName(), project.getName(), actionLink);

		notificationService.sendMail(email, textBody, htmlBody, actionLink, "[ID Data Foundry] Review request",
				username, user.getName(), "Review request: " + actionLink);

		return redirect(PROJECT(id)).addingToSession(request, "message", "Ok, we sent a review request to " + email);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	public Result wearable(Request request, long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check if project exists
		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return redirect("/");
		}

		Project project = wearable.getProject();

		// only show if user is owner or
		if (username.isEmpty()) {
			if (!project.isPublicProject()) {
				return redirect(LANDING);
			}
		} else {
			// check if user exists
			Person user = Person.findByEmail(username).get();
			if (user == null) {
				return redirect(PROJECT(project.getId()));
			}

			// check ownership
			if (!project.belongsTo(username) && !project.collaboratesWith(username)
					&& !project.subscribedBy(username)) {
				// render project view
				return redirect(PROJECT(project.getId()));
			} else {
				// TODO data set access for guests
			}
		}

		if (wearable.getBrand() == null) {
			return redirect(controllers.routes.FitbitWearablesController.view(id));
		}
		// redirect to the right data set
		switch (wearable.getBrand()) {
		case Wearable.FITBIT:
			return redirect(controllers.routes.FitbitWearablesController.view(id));
		case Wearable.GOOGLEFIT:
			return redirect(controllers.routes.GoogleFitWearablesController.view(id));
		default:
			return redirect(PROJECT(project.getId()));
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result publish(Request request, long id) {
		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(routes.HomeController.logout()).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}
		if (!user.canEdit(project)) {
			return redirect(PROJECT(id)).addingToSession(request, "error",
					"You don't have permissions to export or publish this project.");
		}

		// ensure the collaborators are loaded and can be accessed
		project.getCollaborators().stream().forEach(c -> c.getCollaborator().refresh());

		return ok(views.html.projects.publish.render(project, user, csrfToken(request)));
	}

	/**
	 * export the metadata of this project and all the datasets in it
	 * 
	 * @param id
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> publishProject(Request request, Long id) {
		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(routes.HomeController.logout()).addingToSession(request, "error", "User not found."));

		return CompletableFuture.<Result>supplyAsync(() -> {
			// check if project exists
			Project project = Project.find.byId(id);
			if (project == null) {
				return redirect(HOME).addingToSession(request, "error", "Project not found.");
			}
			if (!user.canEdit(project)) {
				return redirect(PROJECT(id)).addingToSession(request, "error",
						"You don't have permissions to export or publish this project.");
			}

			DynamicForm df = formFactory.form().bindFromRequest(request);

			// set user ORCID if provided
			String orcid = nss(df.get("author_orcid_" + user.getId()), 20);
			if (!orcid.isEmpty()) {
				user.setIdentityProperty("orcid", orcid);
				user.update();
			}

			// set collaborator ORCID if provided AND there is none set for the collaborator yet
			project.getCollaborators().stream().map(c -> c.getCollaborator()).forEach(c -> {
				c.refresh();
				String collaboratorOrcid = nss(df.get("author_orcid_" + c.getId()), 20);
				if (!collaboratorOrcid.isEmpty() && c.getIdentityProperty("orcid").isEmpty()) {
					c.setIdentityProperty("orcid", collaboratorOrcid);
					c.update();
				}
			});

			// set project fields with new data
			project.setIntro(htmlTagEscape(nss(df.get("intro"))));
			project.setLicense(nss(df.get("license"), 64));
			project.setKeywords(htmlTagEscape(nss(df.get("keywords"))));
			project.setRelation(nss(df.get("relation")));
			project.setOrganization(htmlTagEscape(nss(df.get("organization"))));
			project.setRemarks(htmlTagEscape(nss(df.get("remarks"))));
			project.update();

			// prepare list of datasets to export
			Map<String, String> allData = df.rawData();
			List<String> datasetIds = allData.keySet().stream().filter(s -> s.startsWith("exportDataset"))
					.map(s -> df.get(s)).collect(Collectors.toList());
			List<Dataset> datasetsToExport = utils.DatasetUtils.scriptsExcluded(project.getOperationalDatasets())
					.stream().filter(ds -> datasetIds.contains(ds.getId().toString())).collect(Collectors.toList());

			try {
				// just sleep to allow for the user to see the progress bar
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}

			// check export option
			String exportOption = (String) df.value("exportOption").orElse("export");
			if (exportOption.equals("export")) {
				// return zip file
				TemporaryFile tf = lifeCycleService.exportProject(Optional.of(request), project, datasetsToExport);
				String token = UUID.randomUUID().toString();
				cache.set("publishingStatus_" + token, tf.path().toFile().getAbsolutePath(), 30);
				return ok("""
						<p>
							🌟 All clear, download should be starting...
						</p>
						<script>
							// Trigger download
							const link = document.createElement("a");
							link.style.display = "none";
							link.href = "%s";
							link.download = "project_export.zip";
							document.body.appendChild(link);
							link.click();

							// Cleanup
							setTimeout(() => {
								link.remove();
							}, 100);
						</script>
						""".formatted(routes.DatasetsController.downloadTemporaryFile(token)));
			} else {
				// check the publishing key
				String zenodoAccessToken = nss(df.get("publishKey"));
				if (zenodoAccessToken.isEmpty()) {
					return ok("""
							<p class="error">
								❌ Zenodo access token is missing. Please provide a valid token.
							</p>
							""");
				}

				// store the access token in the user identity
				user.setIdentityProperty("zenodoAccessToken", zenodoAccessToken);
				user.update();

				// run the publish operation
				String token = UUID.randomUUID().toString();
				cache.set("publishingStatus_" + token, "", 300);
				lifeCycleService.publish(Optional.of(request), project, datasetsToExport, "publishingStatus_" + token,
						zenodoAccessToken);
				return ok("""
						<div hx-get="%s" hx-trigger="every 500ms" hx-swap="innerHTML"></div>
						""".formatted(routes.ProjectsController.publishStatus(token, id)));
			}
		});
	}

	/**
	 * return publishing status
	 * 
	 * @param token
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result publishStatus(String token, long id) {
		return cache.get("publishingStatus_" + token).map(str -> {
			String cachedStatus = (String) str;
			if (cachedStatus.contains("done")) {
				return status(286, cachedStatus);
			} else {
				return ok(cachedStatus);
			}
		}).orElse(notFound());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result search(Request request, String query, String filterStr) {
		List<Project> projects = searchService.search(nss(query));

		// check the filter
		final DatasetType filter;
		switch (nss(filterStr)) {
		case "iot":
			filter = DatasetType.IOT;
			break;

		case "movement":
			filter = DatasetType.MOVEMENT;
			break;

		case "survey":
			filter = DatasetType.FORM;
			break;

		case "media":
			filter = DatasetType.MEDIA;
			break;
		default:
			filter = null;
		}

		// check if user exists
		List<Project> filteredProjects = projects.stream()
				.filter(p -> p.isPublicProject() && p.isActiveOrHasRecentlyEnded() && p.isDFNativeProject()
						&& (filter == null || p.getAllDatasetTypeSet().contains(filter)))
				.collect(Collectors.toList());
		return ok(views.html.projects.search.render(query, filterStr, filteredProjects));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@AddCSRFToken
	public Result license(Request request, Long id, String redirectUrl) {

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		String organization = ConfigurationUtils.configure(config, ConfigurationUtils.DF_LINKS_ORGANIZATION, "");
		String scientificIntegrityLink = ConfigurationUtils.configure(config,
				ConfigurationUtils.DF_LINKS_SCIENTIFIC_INTEGRITY, "");
		return ok(views.html.projects.license.render(project, organization, scientificIntegrityLink, redirectUrl,
				csrfToken(request)));
	}

	@RequireCSRFCheck
	public Result acceptLicense(Request request, Long id, String redirectUrl) {

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return redirect(HOME).addingToSession(request, "error", "Project not found.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(routes.ProjectsController.license(id, redirectUrl)).addingToSession(request, "error",
					"Expecting some data");
		}

		String accepted = df.get("acceptLicense");
		if (accepted == null || !accepted.equals("on")) {
			return redirect(PROJECT(id));
		}

		// redirect and set flag that the license was accepted
		return redirect("/" + redirectUrl).addingToSession(request, "license_p_" + project.getId(), "accepted");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> timeseries(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "User not found."));

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "Project not found."));
		}

		if (!project.belongsTo(username)) {
			return cs(() -> redirect(PROJECT(id)).addingToSession(request, "error", "You need to be project owner."));
		}

		ClusterDS clds = new ClusterDS(datasetConnector);

		// serve this stream with 200 OK
		return CompletableFuture.supplyAsync(() -> createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> clds.timeseries(sourceActor, project));
			return sourceActor;
		})).thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	public Result labnotes(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, noContent());

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			// show nothing
			return ok();
		}

		// check for open project if user is empty
		if (username.isEmpty()) {
			if (!project.isPublicProject()) {
				// show nothing
				return ok();
			}

			// render public project view
			List<LabNotesEntry> entries = LabNotesEntry.findByProject(id);
			return ok(views.html.projects.labnotes.render(entries));
		}

		// check project ownership
		if (!project.visibleFor(username)) {
			// show nothing
			return ok();
		}

		List<LabNotesEntry> entries = LabNotesEntry.findByProject(id);
		return ok(views.html.projects.labnotes.render(entries));
	}

	@AddCSRFToken
	public Result projectApiAccess(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, noContent());

		// check if project exists and belongs to user
		Project project = Project.find.byId(id);
		if (project == null || !project.belongsTo(user)) {
			return noContent();
		}

		// retrieve settings for the API access
		ProjectAPIInfo pai = managedAIAPIService.getProjectAPIAccess(user, project);

		return ok(views.html.projects.projectAPIAccess.render(project.getId(), pai, csrfToken(request)));
	}

	@AddCSRFToken
	public Result activateOpenAIApiAccess(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, noContent());

		// check if project exists and belongs to user
		Project project = Project.find.byId(id);
		if (project == null || !project.belongsTo(user)) {
			return noContent();
		}

		// update key and retrieve settings for the API access
		ProjectAPIInfo pai = managedAIAPIService.activateProjectAPIAccess(user, project);

		return ok(views.html.projects.projectAPIAccess.render(project.getId(), pai, csrfToken(request)));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////

	private void sendConfirmMail(Project project, Person user, String subject, String description,
			String redirectLink) {
		// otherwise send email
		String email = project.getOwner().getEmail();
		// String actionLink = routes.ProjectsController
		// .addSubscription(id, tokenResolverUtil.getSubscriptionToken(project.id, user.id))
		// .absoluteURL(request());

		Html htmlBody = views.html.emails.confirmed.render(subject,
				String.format("%s, we would like to inform you, %s", user.getName(), description), redirectLink);

		String textBody = String.format(
				"Hello! \n\n%s, we would like to inform you, %s "
						+ "You can click the link below to view more details.\n\n%s\n\n",
				user.getName(), description, redirectLink);

		notificationService.sendMail(email, textBody, htmlBody, redirectLink, "[ID Data Foundry] " + subject, null,
				"ID Data Foundry", subject + ": " + project.getOwner().getName());
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
						List<String> lines = com.google.common.io.Files.readLines(f, Charset.defaultCharset());
						return mdr.render(lines.stream().collect(Collectors.joining("\n")));
					} catch (IOException e) {
						logger.error("Error in preparing the announcement.", e);
						return null;
					}
				}).filter(s -> s != null && !s.isEmpty()).collect(Collectors.joining("</p><p>"));

		return result.isEmpty() ? "" : ("<p>" + result + "</p>");
	}
}

package controllers.swagger;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.typesafe.config.Config;

import controllers.auth.V2UserApiAuth;
import datasets.DatasetConnector;
import jakarta.inject.Singleton;
import models.Collaboration;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.Subscription;
import play.Logger;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.data.validation.Constraints.EmailValidator;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import play.twirl.api.Html;
import services.email.NotificationService;
import utils.auth.TokenResolverUtil;
import utils.conf.ConfigurationUtils;
import utils.export.MetaDataUtils;

@Singleton
public class ProjectApiController extends AbstractApiController {

	private final NotificationService notificationService;
	private static final Logger.ALogger logger = Logger.of(ProjectApiController.class);

	private final int MAX_ACTIVE_PROJECTS;

	@Inject
	public ProjectApiController(Config configuration, FormFactory formFactory, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolverUtil, NotificationService notificationService) {
		super(formFactory, datasetConnector, tokenResolverUtil);

		this.notificationService = notificationService;
		this.MAX_ACTIVE_PROJECTS = ConfigurationUtils.configureInt(configuration,
				ConfigurationUtils.DF_MAX_ACTIVE_PROJECTS, 20);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result projects(Request request) {

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return forbidden(errorJSONResponseObject("No user account given or user is not logged in."));
		}

		// find all owned projects
		List<Project> projects = user.projects();

		// find all collaboration projects
		List<Project> collaborations = user.collaborations();

		// find all subscription projects
		List<Project> subscriptions = user.subscriptions();

		// find all archived projects
		List<Project> archivedProjects = user.archivedProjects();

		ObjectNode on = Json.newObject();
		// ObjectNode data = on.putObject("data");
		{
			ArrayNode projectNode = on.putArray("ownProjects");
			projects.stream().forEach(p -> {
				if (!p.isArchivedProject()) {
					projectNode.add(p.getId());
				}
			});
		}
		{
			ArrayNode projectNode = on.putArray("collaborations");
			collaborations.stream().forEach(p -> projectNode.add(p.getId()));
		}
		{
			ArrayNode projectNode = on.putArray("subscriptions");
			subscriptions.stream().forEach(p -> projectNode.add(p.getId()));
		}
		{
			ArrayNode projectNode = on.putArray("archivedProjects");
			archivedProjects.stream().forEach(p -> projectNode.add(p.getId()));
		}

		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result project(Request request, long id) {

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}

		// check for open project if user is empty
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			if (!project.isPublicProject()) {
				return forbidden(
						errorJSONResponseObject("No user account given, or project not publically accessible."));
			}

			// render public project view
			return ok(toJSON(project));
		}

		// check ownership
		if (project.belongsTo(user) || project.collaboratesWith(user) || project.subscribedBy(user)) {
			// render project as JSON
			return ok(toJSON(project));
		}

		return forbidden(errorJSONResponseObject("Project not accessible for given user account."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result labnotes(Request request, long id) {

		// check for open project if user is empty
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return forbidden(errorJSONResponseObject("No user account given."));
		}

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}

		// check ownership
		if (!project.visibleFor(user)) {
			return forbidden(errorJSONResponseObject("Project not accessible for given user account."));
		}

		ArrayNode an = Json.newArray();
		LabNotesEntry.findByProject(id).stream().forEach(ln -> an.add(MetaDataUtils.toJson(ln)));

		// render project labnotes as JSON
		return ok(an);
	}

	/**
	 * API search for projects
	 * 
	 * @param query
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result searchProjects(Request request, String query) {
		// convert to lower case
		query = query.trim().toLowerCase();

		final List<Project> projects;
		// search for non-empty query
		if (query.length() > 0) {
			projects = Project.find.query().where().or().contains("LOWER(name)", query).contains("LOWER(intro)", query)
					.contains("LOWER(description)", query).contains("LOWER(keywords)", query)
					.contains("LOWER(relation)", query).contains("LOWER(organization)", query)
					.contains("LOWER(remarks)", query).contains("LOWER(license)", query).endOr().findList();
		} else {
			// add public projects for empty query
			projects = Project.find.query().findList();
		}

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));

		// compile data
		ObjectNode on = okJSONResponseObject();
		ObjectNode data = on.putObject("data");
		if (user == null) {
			List<Project> filteredProjects = projects.stream().filter(p -> p.isPublicProject())
					.collect(Collectors.toList());

			ArrayNode projectsNode = data.putArray("projects");
			filteredProjects.stream().forEach(p -> projectsNode.add(p.getId()));
		} else {
			List<Project> filteredProjects = projects.stream().filter(p -> p.isPublicProject() || p.visibleFor(user))
					.collect(Collectors.toList());

			ArrayNode projectsNode = data.putArray("projects");
			filteredProjects.stream().forEach(p -> projectsNode.add(p.getId()));
		}

		return ok(on);
	}

	/**
	 * @return id of newly added project
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result add(Request request) {

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}

		// check how many projects user has already
		if (user.projects().stream().filter(p -> !p.isArchivedProject()).count() >= MAX_ACTIVE_PROJECTS) {
			return forbidden(errorJSONResponseObject("You cannot have more than " + MAX_ACTIVE_PROJECTS
					+ " active projects at a time. Consider archiving inactive projects first before creating a new project."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		if (!nnne(df.get("name")) || !nnne(df.get("intro"))) {
			return badRequest(errorJSONResponseObject("Name and intro are required. Please check "));
		}

		Project project = Project.create(nss(df.get("name")), user, nss(df.get("intro")), false, false);

		switch (df.get("license")) {
		case "MIT license":
			project.setLicense("MIT");
			break;
		case "CreativeCommons Attribution":
			project.setLicense("CC BY");
			break;
		case "CreativeCommons Attribution ShareAlike":
			project.setLicense("CC BY-SA");
			break;
		case "CreativeCommons Attribution-NoDerivs":
			project.setLicense("CC BY-ND");
			break;
		case "CreativeCommons Attribution-NonCommercial":
			project.setLicense("CC BY-NC");
			break;
		case "CreativeCommons Attribution-NonCommercial-ShareAlike":
			project.setLicense("CC BY-NC-SA");
			break;
		case "CreativeCommons Attribution-NonCommercial-NoDerivs":
			project.setLicense("CC BY-NC-ND");
			break;
		default:
			project.setLicense("MIT");
		}

		// metadata fields
		if (df.get("keywords") != null) {
			project.setKeywords(df.get("keywords"));
		}
		if (df.get("doi") != null) {
			project.setDoi(df.get("doi"));
		}
		if (df.get("relation") != null) {
			project.setRelation(df.get("relation"));
		}
		if (df.get("organization") != null) {
			project.setOrganization(df.get("organization"));
		}
		if (df.get("remarks") != null) {
			project.setRemarks(df.get("remarks"));
		}
		project.save();

		LabNotesEntry.log(ProjectApiController.class, LabNotesEntryType.CREATE, "Project created: " + project.getName(),
				project);

		ObjectNode on = okJSONResponseObject();
		on.put("id", project.getId());
		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result edit(Request request, Long id) {

		// check project
		Project project = Project.find.byId(id);
		if (project == null) {
			return badRequest(errorJSONResponseObject("No project available."));
		}
		if (project.isArchivedProject()) {
			return forbidden(errorJSONResponseObject("Editing an archived project is forbidden."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}
		if (!project.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("The project does not belong to the user."));
		}

		// check data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		if (df.get("name") != null) {
			project.setName(df.get("name"));
		}
		if (df.get("intro") != null) {
			project.setIntro(df.get("intro"));
		}
		if (df.get("isPublic") != null) {
			project.setPublicProject(df.get("isPublic").equals("true") ? true : false);
		}
		if (df.get("isShareable") != null) {
			project.setShareableProject(df.get("isShareable").equals("true") ? true : false);
		}

		if (df.get("license") != null) {
			switch (df.get("license")) {
			case "MIT license":
				project.setLicense("MIT");
				break;
			case "CreativeCommons Attribution":
				project.setLicense("CC BY");
				break;
			case "CreativeCommons Attribution ShareAlike":
				project.setLicense("CC BY-SA");
				break;
			case "CreativeCommons Attribution-NoDerivs":
				project.setLicense("CC BY-ND");
				break;
			case "CreativeCommons Attribution-NonCommercial":
				project.setLicense("CC BY-NC");
				break;
			case "CreativeCommons Attribution-NonCommercial-ShareAlike":
				project.setLicense("CC BY-NC-SA");
				break;
			case "CreativeCommons Attribution-NonCommercial-NoDerivs":
				project.setLicense("CC BY-NC-ND");
				break;
			default:
				project.setLicense("MIT");
			}
		}

		// metadata fields
		if (df.get("keywords") != null) {
			project.setKeywords(df.get("keywords"));
		}
		if (df.get("doi") != null) {
			project.setDoi(df.get("doi"));
		}
		if (df.get("relation") != null) {
			project.setRelation(df.get("relation"));
		}
		if (df.get("organization") != null) {
			project.setOrganization(df.get("organization"));
		}
		if (df.get("remarks") != null) {
			project.setRemarks(df.get("remarks"));
		}
		project.save();

		LabNotesEntry.log(ProjectApiController.class, LabNotesEntryType.MODIFY, "Project edited: " + project.getName(),
				project);

		return ok(okJSONResponseObject("Information updated."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result archive(Request request, long id) {

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return badRequest(errorJSONResponseObject("This project is archived already."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}

		// check ownership
		if (!project.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("The project does not belong to the user."));
		}

		// archive the project
		project.setArchivedProject(true);
		project.update();

		return ok(okJSONResponseObject("Project archived."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result reopen(Request request, long id) {

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == false) {
			return badRequest(errorJSONResponseObject("This project is opened already."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}

		// check ownership
		if (!project.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("The project does not belong to the user."));
		}

		// archive the project
		project.setArchivedProject(false);
		project.update();

		return ok(okJSONResponseObject("Project reopened."));
	}

	/**
	 * return OK if the project is shareable; else return an token for the project owner to aproof the application
	 * 
	 * @param id
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result subscribe(Request request, long id) {
		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return forbidden(errorJSONResponseObject("Any changes to an archived project is not allowed."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}

		// check whether subscription is allowed
		if (!user.canSubscribe(project)) {
			return forbidden(errorJSONResponseObject("Subsciption is not available."));
		}

		// if project is shareable, create subscription directly
		if (project.isShareableProject()) {
			Subscription s = new Subscription(user, project);
			s.save();
			project.getSubscribers().add(s);
			project.update();

			ObjectNode on = okJSONResponseObject();
			on.put("id", project.getId());
			return ok(on);
		} else {
			// otherwise send email
			String email = project.getOwner().getEmail();
			String actionLink = controllers.routes.ProjectsController
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

			return ok(okJSONResponseObject("Email for subscription sent."));
		}
	}

	/**
	 * add a subscription to a project
	 * 
	 * @param id
	 * @param token
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result addSubscription(Request request, long id, String token) {
		// check if project
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return forbidden(errorJSONResponseObject("Any changes to an archived project is not allowed."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}
		// check ownership
		if (!project.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Subsciption is not available."));
		}

		// check subscriber
		Long subscriberId = tokenResolverUtil.getSubscriberIdFromSubscriptionToken(token);
		Person subscriber = Person.find.byId(subscriberId);
		if (subscriber == null) {
			logger.warn("Subscription-Token problem: " + tokenResolverUtil.getRawSubscriptionToken(token));
			return notFound(errorJSONResponseObject("Not valid subscriber."));
		}

		// check collaboration and subscription
		if (project.collaboratesWith(user) || project.subscribedBy(user)) {
			return badRequest(errorJSONResponseObject(
					"Subscriber has already subscribed to or been a collaborator in this project."));
		}

		// create subscription
		Subscription s = new Subscription(subscriber, project);
		s.save();
		project.getSubscribers().add(s);
		project.update();

		return ok(okJSONResponseObject());
	}

	/**
	 * unsubscribe a project by subscriber
	 * 
	 * @param id
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result unsubscribe(Request request, long id) {
		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return forbidden(errorJSONResponseObject("Any changes to an archived project is not allowed."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null || project.collaboratesWith(user) || !project.subscribedBy(user)) {
			return badRequest(errorJSONResponseObject("Not valid user or project."));
		}

		// check for existing subscription (that can be removed)
		Optional<Subscription> sub = project.getSubscribers().stream().filter(c -> c.getSubscriber().equals(user))
				.findFirst();
		if (!sub.isPresent()) {
			return badRequest(errorJSONResponseObject("Subscription not found."));
		}

		// remove subscription
		Subscription s = sub.get();
		s.delete();
		// s.delete();
		// project.subscribers.remove(s);
		// project.update();

		return ok(okJSONResponseObject("Unsubscribe sucessfully."));
	}

	/**
	 * remove a subscription by project owner
	 * 
	 * @param id
	 * @param subscriberId
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result removeSubscription(Request request, long id, long subscriberId) {
		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return forbidden(errorJSONResponseObject("Any changes to an archived project is not allowed."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}
		if (!project.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("The project does not belong to the user."));
		}

		// check subscriber
		Person subscriber = Person.find.byId(subscriberId);
		if (subscriber == null) {
			return notFound(errorJSONResponseObject("Not valid subscriber."));
		}

		// check for existing subscription (that can be removed)
		Optional<Subscription> sub = project.getSubscribers().stream().filter(c -> c.getSubscriber().equals(subscriber))
				.findFirst();
		if (!sub.isPresent()) {
			return badRequest(errorJSONResponseObject("Subscription not found."));
		}

		// remove subscription
		Subscription s = sub.get();
		s.delete();
		project.getSubscribers().remove(s);
		project.update();

		return ok(okJSONResponseObject("Subscription removed."));
	}

	/**
	 * return OK if the invitation email is sent
	 * 
	 * @param id
	 * @param email
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result inviteCollaborator(Request request, long id, String email) {

		// first normalize the email address
		email = email.toLowerCase();

		// check email address
		if (!new EmailValidator().isValid(email)) {
			return badRequest(
					errorJSONResponseObject("It seems something wrong with the email address, please check."));
		}

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return forbidden(errorJSONResponseObject("Any changes to an archived project is not allowed."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}

		// check whether subscription is allowed
		if (project.collaboratesWith(email) || !project.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Invitation is not available."));
		}

		String actionLink = controllers.routes.ProjectsController
				.addCollaboration(id, tokenResolverUtil.getCollaborationToken(project.getId(), email))
				.absoluteURL(request, true);
		// String actionLink = controllers.swagger.routes.ProjectApiController
		// .addCollaboration(id, tokenResolverUtil.getCollaborationToken(project.id, email))
		// .absoluteURL(request());
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

		return ok(okJSONResponseObject("Email for invitation has been sent."));
	}

	/**
	 * add a collaboration to a project
	 * 
	 * @param id
	 * @param token
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result addCollaboration(Request request, long id, String token) {

		// check if project
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return forbidden(errorJSONResponseObject("Any changes to an archived project is not allowed."));
		}

		// check collaborator
		Person collaborator = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (collaborator == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}
		// check ownership
		if (project.belongsTo(collaborator) || project.collaboratesWith(collaborator)) {
			return forbidden(errorJSONResponseObject("This invitation is not available."));
		}

		// check email
		String collaboratorEmail = tokenResolverUtil.getCollaboratorEmailFromCollaborationToken(token);
		if (!collaboratorEmail.equals(collaborator.getEmail())) {
			logger.warn("Collab-Token problem: " + tokenResolverUtil.getRawCollabToken(token));
			return notFound(errorJSONResponseObject("Not valid collaborator."));
		}

		// create collaboration
		Collaboration s = new Collaboration(collaborator, project);
		s.save();
		project.getCollaborators().add(s);
		project.update();

		return ok(okJSONResponseObject());
	}

	/**
	 * cancel collaboaration to a project by collaborator
	 * 
	 * @param id
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result cancelCollaboration(Request request, long id) {

		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return forbidden(errorJSONResponseObject("Any changes to an archived project is not allowed."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null || !project.collaboratesWith(user)) {
			return badRequest(errorJSONResponseObject("No user available."));
		}

		// check for existing collaboration (that can be removed)
		Optional<Collaboration> col = project.getCollaborators().stream().filter(c -> c.getCollaborator().equals(user))
				.findFirst();
		if (!col.isPresent()) {
			return badRequest(errorJSONResponseObject("Collaboration not found."));
		}

		// remove collaboration
		Collaboration c = col.get();
		c.delete();
		// c.delete();
		// project.collaborators.remove(c);
		// project.update();

		return ok(okJSONResponseObject("Collaboration cancelled"));
	}

	/**
	 * remove a collaborator by project owner
	 * 
	 * @param id
	 * @param collaboratorId
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result removeCollaborator(Request request, long id, long collaboratorId) {
		// check if project exists
		Project project = Project.find.byId(id);
		if (project == null) {
			return notFound(errorJSONResponseObject("No project found for given ID."));
		}
		if (project.isArchivedProject() == true) {
			return forbidden(errorJSONResponseObject("Any changes to an archived project is not allowed."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("No user available."));
		}
		if (!project.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("The project does not belong to the user."));
		}

		// check subscriber
		Person subscriber = Person.find.byId(collaboratorId);
		if (subscriber == null) {
			return notFound(errorJSONResponseObject("No project found for given subscriber ID"));
		}

		// check for existing collaborator (that can be removed)
		Optional<Collaboration> col = project.getCollaborators().stream()
				.filter(c -> c.getCollaborator().equals(subscriber)).findFirst();
		if (!col.isPresent()) {
			return badRequest(errorJSONResponseObject("Collaborator not found."));
		}

		// remove collaboration
		Collaboration c = col.get();
		c.delete();
		project.getCollaborators().remove(c);
		project.update();

		return ok(okJSONResponseObject("Collaborator removed."));
	}

	//////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode toJSON(Project project) {

		ObjectNode newObject = MetaDataUtils.toJson(project);

		// add additional properties
		newObject.set("owner", MetaDataUtils.toJson(project.getOwner()));
		newObject.put("isPublic", project.isPublicProject());
		newObject.put("isShareable", project.isShareableProject());
		newObject.put("isArchived", project.isArchivedProject());

		{
			ArrayNode an = newObject.putArray("collaborators");
			project.getCollaborators().stream().forEach(p -> an.add(MetaDataUtils.toJson(p.getCollaborator())));
		}
		{
			ArrayNode an = newObject.putArray("subscribers");
			project.getSubscribers().stream().forEach(p -> an.add(p.getSubscriber().getId()));
		}
		{
			ArrayNode an = newObject.putArray("datasets");
			project.getDatasets().stream().filter(d -> !d.isScript()).forEach(p -> an.add(p.getId()));
		}
		{
			ArrayNode an = newObject.putArray("scripts");
			project.getOwner().getUserActors(project.getId()).stream().forEach(p -> an.add(p.getId()));
		}
		{
			ArrayNode an = newObject.putArray("participants");
			project.getParticipants().stream().forEach(p -> an.add(p.getId()));
		}
		{
			ArrayNode an = newObject.putArray("devices");
			project.getDevices().stream().forEach(d -> an.add(d.getId()));
		}
		{
			ArrayNode an = newObject.putArray("clusters");
			project.getClusters().stream().forEach(c -> an.add(c.getId()));
		}
		{
			ArrayNode an = newObject.putArray("wearables");
			project.getWearables().stream().forEach(w -> an.add(w.getId()));
		}

		return newObject;
	}
}
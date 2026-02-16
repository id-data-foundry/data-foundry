package controllers.swagger;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.AbstractAsyncController;
import controllers.api.CompleteDSController;
import controllers.auth.V2MultiUserApiAuth;
import controllers.auth.V2UserApiAuth;
import datasets.DatasetConnector;
import io.ebean.ExpressionList;
import jakarta.inject.Singleton;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.ds.MediaDS;
import models.ds.MovementDS;
import models.sr.Participant;
import models.sr.ParticipationStatus;
import models.vm.TimedMedia;
import play.Logger;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.data.validation.Constraints.EmailValidator;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.slack.Slack;
import utils.DateUtils;
import utils.auth.TokenResolverUtil;
import utils.export.MetaDataUtils;
import utils.validators.FileTypeUtils;

@Singleton
public class ApiSpecs extends AbstractAsyncController {

	private static final Logger.ALogger logger = Logger.of(ApiSpecs.class);

	@Inject
	FormFactory formFactory;

	@Inject
	DatasetConnector datasetConnector;

	@Inject
	TokenResolverUtil tokenResolverUtil;

	// @Authenticated(FullApiAuth.class)
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

		// check email
		if (!new EmailValidator().isValid(email)) {
			return notAcceptable(errorJSONResponseObject("The email seems not correct, please check."));
		}

		// check password
		String password = nss(df.get("password"));
		if (password.length() < 8) {
			return forbidden(errorJSONResponseObject("Password is too short. We need at least 8 characters."));
		}
		if (!password.equals(df.get("re_password"))) {
			return forbidden(errorJSONResponseObject("Password does not match."));
		}

		// check essential fields
		String firstName = nss(df.get("first_name"));
		String lastName = nss(df.get("last_name"));
		if (firstName.length() < 3 || lastName.length() < 3) {
			return badRequest(
					errorJSONResponseObject("First or last name is too short. We need at least 3 characters."));
		}

		// register user
		String website = df.get("website");
		Person user = Person.register(df.get("user_id"), firstName, lastName, email, website, password, "VDC API");
		user.save();

		// create default container project
		createPublicProjectForUser(user, website);
		createPrivateProjectForUser(user, website);

		return ok(okJSONResponseObject());
	}

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

		// generate a user token (1 hour timeout)
		String token = tokenResolverUtil.createUserAccessToken(user.getId(),
				System.currentTimeMillis() + V2UserApiAuth.API_TOKEN_VALIDITY);

		ObjectNode okJSONResponseObject = okJSONResponseObject();
		okJSONResponseObject.put("api-token", token);
		okJSONResponseObject.put("first_name", user.getFirstname());
		okJSONResponseObject.put("last_name", user.getLastname());
		okJSONResponseObject.put("email", user.getEmail());

		// ok and set the session
		return ok(okJSONResponseObject).addingToSession(request, "username", user.getEmail());
	}

	public Result logout() {
		// clear session, including the username field
		// redirect home with new session
		return ok(okJSONResponseObject()).withNewSession();
	}

	@Authenticated(V2UserApiAuth.class)
	public Result editUser(Request request) {

		Person user = Person.find.byId(getAuthenticatedUserId(request));
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
		if (df.get("last_name") == null) {
			user.setLastname(df.get("last_name"));
			dirty = true;
		}
		// email is not editable as it is the username
		// if (df.get("email") == null) {
		// user.email = df.get("email");
		// dirty = true;
		// }
		if (df.get("password") == null) {
			user.setPassword(df.get("password"));
			dirty = true;
		}
		if (df.get("website") == null) {
			user.setWebsite(df.get("website"));
			dirty = true;
		}

		// update user if changes have been made
		if (dirty) {
			user.update();
		}

		return ok(okJSONResponseObject());
	}

	@Authenticated(V2UserApiAuth.class)
	public Result projects(Request request) {

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedUserId(request));
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

		ObjectNode on = okJSONResponseObject();
		ObjectNode data = on.putObject("data");
		{
			ArrayNode projectNode = data.putArray("ownProjects");
			projects.stream().forEach(p -> projectNode.add(p.getId()));
		}
		{
			ArrayNode projectNode = data.putArray("collaborations");
			collaborations.stream().forEach(p -> projectNode.add(p.getId()));
		}
		{
			ArrayNode projectNode = data.putArray("subscriptions");
			subscriptions.stream().forEach(p -> projectNode.add(p.getId()));
		}
		{
			ArrayNode projectNode = data.putArray("archivedProjects");
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
		Person user = Person.find.byId(getAuthenticatedUserId(request));
		if (user == null) {
			if (!project.isPublicProject()) {
				return forbidden(
						errorJSONResponseObject("No user account given, or project not publically accessible."));
			}

			// render public project view
			ObjectNode response = okJSONResponseObject();
			response.set("data", toJSON(project));
			return ok(response);
		}

		// check ownership
		if (project.belongsTo(user) || project.collaboratesWith(user) || project.subscribedBy(user)) {
			// render project
			ObjectNode response = okJSONResponseObject();
			response.set("data", toJSON(project));
			return ok(response);
		}

		return forbidden(errorJSONResponseObject("Project not accessible for given user account."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result datasets(Request request) {

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedUserId(request));
		if (user == null) {
			return redirect(controllers.routes.HomeController.logout());
		}

		// find all owned projects
		List<Project> projects = user.projects();

		// find all collaboration projects
		List<Project> collaborations = user.collaborations();

		// find all subscription projects
		List<Project> subscriptions = user.subscriptions();

		// find all archived projects
		List<Project> archivedProjects = user.archivedProjects();

		ObjectNode on = okJSONResponseObject();
		ObjectNode data = on.putObject("data");
		{
			ArrayNode projectNode = data.putArray("ownProjects");
			projects.stream().forEach(p -> {
				p.getDatasets().stream().forEach(ds -> projectNode.add(ds.getId()));
			});
		}
		{
			ArrayNode projectNode = data.putArray("collaborations");
			collaborations.stream().forEach(p -> {
				p.getDatasets().stream().forEach(ds -> projectNode.add(ds.getId()));
			});
		}
		{
			ArrayNode projectNode = data.putArray("subscriptions");
			subscriptions.stream().forEach(p -> {
				p.getDatasets().stream().forEach(ds -> projectNode.add(ds.getId()));
			});
		}
		{
			ArrayNode projectNode = data.putArray("archivedProjects");
			archivedProjects.stream().forEach(p -> {
				p.getDatasets().stream().forEach(ds -> projectNode.add(ds.getId()));
			});
		}

		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result dataset(Request request, long id) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect("/");
		}

		Project project = ds.getProject();
		if (!project.isPublicProject()) {
			// only show if user is owner
			Person user = Person.find.byId(getAuthenticatedUserId(request));
			if (user == null) {
				return forbidden(
						errorJSONResponseObject("No user account given, or project not publically accessible."));
			}

			// check ownership
			if (!project.belongsTo(user) && !project.collaboratesWith(user) && !project.subscribedBy(user)) {
				return forbidden(
						errorJSONResponseObject("No user account given, or project not publically accessible."));
			}
		}

		// render dataset
		ObjectNode response = okJSONResponseObject();
		response.set("data", toJSON(ds));
		return ok(response);
	}

	/**
	 * API search for projects
	 * 
	 * @param query
	 * @return
	 */
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
		Person user = Person.find.byId(getAuthenticatedUserId(request));

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
	 * API search for datasets
	 * 
	 * @param filter       "all" for no filtering, otherwise "complete" or "movement"
	 * @param query
	 * @param relation
	 * @param organization
	 * @return
	 */
	public Result searchDatasets(Request request, String filter, String query, String relation, String organization) {
		// convert to lower case
		query = query.trim().toLowerCase();

		// retrieve user if given
		Person user = Person.find.byId(getAuthenticatedUserId(request));

		final List<Project> projects;
		final List<Dataset> datasets;

		// search for non-empty query
		if (query.length() > 0) {
			ExpressionList<Dataset> esdl = Dataset.find.query().where().and().or().contains("LOWER(name)", query)
					.contains("LOWER(description)", query).contains("LOWER(targetObject)", query)
					.contains("LOWER(keywords)", query).contains("LOWER(relation)", query)
					.contains("LOWER(organization)", query).contains("LOWER(remarks)", query)
					.contains("LOWER(license)", query).endOr();
			ExpressionList<Project> espl = Project.find.query().where().and().or().contains("LOWER(name)", query)
					.contains("LOWER(intro)", query).contains("LOWER(description)", query)
					.contains("LOWER(keywords)", query).contains("LOWER(relation)", query)
					.contains("LOWER(organization)", query).contains("LOWER(remarks)", query)
					.contains("LOWER(license)", query).endOr();

			// add relation filtering
			if (relation.trim().length() > 0) {
				esdl = esdl.contains("LOWER(relation)", relation);
			}

			// add organization filtering
			if (organization.trim().length() > 0) {
				esdl = esdl.contains("LOWER(organization)", organization);
			}

			// add filtering
			if (filter.equals("complete")) {
				esdl = esdl.and().eq("dsType", DatasetType.COMPLETE);
			} else if (filter.equals("movement")) {
				esdl = esdl.and().eq("dsType", DatasetType.MOVEMENT);
			}

			// check access for all datasets
			datasets = esdl.findList().stream()
					.filter(ds -> ds.getProject().isPublicProject() || (user != null && ds.visibleFor(user))).limit(30)
					.collect(Collectors.toList());

			// now search for project metadata that might match
			projects = espl.findList();
		} else {
			// add public datasets for empty query (up to 20+)
			datasets = new LinkedList<Dataset>();
			projects = Project.find.query().findList();
		}

		// configure filtering
		DatasetType filterType = null;
		if (filter.equals("complete")) {
			filterType = DatasetType.COMPLETE;
		} else if (filter.equals("movement")) {
			filterType = DatasetType.MOVEMENT;
		}

		for (Project p : projects) {

			// don't add more than 30 datasets
			if (datasets.size() >= 30) {
				break;
			}

			// check access for project
			if (!(p.isPublicProject() || (user != null && p.visibleFor(user)))) {
				continue;
			}

			for (Dataset ds : p.getDatasets()) {
				if ((filterType == null || ds.getDsType().equals(filterType))
						&& (relation.trim().length() == 0 || nss(p.getRelation()).contains(relation)
								|| nss(ds.getRelation()).contains(relation))
						&& (organization.trim().length() == 0 || nss(p.getOrganization()).contains(organization)
								|| nss(ds.getOrganization()).contains(organization))) {

					// check for existing datasets in result
					if (datasets.stream().noneMatch(d -> d.getId().equals(ds.getId()))) {
						datasets.add(ds);
					}
				}

				if (datasets.size() > 30) {
					break;
				}
			}
		}

		// compile data
		ObjectNode on = okJSONResponseObject();
		ObjectNode data = on.putObject("data");
		ArrayNode datasetsNode = data.putArray("datasets");
		datasets.stream().forEach(ds -> datasetsNode.add(ds.getId()));

		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result addDataset(Request request) {

		Person user = Person.find.byId(getAuthenticatedUserId(request));
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		// find project
		Project p = null;
		try {
			String projectId = df.get("project_id");
			if (projectId == null || projectId.length() == 0) {
				// does user have a project at all? if one, then automatically add to that one
				if (user != null && user.projects().size() == 1) {
					projectId = "" + user.projects().get(0).getId();
				}
				// does user have a project at all? if two, then find the one that matches the public switch
				else if (user != null && user.projects().size() == 2) {
					boolean searchFlag = df.get("isPublic") != null;
					Optional<Project> op = user.projects().stream().filter(up -> up.isPublicProject() == searchFlag)
							.findFirst();
					// choose project
					if (op.isPresent()) {
						projectId = "" + user.projects().get(0).getId();
					} else {
						return badRequest(errorJSONResponseObject("Wrong format of project ID."));
					}
				} else {
					return badRequest(errorJSONResponseObject("Wrong format of project ID."));
				}
			}

			Long id = Long.parseLong(projectId);
			p = Project.find.byId(id);
		} catch (Exception e) {
			return badRequest(errorJSONResponseObject("Wrong format of project ID."));
		}

		if (p == null || !p.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Given project not available or owned by user account."));
		}

		// COMPLETE, LINKED, MOVEMENT
		String datasetType = df.get("dataset_type");
		if (datasetType == null || datasetType.length() == 0) {
			return badRequest(errorJSONResponseObject("Wrong format of dataset type."));
		}

		DatasetType dsType = null;
		try {
			dsType = DatasetType.valueOf(datasetType);
		} catch (Exception e) {
			return badRequest(errorJSONResponseObject("Wrong format of dataset type."));
		}

		if (dsType == null) {
			return badRequest(errorJSONResponseObject("Wrong format of dataset type."));
		}

		Date[] dates = DateUtils.getDates(df.get("start-date"), df.get("end-date"));

		Dataset ds = datasetConnector.create(df.get("dataset_name"), dsType, p, df.get("description"),
				df.get("target_object"), null, df.get("license"));
		ds.setStart(dates[0]);
		ds.setEnd(dates[1]);

		// metadata fields
		if (df.get("keywords") != null) {
			ds.setKeywords(df.get("keywords"));
		}
		if (df.get("doi") != null) {
			ds.setDoi(df.get("doi"));
		}
		if (df.get("relation") != null) {
			ds.setRelation(df.get("relation"));
		}
		if (df.get("organization") != null) {
			ds.setOrganization(df.get("organization"));
		}
		if (df.get("remarks") != null) {
			ds.setRemarks(df.get("remarks"));
		}
		if (df.get("license") != null) {
			ds.setLicense(df.get("license"));
		}
		ds.save();

		LabNotesEntry.log(ApiSpecs.class, LabNotesEntryType.CREATE, "Data set created: " + ds.getName(),
				ds.getProject());

		return ok(okJSONResponseObject());
	}

	@Authenticated(V2UserApiAuth.class)
	public Result editDataset(Request request, Long id) {

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		if (!ds.canAppend()) {
			return notFound(errorJSONResponseObject("Dataset is not active."));
		}

		Person user = Person.find.byId(getAuthenticatedUserId(request));
		Project p = ds.getProject();
		if (!p.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Given project not available or owned by user account."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		Date[] dates = DateUtils.getDates(df.get("start-date"), df.get("end-date"), ds);

		ds.setName(df.get("dataset_name"));
		ds.setStart(dates[0]);
		ds.setEnd(dates[1]);

		if (df.get("target_object") != null) {
			ds.setTargetObject(df.get("target_object"));
		}

		// metadata fields
		if (df.get("description") != null) {
			ds.setDescription(df.get("description"));
		}
		if (df.get("keywords") != null) {
			ds.setKeywords(df.get("keywords"));
		}
		if (df.get("doi") != null) {
			ds.setDoi(df.get("doi"));
		}
		if (df.get("relation") != null) {
			ds.setRelation(df.get("relation"));
		}
		if (df.get("organization") != null) {
			ds.setOrganization(df.get("organization"));
		}
		if (df.get("remarks") != null) {
			ds.setRemarks(df.get("remarks"));
		}
		if (df.get("license") != null) {
			ds.setLicense(df.get("license"));
		}
		// update the dataset
		ds.update();

		// assign to different project?
		String isPublic = df.get("isPublic");
		if (isPublic != null && isPublic.length() > 0) {
			if (isPublic.equals("false") && ds.getProject().isPublicProject()) {
				Optional<Project> op = user.projects().stream().filter(up -> !up.isPublicProject()).findFirst();
				// switch dataset to new project
				final Project newProject;
				if (op.isPresent()) {
					newProject = op.get();
				} else {
					newProject = createPrivateProjectForUser(user, "");
				}

				// remove from old project
				p.getDatasets().remove(ds);
				// add to new project
				newProject.getDatasets().add(ds);
				// re-link project in dataset
				ds.setProject(newProject);

				// update the database
				p.update();
				newProject.update();
				ds.update();
			} else if (isPublic.equals("true") && !ds.getProject().isPublicProject()) {
				Optional<Project> op = user.projects().stream().filter(up -> up.isPublicProject()).findFirst();
				// switch dataset to new project
				final Project newProject;
				if (op.isPresent()) {
					newProject = op.get();
				} else {
					newProject = createPublicProjectForUser(user, "");
				}

				// remove from old project
				p.getDatasets().remove(ds);
				// add to new project
				newProject.getDatasets().add(ds);
				// re-link project in dataset
				ds.setProject(newProject);

				// update the database
				p.update();
				newProject.update();
				ds.update();
			}
		}

		LabNotesEntry.log(ApiSpecs.class, LabNotesEntryType.MODIFY, "Data set edited: " + ds.getName(),
				ds.getProject());

		return ok(okJSONResponseObject());
	}

	@Authenticated(V2UserApiAuth.class)
	public Result uploadDatasetFile(Request request, Long id) {

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		if (!ds.canAppend()) {
			return forbidden(errorJSONResponseObject("Dataset is not active."));
		}

		Person user = Person.find.byId(getAuthenticatedUserId(request));
		Project project = ds.getProject();
		if (!project.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Project not accessible for user."));
		}

		try {
			Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
			if (body == null) {
				return badRequest(errorJSONResponseObject("Bad request."));
			}

			DynamicForm df = formFactory.form().bindFromRequest(request);
			if (df == null) {
				return badRequest(errorJSONResponseObject("No data provided."));
			}

			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
			String description = df.get("description");
			if (ds.getDsType() == DatasetType.COMPLETE) {
				internalAddCompleteDSUpload(ds, description, fileParts);
			} else if (ds.getDsType() == DatasetType.MOVEMENT) {
				String participant = df.get("participant");
				if (participant == null) {
					return badRequest(errorJSONResponseObject("Participant name not provided."));
				}
				internalAddMovementDSUpload(ds, participant, description, fileParts);
			} else {
				return notFound(errorJSONResponseObject("Dataset type not applicable for upload."));
			}

			return ok(okJSONResponseObject());
		} catch (IOException | NullPointerException e) {
			logger.error("Error in uploading dataset file.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return badRequest(errorJSONResponseObject("Invalid request, no files have been included in the request."));
	}

	/**
	 * perform an upload into a COMPLETE dataset
	 * 
	 * @param ds
	 * @param description
	 * @param fileParts
	 * @throws IOException
	 */
	private void internalAddCompleteDSUpload(Dataset ds, String description,
			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts) throws IOException {
		if (!fileParts.isEmpty()) {

			final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

			File theFolder = cpds.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			for (int i = 0; i < fileParts.size(); i++) {
				FilePart<TemporaryFile> filePart = fileParts.get(i);
				TemporaryFile tempfile = filePart.getRef();
				String fileName = filePart.getFilename();

				// restrict file type
				if (FileTypeUtils.looksLikeExecutableFile(fileName)) {
					logger.error("Executable file upload attempt blocked: " + fileName);
					continue;
				}

				// content-based validation
				if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.ANY)) {
					continue;
				}

				Optional<String> storeFile = cpds.storeFile(tempfile.path().toFile(), fileName);
				if (storeFile.isPresent()) {
					cpds.addRecord(storeFile.get(), description, new Date());
				}
			}

			LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.MODIFY,
					"Files uploaded to dataset: " + ds.getName(), ds.getProject());
		}
	}

	/**
	 * perform an upload into a MOVEMENT dataset
	 * 
	 * @param ds
	 * @param description
	 * @param fileParts
	 * @throws IOException
	 */
	private void internalAddMovementDSUpload(Dataset ds, String participantName, String description,
			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts) throws IOException {
		if (!fileParts.isEmpty()) {

			// find or create participant
			Participant participant = ds.getProject().getParticipants().stream()
					.filter(p -> p.getName().equals(participantName)).findAny().orElseGet(() -> {
						// create new participant
						Participant p = Participant.createInstance(participantName, "", "", ds.getProject());
						p.setStatus(ParticipationStatus.ACCEPT);
						p.setProject(ds.getProject());
						p.save();

						return p;
					});

			final MovementDS mvds = (MovementDS) datasetConnector.getDatasetDS(ds);

			File theFolder = mvds.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			for (int i = 0; i < fileParts.size(); i++) {
				FilePart<TemporaryFile> filePart = fileParts.get(i);
				TemporaryFile tempfile = filePart.getRef();
				String fileName = filePart.getFilename();

				// only allow gpx or xml files to be included here
				if (!FileTypeUtils.looksLikeMovementDataFile(fileName)) {
					logger.error("Only GPX / XML data files allowed: " + fileName);
					continue;
				}

				// content-based validation
				if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.XML)) {
					continue;
				}

				Date now = new Date();

				// ensure that filename is unique on disk
				fileName = participant.getId() + "_" + now.getTime() + "_" + fileName;

				// copy file
				mvds.storeFile(tempfile.path().toFile(), fileName);

				// import file contents and add record to database
				if (mvds.importFileContents(tempfile.path().toFile(), participant)) {
					mvds.addRecord(participant, fileName, description, now, "imported");
				} else {
					mvds.addRecord(participant, fileName, description, now, "import failed");
				}
			}

			LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.MODIFY,
					"Files uploaded to dataset: " + ds.getName(), ds.getProject());
		}
	}

	@Authenticated(V2UserApiAuth.class)
	public Result downloadDatasetFile(Request request, Long id, String filename) {

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		Person user = Person.find.byId(getAuthenticatedUserId(request));
		Project project = ds.getProject();
		if (!project.visibleFor(user)) {
			return notFound(errorJSONResponseObject("Project not accessible for user."));
		}

		// compose file path and check existence
		final Optional<File> requestedFile;
		if (ds.getDsType() == DatasetType.COMPLETE) {
			final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
			requestedFile = cpds.getFile(filename);
		} else if (ds.getDsType() == DatasetType.MEDIA) {
			final MediaDS cpds = (MediaDS) datasetConnector.getDatasetDS(ds);
			requestedFile = cpds.getFile(filename);
		} else if (ds.getDsType() == DatasetType.MOVEMENT) {
			final MovementDS cpds = (MovementDS) datasetConnector.getDatasetDS(ds);
			requestedFile = cpds.getFile(filename);
		} else {
			requestedFile = Optional.empty();
		}

		if (!requestedFile.isPresent()) {
			return notFound(errorJSONResponseObject("No file found: " + filename));
		}

		return ok(requestedFile.get()).withHeader("Content-disposition", "attachment; filename=" + filename);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * create a project for the given user for storing public datasets
	 * 
	 * @param user
	 * @param website
	 */
	private Project createPublicProjectForUser(Person user, String website) {
		Project p = Project.create(user.getName() + " @ VDC", user,
				"Container project for public datasets created and shared by " + user.getName()
						+ " from Vitality Data Center",
				true, true);
		p.setLicense("VDC license");
		p.setRelation(website);
		// TODO move to configuration
		p.setOrganization("Vitality Data Center");
		p.setPublicProject(true);
		p.save();
		LabNotesEntry.log(Project.class, LabNotesEntryType.CREATE, "Public project created", p);

		return p;
	}

	/**
	 * create a project for the given user for storing private datasets
	 * 
	 * @param user
	 * @param website
	 */
	private Project createPrivateProjectForUser(Person user, String website) {
		Project p = Project.create(user.getName() + " @ VDC", user,
				"Container project for non-public datasets created and shared by " + user.getName()
						+ " from Vitality Data Center",
				false, false);
		p.setLicense("VDC license");
		p.setRelation(website);
		// TODO move to configuration
		p.setOrganization("Vitality Data Center");
		p.setPublicProject(false);
		p.save();
		LabNotesEntry.log(Project.class, LabNotesEntryType.CREATE, "Non-public project created", p);

		return p;
	}

	private Long getAuthenticatedUserId(Request request) {
		String apiToken = request.headers().get("api-token").orElse("");
		if (apiToken == null || apiToken.length() == 0) {
			return -1l;
		}

		Long userId = tokenResolverUtil.retrieveUserIdFromUserAccessToken(apiToken);
		Long tokenTimeout = tokenResolverUtil.retrieveTimeoutFromUserAccessToken(apiToken);
		if (userId == -1l || tokenTimeout < System.currentTimeMillis()) {
			return -1l;
		}

		return userId;
	}

	private ObjectNode okJSONResponseObject() {
		ObjectNode on = Json.newObject();
		on.put("result", "ok");
		return on;
	}

	private ObjectNode errorJSONResponseObject(String message) {
		ObjectNode on = Json.newObject();
		on.put("result", "error");
		on.put("message", message);
		return on;
	}

	private ObjectNode toJSON(Project project) {
		ObjectNode metadata = MetaDataUtils.toJson(project);

		// add additional information
		metadata.put("isPublic", project.isPublicProject());
		metadata.put("owner", project.getOwner().getName());
		return metadata;
	}

	private ObjectNode toJSON(Dataset dataset) {
		ObjectNode newObject = MetaDataUtils.toJson(dataset);

		// add extra information
		newObject.put("owner", dataset.getProject().getOwner().getName());
		newObject.put("isPublic", dataset.getProject().isPublicProject());
		newObject.put("project_id", dataset.getProject().getId());

		// list contents
		if (dataset.getDsType() == DatasetType.COMPLETE) {
			final CompleteDS cds = (CompleteDS) datasetConnector.getDatasetDS(dataset);
			final List<TimedMedia> files = cds.getFiles();
			addFiles(newObject, files);
		} else if (dataset.getDsType() == DatasetType.MEDIA) {
			final MediaDS cds = (MediaDS) datasetConnector.getDatasetDS(dataset);
			final List<TimedMedia> files = cds.getFiles();
			addFiles(newObject, files);
		} else if (dataset.getDsType() == DatasetType.MOVEMENT) {
			final MovementDS cds = (MovementDS) datasetConnector.getDatasetDS(dataset);
			final List<TimedMedia> files = cds.getFiles();
			addFiles(newObject, files);
		}

		return newObject;
	}

	/**
	 * add a list of files to the data property of an object node
	 * 
	 * @param newObject
	 * @param files
	 */
	private void addFiles(ObjectNode newObject, final List<TimedMedia> files) {
		final ArrayNode an = newObject.putArray("data");
		for (TimedMedia file : files) {
			final ObjectNode fileObject = an.addObject();
			fileObject.put("ts", file.datetime());
			fileObject.put("name", file.link);
			fileObject.put("description", file.caption);
		}
	}

}
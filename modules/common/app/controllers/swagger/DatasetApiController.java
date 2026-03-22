package controllers.swagger;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import controllers.DatasetsController;
import controllers.api.CompleteDSController;
import controllers.api.ExpSamplingDSController;
import controllers.api.MediaDSController;
import controllers.api.MovementDSController;
import controllers.auth.V2UserApiAuth;
import datasets.DatasetConnector;
import io.ebean.ExpressionList;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.ds.AnnotationDS;
import models.ds.CompleteDS;
import models.ds.DiaryDS;
import models.ds.EntityDS;
import models.ds.ExpSamplingDS;
import models.ds.LinkedDS;
import models.ds.MediaDS;
import models.ds.MovementDS;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.ParticipationStatus;
import models.sr.Wearable;
import models.vm.TimedMedia;
import play.Logger;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.slack.Slack;
import utils.DataUtils;
import utils.DateUtils;
import utils.auth.TokenResolverUtil;
import utils.export.MetaDataUtils;
import utils.validators.FileTypeUtils;

@Singleton
public class DatasetApiController extends AbstractApiController {

	private final DatasetsController datasetsController;
	private static final Logger.ALogger logger = Logger.of(DatasetApiController.class);

	@Inject
	public DatasetApiController(DatasetsController datasetsController, FormFactory formFactory,
			DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil) {
		super(formFactory, datasetConnector, tokenResolverUtil);
		this.datasetsController = datasetsController;
	}

	@Authenticated(V2UserApiAuth.class)
	public Result datasets(Request request) {

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("No valid user given."));
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
			ArrayNode projectNode = data.putArray("ownDatasets");
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
			ArrayNode projectNode = data.putArray("inArchivedProjects");
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
			return notFound(errorJSONResponseObject("Dataset not found."));
		}

		Project project = ds.getProject();
		if (!project.isPublicProject()) {
			// only show if user is owner
			Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
			if (user == null) {
				return forbidden(errorJSONResponseObject("No user account given and dataset not publicly accessible."));
			}

			// check ownership
			if (!project.belongsTo(user) && !project.collaboratesWith(user) && !project.subscribedBy(user)) {
				return forbidden(
						errorJSONResponseObject("Dataset not accessible to user and dataset not publicly accessible."));
			}
		}

		return ok(toJSON(ds));
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
	@Authenticated(V2UserApiAuth.class)
	public Result searchDatasets(Request request, String filter, String query, String relation, String organization) {
		// convert to lower case
		query = query.trim().toLowerCase();

		// retrieve user if given
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

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
			if (filter.equals("Complete")) {
				esdl = esdl.and().eq("dsType", DatasetType.COMPLETE);
			} else if (filter.equals("Movement")) {
				esdl = esdl.and().eq("dsType", DatasetType.MOVEMENT);
			} else if (filter.equals("IoT")) {
				esdl = esdl.and().eq("dsType", DatasetType.IOT);
			} else if (filter.equals("Survey/Form")) {
				esdl = esdl.and().eq("dsType", DatasetType.FORM);
			} else if (filter.equals("Media")) {
				esdl = esdl.and().eq("dsType", DatasetType.MEDIA);
			}

			// check access for all datasets
			datasets = esdl.findList().stream().filter(ds -> ds.getProject().isPublicProject() || ds.visibleFor(user))
					.limit(30).collect(Collectors.toList());

			// now search for project metadata that might match
			projects = espl.findList();
		} else {
			// add public datasets for empty query (up to 20+)
			datasets = new LinkedList<Dataset>();
			projects = Project.find.query().findList();
		}

		// configure filtering
		DatasetType filterType = null;
		if (filter.equals("Complete")) {
			filterType = DatasetType.COMPLETE;
		} else if (filter.equals("Movement")) {
			filterType = DatasetType.MOVEMENT;
		} else if (filter.equals("IoT")) {
			filterType = DatasetType.IOT;
		} else if (filter.equals("Survey/Form")) {
			filterType = DatasetType.FORM;
		} else if (filter.equals("Media")) {
			filterType = DatasetType.MEDIA;
		}

		for (Project p : projects) {

			// don't add more than 30 datasets
			if (datasets.size() >= 30) {
				break;
			}

			// check access for project
			if (!(p.isPublicProject() || p.visibleFor(user))) {
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
	public Result add(Request request) {

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("No valid user given."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		JsonNode json = request.body().asJson();

		if (df == null && json == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		// find project
		Project p = null;
		Long id = 0l;
		String projectId = getValue(df, json, "project_id");
		if (nnne(projectId)) {
			id = DataUtils.parseLong(projectId);
		}
		p = Project.find.byId(id);

		if (p == null || !p.belongsTo(user) && !p.collaboratesWith(user)) {
			return forbidden(
					errorJSONResponseObject("Given project not available or not owned / collaborated by user."));
		}

		// COMPLETE, LINKED, MOVEMENT
		String datasetType = getValue(df, json, "dataset_type");
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

		String startDate = getValue(df, json, "start-date");
		String endDate = getValue(df, json, "end-date");
		Date[] dates = DateUtils.getDates(startDate, endDate);

		if (dates[0].compareTo(dates[1]) > 0) {
			return badRequest(errorJSONResponseObject("Please check the dates order."));
		}

		String datasetName = getValue(df, json, "dataset_name");
		String description = getValue(df, json, "description");
		String targetObject = getValue(df, json, "target_object");
		String license = getValue(df, json, "license");

		Dataset ds = datasetConnector.create(datasetName, dsType, p, description, targetObject, null, license);
		ds.setStart(dates[0]);
		ds.setEnd(dates[1]);

		// metadata fields
		String keywords = getValue(df, json, "keywords");
		if (keywords != null) {
			ds.setKeywords(keywords);
		}
		String doi = getValue(df, json, "doi");
		if (doi != null) {
			ds.setDoi(doi);
		}
		String relation = getValue(df, json, "relation");
		if (relation != null) {
			ds.setRelation(relation);
		}
		String organization = getValue(df, json, "organization");
		if (organization != null) {
			ds.setOrganization(organization);
		}
		String remarks = getValue(df, json, "remarks");
		if (remarks != null) {
			ds.setRemarks(remarks);
		}
		// license already used in create

		String isOpen = getValue(df, json, "isOpenParticipation");
		if (isOpen != null) {
			if (isOpen.equals("true")) {
				ds.setOpenParticipation(true);
			} else {
				ds.setOpenParticipation(false);
			}
		}

		ds.save();

		LabNotesEntry.log(DatasetApiController.class, LabNotesEntryType.CREATE, "Data set created: " + ds.getName(),
				ds.getProject());

		ObjectNode on = okJSONResponseObject();
		on.put("id", ds.getId());
		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result edit(Request request, Long id) {

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		if (ds.getProject().isArchivedProject()) {
			return notFound(errorJSONResponseObject("Dataset belongs to an archived project."));
		}

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("No valid user given."));
		}

		Project p = ds.getProject();
		if (!p.belongsTo(user) && !p.collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Project not owned by user."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		JsonNode json = request.body().asJson();

		if (df == null && json == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		String startDate = getValue(df, json, "start-date");
		String endDate = getValue(df, json, "end-date");

		if (nnne(startDate) || nnne(endDate)) {
			Date[] dates = DateUtils.getDates(startDate, endDate, ds);
			ds.setStart(dates[0]);
			ds.setEnd(dates[1]);
		}

		String name = getValue(df, json, "dataset_name");
		if (nnne(name)) {
			ds.setName(htmlTagEscape(nss(name, 64)));
		}

		String targetObj = getValue(df, json, "target_object");
		if (targetObj != null) {
			ds.setTargetObject(targetObj);
		}

		String description = getValue(df, json, "description");
		if (description != null) {
			ds.setDescription(description);
		}

		if (nnne(startDate)) {
			Date tmp = DateUtils.getDate(startDate, "00:00:00");
			if (tmp == null) {
				return badRequest(errorJSONResponseObject("Wrong format for start date, please check."));
			} else {
				ds.setStart(tmp);
			}
		}
		if (nnne(endDate)) {
			Date tmp = DateUtils.getDate(endDate, "23:59:59");
			if (tmp == null) {
				return badRequest(errorJSONResponseObject("Wrong format for end date, please check."));
			} else {
				ds.setEnd(tmp);
			}
		}
		if (ds.getStart().compareTo(ds.getEnd()) > 0) {
			return badRequest(errorJSONResponseObject("Please check the dates order."));
		}

		// metadata fields
		String keywords = getValue(df, json, "keywords");
		if (keywords != null) {
			ds.setKeywords(htmlTagEscape(nss(keywords)));
		}
		String doi = getValue(df, json, "doi");
		if (doi != null) {
			ds.setDoi(doi);
		}
		String relation = getValue(df, json, "relation");
		if (relation != null) {
			ds.setRelation(relation);
		}
		String organization = getValue(df, json, "organization");
		if (organization != null) {
			ds.setOrganization(htmlTagEscape(nss(organization)));
		}
		String remarks = getValue(df, json, "remarks");
		if (remarks != null) {
			ds.setRemarks(htmlTagEscape(nss(remarks)));
		}
		String license = getValue(df, json, "license");
		if (license != null) {
			ds.setLicense(license);
		}

		String isOpen = getValue(df, json, "isOpenParticipation");
		if (isOpen != null) {
			if (isOpen.equals("true")) {
				ds.setOpenParticipation(true);
			} else {
				ds.setOpenParticipation(false);
			}
		}

		// update the dataset
		ds.update();

		LabNotesEntry.log(DatasetApiController.class, LabNotesEntryType.MODIFY, "Data set edited: " + ds.getName(),
				ds.getProject());

		return ok(okJSONResponseObject("Information updated."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result delete(Request request, Long id) {

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("No valid user given."));
		}

		Project p = ds.getProject();
		if (!p.belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Project not owned by user."));
		}

		// Remove data and tables
		LinkedDS lds = datasetConnector.getDatasetDS(ds);
		if (lds != null) {
			lds.resetDataset();
			lds.dropTable();
		}

		// remove dataset from project
		p.getDatasets().remove(ds);
		p.update();

		// delete dataset entity
		ds.delete();

		LabNotesEntry.log(DatasetApiController.class, LabNotesEntryType.DELETE, "Data set deleted: " + ds.getName(), p);

		return ok(okJSONResponseObject("Dataset deleted."));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(V2UserApiAuth.class)
	public Result uploadDatasetFile(Request request, Long id) {

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		if (!ds.canAppend()) {
			return forbidden(errorJSONResponseObject("Dataset is not active."));
		}

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		Project project = ds.getProject();
		if (user == null || !project.belongsTo(user)) {
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

			boolean success = true;
			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
			String description = df.get("description");
			if (ds.getDsType() == DatasetType.COMPLETE) {
				success = internalAddCompleteDSUpload(ds, description, fileParts);
			} else {
				String participant = df.get("participant");
				if (ds.getDsType() == DatasetType.MEDIA) {
					success = internalAddMediaDSUpload(request, ds, participant, description, fileParts);
					logger.info("success: " + success);
				} else if (!nnne(participant)) {
					return badRequest(errorJSONResponseObject("Participant name not provided."));
				} else if (ds.getDsType() == DatasetType.MOVEMENT) {
					success = internalAddMovementDSUpload(ds, participant, description, fileParts);
				} else if (ds.getDsType() == DatasetType.ES) {
					success = internalAddExpSamplingDSUpload(ds, participant, description, fileParts);
				} else {
					return notFound(errorJSONResponseObject("Dataset type not applicable for upload."));
				}
			}

			if (success) {
				return ok(okJSONResponseObject("Uploading finished."));
			} else {
				return badRequest(errorJSONResponseObject("Something wrong with the upload files, please check."));
			}
		} catch (IOException | NullPointerException e) {
			logger.error("Error in uploading dataset file.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return badRequest(errorJSONResponseObject("Invalid request, no files have been included in the request."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result downloadDatasetFile(Request request, Long id, String filename) {

		// check dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		// check user and visibility for the project by use
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		Project project = ds.getProject();
		if (user == null || !project.visibleFor(user)) {
			return notFound(errorJSONResponseObject("Project not accessible for user."));
		}

		// compose file path and check existence for COMPLETE, MEDIA, or MOVEMENT datasets
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

		// check file
		if (!requestedFile.isPresent()) {
			return notFound(errorJSONResponseObject("No file found: " + filename));
		}

		return ok(requestedFile.get()).withHeader("Content-disposition", "attachment; filename=" + filename);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * download data from dataset as CSV
	 * 
	 * @param request
	 * @param id
	 * @param participant_id
	 * @param device_id
	 * @param wearable_id
	 * @param cluster_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public CompletionStage<Result> downloadCSV(Request request, long id, long participant_id, long device_id,
			long wearable_id, long cluster_id, long limit, long start, long end) {

		// check dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFoundCS(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		// check user and visibility for the project by use
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null || !ds.visibleFor(user)) {
			return forbiddenCS(errorJSONResponseObject("Project not accessible for user."));
		}

		// prepare the cluster for filtering
		final Cluster cluster;
		if (cluster_id > -1) {
			cluster = Cluster.find.byId(cluster_id);
		} else {
			cluster = new Cluster("Cluster");
			cluster.setId(-1l);
		}

		// add other ids
		if (participant_id > -1) {
			Participant p = Participant.find.byId(participant_id);
			if (p != null) {
				cluster.getParticipants().add(p);
			}
		}

		if (device_id > -1) {
			Device d = Device.find.byId(device_id);
			if (d != null) {
				cluster.getDevices().add(d);
			}
		}

		if (participant_id > -1) {
			Wearable w = Wearable.find.byId(wearable_id);
			if (w != null) {
				cluster.getWearables().add(w);
			}
		}

		// download the internal version here with filtering by cluster
		return datasetsController.downloadInternal(request, id, ds, cluster, limit, start, end);
	}

	/**
	 * download data from dataset as JSON
	 * 
	 * @param request
	 * @param id
	 * @param participant_id
	 * @param device_id
	 * @param wearable_id
	 * @param cluster_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public CompletionStage<Result> downloadJson(Request request, long id, long participant_id, long device_id,
			long wearable_id, long cluster_id, long limit, long start, long end) {

		// check dataset and dataset type
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFoundCS(errorJSONResponseObject("No or invalid dataset ID given."));
		}

		// check user and visibility for the project by use
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null || !ds.visibleFor(user)) {
			return notFoundCS(errorJSONResponseObject("Project not accessible for user."));
		}

		// prepare the cluster for filtering
		final Cluster cluster;
		if (cluster_id > -1) {
			cluster = Cluster.find.byId(cluster_id);
		} else {
			cluster = new Cluster("Cluster");
			cluster.setId(-1l);
		}

		// add other ids
		if (participant_id > -1) {
			Participant p = Participant.find.byId(participant_id);
			if (p != null) {
				cluster.getParticipants().add(p);
			}
		}

		if (device_id > -1) {
			Device d = Device.find.byId(device_id);
			if (d != null) {
				cluster.getDevices().add(d);
			}
		}

		if (participant_id > -1) {
			Wearable w = Wearable.find.byId(wearable_id);
			if (w != null) {
				cluster.getWearables().add(w);
			}
		}

		return CompletableFuture.supplyAsync(() -> {
			return datasetsController.downloadJsonCluster(request, id, cluster, limit, start, end);
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * log data for IoT dataset
	 * 
	 * @param id
	 * @param dsApiToken
	 * @return
	 */
	public Result addIoTRecord(Request request, final Long id) {

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || ds.getDsType() != DatasetType.IOT) {
			return badRequest(errorJSONResponseObject(
					"No or invalid dataset ID given. Only IoT dataset is available for this request."));
		} else if (!ds.canAppend()) {
			return forbidden(errorJSONResponseObject("Dataset is closed (adjust start and end dates to open)."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		Project project = ds.getProject();
		if (user == null || !project.belongsTo(user) && !project.collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Project not accessible for user."));
		}

		// check data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("Expecting some data"));
		}

		String sourceId = nss(df.get("source_id"));
		if (!ds.isOpenParticipation() && sourceId.isEmpty()) {
			return notFound(errorJSONResponseObject("Source device not found"));
		}

		// record the information in the database for this data set
		final String activity = nss(df.get("activity"));
		final String data = nss(df.get("data"));

		final TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);

		final Optional<Device> opt = Device.find.query().setMaxRows(1).where().eq("refId", sourceId).findOneOrEmpty();
		if (!opt.isPresent()) {
			if (!ds.isOpenParticipation()) {
				// open device id not permitted
				return notFound(errorJSONResponseObject("Source device not registered"));
			} else {
				// submission with textual device id
				CompletableFuture.runAsync(() -> tssc.addRecord(sourceId, new Date(), activity, data));

				// response
				return ok(okJSONResponseObject("Record added."));
			}
		}

		// device id is given and in the project?
		Device device = opt.get();
		if (!ds.isOpenParticipation() && !ds.getProject().getId().equals(device.getProject().getId())) {
			return forbidden(errorJSONResponseObject("Source device not permitted"));
		}

		// submit
		CompletableFuture.runAsync(() -> tssc.addRecord(device, new Date(), activity, data));

		// response
		return ok(okJSONResponseObject("Record added."));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get specific item of the Entity dataset
	 * 
	 * @param id
	 * @param dsApiToken
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public CompletionStage<Result> getItem(Request request, Long id, String resource_id, String token) {
		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFoundCS(errorJSONResponseObject("Dataset not found."));
		}

		// check user
		Optional<Person> user = getAuthenticatedAPIUser(request);
		Project project = ds.getProject();
		project.refresh();
		if (user == null || !project.visibleForPerson(user)) {
			return forbiddenCS(errorJSONResponseObject("Dataset not accessible for user."));
		}

		// check data
		if (!nnne(resource_id)) {
			return badRequestCS(errorJSONResponseObject("No data given."));
		}

		// get ds
		final EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// handle token
		token = token == null ? "" : token;
		Optional<String> tempTokenOpt = Optional.of(token);

		// check authorized used in case of empty token
		if (token.isEmpty()) {
			if (project.editableByPerson(user)) {
				tempTokenOpt = eds.internalGetItemToken(resource_id);
			}
		}

		final Optional<String> finalToken = tempTokenOpt;
		return CompletableFuture.supplyAsync(() -> eds.getItem(resource_id, finalToken)).thenApplyAsync(data -> {
			if (data.isEmpty()) {
				return notFound(errorJSONResponseObject("Item not found")).as("application/json");
			} else {
				return ok(data.get()).as("application/json");
			}
		}).exceptionally(e -> {
			logger.error("Entity dataset get problem", e);
			return badRequest();
		});
	}

	/**
	 * add a new item to the entity dataset
	 * 
	 * @param id
	 * @param dsApiToken
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public CompletionStage<Result> addItem(Request request, Long id) {
		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFoundCS(errorJSONResponseObject("Dataset not found."));
		}

		// check user
		Optional<Person> user = getAuthenticatedAPIUser(request);
		Project project = ds.getProject();
		project.refresh();
		if (user == null || !project.visibleForPerson(user)) {
			return forbiddenCS(errorJSONResponseObject("Dataset not accessible for user."));
		}

		// check data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequestCS(errorJSONResponseObject("Expecting some data"));
		}

		// check access data
		String resource_id = df.get("resource_id");
		if (!nnne(resource_id)) {
			return badRequestCS(errorJSONResponseObject("No resource_id given."));
		}
		String newData = df.get("data");
		if (!nnne(newData)) {
			return badRequestCS(errorJSONResponseObject("No data given."));
		}

		// get ds
		final EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// handle token
		String token = df.get("token");
		token = token == null ? "" : token;
		Optional<String> tempTokenOpt = Optional.of(token);

		// check authorized used in case of empty token
		if (token.isEmpty()) {
			if (project.editableByPerson(user)) {
				tempTokenOpt = eds.internalGetItemToken(resource_id);
			}
		}

		final Optional<String> finalToken = tempTokenOpt;
		return CompletableFuture.supplyAsync(() -> eds.addItem(resource_id, finalToken, newData))
				.thenApplyAsync(data -> {
					if (!data.isPresent()) {
						return badRequest(errorJSONResponseObject("JSON data invalid")).as("application/json");
					} else {
						return ok(data.orElse(Json.newObject())).as("application/json");
					}
				}).exceptionally(e -> {
					logger.error("Entity dataset add problem", e);
					return badRequest(errorJSONResponseObject("Unspecified problem in addItem"));
				});
	}

	/**
	 * add a new item to the entity dataset
	 * 
	 * @param id
	 * @param dsApiToken
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public CompletionStage<Result> updateItem(Request request, Long id) {
		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFoundCS(errorJSONResponseObject("Dataset not found."));
		}

		// check user
		Optional<Person> user = getAuthenticatedAPIUser(request);
		Project project = ds.getProject();
		project.refresh();
		if (user == null || !project.visibleForPerson(user)) {
			return forbiddenCS(errorJSONResponseObject("Dataset not accessible for user."));
		}

		// check data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequestCS(errorJSONResponseObject("Expecting some data"));
		}

		// check access data
		String resource_id = df.get("resource_id");
		if (!nnne(resource_id)) {
			return badRequestCS(errorJSONResponseObject("No resource_id given."));
		}
		String newData = df.get("data");
		if (!nnne(newData)) {
			return badRequestCS(errorJSONResponseObject("No data given."));
		}

		// get ds
		final EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// handle token
		String token = df.get("token");
		token = token == null ? "" : token;
		Optional<String> tempTokenOpt = Optional.of(token);

		// check authorized used in case of empty token
		if (token.isEmpty()) {
			if (project.editableByPerson(user)) {
				tempTokenOpt = eds.internalGetItemToken(resource_id);
			}
		}

		final Optional<String> finalToken = tempTokenOpt;
		return CompletableFuture.supplyAsync(() -> eds.updateItem(resource_id, finalToken, newData))
				.thenApplyAsync(data -> {
					if (!data.isPresent()) {
						return notFound(errorJSONResponseObject("Item not found")).as("application/json");
					} else {
						return ok(data.get()).as("application/json");
					}
				}).exceptionally(e -> {
					logger.error("Entity dataset update problem", e);
					return badRequest();
				});
	}

	/**
	 * delete specific item of the Entity dataset
	 * 
	 * @param id
	 * @param dsApiToken
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public CompletionStage<Result> deleteItem(Request request, Long id) {
		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFoundCS(errorJSONResponseObject("Dataset not found."));
		}

		// check user
		Optional<Person> user = getAuthenticatedAPIUser(request);
		Project project = ds.getProject();
		project.refresh();
		if (user == null || !project.visibleForPerson(user)) {
			return forbiddenCS(errorJSONResponseObject("Dataset not accessible for user."));
		}

		// check data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequestCS(errorJSONResponseObject("Expecting some data"));
		}

		// check access data
		String resource_id = df.get("resource_id");
		if (!nnne(resource_id)) {
			return badRequestCS(errorJSONResponseObject("No resource_id given."));
		}

		// get ds
		final EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// handle token
		String token = df.get("token");
		token = token == null ? "" : token;
		Optional<String> tempTokenOpt = Optional.of(token);

		// check authorized used in case of empty token
		if (token.isEmpty()) {
			if (project.editableByPerson(user)) {
				tempTokenOpt = eds.internalGetItemToken(resource_id);
			}
		}

		final Optional<String> finalToken = tempTokenOpt;
		return CompletableFuture.supplyAsync(() -> eds.deleteItem(resource_id, finalToken)).thenApplyAsync(data -> {
			if (!data.isPresent()) {
				return notFound(errorJSONResponseObject("Item not found")).as("application/json");
			} else {
				return ok(okJSONResponseObject("Item deleted")).as("application/json");
			}
		}).exceptionally(e -> {
			logger.error("Entity dataset delete problem", e);
			return badRequest();
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * log data for Annotation dataset
	 * 
	 * @param id
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result addAnnotationRecord(Request request, Long id) {

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || ds.getDsType() != DatasetType.ANNOTATION) {
			return badRequest(errorJSONResponseObject(
					"No or invalid dataset ID given. Only Annotation dataset is available for this request."));
		} else if (!ds.canAppend()) {
			return forbidden(errorJSONResponseObject("Dataset is closed (adjust start and end dates to open)."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		Project project = ds.getProject();
		if (user == null || !project.belongsTo(user) && !project.collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Project not accessible for user."));
		}

		// check data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("Expecting some data"));
		}

		// check if cluster exists AND does not belong to the project
		String refId = nss(df.get("refId"));
		Cluster cluster = Cluster.find.query().where().eq("refId", refId).findOne();
		if (cluster != null && !cluster.getProject().equals(project)) {
			return forbidden(errorJSONResponseObject("Cluster is not accessible in this project."));
		}

		// record the information in the database for this data set
		AnnotationDS ansc = (AnnotationDS) datasetConnector.getDatasetDS(ds);

		// title
		String title = df.get("title") == null ? null : df.get("title").replace(",", ";");

		// text
		String text = df.get("text");
		if (text == null) {
			return badRequest(errorJSONResponseObject("Empty annotation."));
		} else {
			text = text.replace(",", ";").replace("\n", "<br>");
		}

		// timestamp
		String date = df.get("ts-date");
		String time = df.get("ts-time");

		Date parsed = null;
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			parsed = format.parse(date + " " + time);
		} catch (ParseException pe) {
			// do nothing: if parsing does not work, we insert the current date
		}

		ansc.addRecord(cluster, parsed != null ? parsed : new Date(), title, text);

		// response
		return ok(okJSONResponseObject("Annotation added."));
	}

	/**
	 * add diary entry for a participant
	 * 
	 * @param id
	 * @param participant_id
	 * @return
	 */
	public Result addDiaryRecord(Request request, final Long id, long participant_id) {

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || ds.getDsType() != DatasetType.DIARY) {
			return badRequest(errorJSONResponseObject(
					"No or invalid dataset ID given. Only Diary dataset is available for this request."));
		} else if (!ds.canAppend()) {
			return forbidden(errorJSONResponseObject("Dataset is closed (adjust start and end dates to open)."));
		}

		// check participant id from invite_id
		if (participant_id < 0) {
			return badRequest(errorJSONResponseObject("Participant id not valid"));
		}

		// check participant access
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null || !ds.getProject().hasParticipant(participant)) {
			return forbidden(errorJSONResponseObject("Mismatch with participant and project. Please check."));
		}

		// check data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("Expecting some data"));
		}

		// record the information in the database for this data set
		DiaryDS dysc = (DiaryDS) datasetConnector.getDatasetDS(ds);

		// title
		String title = df.get("title") == null ? null : df.get("title").replace(",", ";");

		// text
		String text = df.get("text");
		if (text == null) {
			return badRequest(errorJSONResponseObject("Empty annotation."));
		} else {
			text = nss(df.get("text")).replace("\n", "<br>").replaceAll("[\\p{Cntrl}\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}]", "");
		}

		// timestamp
		String date = df.get("ts-date");
		String time = df.get("ts-time");

		Date parsed = null;
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			parsed = format.parse(date + " " + time);
		} catch (ParseException pe) {
			// do nothing: if parsing does not work, we insert the current date
		}

		dysc.addRecord(participant, parsed != null ? parsed : new Date(), title, text);

		// response
		return ok(okJSONResponseObject("Diary added."));
	}

	//////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode toJSON(Dataset dataset) {
		ObjectNode newObject = MetaDataUtils.toJson(dataset);

		// add additional information
		newObject.put("isOpenParticipation", dataset.isOpenParticipation());
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

	//////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * perform an upload into a COMPLETE dataset
	 * 
	 * @param ds
	 * @param description
	 * @param fileParts
	 * @throws IOException
	 */
	private boolean internalAddCompleteDSUpload(Dataset ds, String description,
			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts) throws IOException {

		boolean success = true;
		if (!fileParts.isEmpty()) {

			final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

			File theFolder = cpds.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			for (int i = 0; i < fileParts.size(); i++) {
				TemporaryFile tempfile = fileParts.get(i).getRef();
				String fileName = fileParts.get(i).getFilename();

				// restrict file type
				if (FileTypeUtils.looksLikeExecutableFile(fileName)) {
					logger.error("Executable file upload attempt blocked");
					success = false;
					continue;
				}

				Optional<String> storeFile = cpds.storeFile(tempfile.path().toFile(), fileName);
				if (storeFile.isPresent()) {
					cpds.addRecord(storeFile.get(), description, new Date());
				} else {
					success = false;
				}
			}
		} else {
			success = false;
		}

		if (success) {
			LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.MODIFY,
					"Files uploaded to dataset: " + ds.getName(), ds.getProject());
		} else {
			LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.MODIFY,
					"Some files failed to be uploaded to dataset: " + ds.getName(), ds.getProject());
		}

		return success;
	}

	/**
	 * perform an upload into a MOVEMENT dataset
	 * 
	 * @param ds
	 * @param participantName
	 * @param description
	 * @param fileParts
	 * @throws IOException
	 */
	private boolean internalAddMovementDSUpload(Dataset ds, String participantName, String description,
			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts) throws IOException {

		boolean success = true;

		if (!fileParts.isEmpty()) {

			// find or create participant
			Participant participant = ds.getProject().getParticipants().stream()
					.filter(p -> p.getRealName().equals(participantName)).findAny().orElseGet(() -> {
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
				boolean isCorrectFormat = true;
				FilePart<TemporaryFile> filePart = fileParts.get(i);
				TemporaryFile tempfile = filePart.getRef();
				String fileName = filePart.getFilename();

				// only allow gpx or xml files to be upload to movement dataset
				if (!FileTypeUtils.looksLikeMovementDataFile(fileName)) {
					logger.error("Only GPX data files with gpx or xml format are allowed: " + fileName);
					isCorrectFormat = false;
					success = false;
					continue;
				}

				// content-based validation
				if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.XML)) {
					isCorrectFormat = false;
					success = false;
					continue;
				}

				Date now = new Date();

				// ensure that filename is unique on disk
				fileName = participant.getId() + "_" + now.getTime() + "_" + fileName;

				// store files only with correct format
				if (isCorrectFormat) {
					// store the file, and record status after importing the content
					Optional<String> storeFile = mvds.storeFile(tempfile.path().toFile(), fileName);
					if (storeFile.isPresent()) {
						// import file contents and add record to database
						if (mvds.importFileContents(tempfile.path().toFile(), participant)) {
							mvds.addRecord(participant, storeFile.get(), description, now, "imported fully");
						} else {
							mvds.addRecord(participant, storeFile.get(), description, now, "import failed");
							success = false;
						}
					}
				} else {
					mvds.addRecord(participant, fileName, description, now, "wrong file format");
					success = false;
				}
			}
		} else {
			success = false;
		}

		if (success) {
			LabNotesEntry.log(MovementDSController.class, LabNotesEntryType.MODIFY,
					"Files uploaded to dataset: " + ds.getName(), ds.getProject());
		} else {
			LabNotesEntry.log(MovementDSController.class, LabNotesEntryType.MODIFY,
					"Some files failed to be uploaded to dataset: " + ds.getName(), ds.getProject());
		}

		return success;
	}

	/**
	 * perform an upload into an Experience sampling dataset
	 * 
	 * @param ds
	 * @param participantName
	 * @param description
	 * @param fileParts
	 * @throws IOException
	 */
	private boolean internalAddExpSamplingDSUpload(Dataset ds, String participantName, String description,
			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts) throws IOException {

		boolean success = true;

		// find or create participant
		Participant participant = ds.getProject().getParticipants().stream()
				.filter(p -> p.getRealName().equals(participantName)).findAny().orElseGet(() -> {
					// create new participant
					Participant p = Participant.createInstance(participantName, "", "", ds.getProject());
					p.setStatus(ParticipationStatus.ACCEPT);
					p.setProject(ds.getProject());
					p.save();

					return p;
				});

		if (!fileParts.isEmpty()) {

			final ExpSamplingDS esds = (ExpSamplingDS) datasetConnector.getDatasetDS(ds);

			File theFolder = esds.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			for (int i = 0; i < fileParts.size(); i++) {
				FilePart<TemporaryFile> filePart = fileParts.get(i);
				TemporaryFile tempfile = filePart.getRef();
				String fileName = filePart.getFilename();

				// restrict file type
				if (!FileTypeUtils.looksLikeCSVFile(fileName)) {
					logger.error("Only CSV files allowed in this dataset type.");
					success = false;
					continue;
				}

				// content-based validation
				if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.CSV)) {
					continue;
				}

				// ensure that filename is unique on disk
				fileName = participant.getId() + "_" + System.currentTimeMillis() + "_" + fileName;

				// store the file
				File temp = esds.getFile(fileName);
				tempfile.copyTo(temp, true);

				// import file content, and record status after importing the content
				if (esds.importFileContents(tempfile.path().toFile(), participant)) {
					esds.addRecord(participant.getId(), fileName, description, new Date(), "imported fully");
				} else {
					esds.addRecord(participant.getId(), fileName, description, new Date(), "import failed");
					success = false;
				}
			}
		} else {
			success = false;
		}

		if (success) {
			LabNotesEntry.log(ExpSamplingDSController.class, LabNotesEntryType.MODIFY,
					"Files uploaded to dataset: " + ds.getName(), ds.getProject());
		} else {
			LabNotesEntry.log(ExpSamplingDSController.class, LabNotesEntryType.MODIFY,
					"Some files failed to be uploaded to dataset: " + ds.getName(), ds.getProject());
		}

		return success;
	}

	/**
	 * perform an upload into a MEDIA dataset
	 * 
	 * @param ds
	 * @param participantName
	 * @param description
	 * @param fileParts
	 * @throws IOException
	 */
	private boolean internalAddMediaDSUpload(Request request, Dataset ds, String participantName, String description,
			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts) throws IOException {

		boolean success = true;

		// find or create participant
		Participant participant = participantName == null ? Participant.EMPTY_PARTICIPANT
				: ds.getProject().getParticipants().stream().filter(p -> p.getRealName().equals(participantName))
						.findAny().orElseGet(() -> {
							// create new participant
							Participant p = Participant.createInstance(participantName, "", "", ds.getProject());
							p.setStatus(ParticipationStatus.ACCEPT);
							p.setProject(ds.getProject());
							p.save();

							return p;
						});

		if (!fileParts.isEmpty()) {

			final MediaDS mdds = (MediaDS) datasetConnector.getDatasetDS(ds);

			File theFolder = mdds.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			for (int i = 0; i < fileParts.size(); i++) {
				boolean isCorrectFormat = true;
				FilePart<TemporaryFile> filePart = fileParts.get(i);
				TemporaryFile tempfile = filePart.getRef();
				String fileName = filePart.getFilename();
				String fileType = filePart.getContentType();
				String timestamp = fileName;

				// filename-based quick check
				if (!FileTypeUtils.looksLikeImageFile(fileName)) {
					isCorrectFormat = false;
					success = false;
					continue;
				}

				// content-based validation
				if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.IMAGE)) {
					isCorrectFormat = false;
					success = false;
					continue;
				}

				Date now = new Date();

				// ensure that filename is unique on disk
				fileName = participant.getId() + "_" + now.getTime() + "_" + fileName;

				// store files and add record only with correct format
				if (isCorrectFormat) {
					Optional<String> storeFile = mdds.storeFile(tempfile.path().toFile(), fileName);
					if (storeFile.isPresent()) {
						long ts = DataUtils.parseLong(timestamp);
						// import file content, and record status after importing the content
						mdds.addRecord(participant, storeFile.get(), description, now, "imported fully");
						mdds.importFileContents(participant, ds, storeFile.get(), request, fileType, description,
								ts != -1 ? new Date(ts) : now);
					}
				} else {
					mdds.addRecord(participant, fileName, description, now, "wrong file format");
					success = false;
				}
			}

		} else {
			success = false;
		}

		if (success) {
			LabNotesEntry.log(MediaDSController.class, LabNotesEntryType.MODIFY,
					"Files uploaded to dataset: " + ds.getName(), ds.getProject());
		} else {
			LabNotesEntry.log(MediaDSController.class, LabNotesEntryType.MODIFY,
					"Some files failed to be uploaded to dataset: " + ds.getName(), ds.getProject());
		}

		return success;
	}

	private String getValue(DynamicForm df, JsonNode json, String key) {
		String val = df.get(key);
		if (val == null && json != null && json.has(key)) {
			val = json.get(key).asText();
		}
		return val;
	}

}

package controllers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.api.AbstractDSController;
import controllers.api.AnnotationDSController;
import controllers.api.CompleteDSController;
import controllers.api.DiaryDSController;
import controllers.api.EntityDSController;
import controllers.api.ExpSamplingDSController;
import controllers.api.FitbitDSController;
import controllers.api.FormDSController;
import controllers.api.GoogleFitDSController;
import controllers.api.LinkedDSController;
import controllers.api.MediaDSController;
import controllers.api.MovementDSController;
import controllers.api.SurveyDSController;
import controllers.api.TimeseriesDSController;
import controllers.auth.ParticipantAuth;
import controllers.auth.UserAuth;
import controllers.tools.ActorController;
import controllers.tools.ChatbotController;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.ds.LinkedDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import models.vm.TimedMedia;
import play.Logger;
import play.api.mvc.Call;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.libs.Json;
import play.mvc.Http.MimeTypes;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import play.routing.JavaScriptReverseRouter;
import services.inlets.OOCSIService;
import services.inlets.OOCSIService.OOCSIDiagnostics;
import services.outlets.OOCSIStreamOutService;
import utils.DataUtils;
import utils.DatasetUtils;
import utils.auth.TokenResolverUtil;
import utils.rendering.FormMarkdown;
import utils.validators.AbstractValidator;
import utils.validators.Validators;

public class DatasetsController extends AbstractAsyncController {

	@Inject
	TimeseriesDSController timeseriesDSController;
	@Inject
	EntityDSController entityDSController;
	@Inject
	AnnotationDSController annotationDSController;
	@Inject
	DiaryDSController diaryDSController;
	@Inject
	FormDSController formDSController;
	@Inject
	SurveyDSController surveyDSController;
	@Inject
	CompleteDSController completeDSController;
	@Inject
	LinkedDSController linkedDSController;
	@Inject
	ExpSamplingDSController expSamplingDSController;
	@Inject
	MovementDSController movementDSController;
	@Inject
	MediaDSController mediaDSController;
	@Inject
	FitbitDSController fitbitDSController;
	@Inject
	GoogleFitDSController googleFitDSController;
	@Inject
	ActorController actorController;
	@Inject
	ChatbotController chatbotController;
	@Inject
	TokenResolverUtil tokenResolverUtil;
	@Inject
	DatasetConnector datasetConnector;
	@Inject
	SyncCacheApi cache;
	@Inject
	OOCSIService oocsiService;
	@Inject
	FormFactory formFactory;

	private static final Logger.ALogger logger = Logger.of(DatasetsController.class);

	@AddCSRFToken
	public Result view(Request request, long id) {
		// only continue if user is logged in
		Person user = getAuthenticatedUserOrReturn(request, redirect(controllers.routes.HomeController.login("", ""))
				.addingToSession(request, AbstractAsyncController.REDIRECT_URL, request.path()));

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		Project project = ds.getProject();
		if (!project.visibleFor(user)) {
			// non-public project --> home
			if (!project.isPublicProject()) {
				return redirect(HOME);
			}

			// public project
			return ok(views.html.datasets.viewPublic.render(ds, null, request));
		}

		// redirect to the right data set
		switch (ds.getDsType()) {
		case IOT:
		case TIMESERIES:
			return timeseriesDSController.view(request, id);
		case ENTITY:
			return entityDSController.view(request, id);
		case ANNOTATION:
			return annotationDSController.view(request, id);
		case DIARY:
			return diaryDSController.view(request, id);
		case FORM:
			return formDSController.view(request, id);
		case SURVEY:
			return surveyDSController.view(request, id);
		case COMPLETE:
			return ds.getCollectorType() != null ? switch (ds.getCollectorType()) {
			case Dataset.ACTOR:
				yield actorController.view(request, id);
			case Dataset.CHATBOT:
				yield chatbotController.view(request, id);
			default:
				yield completeDSController.view(request, id);
			} : completeDSController.view(request, id);
		case LINKED:
			return linkedDSController.view(request, id);
		case ES:
			return expSamplingDSController.view(request, id);
		case MOVEMENT:
			return movementDSController.view(request, id);
		case MEDIA:
			return mediaDSController.view(request, id);
		case FITBIT:
			return fitbitDSController.view(request, id);
		case GOOGLEFIT:
			return googleFitDSController.view(request, id);
		default:
			return redirect(PROJECT(id));
		}
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long id) {

		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request, redirect(HOME));

		// check if dataset exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		// check ownership
		Project project = ds.getProject();
		if (!project.editableBy(user)) {
			return redirect(PROJECT(project.getId()));
		}

		// redirect to the right data set
		switch (ds.getDsType()) {
		case IOT:
		case TIMESERIES:
			return timeseriesDSController.edit(request, id);
		case ENTITY:
			return entityDSController.edit(request, id);
		case ANNOTATION:
			return annotationDSController.edit(request, id);
		case DIARY:
			return diaryDSController.edit(request, id);
		case FORM:
			return formDSController.edit(request, id);
		case SURVEY:
			return surveyDSController.edit(request, id);
		case COMPLETE:
			return completeDSController.edit(request, id);
		case LINKED:
			return linkedDSController.edit(request, id);
		case ES:
			return expSamplingDSController.edit(request, id);
		case MOVEMENT:
			return movementDSController.edit(request, id);
		case MEDIA:
			return mediaDSController.edit(request, id);
		case FITBIT:
			return fitbitDSController.edit(request, id);
		case GOOGLEFIT:
			return googleFitDSController.edit(request, id);
		default:
			return redirect(routes.ProjectsController.view(id));
		}
	}

	@Authenticated(UserAuth.class)
	public Result delete(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect("/");
		}

		// check ownership
		Project project = ds.getProject();
		if (!project.belongsTo(username)) {
			return redirect(PROJECT(project.getId()));
		}

		// this is ONLY possible for saved exports -- for now
		if (!ds.isSavedExport()) {
			return redirect(PROJECT(project.getId()));
		}

		// remove dataset
		project.getDatasets().remove(ds);
		project.update();
		ds.delete();

		// back to export tool
		return redirect(controllers.tools.routes.DataExportController.index(-1));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * retrieve json array with all available downloads for this user
	 * 
	 * @return
	 */
	public Result allDownloads(Request request) {

		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request, redirect(HOME));

		// retrieve all available project dataset downloads
		ArrayNode an = Json.newArray();
		user.projects().stream().forEach(p -> {
			p.refresh();
			ObjectNode pon = an.addObject();
			pon.put("name", p.getName());
			ArrayNode apon = pon.putArray("datasets");
			p.getDatasets().stream().forEach(ds -> {
				ObjectNode dson = apon.addObject();
				dson.put("name", ds.getName());
				dson.put("url", routes.DatasetsController.download(ds.getId(), -1, -1, -1).path());
			});
		});
		user.collaborations().forEach(p -> {
			p.refresh();
			ObjectNode pon = an.addObject();
			pon.put("name", p.getName());
			ArrayNode apon = pon.putArray("datasets");
			p.getDatasets().stream().forEach(ds -> {
				ObjectNode dson = apon.addObject();
				dson.put("name", ds.getName());
				dson.put("url", routes.DatasetsController.download(ds.getId(), -1, -1, -1).path());
			});
		});

		// TODO this is taken out for now, because we need a better way to deal with license agreements
//		user.get().subscriptions.stream().map(c -> c.project).forEach(p -> {
//			p.refresh();
//			ObjectNode pon = an.addObject();
//			pon.put("name", p.name);
//			ArrayNode apon = pon.putArray("datasets");
//			p.datasets.stream().forEach(ds -> {
//				ObjectNode dson = apon.addObject();
//				dson.put("name", ds.name);
//				dson.put("url", routes.DatasetsController.download(ds.id, -1, -1).path());
//			});
//		});

		return ok(an);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * output HTTP POST diagnostics: the last successful upload
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public Result httpPostDiagnostics(Request request, long id) {
		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request, noContent());

		// check dataset and permissions
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.editableBy(username)) {
			return noContent();
		}

		Optional<String> message = cache.get("DatasetsController_httpPostDiagnostics_" + id);
		return message.isPresent() ? ok("🥁 " + message.get()) : ok("😴");
	}

	/**
	 * output OOCSI channel subscription diagnostics: the last successful message, warnings about event rate, loops,
	 * device_id etc.
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public Result oocsiDiagnostics(Request request, long id) {
		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request, noContent());

		// check dataset and permissions
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.editableBy(username)) {
			return noContent();
		}

		OOCSIDiagnostics od = oocsiService.getDiagnostics(id);
		return ok(views.html.elements.dataset.oocsiDiagnostics.render(od));
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result table(Request request, Long id) {

		// check if user exists
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not accessible.");
		}

		// dynamically add to projection so the table view shows additional properties
		switch (ds.getDsType()) {
		case DIARY:
			ds.getConfiguration().put(Dataset.DATA_PROJECTION, "title,text");
			break;
		case ANNOTATION:
			ds.getConfiguration().put(Dataset.DATA_PROJECTION, "cluster_id,title,text");
			break;
		case COMPLETE:
			ds.getConfiguration().put(Dataset.DATA_PROJECTION, "description,file_name");
			break;
		case MEDIA:
			ds.getConfiguration().put(Dataset.DATA_PROJECTION, "participant_id,description,link");
			break;
		default:
			break;
		}

		return ok(views.html.datasets.table.render(ds));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///
	/// CSV DOWNLOAD
	///
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result downloadTemporaryFile(Request request, String token) {
		Optional<String> filePath = cache.get("cachedTemporaryFile_" + token);
		return filePath.isPresent() ? ok(new File(filePath.get())) : noContent();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * download CSV data, potentially restricted by limit, time start and end
	 * 
	 * @param request
	 * @param id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> download(Request request, Long id, long limit, long start, long end) {
		return downloadCluster(request, id, -1l, limit, start, end);
	}

	/**
	 * download CSV data for a cluster, potentially restricted by limit, time start and end
	 * 
	 * @param request
	 * @param id
	 * @param cluster_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadCluster(Request request, long id, long cluster_id, long limit, long start,
			long end) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// check existing cluster id
		final Cluster cluster;
		if (cluster_id > -1) {
			Optional<Cluster> clusterOpt = project.getClusters().stream().filter(c -> c.getId().equals(cluster_id))
					.findFirst();
			if (!clusterOpt.isPresent()) {
				return cs(() -> redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request,
						"error", "Cluster is not part of this project."));
			} else {
				cluster = clusterOpt.get();
			}
		} else {
			// create empty cluster just in memory
			cluster = new Cluster("data");
			cluster.setId(-1l);
		}

		// only show if user is owner or guest within DF
		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request, "error",
						"Please log in to access the dataset."));

		// check if user exists
		if (!Person.findByEmail(username).isPresent()) {
			return cs(() -> redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request, "error",
					"User not found."));
		}

		// check ownership
		if (!project.visibleFor(username)) {
			return cs(() -> redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request, "error",
					"Project is not accessible."));
		}

		if (acceptLicenseFirst(request, username, project)) {
			// redirect to accept license first
			return redirectCS(
					controllers.routes.ProjectsController.license(project.getId(), controllers.routes.DatasetsController
							.downloadCluster(id, cluster_id, limit, start, end).relativeTo("/")));
		}

		return downloadExternal(request, id, ds, cluster, limit, start, end);
	}

	/**
	 * download CSV data for a device, potentially restricted by limit, time start and end
	 * 
	 * @param request
	 * @param id
	 * @param device_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadDevice(Request request, long id, long device_id, long limit, long start,
			long end) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// check existing cluster id
		final Cluster cluster;
		if (device_id > -1) {
			Optional<Device> deviceOpt = project.getDevices().stream().filter(c -> c.getId().equals(device_id))
					.findFirst();
			if (!deviceOpt.isPresent()) {
				return notFoundCS("Device is not part of this project.");
			} else {
				Device device = deviceOpt.get();

				// try to find a cluster for the device
				Optional<Cluster> clusterOpt = device.getClusters().stream().findAny();
				if (clusterOpt.isPresent()) {
					cluster = clusterOpt.get();
				} else {
					cluster = new Cluster(device.getSlug());
					cluster.getDevices().add(device);
				}
			}
		} else {
			return CompletableFuture.completedFuture(noContent());
		}

		// only show if user is owner or guest within DF
		Person user = getAuthenticatedUserOrReturn(request, notFound("Please log in to access the dataset."));

		// check ownership
		if (!project.visibleFor(user)) {
			return forbiddenCS("Project is not accessible.");
		}

		if (acceptLicenseFirst(request, user.getEmail(), project)) {
			// redirect to accept license first
			return redirectCS(
					controllers.routes.ProjectsController.license(project.getId(), controllers.routes.DatasetsController
							.downloadJsonDevice(id, device_id, limit, start, end).relativeTo("/")));
		}

		// prepare the cluster for proper filtering
		DatasetUtils.fillClusterSlots(cluster);
		return downloadExternal(request, id, ds, cluster, limit, start, end);
	}

	/**
	 * download JSON data for a participant, optionally filtered by limit, start and end timestamp
	 * 
	 * @param request
	 * @param id
	 * @param participant_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadParticipant(Request request, long id, long participant_id, long limit,
			long start, long end) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// check existing cluster id
		final Cluster cluster;
		if (participant_id > -1) {
			Optional<Participant> participantOpt = project.getParticipants().stream()
					.filter(c -> c.getId().equals(participant_id)).findFirst();
			if (!participantOpt.isPresent()) {
				return notFoundCS("Participant is not part of this project.");
			} else {
				Participant participant = participantOpt.get();

				// try to find participant's cluster first
				Optional<Cluster> clusterOpt = participant.getCluster();
				if (clusterOpt.isPresent()) {
					cluster = clusterOpt.get();
				} else {
					cluster = new Cluster(participant.getSlug());
					cluster.getParticipants().add(participant);
				}
			}
		} else {
			return CompletableFuture.completedFuture(noContent());
		}

		// only show if user is owner or guest within DF
		Person user = getAuthenticatedUserOrReturn(request, notFound("Please log in to access the dataset."));

		// check ownership
		if (!project.visibleFor(user)) {
			return forbiddenCS("Project is not accessible.");
		}

		if (acceptLicenseFirst(request, user.getEmail(), project)) {
			// redirect to accept license first
			return redirectCS(
					controllers.routes.ProjectsController.license(project.getId(), controllers.routes.DatasetsController
							.downloadParticipant(id, participant_id, limit, start, end).relativeTo("/")));
		}

		// prepare the cluster for proper filtering
		DatasetUtils.fillClusterSlots(cluster);
		return downloadExternal(request, id, ds, cluster, limit, start, end);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * public token CSV data download; this does NOT need the license acceptance because that would not work with
	 * external clients downloading the data
	 * 
	 * @param request
	 * @param token
	 * @param cluster_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	public CompletionStage<Result> downloadPublic(Request request, String token, Long cluster_id, long limit,
			long start, long end) {

		// retrieve id from token
		Long id = tokenResolverUtil.getDatasetIdFromToken(token);

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check project
		Project project = ds.getProject();

		// check if dataset is active, if not reject
		if (!project.isActive()) {
			return cs(() -> status(GONE, "The project is not active, download is not available anymore."));
		}

		// check if the token is current
		String pat = ds.getConfiguration().get(Dataset.PUBLIC_ACCESS_TOKEN);
		if (!token.equals(pat)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "The dataset is not accessible."));
		}

		project.refresh();

		// check existing cluster id
		final Cluster cluster;
		if (cluster_id > -1) {
			Optional<Cluster> cl = project.getClusters().stream().filter(c -> c.getId().equals(cluster_id)).findFirst();
			if (!cl.isPresent()) {
				return cs(() -> redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request,
						"error", "Cluster is not part of this project."));
			} else {
				cluster = cl.get();
			}
		} else {
			// create empty cluster just in memory
			cluster = new Cluster("data");
			cluster.setId(-1l);
		}

		return downloadExternalPublic(request, id, ds, cluster, limit, start, end);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private CompletionStage<Result> downloadExternal(Request request, long id, Dataset ds, final Cluster cluster,
			long limit, long start, long end) {
		// redirect to the right data set
		switch (ds.getDsType()) {
		case IOT:
		case TIMESERIES:
			return timeseriesDSController.downloadExternal(ds, cluster, limit, start, end);
		case ANNOTATION:
			return annotationDSController.downloadExternal(ds, cluster, limit, start, end);
		case ENTITY:
			return entityDSController.downloadExternal(ds, limit, start, end);
		case DIARY:
			return diaryDSController.downloadExternal(ds, cluster, limit, start, end);
		case FORM:
			return formDSController.downloadExternal(ds, limit, start, end);
		case SURVEY:
			return surveyDSController.downloadExternal(ds, cluster, limit, start, end);
		case COMPLETE:
			// call with transformation function for embedded links
			return completeDSController.downloadExternal(filePath -> {
				String fileName = DataUtils.extractFileNameFromPath(filePath);
				return controllers.api.routes.CompleteDSController.downloadLatestFile(ds.getId(), fileName)
						.absoluteURL(request, true);
			}, ds, limit, start, end);
		case LINKED:
			return redirectCS(Call.apply("GET", ds.getTargetObject(), ""));
		case ES:
			return expSamplingDSController.downloadExternal(ds, cluster, limit, start, end);
		case MOVEMENT:
			return movementDSController.downloadExternal(ds, cluster, limit, start, end);
		case MEDIA:
			// call with transformation function for embedded links
			return mediaDSController.downloadExternal(ds, filePath -> {
				String fileName = DataUtils.extractFileNameFromPath(filePath);
				fileName = utils.StringUtils.url2s(fileName);
				// remove all non-printable, non-ASCII characters
				fileName = fileName.replaceAll("\\P{Print}", "%");
				fileName = fileName.replaceAll("\\s", "%");
				return controllers.api.routes.CompleteDSController.downloadLatestFile(ds.getId(), fileName)
						.absoluteURL(request, true);
			}, cluster, limit, start, end);
		case FITBIT:
			return fitbitDSController.downloadExternal(ds, cluster, limit, start, end);
		case GOOGLEFIT:
			return googleFitDSController.downloadExternal(ds, cluster, limit, start, end);
		default:
			return redirectCS(PROJECT(id));
		}
	}

	private CompletionStage<Result> downloadExternalPublic(Request request, long id, Dataset ds, final Cluster cluster,
			long limit, long start, long end) {
		// redirect to the right data set
		switch (ds.getDsType()) {
		case IOT:
		case TIMESERIES:
			return timeseriesDSController.downloadExternal(ds, cluster, limit, start, end);
		case ANNOTATION:
			return annotationDSController.downloadExternal(ds, cluster, limit, start, end);
		case ENTITY:
			return entityDSController.downloadExternal(ds, limit, start, end);
		case DIARY:
			return diaryDSController.downloadExternal(ds, cluster, limit, start, end);
		case FORM:
			return formDSController.downloadExternal(ds, limit, start, end);
		case SURVEY:
			return surveyDSController.downloadExternal(ds, cluster, limit, start, end);
		case COMPLETE:
			// call with transformation function for embedded links
			return completeDSController.downloadExternal(filePath -> {
				String fileName = DataUtils.extractFileNameFromPath(filePath);
				if (ds.configuration(Dataset.PUBLIC_ACCESS_TOKEN, "").isEmpty()) {
					return controllers.api.routes.CompleteDSController.downloadLatestFile(ds.getId(), fileName)
							.absoluteURL(request, true);
				} else {
					return controllers.api.routes.CompleteDSController
							.downloadFilePublic(fileName, ds.configuration(Dataset.PUBLIC_ACCESS_TOKEN, ""))
							.absoluteURL(request, true);
				}
			}, ds, limit, start, end);
		case LINKED:
			return redirectCS(Call.apply("GET", ds.getTargetObject(), ""));
		case ES:
			return expSamplingDSController.downloadExternal(ds, cluster, limit, start, end);
		case MOVEMENT:
			return movementDSController.downloadExternal(ds, cluster, limit, start, end);
		case MEDIA:
			// call with transformation function for embedded links
			return mediaDSController.downloadExternal(ds, filePath -> {
				String fileName = DataUtils.extractFileNameFromPath(filePath);
				fileName = utils.StringUtils.url2s(fileName);
				// remove all non-printable, non-ASCII characters
				fileName = fileName.replaceAll("\\P{Print}", "%");
				fileName = fileName.replaceAll("\\s", "%");
				if (ds.configuration(Dataset.PUBLIC_ACCESS_TOKEN, "").isEmpty()) {
					return controllers.api.routes.CompleteDSController.downloadLatestFile(ds.getId(), fileName)
							.absoluteURL(request, true);
				} else {
					return controllers.api.routes.CompleteDSController
							.downloadFilePublic(fileName, ds.configuration(Dataset.PUBLIC_ACCESS_TOKEN, ""))
							.absoluteURL(request, true);
				}
			}, cluster, limit, start, end);
		case FITBIT:
			return fitbitDSController.downloadExternal(ds, cluster, limit, start, end);
		case GOOGLEFIT:
			return googleFitDSController.downloadExternal(ds, cluster, limit, start, end);
		default:
			return redirectCS(PROJECT(id));
		}
	}

	/**
	 * internal CSV data download, optionally filtered by cluster, limit, start and end timestamp
	 * 
	 * @param request
	 * @param id
	 * @param ds
	 * @param cluster
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	public CompletionStage<Result> downloadInternal(Request request, long id, Dataset ds, final Cluster cluster,
			long limit, long start, long end) {
		// redirect to the right data set
		switch (ds.getDsType()) {
		case IOT:
		case TIMESERIES:
			return timeseriesDSController.downloadInternal(ds, cluster, limit, start, end);
		case ANNOTATION:
			return annotationDSController.downloadInternal(ds, cluster, limit, start, end);
		case ENTITY:
			return entityDSController.downloadInternal(ds, limit, start, end);
		case DIARY:
			return diaryDSController.downloadInternal(ds, cluster, limit, start, end);
		case FORM:
			return formDSController.downloadInternal(ds, limit, start, end);
		case SURVEY:
			return surveyDSController.downloadInternal(ds, cluster, limit, start, end);
		case COMPLETE:
			return completeDSController.downloadInternal(Function.identity(), ds, limit, start, end);
		case LINKED:
			return redirectCS(Call.apply("GET", ds.getTargetObject(), ""));
		case ES:
			return expSamplingDSController.downloadInternal(ds, cluster, limit, start, end);
		case MOVEMENT:
			return movementDSController.downloadInternal(ds, cluster, limit, start, end);
		case MEDIA:
			return mediaDSController.downloadInternal(ds, cluster, limit, start, end);
		case FITBIT:
			return fitbitDSController.downloadInternal(ds, cluster, limit, start, end);
		case GOOGLEFIT:
			return googleFitDSController.downloadInternal(ds, cluster, limit, start, end);
		default:
			return redirectCS(PROJECT(id));
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///
	/// JSON DOWNLOAD
	///
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * download JSON data, potentially restricted by limit, time start and end
	 * 
	 * @param request
	 * @param id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadJson(Request request, Long id, long limit, long start, long end) {
		return downloadJsonCluster(request, id, -1L, limit, start, end);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * download JSON data for a cluster, potentially restricted by limit, time start and end
	 * 
	 * @param request
	 * @param id
	 * @param cluster_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadJsonCluster(Request request, long id, long cluster_id, long limit,
			long start, long end) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// check existing cluster id
		final Cluster cluster;
		if (cluster_id > -1) {
			Optional<Cluster> cl = project.getClusters().stream().filter(c -> c.getId().equals(cluster_id)).findFirst();
			if (!cl.isPresent()) {
				return cs(() -> redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request,
						"error", "Cluster is not part of this project."));
			} else {
				cluster = cl.get();
			}
		} else {
			// create empty cluster just in memory
			cluster = new Cluster("data");
			cluster.setId(-1l);
		}

		// only show if user is owner or guest within DF
		Person user = getAuthenticatedUserOrReturn(request, redirect(controllers.routes.DatasetsController.view(id))
				.addingToSession(request, "error", "Please log in to access the dataset."));

		// check ownership
		if (!project.visibleFor(user)) {
			return cs(() -> redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request, "error",
					"Project is not accessible."));
		}

		if (acceptLicenseFirst(request, user.getEmail(), project)) {
			// redirect to accept license first
			return redirectCS(
					controllers.routes.ProjectsController.license(project.getId(), controllers.routes.DatasetsController
							.downloadCluster(id, cluster_id, limit, start, end).relativeTo("/")));
		}

		return CompletableFuture.supplyAsync(() -> {
			File tempFile = play.libs.Files.singletonTemporaryFileCreator().create("data", "json").path().toFile();
			try (FileWriter fw = new FileWriter(tempFile)) {
				datasetConnector.getDatasetDS(ds).exportProjectedToFile(fw, cluster, limit, start, end);
			} catch (Exception e) {
				return notFound();
			}

			return ok(tempFile).withHeader(CONTENT_DISPOSITION, "attachment; filename=" + cluster.getSlug() + ".json")
					.as(MimeTypes.JSON);
		});
	}

	/**
	 * PACKAGE INTERNAL download JSON data for a cluster, potentially restricted by time start - end
	 * 
	 * @param request
	 * @param id
	 * @param cluster
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	public Result downloadJsonCluster(Request request, long id, Cluster cluster, long limit, long start, long end) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// only show if user is owner or guest within DF
		Person user = getAuthenticatedUserOrReturn(request, redirect(controllers.routes.DatasetsController.view(id))
				.addingToSession(request, "error", "Please log in to access the dataset."));

		// check ownership
		if (!project.visibleFor(user)) {
			return redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request, "error",
					"Project is not accessible.");
		}

		// generate export
		File tempFile = play.libs.Files.singletonTemporaryFileCreator().create("data", "json").path().toFile();
		try (FileWriter fw = new FileWriter(tempFile)) {
			datasetConnector.getDatasetDS(ds).exportProjectedToFile(fw, cluster, -1l, -1l, -1l);
		} catch (Exception e) {
			return notFound();
		}

		return ok(tempFile).withHeader(CONTENT_DISPOSITION, "attachment; filename=" + cluster.getSlug() + ".json")
				.as(MimeTypes.JSON);
	}

	/**
	 * download JSON data for a device, potentially restricted by limit, time start and end
	 * 
	 * @param request
	 * @param id
	 * @param device_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadJsonDevice(Request request, long id, long device_id, long limit, long start,
			long end) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// check existing device id
		final Cluster cluster;
		if (device_id > -1) {
			Optional<Device> deviceOpt = project.getDevices().stream().filter(c -> c.getId().equals(device_id))
					.findFirst();
			if (!deviceOpt.isPresent()) {
				return notFoundCS("Device is not part of this project.");
			} else {
				Device device = deviceOpt.get();
				cluster = new Cluster(device.getSlug());
				cluster.getDevices().add(device);
			}
		} else {
			return CompletableFuture.completedFuture(noContent());
		}

		// only show if user is owner or guest within DF
		Person user = getAuthenticatedUserOrReturn(request, notFound("Please log in to access the dataset."));

		// check ownership
		if (!project.visibleFor(user)) {
			return forbiddenCS("Project is not accessible.");
		}

		if (acceptLicenseFirst(request, user.getEmail(), project)) {
			// redirect to accept license first
			return redirectCS(
					controllers.routes.ProjectsController.license(project.getId(), controllers.routes.DatasetsController
							.downloadJsonDevice(id, device_id, limit, start, end).relativeTo("/")));
		}

		// prepare the cluster for proper filtering
		DatasetUtils.fillClusterSlots(cluster);
		return CompletableFuture.supplyAsync(() -> {
			File tempFile = play.libs.Files.singletonTemporaryFileCreator().create("data", "json").path().toFile();
			try (FileWriter fw = new FileWriter(tempFile)) {
				datasetConnector.getDatasetDS(ds).exportProjectedToFile(fw, cluster, limit, start, end);
			} catch (Exception e) {
				return notFound();
			}

			return ok(tempFile).withHeader(CONTENT_DISPOSITION, "attachment; filename=" + cluster.getSlug() + ".json")
					.as(MimeTypes.JSON);
		});
	}

	/**
	 * download JSON data for a participant, potentially restricted by limit, time start and end
	 * 
	 * @param request
	 * @param id
	 * @param participant_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadJsonParticipant(Request request, long id, long participant_id, long limit,
			long start, long end) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// check existing cluster id
		final Cluster cluster;
		if (participant_id > -1) {
			Optional<Participant> cl = project.getParticipants().stream().filter(c -> c.getId().equals(participant_id))
					.findFirst();
			if (!cl.isPresent()) {
				return notFoundCS("Participant is not part of this project.");
			} else {
				Participant participant = cl.get();
				cluster = new Cluster(participant.getSlug());
				cluster.getParticipants().add(participant);
			}
		} else {
			return CompletableFuture.completedFuture(noContent());
		}

		// only show if user is owner or guest within DF
		Person user = getAuthenticatedUserOrReturn(request, notFound("Please log in to access the dataset."));

		// check ownership
		if (!project.visibleFor(user)) {
			return forbiddenCS("Project is not accessible.");
		}

		if (acceptLicenseFirst(request, user.getEmail(), project)) {
			// redirect to accept license first
			return redirectCS(
					controllers.routes.ProjectsController.license(project.getId(), controllers.routes.DatasetsController
							.downloadJsonParticipant(id, participant_id, limit, start, end).relativeTo("/")));
		}

		// prepare the cluster for proper filtering
		DatasetUtils.fillClusterSlots(cluster);
		return CompletableFuture.supplyAsync(() -> {
			return ok(datasetConnector.getDatasetDS(ds).retrieveProjected(cluster, limit, start, end));
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * public token Json download; this does NOT need the license acceptance because that would not work with external
	 * clients downloading the data
	 * 
	 * @param token
	 * @param cluster_id
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	public CompletionStage<Result> downloadJsonPublic(Request request, String token, Long cluster_id, long limit,
			long start, long end) {

		// retrieve id from token
		Long id = tokenResolverUtil.getDatasetIdFromToken(token);

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check project
		Project project = ds.getProject();

		// check if dataset is active, if not reject
		if (!project.isActive()) {
			return cs(() -> status(GONE, "The project is not active, download is not available anymore."));
		}

		// check if the token is current
		String pat = ds.getConfiguration().get(Dataset.PUBLIC_ACCESS_TOKEN);
		if (!token.equals(pat)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "The dataset is not accessible."));
		}

		project.refresh();

		// check existing cluster id
		final Cluster cluster;
		if (cluster_id > -1) {
			Optional<Cluster> cl = project.getClusters().stream().filter(c -> c.getId().equals(cluster_id)).findFirst();
			if (!cl.isPresent()) {
				return cs(() -> redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request,
						"error", "Cluster is not part of this project."));
			} else {
				cluster = cl.get();
			}
		} else {
			// create empty cluster just in memory
			cluster = new Cluster("data");
			cluster.setId(-1l);
		}

		return CompletableFuture.supplyAsync(() -> {
			File tempFile = play.libs.Files.singletonTemporaryFileCreator().create("data", "json").path().toFile();
			try (FileWriter fw = new FileWriter(tempFile)) {
				datasetConnector.getDatasetDS(ds).exportProjectedToFile(fw, cluster, -1l, -1l, -1l);
			} catch (Exception e) {
				return notFound();
			}

			return ok(tempFile).withHeader(CONTENT_DISPOSITION, "attachment; filename=" + cluster.getSlug() + ".json")
					.as(MimeTypes.JSON);
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * redirect to index
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public Result webRedirect(Request request, Long id) {
		return redirect(routes.DatasetsController.web(id, ""));
	}

	/**
	 * canonical URL format
	 * 
	 * @param request
	 * @param id
	 * @param filePath
	 * @return
	 */
	public CompletionStage<Result> web(Request request, Long id, String filePath) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// check ownership or collaboration
			Optional<String> usernameOpt = getAuthenticatedUserName(request);

			// check if project exists
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return redirect(HOME);
			}

			// redirect to the right data set
			final Result redirectToDSResponse = redirect(controllers.routes.DatasetsController.view(id));
			if (!ds.getDsType().equals(DatasetType.COMPLETE)) {
				return redirectToDSResponse.addingToSession(request, "error",
						"No web access possible for this dataset.");
			}

			// refresh project
			Project project = ds.getProject();
			project.refresh();

			final Result redirectToProjectResponse = redirect(PROJECT(project.getId()));
			final Result notFoundPageResponse = notFound("Sorry, no content found for file \"" + filePath + "\".");

			final Optional<Boolean> framedPageOpt = request.header("Sec-Fetch-Dest").map(s -> s.indexOf("frame") > -1);
			final boolean framedPage = !framedPageOpt.isEmpty() && framedPageOpt.get();

			// no public access --> check user credentials
			// further checks needed if project is not public
			if (!project.isPublicProject()) {
				// access is ok, if this project belongs to the logged-in user or they are collaborating
				// no user --> check participant
				if (usernameOpt.isEmpty() || !project.visibleFor(usernameOpt.get())) {
					// try to find a participant
					String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
					if (!nnne(invite_token)) {
						return framedPage ? notFoundPageResponse
								: redirectToDSResponse.addingToSession(request, "error",
										"Please log in to access the dataset.");
					}

					// check participant
					long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
					if (participant_id < 0) {
						return framedPage ? notFoundPageResponse
								: redirectToProjectResponse.addingToSession(request, "error",
										"Please log in to access the dataset.");
					}

					// check participant is part of project
					Participant participant = Participant.find.byId(participant_id);
					if (participant == null || !project.hasParticipant(participant)) {
						return framedPage ? notFoundPageResponse
								: redirectToProjectResponse.addingToSession(request, "error",
										"Please log in to access the dataset.");
					}
				}
			}

			CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

			// treatment of index or missing filename
			if (filePath.equals("")) {
				// first check whether we have a configured entry point
				String entryPointFile = ds.configuration(Dataset.WEB_ACCESS_ENTRY, "");
				if (!entryPointFile.isEmpty()) {
					return redirect(routes.DatasetsController.web(id, entryPointFile));
				}

				// if there is no entry point, we try default entry points: index.html
				Optional<File> htmlIndexFile = cpds.getFile("index.html");
				if (htmlIndexFile.isPresent()) {
					return redirect(routes.DatasetsController.web(id, "index.html"));
				}

				// if there is no entry point, we try default entry points: index.md
				Optional<File> mdIndexFile = cpds.getFile("index.md");
				if (mdIndexFile.isPresent()) {
					return redirect(routes.DatasetsController.web(id, "index.md"));
				}

				// no entry points present
				if (usernameOpt.isPresent()) {
					return notFound(views.html.datasets.complete.web.render(project, ds,
							"No entry point (index.html or index.md) available or configured."));
				} else {
					return framedPage ? notFoundPageResponse
							: redirectToProjectResponse.addingToSession(request, "error",
									"No content available at this point.");
				}
			}

			// ensure we have a decoded file path
			final String decodedFilePath = utils.StringUtils.url2s(filePath).trim();

			// remove path from filename
			final String filename = Paths.get(decodedFilePath).getFileName().toString();

			// request the file
			Optional<File> requestedFile = cpds.getFile(filename);
			// file does not exist
			if (!requestedFile.isPresent()) {
				if (usernameOpt.isPresent()) {
					return framedPage ? notFoundPageResponse
							: notFound(views.html.datasets.complete.web.render(project, ds, "File not found."));
				} else {
					return framedPage ? notFoundPageResponse
							: redirectToProjectResponse.addingToSession(request, "error",
									"No content available at this point.");
				}
			}

			// check the file for special website properties
			File file = requestedFile.get();
			if (filename.endsWith(".md") && file.length() < 1024 * 1024) {
				// read file contents
				try {
					String contents = new String(Files.readAllBytes(Paths.get(file.getAbsolutePath())));
					return ok(views.html.datasets.complete.webmd.render(project, ds, FormMarkdown.renderHtml(contents)))
							.as("text/html; charset=utf-8");
				} catch (IOException e) {
					// log and return the file
					logger.error("Markdown transformation failed: " + file.getAbsolutePath());
					return ok(file).as("text/html; charset=utf-8");
				}
			} else if (filename.endsWith(".html")) {
				return ok(file).as("text/html; charset=utf-8");
			} else if (filename.endsWith(".min.js") || filename.endsWith(".min.css")) {
				return ok(file).withHeader("Cache-Control", "max-age=3600");
			} else if (filename.endsWith(".js") || filename.endsWith(".css")) {
				return ok(file).withHeader("Cache-Control", "max-age=60");
			} else {
				return ok(file);
			}
		});
	}

	/**
	 * canonical token URL format
	 * 
	 * @param request
	 * @param webToken
	 * @param filepath
	 * @return
	 */
	public CompletionStage<Result> webToken(Request request, String webToken, String filepath) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			String webTokenStr = webToken;

			// try to resolve the token to the dataset id
			long id = tokenResolverUtil.getDatasetIdFromToken(webTokenStr);
			if (id <= 0) {
				// try without path ending
				webTokenStr = webTokenStr.replaceAll("[/].*", "");
				id = tokenResolverUtil.getDatasetIdFromToken(webTokenStr);
				if (id <= 0) {
					return notFound();
				}
			}

			// check if project exists
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound();
			}

			// check data set type
			if (!ds.getDsType().equals(DatasetType.COMPLETE)) {
				return notFound();
			}

			// check token existance and correctness
			if (ds.configuration(Dataset.WEB_ACCESS_TOKEN, "").isEmpty()
					|| !ds.configuration(Dataset.WEB_ACCESS_TOKEN, "").equals(webTokenStr)) {
				return notFound();
			}

			// check project
			Project project = ds.getProject();
			project.refresh();

			Optional<String> usernameOpt = getAuthenticatedUserName(request);
			// abort if there is no user logged in, or the project does not belong to the logged-in user or they are
			// collaborating, AND web access token is not set
			if ((usernameOpt.isEmpty() || !project.editableBy(usernameOpt.get()))
					&& ds.configuration(Dataset.WEB_ACCESS_TOKEN, "").isEmpty()) {
				return redirect(HOME).addingToSession(request, "error",
						"Website not accessible for guests at the moment.");
			}

			CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

			// treatment of index or missing filename
			if (filepath.equals("")) {

				// first check whether we have a configured entry point
				String entryPointFile = ds.configuration(Dataset.WEB_ACCESS_ENTRY, "");
				if (!entryPointFile.isEmpty()) {
					return redirect(routes.DatasetsController.webToken(webTokenStr, entryPointFile));
				}

				// if there is no entry point, we try default entry points: index.html
				Optional<File> htmlIndexFile = cpds.getFile("index.html");
				if (htmlIndexFile.isPresent()) {
					return redirect(routes.DatasetsController.webToken(webTokenStr, "index.html"));
				}

				// if there is no entry point, we try default entry points: index.md
				Optional<File> mdIndexFile = cpds.getFile("index.md");
				if (mdIndexFile.isPresent()) {
					return redirect(routes.DatasetsController.webToken(webTokenStr, "index.md"));
				}

				// no index files present
				return usernameOpt.isEmpty() ? notFound()
						: redirect(LANDING).addingToSession(request, "error", "No content available at this point.");
			}

			// remove path from filename
			final String filename = Paths.get(filepath).getFileName().toString();

			// request the file
			Optional<File> requestedFile = cpds.getFile(filename);
			// file does not exist
			if (!requestedFile.isPresent()) {
				return usernameOpt.isEmpty() ? notFound()
						: redirect(LANDING).addingToSession(request, "error", "No content available at this point.");
			}

			// check the file for special website properties
			File file = requestedFile.get();
			if (filename.endsWith(".md") && file.length() < 1024 * 1024) {
				// read file contents
				try {
					String contents = new String(Files.readAllBytes(Paths.get(file.getAbsolutePath())));
					return ok(views.html.datasets.complete.webmd.render(project, ds, FormMarkdown.renderHtml(contents)))
							.as("text/html; charset=utf-8");
				} catch (IOException e) {
					// log and return the file
					logger.error("Markdown transformation failed: " + file.getAbsolutePath());
					return ok(file).as("text/html; charset=utf-8");
				}
			} else if (filename.endsWith(".html")) {
				return ok(file).as("text/html; charset=utf-8");
			} else if (filename.endsWith(".min.js") || filename.endsWith(".min.css")) {
				return ok(file).withHeader("Cache-Control", "max-age=3600");
			} else if (filename.endsWith(".js") || filename.endsWith(".css")) {
				return ok(file).withHeader("Cache-Control", "max-age=60");
			} else {
				return ok(file);
			}
		});
	}

	@AddCSRFToken
	public Result webEntryPointSelection(Request request, Long id) {
		Person user = getAuthenticatedUserOrReturn(request, forbidden());

		// find dataset and check access
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !user.canEdit(ds.getProject())) {
			return forbidden();
		}

		// check GET or POST
		if (request.method().equalsIgnoreCase("GET")) {
			// list files to choose an entry point from
			CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
			List<TimedMedia> files = cpds.getFiles();
			return ok(
					views.html.elements.dataset.config.outletWebAccessEntryPoint.render(ds, files, csrfToken(request)));
		} else if (request.method().equalsIgnoreCase("POST")) {

			// parse input and check if an entrypoint or -1 were given
			DynamicForm df = formFactory.form().bindFromRequest(request);
			Optional<Object> filename = df.value("filename");
			if (filename.isEmpty()) {
				return badRequest();
			}

			// we set the entry point here:
			// entrypoint -1 means no or default entry point
			String selectedEntryPoint = filename.get().toString();
			ds.getConfiguration().put(Dataset.WEB_ACCESS_ENTRY,
					selectedEntryPoint.equals("-1") ? "" : selectedEntryPoint);
			ds.update();

			return noContent();
		}

		// if nothing matches...
		return notFound();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletionStage<Result> timeseries(Request request, Long id) {

		// check if project exists
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFoundCS("");
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// only show if user is owner or
		Optional<Person> user = getAuthenticatedUser(request);
		if (!user.isPresent()) {
			if (!project.isPublicProject()) {
				return forbiddenCS("");
			}
		} else {
			// check ownership
			if (!project.visibleFor(user.get())) {
				return forbiddenCS("");
			}
		}

		// don't do this for linked datasets
		if (ds.getDsType() == DatasetType.LINKED) {
			return notFoundCS("");
		}

		return ((AbstractDSController) timeseriesDSController).timeseries(request, ds.getId()).exceptionally((e) -> {
			return ok("");
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result resetDataset(Request request, Long id) {
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset not found.");
		}

		String username = getAuthenticatedUserNameOrReturn(request,
				redirect(HOME).addingToSession(request, "error", "This action is not permitted for you."));
		Project p = Project.find.byId(ds.getProject().getId());
		if (p == null || !p.belongsTo(username)) {
			return redirect(HOME).addingToSession(request, "error", "This action is not permitted for you.");
		}

		// reset the extension column names for the dataset
		ds.getConfiguration().put(Dataset.DATA_PROJECTION, "");
		ds.update();

		// reset data
		LinkedDS lds = datasetConnector.getDatasetDS(ds);
		lds.resetDataset();

		// reset wearables
		if (ds.getDsType() == DatasetType.FITBIT || ds.getDsType() == DatasetType.GOOGLEFIT) {
			for (Wearable w : ds.getProject().getWearables()) {
				w.resetExpiry();
			}

			LabNotesEntry.log(Wearable.class, LabNotesEntryType.DELETE,
					"Expiry of the wearables in the project, " + ds.getProject().getName() + ", are reset.", p);
		}

		LabNotesEntry.log(Dataset.class, LabNotesEntryType.DELETE, "Dataset," + ds.getName() + ", is reset.", p);

		return redirect(routes.DatasetsController.view(id)).addingToSession(request, "message", "Dataset was reset.");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result configure(Request request, Long id, String key, String value) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME);
		}

		// validate the data for the key
		AbstractValidator configurationValidator = Validators.get(key);
		if (!configurationValidator.validate(value)) {
			return badRequest(configurationValidator.explainNo(key, value));
		}

		// configure something
		ds.getConfiguration().put(key, value);
		ds.update();

		LabNotesEntry.log(Dataset.class, LabNotesEntryType.CONFIGURE, "Dataset configured", p);

		// redirect to datasets view if value is empty (we are deleting a configuration), otherwise just ok back
		return value.isEmpty() ? redirect(routes.DatasetsController.view(id)) : ok();
	}

	@Authenticated(UserAuth.class)
	public Result configurePost(Request request, Long id, String key) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME);
		}

		String value = request.body().asText();

		// configure something
		ds.getConfiguration().put(key, value);
		ds.update();

		LabNotesEntry.log(Dataset.class, LabNotesEntryType.CONFIGURE, "Dataset configured", p);

		return ok();
	}

	@Authenticated(UserAuth.class)
	public Result configureWithToken(Request request, Long id, String key) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME);
		}

		// configure the dataset with key and token
		ds.getConfiguration().put(key, tokenResolverUtil.getDatasetToken(ds.getId()));
		ds.update();

		LabNotesEntry.log(Dataset.class, LabNotesEntryType.CONFIGURE, "Dataset configured", p);

		// redirect to dataset view
		return redirect(routes.DatasetsController.view(id));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * checks whether the project license needs to be accepted and was already accepted (cookie as evidence)
	 * 
	 * @param username
	 * @param project
	 * @return true if the license to be first accepted
	 */
	protected boolean acceptLicenseFirst(Request request, String username, Project project) {
		if (username == null || (!project.belongsTo(username) && !project.collaboratesWith(username)
				&& !project.subscribedBy(username))) {
			// check whether license was accepted first
			String licenseAccepted = session(request, "license_p_" + project.getId());
			return !nnne(licenseAccepted);
		}
		return false;
	}

	/**
	 * return the number of other datasets this channel is used for the same target
	 * 
	 * @param id
	 * @param targetChannel
	 * @param channelName
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result hasOOCSIChannel(long id, String targetChannel, String channelName) {

		// check channel name and also the targetChannel
		if (!nnne(channelName) || !(nss(targetChannel).equals(OOCSIService.OOCSI_CHANNEL)
				|| nss(targetChannel).equals(OOCSIService.OOCSI_SERVICE)
				|| nss(targetChannel).equals(OOCSIStreamOutService.OOCSI_OUTPUT))) {
			return ok("");
		}

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return ok("");
		}

		// find all datasets have the same channel name
		List<Dataset> dsList = new ArrayList<>();
		Dataset.find.all().forEach(dataset -> {
			// add to list if channel is present and it's not the current dataset
			if (dataset.hasOOCSIChannel(targetChannel, channelName) && !ds.getId().equals(dataset.getId())) {
				dsList.add(dataset);
			}
		});

		// channel name is valid
		if (dsList.isEmpty()) {
			return ok("");
		}

		// return "in" for the channel name is used by datasets in the same project
		return ok(dsList.size() + "");
	}

	/**
	 * generate Javascript routes resource
	 * 
	 * @param request
	 * @return
	 */
	public Result jsRoutes(Request request) {
		return ok(JavaScriptReverseRouter.create("jsRoutes", "jQuery.ajax", request.host(),
				routes.javascript.DatasetsController.configure(), //
				routes.javascript.DatasetsController.configurePost(), //
				routes.javascript.DatasetsController.download(), //
				routes.javascript.DatasetsController.downloadJson(), //
				routes.javascript.DatasetsController.downloadPublic(), //
				routes.javascript.DatasetsController.web(), //
				routes.javascript.DatasetsController.hasOOCSIChannel(), //
				routes.javascript.UsersController.setState(), //
				routes.javascript.ClustersController.view(), //
				routes.javascript.ClustersController.rename(), //
				routes.javascript.ClustersController.addDevice(), //
				routes.javascript.ClustersController.removeDevice(), //
				routes.javascript.ClustersController.addWearable(), //
				routes.javascript.ClustersController.removeWearable(), //
				routes.javascript.ClustersController.addParticipant(), //
				routes.javascript.ClustersController.removeParticipant(), //
				controllers.api2.routes.javascript.AuthApiController.generate(), //
				controllers.tools.routes.javascript.ActorController.execute(), //
				controllers.tools.routes.javascript.ActorController.save(), //
				controllers.tools.routes.javascript.ActorController.install(), //
				controllers.tools.routes.javascript.ActorController.log(), //
				controllers.tools.routes.javascript.ActorController.fileList(), //
				controllers.tools.routes.javascript.ActorController.file(), //
				controllers.tools.routes.javascript.DataExportController.data(), //
				controllers.tools.routes.javascript.DataExportController.save(), //
				controllers.api.routes.javascript.AnnotationDSController.record(), //
				controllers.api.routes.javascript.AnnotationDSController.recordForProject(), //
				controllers.api.routes.javascript.CompleteDSController.saveFile(), //
				controllers.api.routes.javascript.CompleteDSController.downloadFile(), //
				controllers.api.routes.javascript.CompleteDSController.importFileMe(), //
				controllers.api.routes.javascript.CompleteDSController.importStatus(), //
				controllers.api.routes.javascript.EntityDSController.addItem(), //
				controllers.api.routes.javascript.EntityDSController.getItem(), //
				controllers.api.routes.javascript.EntityDSController.updateItem(), //
				controllers.api.routes.javascript.EntityDSController.deleteItem(), //
				controllers.api.routes.javascript.EntityDSController.getItems(), //
				controllers.api.routes.javascript.EntityDSController.getItemsNested(), //
				controllers.api.routes.javascript.EntityDSController.getItemsMatching(), //
				controllers.api.routes.javascript.TimeseriesDSController.getItemsNested(), //
				controllers.api.routes.javascript.FitbitDSController.heartrate(), //
				controllers.api.routes.javascript.FormDSController.transform(), //
				controllers.api.routes.javascript.SurveyDSController.transform(), //
				controllers.api.routes.javascript.GoogleFitDSController.heartrate() //

		)).as(MimeTypes.JAVASCRIPT);
	}
}

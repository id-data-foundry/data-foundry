package controllers.api.js;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;

import controllers.DatasetsController;
import controllers.ParticipationController;
import controllers.auth.ParticipantAuth;
import controllers.swagger.AbstractApiController;
import controllers.swagger.ParticipantApiController;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.ds.DiaryDS;
import models.ds.EntityDS;
import models.ds.MediaDS;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import play.Logger;
import play.data.FormFactory;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import services.slack.Slack;
import utils.DataUtils;
import utils.auth.TokenResolverUtil;
import utils.validators.FileTypeUtils;

public class ParticipationApiController extends AbstractApiController {

	final DatasetsController datasetsController;
	final TokenResolverUtil tokenResolverUtil;
	final DatasetConnector datasetConnector;

	private static final Logger.ALogger logger = Logger.of(ParticipantApiController.class);

	@Inject
	public ParticipationApiController(DatasetsController datasetsController, FormFactory formFactory,
			TokenResolverUtil tokenResolverUtil, DatasetConnector datasetConnector) {
		super(formFactory, datasetConnector, tokenResolverUtil);

		this.datasetsController = datasetsController;
		this.tokenResolverUtil = tokenResolverUtil;
		this.datasetConnector = datasetConnector;
	}

	public CompletionStage<Result> jsAPI(Request request) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// only resolve the participant from session token
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id <= 0) {
				return ok(views.html.elements.api.participantJSAPI.render(null, request)).as(TEXT_JAVASCRIPT);
			}

			// check participant object
			Participant participant = Participant.find.byId(participant_id);
			if (participant == null) {
				return ok(views.html.elements.api.participantJSAPI.render(null, request)).as(TEXT_JAVASCRIPT);
			}

			participant.getProject().refresh();

			// check participant in the correct project
			long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
			if (project_id != participant.getProject().getId()) {
				return ok(views.html.elements.api.participantJSAPI.render(null, request)).as(TEXT_JAVASCRIPT);
			}

			final String deviceName;
			final String location;
			Dataset wds = participant.getProject().getParticipantStudyDataset();
			if (wds.getId() < 0) {
				// this case should not happen actually, because the PSD needs to be available to log an entry
				deviceName = participant.getName() + " PSP";
				location = "";
			} else {
				deviceName = participant.getName() + " PSP " + wds.getName();
				location = controllers.routes.DatasetsController.web(wds.getId(), "").absoluteURL(request, true);
			}

			// check devices, if empty, create new device and cluster
			List<Device> devices = participant.getClusterDevices();
			if (devices.isEmpty()) {
				// add new device and cluster it around the participant
				Device device = new Device();
				device.create();
				device.setName(deviceName);
				device.setCategory("web");
				device.setSubtype("");
				device.setIpAddress("");
				device.setLocation(location);
				device.setConfiguration("");
				device.setProject(participant.getProject());
				device.setPublicParameter1(participant.getPublicParameter1());
				device.setPublicParameter2(participant.getPublicParameter2());
				device.setPublicParameter3(participant.getPublicParameter3());
				device.save();
				devices.add(device);

				Cluster cluster = new Cluster(participant.getName() + "-" + deviceName);
				cluster.create();
				participant.getProject().getClusters().add(cluster);
				participant.getProject().update();

				// wrap cluster around device and participant
				cluster.add(participant);
				cluster.add(device);
			}

			String output = views.html.elements.api.participantJSAPI.render(participant, request).toString()
					.replace("</script>", "");
			return ok(output).as(TEXT_JAVASCRIPT);
		});
	}

	public CompletionStage<Result> anonymousJSAPI(Request request, long projectId) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// check if a participant is already given in the session
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id > 0) {
				// check participant object
				Participant participant = Participant.find.byId(participant_id);
				if (participant != null) {
					// check participant in the correct project
					long participantProjectId = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
					if (participantProjectId == projectId) {
						return redirect(controllers.api.js.routes.ParticipationApiController.jsAPI());
					}
				}
			}

			// find project and check if the project is open for anonymous participant sign-up
			Project project = Project.find.byId(projectId);
			if (project != null && project.isSignupOpen()) {
				// participant is not available --> create new anonymous participant
				Participant part = Participant.createInstance("-", "-",
						"Anonymous_sign-up_" + System.currentTimeMillis(), project);

				// add to project
				project.getParticipants().add(part);
				project.update();

				String participantId = tokenResolverUtil.getParticipationToken(project.getId(), part.getId());
				return redirect(controllers.api.js.routes.ParticipationApiController.jsAPI()).withNewSession()
						.addingToSession(request, ParticipantAuth.PARTICIPANT_ID, participantId);
			} else {
				// no participant found, and project not available or auto sign-up not granted
				return redirect(controllers.api.js.routes.ParticipationApiController.jsAPI());
			}
		});
	}

	public CompletionStage<Result> diaryEntry(Request request) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// only resolve the participant from session token
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id <= 0) {
				return notFound("Invalid token");
			}

			// check participant object
			Participant participant = Participant.find.byId(participant_id);
			if (participant == null) {
				return notFound("Participant not found");
			}

			participant.getProject().refresh();

			// check participant in the correct project
			long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
			if (project_id != participant.getProject().getId()) {
				return notFound("Project not found");
			}

			Dataset ds = participant.getProject().getDiaryDataset();
			if (ds == Dataset.EMPTY_DATASET) {
				return notFound("Dataset not found");
			}

			JsonNode jn = request.body().asJson();
			if (jn == null || !jn.isObject()) {
				return notFound("Diary entry data not found or not valid");
			}

			String title = jn.has("title") ? nss(jn.get("title").asText()) : "";
			String text = jn.has("text") ? nss(jn.get("text").asText()) : "";
			if (title.isEmpty() && text.isEmpty()) {
				return notFound("Diary entry data not found or not valid");
			}

			DiaryDS dds = (DiaryDS) datasetConnector.getDatasetDS(ds);
			dds.addRecord(participant, new Date(), title, text);

			return ok(Json.newObject()).as("application/json");
		});
	}

	public CompletionStage<Result> logEntry(Request request) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// only resolve the participant from session token
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id <= 0) {
				return notFound("Invalid token");
			}

			// check participant object
			Participant participant = Participant.find.byId(participant_id);
			if (participant == null) {
				return notFound("Participant not found");
			}

			participant.getProject().refresh();

			// check participant in the correct project
			long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
			if (project_id != participant.getProject().getId()) {
				return notFound("Project not found");
			}

			// get the request payload / data
			JsonNode jn = request.body().asJson();
			if (jn == null) {
				return notFound("Data invalid");
			}

			final String deviceName;
			final String location;
			Dataset wds = participant.getProject().getParticipantStudyDataset();
			if (wds.getId() < 0) {
				// this case should not happen actually, because the PSD needs to be available to log an entry
				deviceName = participant.getName() + " PSP";
				location = "";
			} else {
				deviceName = participant.getName() + " PSP " + wds.getName();
				location = controllers.routes.DatasetsController.web(wds.getId(), "").absoluteURL(request, true);
			}

			// check devices, if empty, create new device and cluster
			List<Device> devices = participant.getClusterDevices();
			if (devices.isEmpty()) {
				// add new device and cluster it around the participant
				Device device = new Device();
				device.create();
				device.setName(deviceName);
				device.setCategory("web");
				device.setSubtype("");
				device.setIpAddress("");
				device.setLocation(location);
				device.setConfiguration("");
				device.setProject(participant.getProject());
				device.setPublicParameter1(participant.getPublicParameter1());
				device.setPublicParameter2(participant.getPublicParameter2());
				device.setPublicParameter3(participant.getPublicParameter3());
				device.save();
				devices.add(device);

				Cluster cluster = new Cluster(participant.getName() + "-" + deviceName);
				cluster.create();
				participant.getProject().getClusters().add(cluster);
				participant.getProject().update();

				// wrap cluster around device and participant
				cluster.add(participant);
				cluster.add(device);
			}

			// log item
			Dataset ds = participant.getProject().getIoTDataset();
			if (ds == Dataset.EMPTY_DATASET) {
				return notFound("Dataset not found");
			}

			String activity = jn.has("activity") ? nss(jn.get("activity").asText()) : "";
			String data = jn.has("data") ? nss(jn.get("data").asText()) : "";
			if (activity.isEmpty() && data.isEmpty()) {
				return notFound("Data not found or not valid");
			}

			TimeseriesDS lds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
			lds.addRecord(devices.isEmpty() ? null : devices.get(0), new Date(), activity, data);

			return ok(Json.newObject()).as("application/json");
		});
	}

	public CompletionStage<Result> uploadImage(Request request) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// check invite_token
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check invite_token again
			if (invite_token == null || invite_token.length() == 0) {
				return notFound("Invalid token");
			}

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id < 0) {
				return notFound("Invalid token");
			}

			// check participant
			Participant participant = Participant.find.byId(participant_id);
			if (participant == null) {
				return notFound("Participant not found");
			}

			// retrieve participant project
			Project project = participant.getProject();
			project.refresh();

			// retrieve and check dataset
			final Dataset ds = project.getMediaDataset();
			if (ds == null || !ds.canAppend() || !ds.isOpenParticipation() || ds.getDsType() != DatasetType.MEDIA) {
				return notFound("Dataset not found or misconfigured");
			}

			try {
				Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
				if (body == null) {
					return badRequest("Request missing body");
				}

				List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
				if (!fileParts.isEmpty()) {
					final MediaDS cpds = (MediaDS) datasetConnector.getDatasetDS(ds);

					for (int i = 0; i < fileParts.size(); i++) {
						FilePart<TemporaryFile> filePart = fileParts.get(i);

						// filename-based quick check
						String tempFileName = filePart.getFilename();
						if (!FileTypeUtils.looksLikeImageFile(tempFileName)) {
							continue;
						}

						// content-based validation
						if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.IMAGE)) {
							continue;
						}

						Date now = new Date();

						// ensure that filename is unique on disk
						String fileName = participant.getId() + "_" + now.getTime() + "_upload.png";

						// store file, add record
						TemporaryFile file = filePart.getRef();
						Optional<String> storeFile = cpds.storeFile(file.path().toFile(), fileName);
						if (storeFile.isPresent()) {
							cpds.addRecord(participant, storeFile.get(), "Participant upload", now, "imported fully");
							cpds.importFileContents(
									participant, controllers.api.routes.MediaDSController
											.image(ds.getId(), storeFile.get()).absoluteURL(request, true),
									"png", "Participant upload", now);
						}
					}

					LabNotesEntry.log(ParticipationController.class, LabNotesEntryType.DATA,
							"Files uploaded to dataset: " + ds.getName(), ds.getProject());
				}
			} catch (NullPointerException e) {
				logger.error("Error in uploading dataset file.", e);
				Slack.call("Exception", e.getLocalizedMessage());

				return badRequest("File upload failed");
			}

			return ok();
		});
	}

	public CompletionStage<Result> getItem(Request request, String idStr) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// only resolve the participant from session token
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id <= 0) {
				return notFound("Invalid token");
			}

			// check participant object
			Participant participant = Participant.find.byId(participant_id);
			if (participant == null) {
				return notFound("Participant not found");
			}

			participant.getProject().refresh();

			// check participant in the correct project
			long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
			if (project_id != participant.getProject().getId()) {
				return notFound("Project not found");
			}

			// find the entity dataset
			Dataset ds = Dataset.EMPTY_DATASET;
			long id = DataUtils.parseLong(idStr, -1L);
			if (id == -1L) {
				ds = participant.getProject().getEntityDataset();
			} else {
				Optional<Dataset> dsOpt = participant.getProject().getDatasets().stream().filter(d -> d.getId() == id)
						.findFirst();
				if (dsOpt.isPresent()) {
					ds = dsOpt.get();
				}
			}

			if (ds == Dataset.EMPTY_DATASET) {
				return notFound("Dataset not found");
			}

			// retrieve element
			EntityDS entityDS = (EntityDS) datasetConnector.getDatasetDS(ds);
			return ok(entityDS.getItem(participant.getRefId(), Optional.empty()).orElse(Json.newObject()))
					.as("application/json");
		});
	}

	public CompletionStage<Result> setItem(Request request, String idStr) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// only resolve the participant from session token
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id <= 0) {
				return notFound("Invalid token");
			}

			// check participant object
			Participant participant = Participant.find.byId(participant_id);
			if (participant == null) {
				return notFound("Participant not found");
			}

			participant.getProject().refresh();

			// check participant in the correct project
			long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
			if (project_id != participant.getProject().getId()) {
				return notFound("Project not found");
			}

			// get the request payload / data
			JsonNode jn = request.body().asJson();

			// find the entity dataset
			Dataset ds = Dataset.EMPTY_DATASET;
			long id = DataUtils.parseLong(idStr, -1L);
			if (id == -1L) {
				ds = participant.getProject().getEntityDataset();
			} else {
				Optional<Dataset> dsOpt = participant.getProject().getDatasets().stream().filter(d -> d.getId() == id)
						.findFirst();
				if (dsOpt.isPresent()) {
					ds = dsOpt.get();
				}
			}

			if (ds == Dataset.EMPTY_DATASET) {
				return notFound("Dataset not found");
			}

			// set element
			EntityDS entityDS = (EntityDS) datasetConnector.getDatasetDS(ds);
			return ok(entityDS.updateItem(participant.getRefId(), Optional.empty(), jn).orElse(Json.newObject()))
					.as("application/json");
		});
	}

	public CompletionStage<Result> getDataCSV(Request request, long id, long limit, long start, long end) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// only resolve the participant from session token
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id <= 0) {
				return notFound("Invalid token");
			}

			// check participant object
			Participant participant = Participant.find.byId(participant_id);
			if (participant == null) {
				return notFound("Participant not found");
			}

			participant.getProject().refresh();

			// check participant in the correct project
			long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
			if (project_id != participant.getProject().getId()) {
				return notFound("Project not found");
			}

			// check dataset and dataset type
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound("No or invalid dataset ID given.");
			}

			// configure filtering
			final Cluster cluster;
			Optional<Cluster> clusterOpt = participant.getCluster();
			if (clusterOpt.isPresent()) {
				cluster = clusterOpt.get();
			} else {
				cluster = new Cluster(participant.getSlug());
				cluster.setId(-1l);
				cluster.getParticipants().add(participant);
			}

			try {
				return datasetsController.downloadInternal(request, id, ds, cluster, limit, start, end)
						.toCompletableFuture().get();
			} catch (InterruptedException | ExecutionException e) {
				return tooManyRequests();
			}
		});
	}

	public CompletionStage<Result> getDataJson(Request request, long id, long limit, long start, long end) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// only resolve the participant from session token
			String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

			// check participant id from invite_id
			long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
			if (participant_id <= 0) {
				return notFound("Invalid token");
			}

			// check participant object
			Participant participant = Participant.find.byId(participant_id);
			if (participant == null) {
				return notFound("Participant not found");
			}

			participant.getProject().refresh();

			// check participant in the correct project
			long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
			if (project_id != participant.getProject().getId()) {
				return notFound("Project not found");
			}

			// check dataset and dataset type
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound("No or invalid dataset ID given.");
			}

			// configure filtering
			final Cluster cluster;
			Optional<Cluster> clusterOpt = participant.getCluster();
			if (clusterOpt.isPresent()) {
				cluster = clusterOpt.get();
			} else {
				cluster = new Cluster(participant.getSlug());
				cluster.setId(-1l);
				cluster.getParticipants().add(participant);
			}

			return datasetsController.downloadJsonCluster(request, id, cluster, limit, start, end);
		});
	}
}

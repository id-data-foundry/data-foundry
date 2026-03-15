package controllers.tools;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import jakarta.inject.Inject;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.DiaryDS;
import models.sr.Participant;
import play.Environment;
import play.Logger;
import play.cache.SyncCacheApi;
import play.filters.csrf.AddCSRFToken;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.auth.TokenResolverUtil;

public class ActivityLoggerController extends AbstractAsyncController {

	@Inject
	TokenResolverUtil tokenResolver;

	@Inject
	Environment environment;

	@Inject
	SyncCacheApi cache;

	@Inject
	DatasetConnector datasetConnector;

	@Inject
	TokenResolverUtil tokenResolverUtil;

	private static final Logger.ALogger logger = Logger.of(ActivityLoggerController.class);

	/**
	 * Renders the setup page for the Activity Logger. Authenticated access required.
	 * 
	 * @param request
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result index(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(HOME));

		// Placeholder for logic to retrieve projects and datasets for the user
		// This will be populated similarly to DataLogger.index()
		ObjectNode projectsAndDatasets = Json.newObject();

		user.getOwnAndCollabProjects().stream().forEach(p -> {
			List<Dataset> diaryDatasets = p.getDiaryDatasets().stream().filter(ds -> ds.isActive())
			        .collect(Collectors.toList());

			if (!diaryDatasets.isEmpty()) {
				ObjectNode projectNode = projectsAndDatasets.putObject(p.getId() + "");
				projectNode.put("id", p.getId());
				projectNode.put("name", p.getName());

				ObjectNode datasetsNode = projectNode.putObject("datasets");
				diaryDatasets.forEach(ds -> {

					// make sure the dataset has an API token set
					if (!ds.getConfiguration().containsKey(Dataset.API_TOKEN)) {
						ds.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds.getId()));
					}
					ds.update();

					datasetsNode.putObject(ds.getId() + "").put("id", ds.getId()).put("name", ds.getName())
					        .put("open", ds.isOpenParticipation()).put("token", ds.configuration(Dataset.API_TOKEN, ""))
					        .put("direct_event_activities", ds.configuration(Dataset.DIRECT_EVENT_ACTIVITIES, ""))
					        .put("state_event_activities", ds.configuration(Dataset.STATE_EVENT_ACTIVITIES, ""));
				});

				ArrayNode participants = projectNode.putArray("participants");
				p.getParticipants().stream().forEach(participant -> participants.addObject()
				        .put("id", participant.getRefId()).put("name", participant.getName()));
			}
		});

		return ok(views.html.tools.activitylogger.index.render(projectsAndDatasets,
		        controllers.tools.routes.ActivityLoggerController.updateActivities(1L).absoluteURL(request,
		                !environment.isDev()),
		        controllers.tools.routes.ActivityLoggerController.logger(1L, 1L, "token", "NO_PARTICIPANT")
		                .absoluteURL(request, !environment.isDev()),
		        csrfToken(request)));
	}

	/**
	 * Renders the main logging interface for a specific dataset and participant. Accessible via API token (similar to
	 * DataLogger.datalogger).
	 * 
	 * @param request
	 * @param projectId
	 * @param datasetId
	 * @param datasetToken
	 * @param deviceId
	 * @return
	 */
	public Result logger(Request request, long projectId, long datasetId, String datasetToken, String participantId) {
		Project project = Project.find.byId(projectId);
		Dataset ds = Dataset.find.byId(datasetId);

		// Basic authorization checks
		if (ds == null || !ds.getProject().getId().equals(projectId) || !ds.isActive()
		        || !datasetToken.equals(ds.configuration(Dataset.API_TOKEN, ""))) {
			logger.warn("Access denied for activity logger: datasetId={}, projectId={}, participantId={}", datasetId,
			        projectId, participantId);
			return redirect(HOME);
		}

		Optional<Participant> part = project.getParticipants().stream().filter(p -> p.getRefId().equals(participantId))
		        .findFirst();

		// Check whether participant Id belongs to project if not open participation
		if (!ds.isOpenParticipation() && part.isEmpty()) {
			logger.warn("Access denied for activity logger: participant {} not registered for project {}",
			        participantId, projectId);
			return redirect(HOME);
		}

		String logEndpointUrl = controllers.tools.routes.ActivityLoggerController
		        .log(datasetId, datasetToken, participantId).absoluteURL(request, !environment.isDev());

		// Retrieve configured activities from dataset
		String directActivities = ds.configuration(Dataset.DIRECT_EVENT_ACTIVITIES, "");
		String stateActivities = ds.configuration(Dataset.STATE_EVENT_ACTIVITIES, "");

		return ok(views.html.tools.activitylogger.activityLogger.render(ds.getName(),
		        part.isPresent() ? part.get().getName() : "No participant selected", participantId, directActivities,
		        stateActivities, logEndpointUrl));
	}

	/**
	 * API endpoint to update the dataset configuration with activity lists. Authenticated access required.
	 * 
	 * @param request
	 * @param datasetId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> updateActivities(Request request, long datasetId) {
		return CompletableFuture.supplyAsync(() -> {
			Person user = getAuthenticatedUserOrReturn(request, forbidden("Not logged in"));
			Dataset ds = Dataset.find.byId(datasetId);

			if (ds == null) {
				return notFound("Dataset not found");
			}
			if (!ds.editableBy(user)) {
				return forbidden("User not authorized to edit this dataset");
			}

			JsonNode json = request.body().asJson();
			if (json == null || !json.isObject()) {
				return badRequest("Expecting Json data");
			}

			String directEventActivities = json.has("directEventActivities")
			        ? json.get("directEventActivities").asText()
			        : "";
			String stateActivities = json.has("stateEventActivities") ? json.get("stateEventActivities").asText() : "";

			ds.getConfiguration().put(Dataset.DIRECT_EVENT_ACTIVITIES, directEventActivities);
			ds.getConfiguration().put(Dataset.STATE_EVENT_ACTIVITIES, stateActivities);
			ds.update();

			return ok("Activities updated successfully");
		}).exceptionally(e -> {
			logger.error("Error updating activities for dataset " + datasetId, e);
			return internalServerError("Failed to update activities: " + e.getMessage());
		});
	}

	/**
	 * API endpoint for logging direct events and state changes to DiaryDS. Accessible via API token.
	 * 
	 * @param request
	 * @param datasetId
	 * @param datasetToken
	 * @param participantId
	 * @return
	 */
	public CompletionStage<Result> log(Request request, final Long datasetId, final String datasetToken,
	        final String participantId) {
		return CompletableFuture.supplyAsync(() -> {
			final JsonNode jn = request.body().asJson();
			if (jn == null || !jn.isObject()) {
				return badRequest("No data (object) given.");
			}

			Dataset ds = Dataset.find.byId(datasetId);
			if (ds == null) {
				return notFound("Dataset not found");
			}
			if (!ds.canAppend()) {
				return forbidden("Dataset not accessible for appending data.");
			}
			if (!datasetToken.equals(ds.configuration(Dataset.API_TOKEN, ""))) {
				return forbidden("Api token is not correct.");
			}

			ds.getProject().refresh();
			Optional<Participant> participantOpt = ds.getProject().getParticipants().stream()
			        .filter(d -> d.getRefId().equals(participantId)).findFirst();

			if (participantOpt.isEmpty() && !ds.isOpenParticipation()) {
				cache.set("ActivityLogger_httpPostDiagnostics_" + datasetId,
				        "Participant " + participantId + " not found at " + new Date(), 300);
				return notFound("Source participant not registered");
			}

			final DiaryDS diaryDS = (DiaryDS) datasetConnector.getDatasetDS(ds);
			ObjectNode dataToLog = (ObjectNode) jn;

			// The 'activity' field should be present in the incoming JSON
			if (!dataToLog.has("activity")) {
				return badRequest("Missing 'activity' field in log data.");
			}

			String activity = dataToLog.get("activity").asText();
			String text = dataToLog.has("text") ? dataToLog.get("text").asText() : "";

			// Log the activity.
			if (participantOpt.isPresent()) {
				diaryDS.addRecord(participantOpt.get(), new Date(), activity, text);
			} else {
				diaryDS.addRecord(participantId, new Date(), activity, text);
			}

			cache.set("ActivityLogger_httpPostDiagnostics_" + datasetId,
			        "Participant " + participantId + " logged activity at " + new Date(), 300);

			return ok("Activity logged successfully.");
		}).exceptionally(e -> {
			logger.error("Error logging activity for dataset " + datasetId, e);
			return internalServerError("Failed to log activity: " + e.getMessage());
		});
	}
}

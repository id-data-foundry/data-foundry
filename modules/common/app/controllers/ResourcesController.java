package controllers;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import datasets.DatasetConnector;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.DiaryDS;
import models.ds.ExpSamplingDS;
import models.ds.FitbitDS;
import models.ds.GoogleFitDS;
import models.ds.MediaDS;
import models.ds.MovementDS;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import play.cache.SyncCacheApi;
import play.mvc.Http.Request;
import play.mvc.Result;

public class ResourcesController extends AbstractAsyncController {

	@Inject
	DatasetConnector datasetsConnector;

	@Inject
	DatasetsController datasetsController;

	@Inject
	SyncCacheApi cache;

	public Result devices(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, noContent());
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(user)) {
			return noContent();
		}

		// cache response for 30 seconds to take load off the database
		return cache.getOrElseUpdate("project_" + id + "_resource_table_devices", () -> {
			Map<Long, Long> deviceUpdates = new HashMap<>();
			project.getDevices().stream().forEach(d -> {
				deviceUpdates.put(d.getId(), 0L);
			});
			project.getIoTDatasets().stream().forEach(ds -> {
				TimeseriesDS tds = datasetsConnector.getTypedDatasetDS(ds);
				tds.lastUpdatedSource(deviceUpdates);
			});
			return ok(views.html.sources.device.index.render(project.getDevices(), deviceUpdates));
		}, 10);
	}

	/**
	 * download data for given project, dataset and device
	 * 
	 * @param request
	 * @param id
	 * @param dsId
	 * @param deviceId
	 * @return
	 */
	public CompletionStage<Result> deviceData(Request request, long id, long dsId, long deviceId) {
		Person user = getAuthenticatedUserOrReturn(request, noContent());
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(user)) {
			return cs(() -> noContent());
		}

		Optional<Dataset> dsOpt = project.getDatasets().stream().filter(d -> d.getId().equals(dsId)).findFirst();
		if (dsOpt.isEmpty()) {
			return cs(() -> noContent());
		}

		Optional<Device> deviceOpt = project.getDevices().stream().filter(d -> d.getId().equals(deviceId)).findFirst();
		if (deviceOpt.isEmpty()) {
			return cs(() -> noContent());
		}

		return datasetsController.downloadInternal(request, id, dsOpt.get(), new Cluster(deviceOpt.get()), -1L, -1L,
				-1L);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Result participants(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, noContent());
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(user)) {
			return noContent();
		}

		// cache response for 30 seconds to take load off the database
		return cache.getOrElseUpdate("project_" + id + "_resource_table_participants", () -> {
			Map<Long, Long> participantUpdates = new HashMap<>();
			project.getParticipants().stream().forEach(pa -> {
				participantUpdates.put(pa.getId(), 0L);
			});
			project.getDiaryDatasets().stream().forEach(ds -> {
				DiaryDS tds = datasetsConnector.getTypedDatasetDS(ds);
				tds.lastUpdatedSource(participantUpdates);
			});
			project.getExpSamplingDatasets().stream().forEach(ds -> {
				ExpSamplingDS tds = datasetsConnector.getTypedDatasetDS(ds);
				tds.lastUpdatedSource(participantUpdates);
			});
			project.getMediaDatasets().stream().forEach(ds -> {
				MediaDS tds = datasetsConnector.getTypedDatasetDS(ds);
				tds.lastUpdatedSource(participantUpdates);
			});
			project.getMovementDatasets().stream().forEach(ds -> {
				MovementDS tds = datasetsConnector.getTypedDatasetDS(ds);
				tds.lastUpdatedSource(participantUpdates);
			});
			return ok(views.html.sources.participant.index.render(project.getParticipants(), participantUpdates));
		}, 10);
	}

	/**
	 * download CSV data for given project, dataset and participant
	 * 
	 * @param request
	 * @param id
	 * @param dsId
	 * @param participantId
	 * @return
	 */
	public CompletionStage<Result> participantData(Request request, long id, long dsId, long participantId) {
		Person user = getAuthenticatedUserOrReturn(request, noContent());
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(user)) {
			return cs(() -> noContent());
		}

		Optional<Dataset> dsOpt = project.getDatasets().stream().filter(d -> d.getId().equals(dsId)).findFirst();
		if (dsOpt.isEmpty()) {
			return cs(() -> noContent());
		}

		Optional<Participant> participantOpt = project.getParticipants().stream()
				.filter(p -> p.getId().equals(participantId)).findFirst();
		if (participantOpt.isEmpty()) {
			return cs(() -> noContent());
		}

		return datasetsController.downloadInternal(request, id, dsOpt.get(), new Cluster(participantOpt.get()), -1L,
				-1L, -1L);
	}
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Result wearables(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, noContent());
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(user)) {
			return noContent();
		}

		// cache response for 30 seconds to take load off the database
		return cache.getOrElseUpdate("project_" + id + "_resource_table_wearables", () -> {
			Map<Long, Long> wearablesUpdates = new HashMap<>();
			project.getWearables().stream().forEach(pa -> {
				wearablesUpdates.put(pa.getId(), 0L);
			});
			project.getFitbitDatasets().stream().forEach(ds -> {
				FitbitDS tds = datasetsConnector.getTypedDatasetDS(ds);
				tds.lastUpdatedSource(wearablesUpdates);
			});
			project.getGoogleFitDatasets().stream().forEach(ds -> {
				GoogleFitDS tds = datasetsConnector.getTypedDatasetDS(ds);
				tds.lastUpdatedSource(wearablesUpdates);
			});
			return ok(views.html.sources.wearable.index.render(project.getWearables(), wearablesUpdates));
		}, 10);
	}

}

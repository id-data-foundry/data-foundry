package controllers.api;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Project;
import models.ds.AnnotationDS;
import models.sr.Cluster;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.components.OnboardingMessage;
import utils.components.OnboardingSupport;

public class AnnotationDSController extends AbstractDSController {

	@Inject
	public AnnotationDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
			OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);

	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// find data set
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		// check ownership and collaboratorship
		if (!ds.visibleFor(username)) {
			return redirect(PROJECT(ds.getProject().getId()));
		}

		// if no participant/wearable/cluster are in this project, then popup dialog
		Optional<OnboardingMessage> msg = onboardingSupport.setFlashMsg(ds.getProject(), username, "annotation_ds");
		return ok(views.html.datasets.annotation.view.render(ds, username, msg, request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// check authorization
		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"Project not valid or you don't have permissions for this action. Need to be the owner or "
							+ "a collaborator of the project.");
		}

		// display the add form page
		return ok(views.html.datasets.annotation.add.render(csrfToken(request), p));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// check authorization
		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"Project not valid or you don't have permissions for this action. Need to be the owner or "
							+ "a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data");
		}

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.ANNOTATION, p, df.get("description"),
				df.get("target_object"), "", df.get("license"));

		// dates
		storeDates(ds, df);
		ds.save();

		onboardingSupport.updateAfterDone(username, "new_dataset");

		// display the add form page
		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You don't have permissions for this action. Need to be the owner or a collaborator of the "
							+ "project.");
		}

		return ok(views.html.datasets.annotation.edit.render(csrfToken(request), ds));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You don't have permissions for this action. Need to be the owner or a collaborator of the "
							+ "project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Expecting some data");
		}

		ds.setName(htmlTagEscape(nss(df.get("dataset_name"), 64)));
		ds.setDescription(htmlTagEscape(nss(df.get("description"))));
		ds.setOpenParticipation(df.get("isPublic") == null ? false : true);
		ds.setLicense(df.get("license"));

		// dates
		storeDates(ds, df);

		// metadata
		storeMetadata(ds, df);
		ds.update();

		// display the add form page
		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	public Result recordForm(Request request, Long id, String cluster) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Project project = ds.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You need to be either project owner or collaborator to perform this action.");
		}

		// try to add pre-selection
		final List<Cluster> clusterSelection = new LinkedList<Cluster>();
		if (cluster != null && cluster.length() > 0) {
			Cluster c = Cluster.find.query().where().eq("refId", cluster).findOne();
			if (c != null) {
				clusterSelection.add(c);
			}
		}

		// add up to 10 clusters if no pre-selection
		if (clusterSelection.size() == 0) {
			for (Cluster c : project.getClusters()) {
				if (clusterSelection.size() < 10) {
					clusterSelection.add(c);
				}
			}
		}

		return ok(views.html.datasets.annotation.record.render(ds, clusterSelection));
	}

	@Authenticated(UserAuth.class)
	public Result record(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Project project = ds.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You need to be either project owner or collaborator to perform this action.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest("Expecting some data");
		}

		String clusterId = df.get("cluster_id");
		Cluster cluster = Cluster.find.query().where().eq("refId", clusterId).findOne();
		// check if cluster exists AND does not belong to the project
		if (cluster != null && !cluster.getProject().equals(project)) {
			return redirect(controllers.routes.DatasetsController.view(id));
		}

		// try to find cluster by participant
		if (cluster == null) {
			// String participantId = df.get("participant_id");
			// if(participantId != null) {
			// long pid = Long.parseLong(participantId);
			// Participant p = Participant.find.byId(pid);
			//// if()
			// }
		}

		// openness or user id
		// if (!ds.openParticipation && ds.project.id != cluster.project.id) {
		// return redirect(routes.AnnotationDSController.view(id));
		// }

		// record the information in the database for this data set
		AnnotationDS ansc = (AnnotationDS) datasetConnector.getDatasetDS(ds);

		// title
		String title = df.get("title").replace(",", ";");

		// text
		String text = df.get("text").replace(",", ";").replace("\n", "<br>");

		// timestamp
		String timestamp = df.get("timestamp");
		String date = df.get("ts-date");
		String time = df.get("ts-time");

		Date parsed = null;
		if (timestamp == null || timestamp.length() == 0) {
			try {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				parsed = format.parse(date + " " + time);
			} catch (ParseException pe) {
				// do nothing: if parsing does not work, we insert the current date
			}
		} else {
			parsed = new Date(Long.parseLong(timestamp));
		}

		ansc.addRecord(cluster, parsed != null ? parsed : new Date(), title, text);

		return redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request, "message",
				"Annotation added.");
	}

	@Authenticated(UserAuth.class)
	public Result recordForProject(Request request, Long id) {
		Project project = Project.find.byId(id);
		if (project == null) {
			return badRequest();
		}

		Dataset ds = project.getAnnotationDataset();
		if (ds == Dataset.EMPTY_DATASET) {
			// if its empty, make one, so we can continue here
			ds = datasetConnector.create("Annotations", DatasetType.ANNOTATION, project,
					"This dataset was created automatically because a researcher or participant in the project added an annotation.",
					"", "", null);
			ds.save();
		}

		return record(request, ds.getId());
	}

	public CompletionStage<Result> downloadExternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		return CompletableFuture.supplyAsync(() -> internalExport(ds, cluster.getId(), limit, start, end))
				.thenApplyAsync(chunks -> ok().chunked(chunks)
						.withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
						.as("text/csv"));

	}

	public CompletionStage<Result> downloadInternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		return CompletableFuture.supplyAsync(() -> internalExport(ds, cluster.getId(), limit, start, end))
				.thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));

	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * internal export projected data (if projection is available)
	 * 
	 * @param ds
	 * @param end
	 * @param start
	 * @return
	 */
	private Source<ByteString, ?> internalExport(Dataset ds, long cluster_id, long limit, long start, long end) {
		AnnotationDS ansc = (AnnotationDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> ansc.export(sourceActor, cluster_id, limit, start, end));
			return sourceActor;
		});
	}
}

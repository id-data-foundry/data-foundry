package controllers.api;

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
import models.ds.DiaryDS;
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

public class DiaryDSController extends AbstractDSController {

	@Inject
	public DiaryDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
	        OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// find data set
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(controllers.routes.HomeController.index()).addingToSession(request, "error",
			        "We could not find this dataset.");
		}

		// check ownership and collaboratorship
		if (!ds.visibleFor(username)) {
			return redirect(controllers.routes.ProjectsController.view(ds.getProject().getId()));
		}

		// if no participant are in this project, then popup dialog
		Optional<OnboardingMessage> msg = onboardingSupport.setFlashMsg(ds.getProject(), username, "diary_ds");
		return ok(views.html.datasets.diary.view.render(ds, username, msg, request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or "
			                + "a collaborator of the project.");
		}

		// display the add form page
		return ok(views.html.datasets.diary.add.render(csrfToken(request), p));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

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

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.DIARY, p, df.get("description"),
		        df.get("target_object"), "", df.get("license"));

		// dates
		storeDates(ds, df);
		ds.save();

		onboardingSupport.updateAfterDone(username, "new_dataset");

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

		return ok(views.html.datasets.diary.edit.render(csrfToken(request), ds));
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

		return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "message",
		        "Changes saved.");
	}

	public CompletionStage<Result> downloadExternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get participant ids from cluster
		final List<Long> participantIds = cluster.getParticipantList();
		return CompletableFuture.supplyAsync(() -> internalExport(ds, participantIds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks)
		                .withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
		                .as("text/csv"));
	}

	public CompletionStage<Result> downloadInternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get participant ids from cluster
		final List<Long> participantIds = cluster.getParticipantList();
		return CompletableFuture.supplyAsync(() -> internalExport(ds, participantIds, limit, start, end))
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
	private Source<ByteString, ?> internalExport(Dataset ds, List<Long> participantIds, long limit, long start,
	        long end) {
		DiaryDS dysc = (DiaryDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> dysc.export(sourceActor, participantIds, start, end));
			return sourceActor;
		});
	}
}

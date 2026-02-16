package controllers.api;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import com.google.inject.Inject;

import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Project;
import models.ds.FitbitDS;
import models.ds.LinkedDS;
import models.sr.Cluster;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.DatasetUtils;
import utils.DateUtils;
import utils.components.OnboardingMessage;
import utils.components.OnboardingSupport;
import utils.conf.Configurator;

public class FitbitDSController extends AbstractDSController {

	private final Configurator configurator;

	@Inject
	public FitbitDSController(Configurator configurator, FormFactory formFactory, SyncCacheApi cache,
	        DatasetConnector datasetConnector, OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);
		this.configurator = configurator;
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		// check ownership and collaboratorship
		if (!ds.visibleFor(username)) {
			return redirect(controllers.routes.ProjectsController.view(ds.getProject().getId()));
		}

		// if no participant/wearable/cluster are in this project, then popup dialog
		Optional<OnboardingMessage> msg = onboardingSupport.setFlashMsg(ds.getProject(), username, "fitbit_ds");
		return ok(views.html.datasets.fitbit.view.render(ds, username, msg, request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		// check configurations
		if (!configurator.isFitbitAvailable()) {
			return redirect(HOME).addingToSession(request, "error", "Fitbit service is not available.");
		}

		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		// display the add form page
		return ok(views.html.datasets.fitbit.add.render(csrfToken(request), p));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data");
		}

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.FITBIT, p, df.get("description"),
		        df.get("scopes"), df.get("isPublic"), df.get("license"));

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
			return redirect(HOME);
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		return ok(views.html.datasets.fitbit.edit.render(csrfToken(request), ds));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
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
		updateDates(ds, df);

		// metadata
		storeMetadata(ds, df);
		ds.update();

		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private void updateDates(Dataset ds, DynamicForm df) {
		Date[] dates = DateUtils.getDates(df.get("start-date"), df.get("end-date"), ds);

		if (ds.getStart() != null) {
			Date[] oldDates = { ds.getStart(), ds.getEnd() };
			DatasetUtils.updateWearables(oldDates, dates, ds);
		}

		if (ds.isEditable()) {
			ds.setStart(dates[0]);
		}
		ds.setEnd(dates[1]);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletionStage<Result> heartrate(Request request, Long id) {

		String username = getAuthenticatedUserNameOrReturn(request, notFound());

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirectCS(HOME);
		} else if (!ds.belongsTo(username)) {
			return redirectCS(HOME);
		}

		// record the information in the database for this dataset
		LinkedDS lsc = datasetConnector.getDatasetDS(ds);

		// serve this stream with 200 OK
		return CompletableFuture.supplyAsync(() -> createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> lsc.timeseries(sourceActor));
			return sourceActor;
		})).thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * download data as csv file
	 * 
	 * @param id
	 * @param cluster_id
	 * @param end
	 * @param start
	 * @return
	 */
	public CompletionStage<Result> downloadExternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get wearable ids (if cluster is given)
		final List<Long> wearablesIds = cluster.getWearableList();
		return CompletableFuture.supplyAsync(() -> internalExportRaw(ds, wearablesIds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks)
		                .withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
		                .as("text/csv"));
	}

	/**
	 * download data as csv text
	 * 
	 * @param id
	 * @param cluster_id
	 * @param end
	 * @param start
	 * @return
	 */
	public CompletionStage<Result> downloadInternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get wearable ids (if cluster is given)
		final List<Long> wearablesIds = cluster.getWearableList();
		return CompletableFuture.supplyAsync(() -> internalExportRaw(ds, wearablesIds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	/**
	 * internal export just the raw data
	 * 
	 * @param ds
	 * @param end
	 * @param start
	 * @param deviceIds
	 * @return
	 */
	private Source<ByteString, ?> internalExportRaw(Dataset ds, List<Long> wearablesIds, long limit, long start,
	        long end) {
		FitbitDS fbds = (FitbitDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> fbds.export(sourceActor, limit, start, end));
			return sourceActor;
		});
	}

}

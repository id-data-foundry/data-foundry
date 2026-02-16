package controllers.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.google.inject.Inject;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.Project;
import models.ds.LinkedDS;
import play.cache.SyncCacheApi;
import play.data.FormFactory;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.auth.TokenResolverUtil;
import utils.components.OnboardingSupport;

public abstract class AbstractDSController extends AbstractAsyncController {

	protected final FormFactory formFactory;
	protected final DatasetConnector datasetConnector;
	protected final SyncCacheApi cache;
	protected final OnboardingSupport onboardingSupport;

	@Inject
	protected TokenResolverUtil tokenResolverUtil;

	@Inject
	public AbstractDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
	        OnboardingSupport onboardingSupport) {
		this.formFactory = formFactory;
		this.cache = cache;
		this.datasetConnector = datasetConnector;
		this.onboardingSupport = onboardingSupport;
	}

	/**
	 * checks whether the project license needs to be accepted and was already accepted (cookie as evidence)
	 * 
	 * @param username
	 * @param project
	 * @return true if the license to be first accepted
	 */
	protected boolean acceptLicenseFirst(Request request, String username, Project project) {
		if (username == null || !project.editableBy(username)) {
			// check whether license was accepted first
			String licenseAccepted = session(request, "license_p_" + project.getId());
			return licenseAccepted.isEmpty();
		}
		return false;
	}

	/**
	 * deliver timeseries data for all dataset types
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> timeseries(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirectCS(HOME);
		}

		// check authorization
		if (!ds.visibleFor(username)) {
			return redirectCS(HOME);
		}

		// access the database for this data set
		LinkedDS linkedDS = datasetConnector.getDatasetDS(ds);

		// serve this stream with 200 OK
		return CompletableFuture.supplyAsync(() -> createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> linkedDS.timeseries(sourceActor));
			return sourceActor;
		})).thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	protected Source<ByteString, SourceQueueWithComplete<ByteString>> createStream() {
		return Source.<ByteString>queue(1024, OverflowStrategy.backpressure());
	}
}

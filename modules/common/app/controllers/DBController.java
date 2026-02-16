package controllers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.google.inject.Inject;

import datasets.DatasetConnector;
import models.Project;
import play.Environment;
import play.Logger;
import play.api.db.evolutions.ApplicationEvolutions;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.Request;
import play.mvc.Result;
import utils.admin.DeveloperDataUtil;
import utils.auth.TokenResolverUtil;

public class DBController extends AbstractAsyncController {

	@Inject
	private Environment environment;

	@Inject
	private ApplicationEvolutions evolutions;

	@Inject
	private DatasetConnector datasetConnector;

	@Inject
	private TokenResolverUtil tokenResolverUtil;

	@Inject
	private DeveloperDataUtil developerDataUtil;

	private static final Logger.ALogger logger = Logger.of(DBController.class);

	@AddCSRFToken
	public Result index(Request request) {
		if (!environment.isDev() || Project.find.all().size() > 0) {
			return redirect(routes.HomeController.index());
		}

		if (!evolutions.upToDate()) {
			return internalServerError("Application DB is still in progress.");
		}

		return ok(views.html.admin.devinit.render(csrfToken(request)));
	}

	@RequireCSRFCheck
	public CompletionStage<Result> generateData(Request request) {
		return CompletableFuture.supplyAsync(() -> {
			// check permissions
			if (!environment.isDev() || Project.find.all().size() > 0) {
				return redirect(routes.HomeController.index());
			}

			// generate data with a util
			try {
				developerDataUtil.initializeDevelopmentDB(datasetConnector, tokenResolverUtil);
			} catch (Exception e) {
				logger.error("Problem during dataset initialization with mock data", e);
			}

			// report generated data
			logger.info("Database initialized and set up with mock data");

			return ok("""
			        <hr>
			        <p>
			        	Fresh data has arrived. 👍
			        </p>
			        <a href="/" class="btn">Back to home</a>
			        """);
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Result resetDB() {

		if (!environment.isDev()) {
			return redirect(routes.HomeController.index());
		}

		// only accessible during test
		if (!environment.isTest()) {
			return forbidden();
		}

		logger.info("reset DB during test");

		developerDataUtil.resetDB();
		return ok();
	}

	public Result timeout() {
		try {
			Thread.sleep(10000l);
		} catch (InterruptedException e) {
			return badRequest();
		}

		return ok();
	}

}

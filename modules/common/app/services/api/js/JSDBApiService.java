package services.api.js;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.typesafe.config.Config;

import datasets.DatasetConnector;
import models.Project;
import services.api.GenericApiService;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;
import utils.concurrent.DatabaseExecutionContext;

@Singleton
public class JSDBApiService extends GenericApiService {

	private final DatabaseExecutionContext dbExecutionContext;

	@Inject
	public JSDBApiService(Config configuration, AdminUtils adminUtils, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolver, DatabaseExecutionContext dbExecutionContext) {
		super(configuration, adminUtils, datasetConnector, tokenResolver);
		this.dbExecutionContext = dbExecutionContext;
	}

	public Future<Void> submitApiRequest(JSDBApiRequest request) {
		return CompletableFuture.runAsync(() -> {
			try {
				Project project = Project.find.byId(request.getProjectId());
				String result = request.run(project, datasetConnector);
				request.setResult(Optional.ofNullable(result));
			} catch (Exception e) {
				request.setResult(Optional.empty());
			}
		}, dbExecutionContext);
	}
}


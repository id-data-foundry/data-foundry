package services.api.js;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.typesafe.config.Config;

import datasets.DatasetConnector;
import models.Project;
import services.api.GenericApiService;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;

@Singleton
public class JSDBApiService extends GenericApiService {

	private static final int JS_EXECUTION_GRANULARITY = 50;

	// we need at least 2 threads here to allow for the watchdog to activate while a job is running; regardless there
	// will never be two jobs running at the same time
	private final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(2);
	private final Queue<JSDBApiRequest> requests = new LinkedList<>();

	@Inject
	public JSDBApiService(Config configuration, AdminUtils adminUtils, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolver) {
		super(configuration, adminUtils, datasetConnector, tokenResolver);

		// check the post box regularly
		EXECUTOR.scheduleWithFixedDelay(() -> {
			JSDBApiRequest request = requests.poll();
			if (request != null) {
				try {
					Project project = Project.find.byId(request.getProjectId());
					String result = request.run(project, datasetConnector);
					request.setResult(Optional.ofNullable(result));
				} catch (Exception e) {
					request.setResult(Optional.empty());
				}
			}
		}, 100, JS_EXECUTION_GRANULARITY, TimeUnit.MILLISECONDS);
	}

	public Future<Void> submitApiRequest(JSDBApiRequest request) {
		// first offer the request to the queue
		requests.offer(request);

		// then wait until it's perhaps done
		return EXECUTOR.submit(() -> {
			for (int i = 0; i < 10; i++) {
				if (request.isCompleted()) {
					return null;
				}

				try {
					Thread.sleep(JS_EXECUTION_GRANULARITY);
				} catch (InterruptedException e) {
					throw e;
				}

				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
			}

			return null;
		});
	}
}

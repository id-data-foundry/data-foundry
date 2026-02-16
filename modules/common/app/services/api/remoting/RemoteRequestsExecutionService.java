package services.api.remoting;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import javax.inject.Singleton;

import jakarta.inject.Inject;
import play.Logger;
import services.maintenance.RealTimeNotificationService;

@Singleton
public class RemoteRequestsExecutionService {

	private final ExecutorService EXECUTOR;

	private RealTimeNotificationService realtimeNofifications;

	private static final Logger.ALogger logger = Logger.of(RemoteRequestsExecutionService.class);

	/**
	 * simple constructor for dependency injection, 8 parallel requests
	 * 
	 */
	@Inject
	public RemoteRequestsExecutionService(RealTimeNotificationService realtimeNotifications) {
		this(realtimeNotifications, 8);
	}

	public RemoteRequestsExecutionService(RealTimeNotificationService realtimeNotifications, int parallism) {
		EXECUTOR = Executors.newWorkStealingPool(parallism);
		this.realtimeNofifications = realtimeNotifications;
	}

	public Future<Void> submitRequest(RemoteApiRequest request, Consumer<RemoteApiRequest> function, int msTimeout) {
		return queueRequest(new RequestsWorklet(function, request, msTimeout));
	}

	private Future<Void> queueRequest(RequestsWorklet rw) {

		// notify system of request
		realtimeNofifications.notifyRemoteApiRequest(rw.request);

		// log start
		logger.trace("Queueing request " + formatRequestId(rw.request.id) + " with TTL "
		        + new Date(rw.request.getValidUntil()));
		rw.request.setState("queued");

		// then wait until it's perhaps done
		return EXECUTOR.submit((Callable<Void>) () -> {

			// if it's too late for this request to run; purging queue
			if (!rw.request.isValid()) {
				logger.trace("Discarding invalid request " + formatRequestId(rw.request.id));
				rw.request.setState("invalid");
				rw.request.timeout();
				return null;
			}

			// if it's too late for this request to run; purging queue
			if (rw.request.getValidUntil() < System.currentTimeMillis()) {
				logger.trace("Discarding (late) timed-out request " + formatRequestId(rw.request.id));
				rw.request.setState("timeout");
				rw.request.timeout();
				return null;
			}

			// if there is still time
			logger.trace("Starting request " + formatRequestId(rw.request.id) + " with TTL "
			        + new Date(rw.request.getValidUntil()));
			rw.request.setState("starting");

			rw.run();

			// we are done
			logger.trace("Finished request " + formatRequestId(rw.request.id));
			rw.request.setState("finished");

			return null;
		});
	}

	private String formatRequestId(String rid) {
		return rid.substring(0, 8);
	}

	class RequestsWorklet {
		Consumer<RemoteApiRequest> function;
		RemoteApiRequest request;

		public RequestsWorklet(Consumer<RemoteApiRequest> function, RemoteApiRequest request, int msTimeout) {
			this.function = function;
			this.request = request;
			request.setValidUntil(System.currentTimeMillis() + msTimeout);
		}

		public void run() {
			function.accept(request);
		}

		public boolean isValid() {
			return System.currentTimeMillis() < request.getValidUntil();
		}
	}
}

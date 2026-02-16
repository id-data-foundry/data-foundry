package services.maintenance;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pekko.actor.ActorSystem;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import scala.concurrent.ExecutionContext;
import services.api.remoting.RemoteApiRequest;

@Singleton
public class RealTimeNotificationService {

	// collectors
	AtomicInteger incomingDataCount = new AtomicInteger();
	Map<String, Date> activeUsers = new ConcurrentHashMap<>();
	int RUNNING_REQUESTS_CAPACITY = 20;
	Queue<RemoteApiRequest> runningRequests = new ConcurrentLinkedQueue<>();

	// stats
	float currentIncomingDataRate = 0;

	@Inject
	public RealTimeNotificationService(ActorSystem actorSystem, ExecutionContext executionContext) {
		actorSystem.scheduler().scheduleAtFixedRate(Duration.ofSeconds(10), Duration.ofSeconds(5), () -> {
			// calculate current stats
			currentIncomingDataRate = Math.round(10 * incomingDataCount.getAndSet(0) / 5.f) / 10.f;

			// prune the list of active users
			Date now = new Date(System.currentTimeMillis() - 1000 * 60 * 5);
			activeUsers.forEach((s, d) -> {
				if (now.after(d)) {
					activeUsers.remove(s);
				}
			});

			synchronized (runningRequests) {
				// discard old events
				for (RemoteApiRequest remoteApiRequest : runningRequests) {
					switch (remoteApiRequest.getState()) {
					case "invalid":
					case "timeout":
					case "finished":
						runningRequests.remove(remoteApiRequest);
					}
				}

				// check capacity, poll if full
				while (runningRequests.size() > RUNNING_REQUESTS_CAPACITY - 1) {
					runningRequests.poll();
				}
			}
		}, executionContext);
	}

	public void notifyRemoteApiRequest(RemoteApiRequest request) {
		runningRequests.offer(request);
	}

	public void notifyIncomingData(String inlet) {
		incomingDataCount.incrementAndGet();
	}

	public void notifyUser(String username) {
		activeUsers.computeIfAbsent(username, (s) -> new Date()).setTime(System.currentTimeMillis());
	}

	public Collection<RemoteApiRequest> runningRequests() {
		return Collections.unmodifiableCollection(runningRequests);
	}

	public Collection<String> activeUsers() {
		return Collections.unmodifiableCollection(activeUsers.keySet());
	}

	public float getIncomingRequestRate() {
		return currentIncomingDataRate;
	}

}

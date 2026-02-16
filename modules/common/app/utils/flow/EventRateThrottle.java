package utils.flow;

import java.util.LinkedList;
import java.util.Queue;

public class EventRateThrottle extends Throttle {

	final long eventRateDuration;
	final Queue<Long> events = new LinkedList<>();

	public EventRateThrottle(int ms, int eventRateDuration) {
		super(ms);

		this.eventRateDuration = eventRateDuration;
	}

	@Override
	public boolean check(long timestamp) {
		cleanQueue();

		// offer new event
		events.offer(timestamp);

		return super.check(timestamp);
	}

	public float getEventRate() {
		cleanQueue();

		return Math.round(10 * events.size() / ((float) eventRateDuration / 1000)) / 10F;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private void cleanQueue() {
		long cutoff = System.currentTimeMillis() - eventRateDuration;

		// first clean
		while (events.peek() != null && events.peek().longValue() < cutoff) {
			events.poll();
		}
	}

}

package utils.flow;

public class Throttle {

	private long lastTimestamp = 0;
	protected final long timeout;

	public Throttle(int ms) {
		this.timeout = ms;
	}

	/**
	 * check whether we need to throttle
	 * 
	 * @return true if we need to throttle, false otherwise
	 */
	public boolean check() {
		return check(System.currentTimeMillis());
	}

	/**
	 * check whether we need to throttle
	 * 
	 * @param timestamp
	 * @return true if we need to throttle, false otherwise
	 */
	public boolean check(long timestamp) {
		// check if the timeout has passed
		boolean needToThrottle = lastTimestamp + timeout > timestamp;

		// only update for successful (unthrottled) events
		if (!needToThrottle) {
			lastTimestamp = timestamp;
		}

		return needToThrottle;
	}
}

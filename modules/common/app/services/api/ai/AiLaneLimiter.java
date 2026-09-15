package services.api.ai;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import play.libs.Json;

/**
 * Per-category concurrency limiting that never parks a thread. A permit is a counter plus a queue of futures, not a
 * worker.
 */
@Singleton
public class AiLaneLimiter {

	/** Released exactly once; safe to call close() repeatedly. */
	public static final class Permit implements AutoCloseable {
		public static final Permit NOOP = new Permit(() -> {
		});
		private final Runnable release;
		private boolean done;

		Permit(Runnable release) {
			this.release = release;
		}

		@Override
		public void close() {
			if (!done) {
				done = true;
				release.run();
			}
		}
	}

	/** Thrown (as a failed stage) when a lane's wait queue is full → map to HTTP 429. */
	public static final class LaneFull extends RuntimeException {
		private static final long serialVersionUID = 1L;
		public final AiLane lane;

		public LaneFull(AiLane lane) {
			super("Lane " + lane + " queue is full");
			this.lane = lane;
		}
	}

	private static final class Gate {
		final int limit;
		final int maxQueue;
		int inFlight;
		final Deque<CompletableFuture<Permit>> waiting = new ArrayDeque<>();
		final AtomicInteger rejected = new AtomicInteger();

		Gate(int limit, int maxQueue) {
			this.limit = limit;
			this.maxQueue = maxQueue;
		}
	}

	private final Map<AiLane, Gate> gates = new EnumMap<>(AiLane.class);

	@Inject
	public AiLaneLimiter(Config config) {
		for (AiLane lane : AiLane.values()) {
			String base = "df.ai.lanes." + lane.name().toLowerCase();
			int limit = config.hasPath(base + ".concurrency") ? config.getInt(base + ".concurrency")
					: lane.concurrency();
			int queue = config.hasPath(base + ".queue") ? config.getInt(base + ".queue") : 64;
			gates.put(lane, new Gate(limit, queue));
		}
	}

	public CompletionStage<Permit> acquire(AiLane lane) {
		Gate g = gates.get(lane);
		if (g.limit <= 0) { // unlimited lane
			return CompletableFuture.completedFuture(Permit.NOOP);
		}
		synchronized (g) {
			if (g.inFlight < g.limit) {
				g.inFlight++;
				return CompletableFuture.completedFuture(new Permit(() -> release(lane)));
			}
			if (g.waiting.size() >= g.maxQueue) {
				g.rejected.incrementAndGet();
				return CompletableFuture.failedFuture(new LaneFull(lane));
			}
			CompletableFuture<Permit> f = new CompletableFuture<>();
			g.waiting.add(f); // no thread parked here
			return f;
		}
	}

	private void release(AiLane lane) {
		Gate g = gates.get(lane);
		while (true) {
			CompletableFuture<Permit> next;
			synchronized (g) {
				next = g.waiting.poll();
				if (next == null) { // nobody waiting
					g.inFlight--;
					return;
				}
			}
			// complete OUTSIDE the lock so downstream callbacks don't run holding the gate
			if (next.complete(new Permit(() -> release(lane)))) {
				return;
			}
			// that waiter already gave up (timed out) — keep the slot, try the next one
		}
	}

	public int limit(AiLane lane) {
		return gates.get(lane).limit;
	}

	public int rejected(AiLane lane) {
		return gates.get(lane).rejected.get();
	}

	public int inFlight(AiLane lane) {
		Gate g = gates.get(lane);
		synchronized (g) {
			return g.inFlight;
		}
	}

	public int queued(AiLane lane) {
		Gate g = gates.get(lane);
		synchronized (g) {
			return g.waiting.size();
		}
	}

	public ObjectNode snapshot() {
		ObjectNode on = Json.newObject();
		for (AiLane lane : AiLane.values()) {
			on.putObject(lane.name().toLowerCase()).put("limit", limit(lane)).put("inFlight", inFlight(lane))
					.put("queued", queued(lane)).put("rejected", rejected(lane));
		}
		return on;
	}
}

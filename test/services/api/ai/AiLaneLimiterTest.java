package services.api.ai;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import services.api.ai.AiLaneLimiter.Permit;

public class AiLaneLimiterTest {

	@Test
	public void testImmediateAcquisitionWhenSlotsAvailable() throws Exception {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("df.ai.lanes.llm.concurrency", 2);
		configMap.put("df.ai.lanes.llm.queue", 5);
		Config config = ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load());

		AiLaneLimiter limiter = new AiLaneLimiter(config);

		CompletableFuture<Permit> p1Future = limiter.acquire(AiLane.LLM).toCompletableFuture();
		assertTrue(p1Future.isDone());
		Permit p1 = p1Future.get(100, TimeUnit.MILLISECONDS);
		assertNotNull(p1);

		JsonNode stats = limiter.snapshot();
		assertEquals(1, stats.path("llm").path("inFlight").asInt());
		assertEquals(0, stats.path("llm").path("queued").asInt());

		p1.close();

		stats = limiter.snapshot();
		assertEquals(0, stats.path("llm").path("inFlight").asInt());
	}

	@Test
	public void testQueueingAndPermitRecycling() throws Exception {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("df.ai.lanes.tts.concurrency", 1);
		configMap.put("df.ai.lanes.tts.queue", 2);
		Config config = ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load());

		AiLaneLimiter limiter = new AiLaneLimiter(config);

		// First acquire takes the single concurrency slot
		Permit p1 = limiter.acquire(AiLane.TTS).toCompletableFuture().get(100, TimeUnit.MILLISECONDS);
		assertNotNull(p1);

		// Second acquire enters the queue
		CompletableFuture<Permit> p2Future = limiter.acquire(AiLane.TTS).toCompletableFuture();
		assertFalse(p2Future.isDone());

		JsonNode stats = limiter.snapshot();
		assertEquals(1, stats.path("tts").path("inFlight").asInt());
		assertEquals(1, stats.path("tts").path("queued").asInt());

		// Releasing p1 transfers permit to queued p2
		p1.close();

		Permit p2 = p2Future.get(500, TimeUnit.MILLISECONDS);
		assertNotNull(p2);

		stats = limiter.snapshot();
		assertEquals(1, stats.path("tts").path("inFlight").asInt());
		assertEquals(0, stats.path("tts").path("queued").asInt());

		p2.close();
		stats = limiter.snapshot();
		assertEquals(0, stats.path("tts").path("inFlight").asInt());
	}

	@Test
	public void testLaneFullRejectionWhenQueueExceeded() throws Exception {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("df.ai.lanes.image.concurrency", 1);
		configMap.put("df.ai.lanes.image.queue", 1);
		Config config = ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load());

		AiLaneLimiter limiter = new AiLaneLimiter(config);

		// Slot 1: Active in-flight
		Permit p1 = limiter.acquire(AiLane.IMAGE).toCompletableFuture().get(100, TimeUnit.MILLISECONDS);

		// Slot 2: Queue size 1 (reaches queue = 1)
		CompletableFuture<Permit> p2Future = limiter.acquire(AiLane.IMAGE).toCompletableFuture();
		assertFalse(p2Future.isDone());

		// Slot 3: Exceeds queue -> must immediately fail with LaneFull
		CompletableFuture<Permit> p3Future = limiter.acquire(AiLane.IMAGE).toCompletableFuture();
		assertTrue(p3Future.isCompletedExceptionally());

		try {
			p3Future.get();
			fail("Expected ExecutionException wrapping LaneFull");
		} catch (ExecutionException ee) {
			assertTrue(ee.getCause() instanceof AiLaneLimiter.LaneFull);
			AiLaneLimiter.LaneFull laneFull = (AiLaneLimiter.LaneFull) ee.getCause();
			assertEquals(AiLane.IMAGE, laneFull.lane);
		}

		// Clean up
		p1.close();
		Permit p2 = p2Future.get(500, TimeUnit.MILLISECONDS);
		p2.close();
	}

	@Test
	public void testIndependentLanesDoNotBlockEachOther() throws Exception {
		Map<String, Object> configMap = new HashMap<>();
		configMap.put("df.ai.lanes.stt.concurrency", 1);
		configMap.put("df.ai.lanes.stt.queue", 0);
		configMap.put("df.ai.lanes.llm.concurrency", 2);
		configMap.put("df.ai.lanes.llm.queue", 5);
		Config config = ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load());

		AiLaneLimiter limiter = new AiLaneLimiter(config);

		// Exhaust STT
		Permit sttPermit = limiter.acquire(AiLane.STT).toCompletableFuture().get(100, TimeUnit.MILLISECONDS);

		// STT is full and rejects further acquires
		assertTrue(limiter.acquire(AiLane.STT).toCompletableFuture().isCompletedExceptionally());

		// LLM lane is completely unaffected
		CompletableFuture<Permit> llmFuture = limiter.acquire(AiLane.LLM).toCompletableFuture();
		assertTrue(llmFuture.isDone());
		Permit llmPermit = llmFuture.get(100, TimeUnit.MILLISECONDS);
		assertNotNull(llmPermit);

		sttPermit.close();
		llmPermit.close();
	}
}

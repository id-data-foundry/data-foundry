package services.api.ai;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import com.typesafe.config.ConfigFactory;

import services.api.GenericApiService;

/**
 * Unit tests verifying that GenericApiService's 64-stripe ReentrantLock mechanism
 * distributes tokens uniformly and guarantees per-token mutual exclusion.
 */
public class GenericApiServiceStripedLockTest {

	static class TestGenericApiService extends GenericApiService {
		public TestGenericApiService() {
			super(ConfigFactory.load(), null, null, null);
		}

		@Override
		public ReentrantLock getLockForToken(String token) {
			return super.getLockForToken(token);
		}
	}

	@Test
	public void testLockResolutionAndDistribution() {
		TestGenericApiService service = new TestGenericApiService();

		// Null and empty tokens map safely to stripe 0
		ReentrantLock nullLock = service.getLockForToken(null);
		ReentrantLock emptyLock = service.getLockForToken("");
		assertNotNull(nullLock);
		assertSame(nullLock, emptyLock);

		// Identical tokens always resolve to the exact same lock instance
		String tokenA1 = new String("df-token-user-123");
		String tokenA2 = new String("df-token-user-123");
		assertSame(service.getLockForToken(tokenA1), service.getLockForToken(tokenA2));

		// Distinct tokens distribute across multiple stripes
		Set<ReentrantLock> observedLocks = new HashSet<>();
		for (int i = 0; i < 200; i++) {
			observedLocks.add(service.getLockForToken("user-token-prefix-" + i));
		}

		// With 64 stripes and 200 keys, we expect high stripe utilization (> 40 distinct stripes)
		assertTrue("Expected distributed stripe utilization across 64 locks, observed: " + observedLocks.size(),
				observedLocks.size() > 40);
	}
}

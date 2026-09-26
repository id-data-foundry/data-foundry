package services.api.ai;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import services.api.ThrottlingService;

/**
 * Unit test for ThrottlingService bounded cache.
 */
public class ThrottlingServiceSecurityTest {

	@Test
	public void testThrottlingServiceAllocatesAndConsumes() {
		ThrottlingService throttlingService = new ThrottlingService();

		// Should successfully resolve bucket and consume tokens
		boolean allowed = throttlingService.tryConsume("test-api-key");
		assertTrue("Initial token consumption should succeed", allowed);
		assertNotNull("Bucket should be resolved", throttlingService.resolveBucket("test-api-key"));
	}

	@Test
	public void testThrottlingServiceHandlesManyDistinctKeys() {
		ThrottlingService throttlingService = new ThrottlingService();

		for (int i = 0; i < 200; i++) {
			boolean allowed = throttlingService.tryConsume("key-" + i);
			assertTrue("Consumption for distinct key " + i + " should succeed", allowed);
		}
	}
}

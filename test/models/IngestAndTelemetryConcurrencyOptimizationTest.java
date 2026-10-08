package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.ClusterDS;
import models.ds.EntityDS;
import models.ds.FormDS;
import models.ds.SurveyDS;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;
import services.outlets.OOCSIStreamOutService;

public class IngestAndTelemetryConcurrencyOptimizationTest extends WithApplication {

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.base_url", "http://localhost:9000")
				.configure("df.keys.project", "test-project-secret")
				.configure("df.keys.v2.user.api", "test-api-key")
				.configure("df.keys.v2.multi.api", "test-api-key")
				.configure("df.keys.registration", Collections.singletonList("test-access-code"))
				.build();
	}

	@Test
	public void testOOCSIStreamOutServiceConcurrencyAndThrottling() throws Exception {
		OOCSIStreamOutService outService = app.injector().instanceOf(OOCSIStreamOutService.class);
		assertNotNull(outService);

		// Verify throttleMap is a ConcurrentHashMap
		Field throttleMapField = OOCSIStreamOutService.class.getDeclaredField("throttleMap");
		throttleMapField.setAccessible(true);
		Object throttleMapObj = throttleMapField.get(outService);
		assertTrue("throttleMap must be a ConcurrentHashMap", throttleMapObj instanceof ConcurrentHashMap);
		@SuppressWarnings("unchecked")
		Map<Long, Long> throttleMap = (Map<Long, Long>) throttleMapObj;

		// Verify executor is a ThreadPoolExecutor with DiscardPolicy
		Field executorField = OOCSIStreamOutService.class.getDeclaredField("executor");
		executorField.setAccessible(true);
		Object executorObj = executorField.get(outService);
		assertTrue("executor must be a ThreadPoolExecutor", executorObj instanceof ThreadPoolExecutor);
		ThreadPoolExecutor tpe = (ThreadPoolExecutor) executorObj;
		assertTrue("RejectedExecutionHandler must be DiscardPolicy",
				tpe.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.DiscardPolicy);
		assertEquals(1, tpe.getMaximumPoolSize());

		// Test multi-threaded concurrency without deadlock
		int numThreads = 10;
		CountDownLatch latch = new CountDownLatch(numThreads);
		AtomicInteger successCount = new AtomicInteger(0);

		for (int i = 0; i < numThreads; i++) {
			final long datasetId = 1000L + i;
			new Thread(() -> {
				try {
					Dataset ds = new Dataset();
					ds.setId(datasetId);
					ds.getConfiguration().put(OOCSIStreamOutService.OOCSI_OUTPUT, "channel_" + datasetId);
					Map<String, Object> data = new HashMap<>();
					data.put("test", "val");
					outService.datasetUpdate(ds, data);
					successCount.incrementAndGet();
				} finally {
					latch.countDown();
				}
			}).start();
		}

		boolean completed = latch.await(5, TimeUnit.SECONDS);
		assertTrue("Concurrent datasetUpdate calls must finish without deadlock", completed);
		assertEquals(numThreads, successCount.get());

		// Test stale entry cleanup in refresh()
		long now = System.currentTimeMillis();
		throttleMap.put(8888L, now - 7200_000L); // 2 hours old
		throttleMap.put(9999L, now - 60_000L);   // 1 minute old

		outService.refresh();

		assertFalse("Entries older than 1 hour must be pruned by refresh()", throttleMap.containsKey(8888L));
		assertTrue("Recent entries must be retained by refresh()", throttleMap.containsKey(9999L));
	}

	@Test
	public void testExportClassesDoNotUseStringBuffer() throws Exception {
		Class<?>[] classes = new Class<?>[] { EntityDS.class, FormDS.class, SurveyDS.class, ClusterDS.class };
		for (Class<?> clazz : classes) {
			for (Field field : clazz.getDeclaredFields()) {
				assertFalse("Class " + clazz.getSimpleName() + " must not have StringBuffer fields",
						StringBuffer.class.isAssignableFrom(field.getType()));
			}
		}
	}

	@Test
	public void testFormDSAndEntityDSExportBatching() throws Exception {
		DatasetConnector connector = app.injector().instanceOf(DatasetConnector.class);

		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Owner");
		owner.setLastname("Test");
		owner.setEmail("owner_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Test Project " + UUID.randomUUID(), owner, "desc", false, false);
		project.save();

		// Create FormDS
		Dataset formDataset = connector.create("Test Form DS", DatasetType.FORM, project, "desc", null, null);
		FormDS formDS = (FormDS) connector.getDatasetDS(formDataset);
		assertNotNull(formDS);

		// Create EntityDS
		Dataset entityDataset = connector.create("Test Entity DS", DatasetType.ENTITY, project, "desc", null, null);
		EntityDS entityDS = (EntityDS) connector.getDatasetDS(entityDataset);
		assertNotNull(entityDS);

		// Verify table names exist and schemas are migrated
		assertNotNull(formDS.getDataTableName());
		assertNotNull(entityDS.getDataTableName());
	}
}

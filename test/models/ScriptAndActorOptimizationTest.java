package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;

import datasets.DatasetConnector;
import models.ds.CompleteDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;
import services.api.js.JSDBApiRequest;
import services.api.js.JSDBApiService;
import services.jsexecutor.JSActor;
import services.jsexecutor.JSActorProxy;
import services.jsexecutor.JSExecutorService;

public class ScriptAndActorOptimizationTest extends WithApplication {

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
	public void testJSDBApiServiceDirectExecutionOnDatabaseExecutionContext() throws Exception {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("JSDB");
		owner.setLastname("Tester");
		owner.setEmail("jsdb_test_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("JSDB Project", owner, "Intro", false, false);
		project.save();
		assertNotNull(project.getId());

		JSDBApiService jsdbApiService = app.injector().instanceOf(JSDBApiService.class);
		assertNotNull(jsdbApiService);

		AtomicBoolean executed = new AtomicBoolean(false);
		JSDBApiRequest request = new JSDBApiRequest(project.getId()) {
			@Override
			public String run(Project p, DatasetConnector dc) {
				executed.set(true);
				return "result_for_" + p.getName();
			}
		};

		long startTime = System.currentTimeMillis();
		Future<Void> future = jsdbApiService.submitApiRequest(request);
		future.get(500, TimeUnit.MILLISECONDS);
		long duration = System.currentTimeMillis() - startTime;

		assertTrue("Request should be completed", request.isCompleted());
		assertTrue("Run method should have been executed", executed.get());
		assertEquals("result_for_" + project.getName(), request.getResult());
		assertTrue("Execution on managed pool should complete quickly without 50ms polling delay (took " + duration + "ms)",
				duration < 300);
	}

	@Test
	public void testActorProxyRelationshipMappingAndWearableBugFix() {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("ActorProxy");
		owner.setLastname("Tester");
		owner.setEmail("actor_proxy_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Actor Proxy Project", owner, "Intro", false, false);
		project.save();

		Participant participant = Participant.createInstance("Actor", "Participant",
				"actor_part_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com", project);
		participant.save();

		Device device = new Device();
		device.setName("Proxy Device");
		device.setProject(project);
		device.create();
		device.save();

		Wearable wearable = new Wearable();
		wearable.setName("Proxy Wearable");
		wearable.setBrand("Fitbit");
		wearable.setProject(project);
		wearable.create();
		wearable.save();

		Cluster cluster = new Cluster("actor_proxy_cluster_" + UUID.randomUUID().toString().substring(0, 8));
		cluster.setProject(project);
		cluster.save();
		cluster.add(participant);
		cluster.add(device);
		cluster.add(wearable);
		cluster.save();

		DatasetConnector connector = app.injector().instanceOf(DatasetConnector.class);
		Dataset scriptDs = connector.create("ActorScript", DatasetType.COMPLETE, project, "Actor script ds", "target", null);
		scriptDs.setCollectorType(Dataset.ACTOR);
		scriptDs.save();

		JSExecutorService executorService = app.injector().instanceOf(JSExecutorService.class);
		JSActor actor = executorService.addActor(scriptDs);
		assertNotNull(actor);

		// Generate the proxy graph
		JSActorProxy proxy = actor.createActorProxy(new JsonObject());
		assertNotNull(proxy);

		// 1. Verify clusters array
		assertEquals(1, proxy.clusters.size());
		JsonNode clusterNode = proxy.clusters.get(0);
		assertEquals(1, clusterNode.get("participants").size());
		assertEquals((long) participant.getId(), clusterNode.get("participants").get(0).asLong());
		assertEquals(1, clusterNode.get("devices").size());
		assertEquals((long) device.getId(), clusterNode.get("devices").get(0).asLong());
		// Verify wearable bug fix: cluster.wearables must contain wearable ID, NOT device ID
		assertEquals(1, clusterNode.get("wearables").size());
		assertEquals((long) wearable.getId(), clusterNode.get("wearables").get(0).asLong());

		// 2. Verify participants array
		assertEquals(1, proxy.participants.size());
		JsonNode participantNode = proxy.participants.get(0);
		assertEquals((long) participant.getId(), participantNode.get("id").asLong());
		assertEquals(1, participantNode.get("devices").size());
		assertEquals((long) device.getId(), participantNode.get("devices").get(0).asLong());
		// Verify wearable bug fix: participant.wearables must contain wearable ID, NOT device ID
		assertEquals(1, participantNode.get("wearables").size());
		assertEquals((long) wearable.getId(), participantNode.get("wearables").get(0).asLong());

		// 3. Verify devices array
		assertEquals(1, proxy.devices.size());
		JsonNode deviceNode = proxy.devices.get(0);
		assertEquals((long) device.getId(), deviceNode.get("id").asLong());
		assertEquals(1, deviceNode.get("participants").size());
		assertEquals((long) participant.getId(), deviceNode.get("participants").get(0).asLong());

		// 4. Verify wearables array
		assertEquals(1, proxy.wearables.size());
		JsonNode wearableNode = proxy.wearables.get(0);
		assertEquals((long) wearable.getId(), wearableNode.get("id").asLong());
		assertEquals("Fitbit", wearableNode.get("brand").asText());
		assertEquals(1, wearableNode.get("participants").size());
		assertEquals((long) participant.getId(), wearableNode.get("participants").get(0).asLong());
	}

	@Test
	public void testActorLogFileCachingAndEmptyShortCircuit() throws Exception {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("LogCache");
		owner.setLastname("Tester");
		owner.setEmail("log_cache_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Log Cache Project", owner, "Intro", false, false);
		project.save();

		DatasetConnector connector = app.injector().instanceOf(DatasetConnector.class);
		Dataset scriptDs = connector.create("LogScript", DatasetType.COMPLETE, project, "Log script ds", "target", null);
		scriptDs.setCollectorType(Dataset.ACTOR);
		scriptDs.save();

		JSExecutorService executorService = app.injector().instanceOf(JSExecutorService.class);
		JSActor actor = executorService.addActor(scriptDs);
		assertNotNull(actor);

		// Initially, cachedLogFile should be null
		assertNull(actor.getCachedLogFile());

		// Reflection access to private appendLogOutput
		Method appendLogMethod = JSActor.class.getDeclaredMethod("appendLogOutput", String.class, Date.class);
		appendLogMethod.setAccessible(true);

		// Short-circuit: empty or null output should NOT initialize log file or do I/O
		appendLogMethod.invoke(actor, "", new Date());
		assertNull("Cached log file should remain null when output is empty", actor.getCachedLogFile());

		appendLogMethod.invoke(actor, null, new Date());
		assertNull("Cached log file should remain null when output is null", actor.getCachedLogFile());

		// Non-empty output: should initialize and cache log file
		appendLogMethod.invoke(actor, "Log message line 1", new Date());
		File cachedFile = actor.getCachedLogFile();
		assertNotNull("Cached log file should be populated after non-empty write", cachedFile);
		assertTrue("Cached log file should exist on disk", cachedFile.exists());

		// Second non-empty output: should reuse cached file handle without re-resolving
		appendLogMethod.invoke(actor, "Log message line 2", new Date());
		assertSame("Cached log file reference should be reused", cachedFile, actor.getCachedLogFile());

		// Verify content in file
		String content = new String(Files.readAllBytes(cachedFile.toPath()));
		assertTrue("Log file should contain message 1", content.contains("Log message line 1"));
		assertTrue("Log file should contain message 2", content.contains("Log message line 2"));
	}
}

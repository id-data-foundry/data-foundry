package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import controllers.tools.ChatbotController;
import datasets.DatasetConnector;
import models.ds.CompleteDS;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;
import services.jsexecutor.graalsandbox.internal.GraalSandboxImpl;
import services.processing.AnalyticsService;
import services.processing.AnalyticsService.MetricsType;
import services.search.SearchService;

public class EngineAndBackgroundOptimizationTest extends WithApplication {

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
	public void testGraalSandboxSharedEngine() {
		GraalSandboxImpl sandbox1 = new GraalSandboxImpl();
		GraalSandboxImpl sandbox2 = new GraalSandboxImpl();

		assertNotNull("Sandbox 1 should have a polyglot engine", sandbox1.getPolyglotEngine());
		assertNotNull("Sandbox 2 should have a polyglot engine", sandbox2.getPolyglotEngine());

		// Verify both sandboxes share the exact same Engine instance
		assertSame("Sandboxes must share the exact same Engine instance",
				sandbox1.getPolyglotEngine(), sandbox2.getPolyglotEngine());
		assertSame("Sandboxes must share the static SHARED_ENGINE",
				GraalSandboxImpl.getSharedEngine(), sandbox1.getPolyglotEngine());
	}

	@Test
	public void testGraalSandboxDataAndScopeIsolation() throws Exception {
		GraalSandboxImpl sandboxA = new GraalSandboxImpl();
		GraalSandboxImpl sandboxB = new GraalSandboxImpl();

		// Sandbox A defines secret variables and data in its scope
		sandboxA.eval("var secretToken = 'user_A_private_key'; leakedGlobal = 12345;");

		// Sandbox B checks that Sandbox A's variables are completely undefined
		Object checkB = sandboxB.eval("typeof secretToken === 'undefined' && typeof leakedGlobal === 'undefined';");
		assertEquals("Sandbox B must have no visibility into Sandbox A's variables", Boolean.TRUE, checkB);

		// Sandbox A modifies an object prototype
		sandboxA.eval("Object.prototype.polluted = 'compromised';");

		// Sandbox B checks that its own Object prototype is unpolluted
		Object checkPollution = sandboxB.eval("typeof Object.prototype.polluted === 'undefined';");
		assertEquals("Sandbox B prototypes must remain completely unpolluted by Sandbox A", Boolean.TRUE, checkPollution);
	}

	@Test
	public void testAnalyticsServiceMetricCoalescing() {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Analytics");
		owner.setLastname("Tester");
		owner.setEmail("analytics_test_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project p1 = Project.create("Analytics Project 1", owner, "Intro", false, false);
		p1.save();
		Project p2 = Project.create("Analytics Project 2", owner, "Intro", false, false);
		p2.save();

		AnalyticsService analyticsService = app.injector().instanceOf(AnalyticsService.class);
		assertNotNull(analyticsService);

		// Queue multiple metrics for project 1 and project 2
		analyticsService.incMetric(p1.getId(), MetricsType.PROJECT_VIEWS, 5);
		analyticsService.incMetric(p1.getId(), MetricsType.PROJECT_VIEWS, 10);
		analyticsService.incMetric(p1.getId(), MetricsType.DATASET_DOWNLOADS, 3);
		analyticsService.incMetric(p2.getId(), MetricsType.PROJECT_VIEWS, 7);
		analyticsService.incMetric(p2.getId(), MetricsType.WEBSITE_VIEWS, 4);

		assertEquals("Queue should contain 5 entries before refresh", 5, analyticsService.getQueueSize());

		// Flush metrics to DB with coalescing
		analyticsService.refresh();

		assertEquals("Queue should be empty after refresh", 0, analyticsService.getQueueSize());

		// Verify aggregated sums in database
		assertEquals(15, analyticsService.getMetric(p1.getId(), MetricsType.PROJECT_VIEWS));
		assertEquals(3, analyticsService.getMetric(p1.getId(), MetricsType.DATASET_DOWNLOADS));
		assertEquals(7, analyticsService.getMetric(p2.getId(), MetricsType.PROJECT_VIEWS));
		assertEquals(4, analyticsService.getMetric(p2.getId(), MetricsType.WEBSITE_VIEWS));
	}

	@Test
	public void testChatbotRagSearchGracefulHandlingOnEmptyIndex() {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Chatbot");
		owner.setLastname("Tester");
		owner.setEmail("chatbot_test_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Chatbot Project", owner, "Intro", false, false);
		project.save();

		DatasetConnector connector = app.injector().instanceOf(DatasetConnector.class);
		Dataset ds = connector.create("CompleteDS_RAG", DatasetType.COMPLETE, project, "Complete dataset", "target", null);
		ds.save();

		CompleteDS cpds = connector.getTypedDatasetDS(ds);
		assertNotNull(cpds);

		ChatbotController chatbotController = app.injector().instanceOf(ChatbotController.class);
		assertNotNull(chatbotController);

		// With no search index created yet, searchIndex should return an empty collection gracefully
		Collection<?> results = chatbotController.searchIndex(cpds, "test query", 3, 0.5f);
		assertNotNull(results);
		assertTrue(results.isEmpty());
	}

	@Test
	public void testSearchServiceVolatilePublication() {
		SearchService searchService = app.injector().instanceOf(SearchService.class);
		assertNotNull(searchService);

		List<Project> results = searchService.search("NonExistentQuery");
		assertNotNull("Search results must be non-null", results);
	}
}

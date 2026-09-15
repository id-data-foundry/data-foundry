package controllers.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.GONE;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.mvc.Http.Status.SEE_OTHER;
import static play.test.Helpers.GET;
import static play.test.Helpers.contentAsString;
import static play.test.Helpers.route;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class CompleteDSControllerPublicDownloadTest extends WithApplication {

	private DatasetConnector datasetConnector;
	private TokenResolverUtil tokenResolverUtil;
	private Project project;
	private Dataset dataset;
	private CompleteDS completeDS;
	private String publicToken;

	private static final String TEST_PROJECT_SECRET = "test-project-token-secret-123456";

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.keys.project", TEST_PROJECT_SECRET)
				.configure("df.keys.registration", Collections.singletonList("reg-key"))
				.build();
	}

	@Before
	public void setUp() throws Exception {
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
		tokenResolverUtil = app.injector().instanceOf(TokenResolverUtil.class);

		Person user = new Person();
		user.setFirstname("Researcher");
		user.setLastname("User");
		user.setEmail("researcher_" + UUID.randomUUID() + "@example.com");
		user.setUser_id(UUID.randomUUID().toString());
		user.save();

		project = Project.create("Test Project " + UUID.randomUUID(), user, "Description", false, false);
		project.save();

		dataset = datasetConnector.create("Test Complete DS", DatasetType.COMPLETE, project, "Desc", "Target", "true");
		dataset.save();

		publicToken = tokenResolverUtil.getDatasetToken(dataset.getId());
		dataset.getConfiguration().put(Dataset.PUBLIC_ACCESS_TOKEN, publicToken);
		dataset.save();

		completeDS = (CompleteDS) datasetConnector.getDatasetDS(dataset);

		// Store a legitimate test file
		File tempFile = File.createTempFile("legit_test", ".txt");
		Files.write(tempFile.toPath(), "Public File Content 12345".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(tempFile, "sample.txt");
		tempFile.delete();
	}

	@Test
	public void testDirectGetFileSafetyAndContainment() {
		// 1. Legitimate file lookup
		Optional<File> validFile = completeDS.getFile("sample.txt");
		assertTrue("Legitimate file should be found", validFile.isPresent());
		assertTrue("Resolved file must be a regular file", validFile.get().isFile());

		// 2. Traversal strings must return empty
		assertFalse("Parent directory traversal should be blocked",
				completeDS.getFile("../../conf/application.conf").isPresent());
		assertFalse("Simple parent traversal should be blocked",
				completeDS.getFile("../").isPresent());
		assertFalse("Root directory should be blocked",
				completeDS.getFile("/").isPresent());
		assertFalse("Current directory should be blocked",
				completeDS.getFile(".").isPresent());
		assertFalse("Double-encoded traversal should be blocked",
				completeDS.getFile("%2e%2e%2fconf%2fapplication.conf").isPresent());

		// 3. Null / empty checks
		assertFalse("Null filename should return empty", completeDS.getFile((String) null).isPresent());
		assertFalse("Empty filename should return empty", completeDS.getFile("").isPresent());
		assertFalse("Whitespace filename should return empty", completeDS.getFile("   ").isPresent());

		// 4. Non-existent file
		assertFalse("Non-existent file should return empty",
				completeDS.getFile("non_existent_file_xyz.txt").isPresent());
	}

	@Test
	public void testPublicDownloadSuccess() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/existing/filePublic/sample.txt/" + publicToken);

		Result result = route(app, request);
		assertEquals(OK, result.status());
		org.apache.pekko.stream.Materializer mat = app.injector().instanceOf(org.apache.pekko.stream.Materializer.class);
		assertEquals("Public File Content 12345", contentAsString(result, mat));
	}

	@Test
	public void testPublicDownloadPathTraversalBlocked() {
		// Traversal with encoded slashes
		Http.RequestBuilder req1 = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/existing/filePublic/..%2f..%2fconf%2fapplication.conf/" + publicToken);

		Result res1 = route(app, req1);
		assertEquals("Traversal attempt should return 404", NOT_FOUND, res1.status());

		// Double encoded traversal
		Http.RequestBuilder req2 = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/existing/filePublic/%2e%2e%2f%2e%2e%2fconf%2fapplication.conf/" + publicToken);

		Result res2 = route(app, req2);
		assertEquals("Double encoded traversal attempt should return 404", NOT_FOUND, res2.status());
	}

	@Test
	public void testPublicDownloadMissingFileReturnsNotFound() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/existing/filePublic/missing_file.txt/" + publicToken);

		Result result = route(app, request);
		assertEquals("Missing file should return 404 instead of redirecting", NOT_FOUND, result.status());
	}

	@Test
	public void testPublicDownloadInvalidTokenRedirects() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/existing/filePublic/sample.txt/invalid-token-value");

		Result result = route(app, request);
		assertEquals("Invalid token should redirect to HOME", SEE_OTHER, result.status());
	}

	@Test
	public void testPublicDownloadInactiveProjectReturnsGone() {
		// Set dataset dates into the past so isActive() is false
		long now = System.currentTimeMillis();
		dataset.setStart(new Date(now - 20L * 86400000L));
		dataset.setEnd(new Date(now - 10L * 86400000L));
		dataset.save();
		project.refresh();
		assertFalse("Project should be inactive when all datasets have ended", project.isActive());

		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/existing/filePublic/sample.txt/" + publicToken);

		Result result = route(app, request);
		assertEquals("Inactive project should return 410 GONE", GONE, result.status());
	}
}

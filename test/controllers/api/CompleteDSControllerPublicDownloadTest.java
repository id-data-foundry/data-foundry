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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.vm.TimedMedia;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

/**
 * Comprehensive integration tests for all CompleteDSController file download and access actions:
 * - downloadFilePublic: public token-authenticated downloads, traversal prevention, inactive project handling
 * - downloadLatestFile: user-authenticated filename downloads, traversal prevention, cross-tenant isolation
 * - downloadFile: user-authenticated file ID downloads, cross-tenant isolation, missing file handling
 * - CompleteDS.getFile / getFileTemp: direct storage layer canonical containment and traversal prevention
 */
public class CompleteDSControllerPublicDownloadTest extends WithApplication {

	private DatasetConnector datasetConnector;
	private TokenResolverUtil tokenResolverUtil;
	private CompleteDSController completeDSController;
	private SessionStore sessionStore;

	private Person ownerUser;
	private Person unauthorizedUser;
	private Project project;
	private Dataset dataset;
	private CompleteDS completeDS;
	private String publicToken;
	private Long sampleFileId;

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
		completeDSController = app.injector().instanceOf(CompleteDSController.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);

		// 1. Owner user
		ownerUser = new Person();
		ownerUser.setFirstname("Owner");
		ownerUser.setLastname("User");
		ownerUser.setEmail("owner_" + UUID.randomUUID() + "@example.com");
		ownerUser.setUser_id(UUID.randomUUID().toString());
		ownerUser.save();

		// 2. Unauthorized / foreign user
		unauthorizedUser = new Person();
		unauthorizedUser.setFirstname("Attacker");
		unauthorizedUser.setLastname("User");
		unauthorizedUser.setEmail("attacker_" + UUID.randomUUID() + "@example.com");
		unauthorizedUser.setUser_id(UUID.randomUUID().toString());
		unauthorizedUser.save();

		// 3. Private project belonging to ownerUser
		project = Project.create("Private Project " + UUID.randomUUID(), ownerUser, "Private Description", false, false);
		project.save();

		// 4. CompleteDS dataset
		dataset = datasetConnector.create("Test Complete DS", DatasetType.COMPLETE, project, "Desc", "Target", "true");
		dataset.save();

		// 5. Public access token
		publicToken = tokenResolverUtil.getDatasetToken(dataset.getId());
		dataset.getConfiguration().put(Dataset.PUBLIC_ACCESS_TOKEN, publicToken);
		dataset.save();

		completeDS = (CompleteDS) datasetConnector.getDatasetDS(dataset);

		// 6. Store a legitimate test file on disk and in database
		File tempFile = File.createTempFile("legit_test", ".txt");
		Files.write(tempFile.toPath(), "Public File Content 12345".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(tempFile, "sample.txt");
		completeDS.addRecord("sample.txt", "Sample Description", new Date());
		tempFile.delete();

		List<TimedMedia> files = completeDS.getFiles();
		assertFalse("Files list should contain the newly added record", files.isEmpty());
		sampleFileId = files.get(0).id;
	}

	/**
	 * Helper to create an authenticated Http.Request with Pac4j session for the given person.
	 */
	private Http.Request createAuthenticatedRequest(String method, String uri, Person user) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method(method).uri(uri);
		Http.Request request = requestBuilder.build();
		if (user != null) {
			PlayWebContext context = new PlayWebContext(request);
			ProfileManager manager = new ProfileManager(context, sessionStore);
			CommonProfile profile = new CommonProfile();
			profile.setId(user.getEmail());
			profile.addAttribute(Person.USER_NAME, user.getEmail());
			profile.addAttribute(Person.USER_ID, user.getId());
			manager.save(true, profile, false);
			request = context.supplementRequest(request);
		}
		return request;
	}

	// =========================================================================
	// 1. Direct CompleteDS Storage Layer Safety
	// =========================================================================

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
	public void testDirectGetFileTempSafety() {
		assertFalse("getFileTemp with parent traversal should be blocked",
				completeDS.getFileTemp("..").isPresent());
		assertFalse("getFileTemp with nested traversal should be blocked",
				completeDS.getFileTemp("../../etc/passwd").isPresent());
		assertFalse("getFileTemp with empty name should be blocked",
				completeDS.getFileTemp("").isPresent());
		assertFalse("getFileTemp with null name should be blocked",
				completeDS.getFileTemp(null).isPresent());
		assertTrue("getFileTemp with valid name should succeed",
				completeDS.getFileTemp("valid_name.txt").isPresent());
	}

	// =========================================================================
	// 2. Public Download Action: downloadFilePublic
	// =========================================================================

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

	// =========================================================================
	// 3. Authenticated Latest File Download: downloadLatestFile
	// =========================================================================

	@Test
	public void testDownloadLatestFileUnauthenticatedRedirects() {
		// Unauthenticated request without session must be redirected by UserAuth
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/existing/downloadLatest/" + dataset.getId() + "/sample.txt");

		Result result = route(app, request);
		assertEquals("Unauthenticated download attempt should redirect", SEE_OTHER, result.status());
	}

	@Test
	public void testDownloadLatestFileAuthorizedSuccess() throws Exception {
		// Authenticated request by the project owner
		Http.Request authRequest = createAuthenticatedRequest(
				GET,
				"/datasets/existing/downloadLatest/" + dataset.getId() + "/sample.txt",
				ownerUser);

		Result result = completeDSController.downloadLatestFile(authRequest, dataset.getId(), "sample.txt")
				.toCompletableFuture().get(5, TimeUnit.SECONDS);

		assertEquals(OK, result.status());
		org.apache.pekko.stream.Materializer mat = app.injector().instanceOf(org.apache.pekko.stream.Materializer.class);
		assertEquals("Public File Content 12345", contentAsString(result, mat));
	}

	@Test
	public void testDownloadLatestFileUnauthorizedUserBlocked() throws Exception {
		// An authenticated user attempting to download from another user's private project
		Http.Request authRequest = createAuthenticatedRequest(
				GET,
				"/datasets/existing/downloadLatest/" + dataset.getId() + "/sample.txt",
				unauthorizedUser);

		Result result = completeDSController.downloadLatestFile(authRequest, dataset.getId(), "sample.txt")
				.toCompletableFuture().get(5, TimeUnit.SECONDS);

		// Must redirect to HOME with error session, never revealing the file
		assertEquals(SEE_OTHER, result.status());
		assertTrue("Error flash must indicate project not accessible",
				result.session().get("error").orElse("").contains("Project is not accessible"));
	}

	@Test
	public void testDownloadLatestFilePathTraversalBlocked() throws Exception {
		// Even an authorized owner must not be allowed to traverse outside the dataset directory
		Http.Request authRequest = createAuthenticatedRequest(
				GET,
				"/datasets/existing/downloadLatest/" + dataset.getId() + "/..%2f..%2fconf%2fapplication.conf",
				ownerUser);

		Result result = completeDSController.downloadLatestFile(authRequest, dataset.getId(), "../../conf/application.conf")
				.toCompletableFuture().get(5, TimeUnit.SECONDS);

		// Controller redirects to dataset view with "No file found" error, never returning application.conf
		assertEquals(SEE_OTHER, result.status());
		assertTrue("Flash error should indicate no file found",
				result.session().get("error").orElse("").contains("No file found"));
	}

	@Test
	public void testDownloadLatestFileMissingFile() throws Exception {
		Http.Request authRequest = createAuthenticatedRequest(
				GET,
				"/datasets/existing/downloadLatest/" + dataset.getId() + "/does_not_exist.txt",
				ownerUser);

		Result result = completeDSController.downloadLatestFile(authRequest, dataset.getId(), "does_not_exist.txt")
				.toCompletableFuture().get(5, TimeUnit.SECONDS);

		assertEquals(SEE_OTHER, result.status());
		assertTrue("Flash error should indicate no file found",
				result.session().get("error").orElse("").contains("No file found"));
	}

	// =========================================================================
	// 4. Authenticated Download by File ID: downloadFile
	// =========================================================================

	@Test
	public void testDownloadFileUnauthenticatedRedirects() {
		// Unauthenticated request without session
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/existing/download/" + dataset.getId() + "/" + sampleFileId);

		Result result = route(app, request);
		assertEquals("Unauthenticated download attempt should redirect", SEE_OTHER, result.status());
	}

	@Test
	public void testDownloadFileAuthorizedSuccess() throws Exception {
		Http.Request authRequest = createAuthenticatedRequest(
				GET,
				"/datasets/existing/download/" + dataset.getId() + "/" + sampleFileId,
				ownerUser);

		Result result = completeDSController.downloadFile(authRequest, dataset.getId(), sampleFileId)
				.toCompletableFuture().get(5, TimeUnit.SECONDS);

		assertEquals(OK, result.status());
		org.apache.pekko.stream.Materializer mat = app.injector().instanceOf(org.apache.pekko.stream.Materializer.class);
		assertEquals("Public File Content 12345", contentAsString(result, mat));
	}

	@Test
	public void testDownloadFileUnauthorizedUserBlocked() throws Exception {
		// Unauthorized user trying to download a file by ID from another user's private project
		Http.Request authRequest = createAuthenticatedRequest(
				GET,
				"/datasets/existing/download/" + dataset.getId() + "/" + sampleFileId,
				unauthorizedUser);

		Result result = completeDSController.downloadFile(authRequest, dataset.getId(), sampleFileId)
				.toCompletableFuture().get(5, TimeUnit.SECONDS);

		assertEquals(SEE_OTHER, result.status());
		assertTrue("Error flash must indicate project not accessible",
				result.session().get("error").orElse("").contains("Project is not accessible"));
	}

	@Test
	public void testDownloadFileNonExistentIdReturnsNotFound() throws Exception {
		Http.Request authRequest = createAuthenticatedRequest(
				GET,
				"/datasets/existing/download/" + dataset.getId() + "/9999999",
				ownerUser);

		Result result = completeDSController.downloadFile(authRequest, dataset.getId(), 9999999L)
				.toCompletableFuture().get(5, TimeUnit.SECONDS);

		assertEquals("Non-existent file ID should return 404", NOT_FOUND, result.status());
	}
}

package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.mvc.Http.Status.UNAUTHORIZED;
import static play.test.Helpers.GET;
import static play.test.Helpers.contentAsString;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import controllers.api.CompleteDSController;
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

public class CompleteDSViewTest extends WithApplication {

	private DatasetConnector datasetConnector;
	private TokenResolverUtil tokenResolverUtil;
	private CompleteDSController completeDSController;
	private DatasetsController datasetsController;
	private SessionStore sessionStore;

	private Person ownerUser;
	private Project project;
	private Dataset dataset;
	private CompleteDS completeDS;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.keys.project", "test-project-token-secret-123456")
				.configure("df.keys.registration", Collections.singletonList("reg-key"))
				.build();
	}

	@Before
	public void setUp() throws Exception {
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
		tokenResolverUtil = app.injector().instanceOf(TokenResolverUtil.class);
		completeDSController = app.injector().instanceOf(CompleteDSController.class);
		datasetsController = app.injector().instanceOf(DatasetsController.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);

		ownerUser = new Person();
		ownerUser.setFirstname("Owner");
		ownerUser.setLastname("User");
		ownerUser.setEmail("owner_" + UUID.randomUUID() + "@example.com");
		ownerUser.setUser_id(UUID.randomUUID().toString());
		ownerUser.save();

		project = Project.create("Test Project " + UUID.randomUUID(), ownerUser, "Test Description", false, false);
		project.save();

		dataset = datasetConnector.create("Test Complete DS", DatasetType.COMPLETE, project, "Desc", "Target", "true");
		dataset.save();

		completeDS = (CompleteDS) datasetConnector.getDatasetDS(dataset);

		File tempFile = File.createTempFile("view_test", ".txt");
		Files.write(tempFile.toPath(), "View Test Content".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(tempFile, "testfile.txt");
		completeDS.addRecord("testfile.txt", "Description", new Date());
		tempFile.delete();
	}

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

	@Test
	public void testRenderTemplateDirectly() {
		Http.Request request = createAuthenticatedRequest(GET, "/datasets/" + dataset.getId(), ownerUser);
		List<TimedMedia> files = completeDS.getFiles();
		play.twirl.api.Html html = views.html.datasets.complete.view.render(dataset, ownerUser.getEmail(), files, "dummy-token", request);
		assertNotNull(html);
		String body = html.body();
		assertNotNull(body);
	}

	@Test
	public void testCompleteDSControllerView() {
		Http.Request request = createAuthenticatedRequest(GET, "/datasets/" + dataset.getId(), ownerUser);
		Result result = completeDSController.view(request, dataset.getId());
		assertEquals(OK, result.status());
		String body = contentAsString(result);
		assertNotNull(body);
	}

	@Test
	public void testDatasetsControllerView() {
		Http.Request request = createAuthenticatedRequest(GET, "/datasets/" + dataset.getId(), ownerUser);
		Result result = datasetsController.view(request, dataset.getId());
		assertEquals(OK, result.status());
		String body = contentAsString(result);
		assertNotNull(body);
	}

	@Test
	public void testWebAssetLoading() throws Exception {
		File htmlFile = File.createTempFile("index", ".html");
		Files.write(htmlFile.toPath(), "<html><head><link rel=\"stylesheet\" href=\"styles.css\"></head><body><h1>Hi</h1></body></html>".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(htmlFile, "index.html");
		completeDS.addRecord("index.html", "HTML", new Date());
		htmlFile.delete();

		File cssFile = File.createTempFile("styles", ".css");
		Files.write(cssFile.toPath(), "body { color: red; }".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(cssFile, "styles.css");
		completeDS.addRecord("styles.css", "CSS", new Date());
		cssFile.delete();

		File jsFile = File.createTempFile("script", ".js");
		Files.write(jsFile.toPath(), "console.log('hello');".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(jsFile, "script.js");
		completeDS.addRecord("script.js", "JS", new Date());
		jsFile.delete();

		// 1. Verify index.html contains allow-same-origin in CSP
		Http.Request htmlRequest = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/index.html", ownerUser);
		Result htmlResult = datasetsController.web(htmlRequest, dataset.getId(), "index.html").toCompletableFuture().get();
		assertEquals(OK, htmlResult.status());
		assertTrue(htmlResult.contentType().isPresent());
		assertTrue(htmlResult.contentType().get().contains("text/html"));
		assertTrue(htmlResult.header("Content-Security-Policy").isPresent());
		assertTrue(htmlResult.header("Content-Security-Policy").get().contains("allow-same-origin"));
		assertEquals("nosniff", htmlResult.header("X-Content-Type-Options").orElse(""));

		// 2. Verify styles.css returns text/css and nosniff
		Http.Request request = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/styles.css", ownerUser);
		Result cssResult = datasetsController.web(request, dataset.getId(), "styles.css").toCompletableFuture().get();
		assertEquals(OK, cssResult.status());
		assertTrue(cssResult.contentType().isPresent());
		assertTrue(cssResult.contentType().get().contains("text/css"));
		assertEquals("nosniff", cssResult.header("X-Content-Type-Options").orElse(""));

		// 3. Verify script.js returns javascript MIME and nosniff
		Http.Request jsRequest = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/script.js", ownerUser);
		Result jsResult = datasetsController.web(jsRequest, dataset.getId(), "script.js").toCompletableFuture().get();
		assertEquals(OK, jsResult.status());
		assertTrue(jsResult.contentType().isPresent());
		assertTrue(jsResult.contentType().get().contains("javascript"));
		assertEquals("nosniff", jsResult.header("X-Content-Type-Options").orElse(""));

		// 4. Verify unauthenticated web asset returns 401 Unauthorized (not 303 redirect to HTML)
		Http.Request unauthRequest = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/styles.css", null);
		Result unauthResult = datasetsController.web(unauthRequest, dataset.getId(), "styles.css").toCompletableFuture().get();
		assertEquals(UNAUTHORIZED, unauthResult.status());

		// 5. Verify missing asset returns 404 without HTML error page
		Http.Request missingRequest = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/missing.css", ownerUser);
		Result missingResult = datasetsController.web(missingRequest, dataset.getId(), "missing.css").toCompletableFuture().get();
		assertEquals(NOT_FOUND, missingResult.status());

		// 6. Verify webToken serving of styles.css and missing asset
		String webToken = tokenResolverUtil.getDatasetToken(dataset.getId());
		dataset.getConfiguration().put(Dataset.WEB_ACCESS_TOKEN, webToken);
		dataset.update();

		Http.Request tokenRequest = createAuthenticatedRequest(GET, "/web/" + webToken + "/styles.css", null);
		Result tokenCssResult = datasetsController.webToken(tokenRequest, webToken, "styles.css").toCompletableFuture().get();
		assertEquals(OK, tokenCssResult.status());
		assertTrue(tokenCssResult.contentType().isPresent());
		assertTrue(tokenCssResult.contentType().get().contains("text/css"));
		assertEquals("nosniff", tokenCssResult.header("X-Content-Type-Options").orElse(""));

		Http.Request tokenMissingRequest = createAuthenticatedRequest(GET, "/web/" + webToken + "/missing.css", null);
		Result tokenMissingResult = datasetsController.webToken(tokenMissingRequest, webToken, "missing.css").toCompletableFuture().get();
		assertEquals(NOT_FOUND, tokenMissingResult.status());
	}
}

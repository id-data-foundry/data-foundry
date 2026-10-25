package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.FORBIDDEN;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.mvc.Http.Status.SEE_OTHER;
import static play.mvc.Http.Status.UNAUTHORIZED;
import static play.test.Helpers.GET;
import static play.test.Helpers.contentAsString;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Key;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import controllers.api.CompleteDSController;
import controllers.tools.StarboardController;
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
	private StarboardController starboardController;
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
		starboardController = app.injector().instanceOf(StarboardController.class);
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
		return createAuthenticatedRequest(method, uri, user, Collections.emptyMap());
	}

	private Http.Request createAuthenticatedRequest(String method, String uri, Person user, java.util.Map<String, String> headers) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method(method).uri(uri);
		if (headers != null) {
			for (java.util.Map.Entry<String, String> entry : headers.entrySet()) {
				requestBuilder.header(entry.getKey(), entry.getValue());
			}
		}
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

		// 1. Verify index.html navigation on web() redirects to webViewer with an ephemeral viewer token (Flaw 1 fix)
		Http.Request htmlRequest = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/index.html", ownerUser);
		Result htmlResult = datasetsController.web(htmlRequest, dataset.getId(), "index.html").toCompletableFuture().get();
		assertEquals(SEE_OTHER, htmlResult.status());
		assertTrue(htmlResult.redirectLocation().isPresent());
		String viewerRedirect = htmlResult.redirectLocation().get();
		assertTrue(viewerRedirect.startsWith("/web/view/"));

		// Follow redirect to webViewer and verify CSP and storage polyfills (Flaw 4 fix)
		String viewerTokenFromRedirect = viewerRedirect.replace("/web/view/", "").replaceAll("/.*", "");
		Http.Request viewerRequest = createAuthenticatedRequest(GET, viewerRedirect, null);
		Result viewerResult = datasetsController.webViewer(viewerRequest, viewerTokenFromRedirect, "index.html").toCompletableFuture().get();
		assertEquals(OK, viewerResult.status());
		assertTrue(viewerResult.contentType().isPresent());
		assertTrue(viewerResult.contentType().get().contains("text/html"));
		assertTrue(viewerResult.header("Content-Security-Policy").isPresent());
		String htmlCsp = viewerResult.header("Content-Security-Policy").get();
		assertFalse("CSP must NOT contain allow-same-origin", htmlCsp.contains("allow-same-origin"));
		assertTrue(htmlCsp.contains("allow-popups"));
		assertTrue(htmlCsp.contains("connect-src 'self' https: wss:"));
		assertTrue(htmlCsp.contains("form-action 'self'"));
		assertTrue(htmlCsp.contains("base-uri 'self'"));
		assertEquals("nosniff", viewerResult.header("X-Content-Type-Options").orElse(""));
		String htmlBody = contentAsString(viewerResult);
		assertTrue("HTML must include in-memory localStorage polyfill", htmlBody.contains("window.localStorage"));
		assertTrue("HTML must include in-memory sessionStorage polyfill", htmlBody.contains("window.sessionStorage"));

		// 2. Verify styles.css returns text/css, nosniff, and Access-Control-Allow-Origin: *
		Http.Request request = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/styles.css", ownerUser);
		Result cssResult = datasetsController.web(request, dataset.getId(), "styles.css").toCompletableFuture().get();
		assertEquals(OK, cssResult.status());
		assertTrue(cssResult.contentType().isPresent());
		assertTrue(cssResult.contentType().get().contains("text/css"));
		assertEquals("nosniff", cssResult.header("X-Content-Type-Options").orElse(""));
		assertEquals("*", cssResult.header("Access-Control-Allow-Origin").orElse(""));

		// 3. Verify script.js returns javascript MIME, nosniff, and Access-Control-Allow-Origin: *
		Http.Request jsRequest = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/script.js", ownerUser);
		Result jsResult = datasetsController.web(jsRequest, dataset.getId(), "script.js").toCompletableFuture().get();
		assertEquals(OK, jsResult.status());
		assertTrue(jsResult.contentType().isPresent());
		assertTrue(jsResult.contentType().get().contains("javascript"));
		assertEquals("nosniff", jsResult.header("X-Content-Type-Options").orElse(""));
		assertEquals("*", jsResult.header("Access-Control-Allow-Origin").orElse(""));

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
		assertEquals("*", tokenCssResult.header("Access-Control-Allow-Origin").orElse(""));

		Http.Request tokenMissingRequest = createAuthenticatedRequest(GET, "/web/" + webToken + "/missing.css", null);
		Result tokenMissingResult = datasetsController.webToken(tokenMissingRequest, webToken, "missing.css").toCompletableFuture().get();
		assertEquals(NOT_FOUND, tokenMissingResult.status());

		// 7. Verify webToken serving of index.html enforces connect-src and sandbox but omits frame-ancestors, and NOT allow-same-origin
		Http.Request tokenHtmlRequest = createAuthenticatedRequest(GET, "/web/" + webToken + "/index.html", null);
		Result tokenHtmlResult = datasetsController.webToken(tokenHtmlRequest, webToken, "index.html").toCompletableFuture().get();
		assertEquals(OK, tokenHtmlResult.status());
		assertTrue(tokenHtmlResult.header("Content-Security-Policy").isPresent());
		String tokenCsp = tokenHtmlResult.header("Content-Security-Policy").get();
		assertFalse("Token CSP must NOT contain allow-same-origin", tokenCsp.contains("allow-same-origin"));
		assertTrue(tokenCsp.contains("allow-popups"));
		assertTrue(tokenCsp.contains("connect-src 'self' https: wss:"));
		assertTrue(tokenCsp.contains("form-action 'self'"));
		assertTrue(tokenCsp.contains("base-uri 'self'"));
		assertTrue(!tokenCsp.contains("frame-ancestors"));

		// 8. Verify webViewer serving with valid ephemeral viewer token
		String viewerToken = tokenResolverUtil.createViewerToken(dataset.getId(), System.currentTimeMillis() + 3600000L);
		Http.Request viewerHtmlRequest = createAuthenticatedRequest(GET, "/web/view/" + viewerToken + "/index.html", null);
		Result viewerHtmlResult = datasetsController.webViewer(viewerHtmlRequest, viewerToken, "index.html").toCompletableFuture().get();
		assertEquals(OK, viewerHtmlResult.status());
		assertTrue(viewerHtmlResult.header("Content-Security-Policy").isPresent());
		assertFalse(viewerHtmlResult.header("Content-Security-Policy").get().contains("allow-same-origin"));

		Http.Request viewerCssRequest = createAuthenticatedRequest(GET, "/web/view/" + viewerToken + "/styles.css", null);
		Result viewerCssResult = datasetsController.webViewer(viewerCssRequest, viewerToken, "styles.css").toCompletableFuture().get();
		assertEquals(OK, viewerCssResult.status());
		assertEquals("*", viewerCssResult.header("Access-Control-Allow-Origin").orElse(""));

		// 9. Verify webViewer with expired token: assets return 403, HTML returns 303 auto-renewal redirect
		String expiredViewerToken = tokenResolverUtil.createViewerToken(dataset.getId(), System.currentTimeMillis() - 60000L);
		Http.Request expiredAssetRequest = createAuthenticatedRequest(GET, "/web/view/" + expiredViewerToken + "/styles.css", null);
		Result expiredAssetResult = datasetsController.webViewer(expiredAssetRequest, expiredViewerToken, "styles.css").toCompletableFuture().get();
		assertEquals(FORBIDDEN, expiredAssetResult.status());

		Http.Request expiredHtmlRequest = createAuthenticatedRequest(GET, "/web/view/" + expiredViewerToken + "/index.html", null);
		Result expiredHtmlResult = datasetsController.webViewer(expiredHtmlRequest, expiredViewerToken, "index.html").toCompletableFuture().get();
		assertEquals(SEE_OTHER, expiredHtmlResult.status());
		assertTrue(expiredHtmlResult.redirectLocation().isPresent());
		String renewalUrl = expiredHtmlResult.redirectLocation().get();
		assertTrue(renewalUrl.contains("/datasets/web/" + dataset.getId() + "/index.html"));

		// 10. Verify auto-renewal: following redirect to web() with Referer from /web/view/
		// 10a. Dataset with public webToken redirects to /web/<webToken>/index.html
		Http.Request renewalRequestPublic = createAuthenticatedRequest(GET, renewalUrl, ownerUser,
				Collections.singletonMap("Referer", "http://localhost:9000/web/view/" + expiredViewerToken + "/index.html"));
		Result renewalResultPublic = datasetsController.web(renewalRequestPublic, dataset.getId(), "index.html").toCompletableFuture().get();
		assertEquals(SEE_OTHER, renewalResultPublic.status());
		assertTrue(renewalResultPublic.redirectLocation().isPresent());
		assertTrue(renewalResultPublic.redirectLocation().get().contains("/web/" + webToken + "/index.html"));

		// 10b. Dataset without public webToken mints fresh viewer token /web/view/<newViewerToken>/index.html (Flaw 1 & 2 fix)
		dataset.getConfiguration().remove(Dataset.WEB_ACCESS_TOKEN);
		dataset.update();

		Http.Request renewalRequestPrivate = createAuthenticatedRequest(GET, renewalUrl, ownerUser,
				Collections.singletonMap("Referer", "http://localhost:9000/web/view/" + expiredViewerToken + "/index.html"));
		Result renewalResultPrivate = datasetsController.web(renewalRequestPrivate, dataset.getId(), "index.html").toCompletableFuture().get();
		assertEquals(SEE_OTHER, renewalResultPrivate.status());
		assertTrue(renewalResultPrivate.redirectLocation().isPresent());
		assertTrue(renewalResultPrivate.redirectLocation().get().startsWith("/web/view/"));
		assertFalse(renewalResultPrivate.redirectLocation().get().contains(expiredViewerToken));

		// 11. Verify markdown standalone rendering with green theme and bridge script (Flaw 12 fix)
		File mdFile = File.createTempFile("index", ".md");
		Files.write(mdFile.toPath(), "# Hello Markdown\nThis is a test.".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(mdFile, "index.md");
		completeDS.addRecord("index.md", "MD", new Date());
		mdFile.delete();

		String mdViewerToken = tokenResolverUtil.createViewerToken(dataset.getId(), System.currentTimeMillis() + 3600000L);
		Http.Request viewerMdRequest = createAuthenticatedRequest(GET, "/web/view/" + mdViewerToken + "/index.md", null);
		Result viewerMdResult = datasetsController.webViewer(viewerMdRequest, mdViewerToken, "index.md").toCompletableFuture().get();
		assertEquals(OK, viewerMdResult.status());
		assertTrue(viewerMdResult.contentType().isPresent());
		assertTrue(viewerMdResult.contentType().get().contains("text/html"));
		String mdBody = contentAsString(viewerMdResult);
		assertTrue("Standalone markdown must include markdown-body styling", mdBody.contains("markdown-body"));
		assertTrue("Standalone markdown must include green primary theme color", mdBody.contains("#1b5e20"));
		assertTrue("Standalone markdown must include Playground bridge script", mdBody.contains("window.localStorage"));
		assertFalse("Standalone markdown must NOT contain main navigation bar", mdBody.contains("id=\"homeBtn\""));

		// 12. Verify path traversal rejection in web(), webToken(), and webViewer() (Flaw 13 fix)
		Http.Request traversalRequest = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/../testfile.txt", ownerUser);
		Result traversalResult = datasetsController.web(traversalRequest, dataset.getId(), "../testfile.txt").toCompletableFuture().get();
		assertEquals(NOT_FOUND, traversalResult.status());

		dataset.getConfiguration().put(Dataset.WEB_ACCESS_TOKEN, webToken);
		dataset.update();

		Http.Request traversalTokenRequest = createAuthenticatedRequest(GET, "/web/" + webToken + "/../testfile.txt", null);
		Result traversalTokenResult = datasetsController.webToken(traversalTokenRequest, webToken, "../testfile.txt").toCompletableFuture().get();
		assertEquals(NOT_FOUND, traversalTokenResult.status());

		Http.Request traversalViewerRequest = createAuthenticatedRequest(GET, "/web/view/" + mdViewerToken + "/../testfile.txt", null);
		Result traversalViewerResult = datasetsController.webViewer(traversalViewerRequest, mdViewerToken, "../testfile.txt").toCompletableFuture().get();
		assertEquals(NOT_FOUND, traversalViewerResult.status());

		// 13. Verify URL-encoded path traversal and "/" path are rejected safely (Finding 6 fix)
		Http.Request encodedTraversalReq = createAuthenticatedRequest(GET, "/datasets/web/" + dataset.getId() + "/%2e%2e%2ftestfile.txt", ownerUser);
		Result encodedTraversalRes = datasetsController.web(encodedTraversalReq, dataset.getId(), "%2e%2e%2ftestfile.txt").toCompletableFuture().get();
		assertEquals(NOT_FOUND, encodedTraversalRes.status());

		Http.Request slashViewerReq = createAuthenticatedRequest(GET, "/web/view/" + mdViewerToken + "/", null);
		Result slashViewerRes = datasetsController.webViewer(slashViewerReq, mdViewerToken, "/").toCompletableFuture().get();
		assertEquals(NOT_FOUND, slashViewerRes.status());

		// 14. Verify .xml and Tika-sniffed HTML with unknown extensions always have CSP sandbox and safe MIME type (Finding 2 fix)
		File xmlFile = File.createTempFile("data", ".xml");
		Files.write(xmlFile.toPath(), "<?xml version=\"1.0\"?><root><script xmlns=\"http://www.w3.org/1999/xhtml\">alert(1)</script></root>".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(xmlFile, "data.xml");
		completeDS.addRecord("data.xml", "XML", new Date());
		xmlFile.delete();

		Result xmlResult = datasetsController.webViewer(viewerRequest, mdViewerToken, "data.xml").toCompletableFuture().get();
		assertEquals(OK, xmlResult.status());
		assertTrue("XML response must include Content-Security-Policy sandbox", xmlResult.header("Content-Security-Policy").isPresent());
		assertTrue(xmlResult.header("Content-Security-Policy").get().contains("sandbox"));

		File sniffFile = File.createTempFile("exploit", ".xhtml");
		Files.write(sniffFile.toPath(), "<!DOCTYPE html><html><body><script>alert(1)</script></body></html>".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(sniffFile, "exploit.xhtml");
		completeDS.addRecord("exploit.xhtml", "XHTML", new Date());
		sniffFile.delete();

		Result sniffResult = datasetsController.webViewer(viewerRequest, mdViewerToken, "exploit.xhtml").toCompletableFuture().get();
		assertEquals(OK, sniffResult.status());
		assertTrue("Unrecognized extension with HTML magic bytes must be downgraded to text/plain", sniffResult.contentType().orElse("").startsWith("text/plain"));
		assertTrue("All webViewer responses must include Content-Security-Policy", sniffResult.header("Content-Security-Policy").isPresent());

		// 15. Verify CompleteDSController.downloadFilePublic redirects web pages/assets to webToken (legacy support)
		// and forces attachment download, nosniff, and CSP sandbox for data files (Finding 1 fix)
		String publicAccessToken = tokenResolverUtil.getDatasetToken(dataset.getId());
		dataset.getConfiguration().put(Dataset.PUBLIC_ACCESS_TOKEN, publicAccessToken);
		dataset.getConfiguration().remove(Dataset.WEB_ACCESS_TOKEN);
		dataset.update();

		assertTrue("Dataset should report website is live with legacy public token", dataset.isWebsiteLive());
		assertEquals(publicAccessToken, dataset.getEffectiveWebToken());

		// 15a. HTML requests via downloadFilePublic must redirect to /web/:token/:filepath
		Result publicHtmlRedirectRes = completeDSController.downloadFilePublic("index.html", publicAccessToken).toCompletableFuture().get();
		assertEquals(SEE_OTHER, publicHtmlRedirectRes.status());
		assertEquals("/web/" + publicAccessToken + "/index.html", publicHtmlRedirectRes.redirectLocation().orElse(""));

		// 15b. Legacy public token must be accepted by DatasetsController.webToken with sandbox CSP
		Http.Request legacyTokenReq = createAuthenticatedRequest(GET, "/web/" + publicAccessToken + "/index.html", null);
		Result legacyWebTokenRes = datasetsController.webToken(legacyTokenReq, publicAccessToken, "index.html").toCompletableFuture().get();
		assertEquals(OK, legacyWebTokenRes.status());
		assertTrue(legacyWebTokenRes.contentType().orElse("").contains("text/html"));
		assertTrue(legacyWebTokenRes.header("Content-Security-Policy").orElse("").contains("sandbox"));
		assertFalse("WebToken CSP must NOT contain allow-same-origin", legacyWebTokenRes.header("Content-Security-Policy").orElse("").contains("allow-same-origin"));

		// 15c. Non-web data files via downloadFilePublic force attachment download
		File csvFile = File.createTempFile("data", ".csv");
		Files.write(csvFile.toPath(), "col1,col2\nval1,val2\n".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(csvFile, "data.csv");
		completeDS.addRecord("data.csv", "CSV", new Date());
		csvFile.delete();

		Result publicCsvRes = completeDSController.downloadFilePublic("data.csv", publicAccessToken).toCompletableFuture().get();
		assertEquals(OK, publicCsvRes.status());
		assertEquals("application/x-download", publicCsvRes.contentType().orElse(""));
		assertEquals("nosniff", publicCsvRes.header("X-Content-Type-Options").orElse(""));
		assertEquals("*", publicCsvRes.header("Access-Control-Allow-Origin").orElse(""));
		assertEquals("sandbox", publicCsvRes.header("Content-Security-Policy").orElse(""));
		assertTrue("Content-Disposition must be attachment", publicCsvRes.header("Content-Disposition").orElse("").startsWith("attachment"));
	}

	@Test
	public void testStarboardSandboxing() throws Exception {
		// 1. Create a Starboard .gg file in the complete dataset
		File ggFile = File.createTempFile("notebook", ".gg");
		Files.write(ggFile.toPath(), "# %% [javascript]\nconsole.log('hello');\n".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(ggFile, "analysis.gg");
		completeDS.addRecord("analysis.gg", "Starboard notebook", new Date());
		ggFile.delete();

		long ggFileId = completeDS.getLatestFileVersionId("analysis.gg").orElse(-1L);
		assertTrue("Starboard file ID should be valid", ggFileId > 0);

		// 2. Test editFile shell renders sandboxed iframe
		Http.Request editRequest = createAuthenticatedRequest(GET, "/datasets/existing/edit/" + dataset.getId() + "/" + ggFileId, ownerUser);
		Result editResult = completeDSController.editFile(editRequest, dataset.getId(), ggFileId);
		assertEquals(OK, editResult.status());
		String editBody = contentAsString(editResult);
		assertTrue("Edit page should use collapsed-layout bodyClass", editBody.contains("class=\"collapsed-layout\""));
		assertTrue("Edit page should include sidenav", editBody.contains("id=\"sidenav\""));
		assertTrue("Edit page should include breadcrumbs navigation", editBody.contains("class=\"breadcrumb\""));
		assertTrue("Edit page should contain sandboxed starboardFrame", editBody.contains("id=\"starboardFrame\""));
		assertTrue("starboardFrame should have sandbox attribute", editBody.contains("sandbox=\"allow-scripts allow-modals allow-downloads allow-forms allow-popups\""));
		assertTrue("starboardFrame should target runner endpoint", editBody.contains("/tools/starboard/runner/" + dataset.getId() + "/" + ggFileId));
		assertFalse("Edit page should NOT load starboard-notebook.js directly in host origin", editBody.contains("vendor/starboard/starboard-notebook.js"));
		assertFalse("Edit page should not have COEP", editResult.header("Cross-Origin-Embedder-Policy").isPresent());

		// 3. Test runner endpoint delivers strict CSP sandbox without allow-same-origin
		Http.Request runnerRequest = createAuthenticatedRequest(GET, "/tools/starboard/runner/" + dataset.getId() + "/" + ggFileId, ownerUser);
		Result runnerResult = starboardController.runner(runnerRequest, dataset.getId(), ggFileId);
		assertEquals(OK, runnerResult.status());
		assertFalse("Runner must NOT require COEP so opaque origin can load subresources without CORP", runnerResult.header("Cross-Origin-Embedder-Policy").isPresent());
		assertFalse("Runner must NOT set COOP same-origin", runnerResult.header("Cross-Origin-Opener-Policy").isPresent());
		assertTrue("Runner response must include Content-Security-Policy", runnerResult.header("Content-Security-Policy").isPresent());
		String csp = runnerResult.header("Content-Security-Policy").get();
		assertTrue("Runner CSP must enforce sandbox", csp.contains("sandbox allow-scripts"));
		assertFalse("Runner CSP must NOT include allow-same-origin (must remain opaque null origin)", csp.contains("allow-same-origin"));
		assertTrue("Runner CSP must permit connect-src for external libraries and datasets", csp.contains("connect-src * data: blob: 'self' http: https: ws: wss:;"));
		assertEquals("nosniff", runnerResult.header("X-Content-Type-Options").orElse(""));

		String runnerBody = contentAsString(runnerResult);
		assertTrue("Runner body must contain storage polyfill", runnerBody.contains("createStore"));
		assertTrue("Runner body must set initialNotebookContent", runnerBody.contains("window.initialNotebookContent"));
		assertTrue("Runner body must configure starboardArtifactsUrl", runnerBody.contains("window.starboardArtifactsUrl"));

		// 4. Test unauthorized user cannot access runner
		Person otherUser = new Person();
		otherUser.setFirstname("Other");
		otherUser.setLastname("User");
		otherUser.setEmail("other_" + UUID.randomUUID() + "@example.com");
		otherUser.setUser_id(UUID.randomUUID().toString());
		otherUser.save();

		Http.Request unauthorizedRunnerReq = createAuthenticatedRequest(GET, "/tools/starboard/runner/" + dataset.getId() + "/" + ggFileId, otherUser);
		Result unauthResult = starboardController.runner(unauthorizedRunnerReq, dataset.getId(), ggFileId);
		assertEquals(FORBIDDEN, unauthResult.status());

		// 5. Test datasets helper endpoint (HTMX target) for tokenized vs un-tokenized datasets
		Http.Request datasetsReq = createAuthenticatedRequest(GET, "/tools/starboard/datasets", ownerUser);
		Result datasetsRes = starboardController.datasets(datasetsReq);
		assertEquals(OK, datasetsRes.status());
		String datasetsBody = contentAsString(datasetsRes);
		// dataset currently has no PUBLIC_ACCESS_TOKEN
		assertTrue("Datasets helper should indicate unconfigured token", datasetsBody.contains("No public access token configured"));
		assertTrue("Datasets helper should link to dataset configuration", datasetsBody.contains("/datasets/" + dataset.getId()));

		// Now configure PUBLIC_ACCESS_TOKEN on dataset
		String testToken = tokenResolverUtil.getDatasetToken(dataset.getId());
		dataset.getConfiguration().put(Dataset.PUBLIC_ACCESS_TOKEN, testToken);
		dataset.update();

		Result datasetsResWithToken = starboardController.datasets(datasetsReq);
		assertEquals(OK, datasetsResWithToken.status());
		String datasetsBodyWithToken = contentAsString(datasetsResWithToken);
		assertTrue("Datasets helper should show JavaScript snippet with public token", datasetsBodyWithToken.contains("/datasets/downloadPublic/json/" + testToken));
		assertTrue("Datasets helper should show Python snippet with public token", datasetsBodyWithToken.contains("/datasets/downloadPublic/" + testToken));

		// 6. Test public token endpoints return Access-Control-Allow-Origin: *
		Http.Request publicJsonReq = createAuthenticatedRequest(GET, "/datasets/downloadPublic/json/" + testToken, null);
		Result publicJsonRes = datasetsController.downloadJsonPublic(publicJsonReq, testToken, -1L, -1L, -1L, -1L).toCompletableFuture().get();
		assertEquals(OK, publicJsonRes.status());
		assertEquals("*", publicJsonRes.header("Access-Control-Allow-Origin").orElse(""));

		Http.Request publicCsvReq = createAuthenticatedRequest(GET, "/datasets/downloadPublic/" + testToken, null);
		Result publicCsvRes2 = datasetsController.downloadPublic(publicCsvReq, testToken, -1L, -1L, -1L, -1L).toCompletableFuture().get();
		assertEquals(OK, publicCsvRes2.status());
		assertEquals("*", publicCsvRes2.header("Access-Control-Allow-Origin").orElse(""));
	}

	private String generateLegacyToken(String payload, String secretKey) throws Exception {
		byte[] keyVal = Base64.getEncoder().encode(secretKey.getBytes(StandardCharsets.UTF_8));
		Key key = new SecretKeySpec(Arrays.copyOf(keyVal, 16), "AES");
		Cipher c = Cipher.getInstance("AES");
		c.init(Cipher.ENCRYPT_MODE, key);
		byte[] enc = c.doFinal(payload.getBytes(StandardCharsets.UTF_8));
		return Base64.getEncoder().encodeToString(enc);
	}

	@Test
	public void testLegacyTokenAndWebTokenPublicDownload() throws Exception {
		// 1. Test dataset 15 with the user's exact token: QWw4bDRpOEV6Y3RaQTBtUlY1UXAzZz09
		// Create datasets until ID 15 exists in H2
		Dataset ds15 = dataset;
		while (ds15.getId() < 15L) {
			ds15 = datasetConnector.create("Dataset " + UUID.randomUUID(), DatasetType.COMPLETE, project, "Desc", "Target", "true");
			ds15.save();
		}
		assertEquals(Long.valueOf(15L), ds15.getId());

		String userToken = "QWw4bDRpOEV6Y3RaQTBtUlY1UXAzZz09";
		ds15.getConfiguration().put(Dataset.PUBLIC_ACCESS_TOKEN, userToken);
		ds15.update();

		// A. JSON download with exact token
		Http.Request jsonReq = createAuthenticatedRequest(GET, "/datasets/downloadPublic/json/" + userToken, null);
		Result jsonRes = datasetsController.downloadJsonPublic(jsonReq, userToken, -1L, -1L, -1L, -1L).toCompletableFuture().get();
		assertEquals(OK, jsonRes.status());
		assertEquals("*", jsonRes.header("Access-Control-Allow-Origin").orElse(""));

		// B. CSV download with exact token
		Http.Request csvReq = createAuthenticatedRequest(GET, "/datasets/downloadPublic/" + userToken, null);
		Result csvRes = datasetsController.downloadPublic(csvReq, userToken, -1L, -1L, -1L, -1L).toCompletableFuture().get();
		assertEquals(OK, csvRes.status());
		assertEquals("*", csvRes.header("Access-Control-Allow-Origin").orElse(""));

		// C. Single-encoded legacy token Al8l4i8EzctZA0mRV5Qp3g==
		String singleEncodedToken = "Al8l4i8EzctZA0mRV5Qp3g==";
		Http.Request singleReq = createAuthenticatedRequest(GET, "/datasets/downloadPublic/json/" + singleEncodedToken, null);
		Result singleRes = datasetsController.downloadJsonPublic(singleReq, singleEncodedToken, -1L, -1L, -1L, -1L).toCompletableFuture().get();
		assertEquals(OK, singleRes.status());

		// D. Percent-encoded legacy token Al8l4i8EzctZA0mRV5Qp3g%3D%3D
		String percentEncodedToken = "Al8l4i8EzctZA0mRV5Qp3g%3D%3D";
		Http.Request percentReq = createAuthenticatedRequest(GET, "/datasets/downloadPublic/json/" + percentEncodedToken, null);
		Result percentRes = datasetsController.downloadJsonPublic(percentReq, percentEncodedToken, -1L, -1L, -1L, -1L).toCompletableFuture().get();
		assertEquals(OK, percentRes.status());

		// 2. Test WEB_ACCESS_TOKEN configuration allows download without PUBLIC_ACCESS_TOKEN
		ds15.getConfiguration().remove(Dataset.PUBLIC_ACCESS_TOKEN);
		ds15.getConfiguration().put(Dataset.WEB_ACCESS_TOKEN, userToken);
		ds15.update();

		Result webTokenJsonRes = datasetsController.downloadJsonPublic(jsonReq, userToken, -1L, -1L, -1L, -1L).toCompletableFuture().get();
		assertEquals(OK, webTokenJsonRes.status());

		Result webTokenCsvRes = datasetsController.downloadPublic(csvReq, userToken, -1L, -1L, -1L, -1L).toCompletableFuture().get();
		assertEquals(OK, webTokenCsvRes.status());

		// 3. Test starboard datasets helper snippet rendering with WEB_ACCESS_TOKEN
		Http.Request helperReq = createAuthenticatedRequest(GET, "/tools/starboard/datasets", ownerUser);
		Result helperRes = starboardController.datasets(helperReq);
		assertEquals(OK, helperRes.status());
		String helperBody = contentAsString(helperRes);
		assertTrue(helperBody.contains("/datasets/downloadPublic/json/" + userToken));
		assertTrue(helperBody.contains("/datasets/downloadPublic/" + userToken));
		assertFalse("datasets helper should not contain material-icons font icons", helperBody.contains("material-icons"));
		assertFalse("datasets helper should not contain unicode triangle bullet", helperBody.contains("&#5125;"));
		assertTrue("datasets helper should render project-icon svg", helperBody.contains("project-icon"));
		assertTrue("datasets helper should render dataset-icon svg", helperBody.contains("dataset-icon"));
		assertTrue("datasets helper should render dataset-lock-icon svg for unconfigured dataset", helperBody.contains("dataset-lock-icon"));

		// 4. Test CompleteDSController.downloadFilePublic with legacy token
		CompleteDS cpds15 = (CompleteDS) datasetConnector.getDatasetDS(ds15);
		File testFile = File.createTempFile("legacy_pub_test", ".txt");
		Files.write(testFile.toPath(), "Legacy File Content".getBytes(StandardCharsets.UTF_8));
		cpds15.storeFile(testFile, "public_test.txt");
		cpds15.addRecord("public_test.txt", "Description", new Date());
		testFile.delete();

		Result fileRes = completeDSController.downloadFilePublic("public_test.txt", userToken).toCompletableFuture().get();
		assertEquals(OK, fileRes.status());
	}
}

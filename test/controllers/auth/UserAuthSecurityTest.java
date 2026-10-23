package controllers.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.mvc.Http.Status.SEE_OTHER;
import static play.test.Helpers.contentAsString;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import controllers.UsersController;
import models.Person;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;

public class UserAuthSecurityTest extends WithApplication {

	private UserAuth userAuth;
	private SessionStore sessionStore;
	private UsersController usersController;
	private Person testUser;

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
	public void setUp() {
		userAuth = app.injector().instanceOf(UserAuth.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);
		usersController = app.injector().instanceOf(UsersController.class);

		testUser = new Person();
		testUser.setFirstname("Security");
		testUser.setLastname("Tester");
		testUser.setEmail("tester_" + UUID.randomUUID() + "@example.com");
		testUser.setUser_id(UUID.randomUUID().toString());
		testUser.save();
	}

	private Http.Request createAuthenticatedRequest(String method, String uri) {
		return createAuthenticatedRequest(method, uri, Collections.emptyMap());
	}

	private Http.Request createAuthenticatedRequest(String method, String uri, java.util.Map<String, String> headers) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method(method).uri(uri);
		if (headers != null) {
			for (java.util.Map.Entry<String, String> entry : headers.entrySet()) {
				requestBuilder.header(entry.getKey(), entry.getValue());
			}
		}
		Http.Request request = requestBuilder.build();
		PlayWebContext context = new PlayWebContext(request);
		ProfileManager profileManager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(testUser.getEmail());
		profile.addAttribute("id", testUser.getId());
		profile.addAttribute(Person.USER_NAME, testUser.getEmail());
		profile.addAttribute(Person.USER_ID, testUser.getId());
		profile.addAttribute("email", testUser.getEmail());
		profileManager.save(true, profile, false);
		return context.supplementRequest(request);
	}

	@Test
	public void testLegitimateRequestAuthenticates() {
		Http.Request request = createAuthenticatedRequest("GET", "/users/profile");
		Optional<String> username = userAuth.getUsername(request);
		assertTrue(username.isPresent());
		assertEquals(testUser.getEmail(), username.get());
	}

	@Test
	public void testOriginNullIsRejected() {
		Http.Request request = createAuthenticatedRequest("POST", "/users/profile", Collections.singletonMap("Origin", "null"));
		Optional<String> username = userAuth.getUsername(request);
		assertFalse("Requests with Origin: null must be denied", username.isPresent());
	}

	@Test
	public void testSecFetchCrossSiteSubresourceIsRejectedEvenWithoutReferer() {
		// Simulating exploit: fetch with referrerPolicy: 'no-referrer' (absent Referer)
		java.util.Map<String, String> headers = new java.util.HashMap<>();
		headers.put("Sec-Fetch-Site", "cross-site");
		headers.put("Sec-Fetch-Mode", "cors");
		headers.put("Sec-Fetch-Dest", "empty");
		Http.Request request = createAuthenticatedRequest("GET", "/users/apiAccess", headers);
		Optional<String> username = userAuth.getUsername(request);
		assertFalse("Cross-site CORS fetch without Referer must be denied", username.isPresent());
	}

	@Test
	public void testSecFetchCrossSiteNavigationPermittedForGet() {
		java.util.Map<String, String> headers = new java.util.HashMap<>();
		headers.put("Sec-Fetch-Site", "cross-site");
		headers.put("Sec-Fetch-Mode", "navigate");
		headers.put("Sec-Fetch-Dest", "document");
		Http.Request request = createAuthenticatedRequest("GET", "/users/profile", headers);
		Optional<String> username = userAuth.getUsername(request);
		assertTrue("Top-level cross-site navigation GET should be permitted", username.isPresent());
	}

	@Test
	public void testSecFetchCrossSiteNavigationDeniedForPost() {
		java.util.Map<String, String> headers = new java.util.HashMap<>();
		headers.put("Sec-Fetch-Site", "cross-site");
		headers.put("Sec-Fetch-Mode", "navigate");
		headers.put("Sec-Fetch-Dest", "document");
		Http.Request request = createAuthenticatedRequest("POST", "/users/generateToken", headers);
		Optional<String> username = userAuth.getUsername(request);
		assertFalse("Cross-site POST navigation must be denied", username.isPresent());
	}

	@Test
	public void testRefererContainingWebIsDeniedForSubresource() {
		Http.Request request = createAuthenticatedRequest("GET", "/users/profile", Collections.singletonMap("Referer", "http://localhost:9000/datasets/web/6/index.html"));
		Optional<String> username = userAuth.getUsername(request);
		assertFalse("Referer containing /datasets/web/ must be denied for subresource fetches", username.isPresent());
	}

	@Test
	public void testRefererContainingWebAllowedForTopLevelGetNavigation() {
		java.util.Map<String, String> headers = new java.util.HashMap<>();
		headers.put("Referer", "http://localhost:9000/datasets/web/view/token123/index.html");
		headers.put("Sec-Fetch-Mode", "navigate");
		Http.Request request = createAuthenticatedRequest("GET", "/projects", headers);
		Optional<String> username = userAuth.getUsername(request);
		assertTrue("Top-level GET navigation from hosted page must be permitted", username.isPresent());
	}

	@Test
	public void testRefererContainingWebAllowedForRenewalRoute() {
		java.util.Map<String, String> headers = new java.util.HashMap<>();
		headers.put("Referer", "http://localhost:9000/datasets/web/view/expired_token/index.html");
		Http.Request request = createAuthenticatedRequest("GET", "/datasets/web/6/index.html", headers);
		Optional<String> username = userAuth.getUsername(request);
		assertTrue("Token renewal gateway route must permit Referer from /datasets/web/view/", username.isPresent());
	}

	@Test
	public void testAbstractAsyncControllerBlocksOriginNull() {
		Http.Request request = createAuthenticatedRequest("POST", "/users/profile", Collections.singletonMap("Origin", "null"));
		Optional<Person> userOpt = usersController.getAuthenticatedUser(request);
		Optional<String> nameOpt = usersController.getAuthenticatedUserName(request);
		assertFalse("AbstractAsyncController must reject requests with Origin: null", userOpt.isPresent());
		assertFalse("AbstractAsyncController must reject requests with Origin: null", nameOpt.isPresent());
	}

	@Test
	public void testAbstractAsyncControllerBlocksCrossSiteSubresource() {
		java.util.Map<String, String> headers = new java.util.HashMap<>();
		headers.put("Sec-Fetch-Site", "cross-site");
		headers.put("Sec-Fetch-Mode", "cors");
		headers.put("Sec-Fetch-Dest", "empty");
		Http.Request request = createAuthenticatedRequest("GET", "/users/apiAccess", headers);
		Optional<Person> userOpt = usersController.getAuthenticatedUser(request);
		Optional<String> nameOpt = usersController.getAuthenticatedUserName(request);
		assertFalse("AbstractAsyncController must reject cross-site CORS fetch", userOpt.isPresent());
		assertFalse("AbstractAsyncController must reject cross-site CORS fetch", nameOpt.isPresent());
	}

	@Test
	public void testAbstractAsyncControllerPermitsLegitimateRequest() {
		Http.Request request = createAuthenticatedRequest("GET", "/users/profile");
		Optional<Person> userOpt = usersController.getAuthenticatedUser(request);
		Optional<String> nameOpt = usersController.getAuthenticatedUserName(request);
		assertTrue(userOpt.isPresent());
		assertEquals(testUser.getId(), userOpt.get().getId());
		assertTrue(nameOpt.isPresent());
		assertEquals(testUser.getEmail(), nameOpt.get());
	}

	@Test
	public void testRevealTokenReturnsTokenForAuthenticatedUser() {
		testUser.setAccesscode("test-secret-token-12345");
		testUser.update();

		Http.Request request = createAuthenticatedRequest("POST", "/users/revealToken");
		Result result = usersController.revealToken(request);
		assertEquals(OK, result.status());
		assertTrue(contentAsString(result).contains("test-secret-token-12345"));
	}

	@Test
	public void testRevealTokenReturnsNotFoundWhenEmpty() {
		testUser.setAccesscode("");
		testUser.update();

		Http.Request request = createAuthenticatedRequest("POST", "/users/revealToken");
		Result result = usersController.revealToken(request);
		assertEquals(NOT_FOUND, result.status());
	}

	@Test
	public void testGenerateTokenCreatesTokenAndRedirects() {
		testUser.setAccesscode("");
		testUser.update();

		Http.Request request = createAuthenticatedRequest("POST", "/users/generateToken");
		Result result = usersController.generateToken(request, "");
		assertEquals(SEE_OTHER, result.status());

		testUser.refresh();
		assertNotNull(testUser.getAccesscode());
		assertFalse(testUser.getAccesscode().isEmpty());
	}

	@Test
	public void testCrossSiteIframeNavigationBlockedExceptRenewalRoute() {
		java.util.Map<String, String> headers = new java.util.HashMap<>();
		headers.put("Sec-Fetch-Site", "cross-site");
		headers.put("Sec-Fetch-Mode", "navigate");
		headers.put("Sec-Fetch-Dest", "iframe");

		Http.Request profileReq = createAuthenticatedRequest("GET", "/users/profile", headers);
		assertFalse("Cross-site iframe navigation to /users/profile must be denied", userAuth.getUsername(profileReq).isPresent());

		Http.Request renewalReq = createAuthenticatedRequest("GET", "/datasets/web/6/index.html", headers);
		assertTrue("Cross-site iframe navigation to token renewal route must be permitted", userAuth.getUsername(renewalReq).isPresent());

		Http.Request entrypointReq = createAuthenticatedRequest("GET", "/datasets/web/6/entrypoint", headers);
		assertFalse("Cross-site iframe navigation to /datasets/web/6/entrypoint must be denied", userAuth.getUsername(entrypointReq).isPresent());
	}

	@Test
	public void testCrossSiteAndWebRefererPopupToStateChangingGetBlocked() {
		java.util.Map<String, String> crossSiteNavHeaders = new java.util.HashMap<>();
		crossSiteNavHeaders.put("Sec-Fetch-Site", "cross-site");
		crossSiteNavHeaders.put("Sec-Fetch-Mode", "navigate");
		crossSiteNavHeaders.put("Sec-Fetch-Dest", "document");

		assertFalse("Popup GET to /datasets/delete/6 must be blocked",
				userAuth.getUsername(createAuthenticatedRequest("GET", "/datasets/delete/6", crossSiteNavHeaders)).isPresent());
		assertFalse("Popup GET to /datasets/reset/6 must be blocked",
				userAuth.getUsername(createAuthenticatedRequest("GET", "/datasets/reset/6", crossSiteNavHeaders)).isPresent());
		assertFalse("Popup GET to /datasets/configureToken/6/api_token must be blocked",
				userAuth.getUsername(createAuthenticatedRequest("GET", "/datasets/configureToken/6/api_token", crossSiteNavHeaders)).isPresent());

		java.util.Map<String, String> webRefererNavHeaders = new java.util.HashMap<>();
		webRefererNavHeaders.put("Referer", "http://localhost:9000/web/view/tok/index.html");
		webRefererNavHeaders.put("Sec-Fetch-Mode", "navigate");
		webRefererNavHeaders.put("Sec-Fetch-Dest", "document");

		assertFalse("Navigation from /web/ to /datasets/delete/6 must be blocked",
				userAuth.getUsername(createAuthenticatedRequest("GET", "/datasets/delete/6", webRefererNavHeaders)).isPresent());
	}

	@Test
	public void testMismatchedOriginHostIsRejected() {
		Http.RequestBuilder builder = new Http.RequestBuilder()
				.method("GET")
				.uri("/users/profile")
				.host("datafoundry.id.tue.nl")
				.header("Origin", "https://evil.example.com");
		assertFalse("Mismatched Origin host must be rejected", UserAuth.isValidRequestContext(builder.build()));

		Http.RequestBuilder matchingBuilder = new Http.RequestBuilder()
				.method("GET")
				.uri("/users/profile")
				.host("datafoundry.id.tue.nl")
				.header("Origin", "https://datafoundry.id.tue.nl");
		assertTrue("Matching Origin host must be permitted", UserAuth.isValidRequestContext(matchingBuilder.build()));
	}

	@Test
	public void testJSActorProxyEscapesHtmlInLogs() {
		services.jsexecutor.JSActorProxy proxy = new services.jsexecutor.JSActorProxy(null);
		proxy.print("<script>alert('xss')</script>\nline2");
		proxy.error("<img src=x onerror=alert(1)>");
		String logs = proxy.getMessages();
		assertFalse("Logs must not contain unescaped <script>", logs.contains("<script>"));
		assertTrue("Logs must contain escaped &lt;script&gt;", logs.contains("&lt;script&gt;alert(&#39;xss&#39;)&lt;/script&gt;<br>line2"));
		assertFalse("Logs must not contain unescaped <img", logs.contains("<img"));
		assertTrue("Logs must contain escaped &lt;img", logs.contains("&lt;img src=x onerror=alert(1)&gt;"));
	}
}

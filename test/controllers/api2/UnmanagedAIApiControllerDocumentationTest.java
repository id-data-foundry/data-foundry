package controllers.api2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.ConfigFactory;

import controllers.api2.UnmanagedAIApiController.ApiCall;
import play.mvc.Http;
import play.mvc.Http.Request;
import services.api.ai.LocalModelMetadata;
import services.api.ai.UnmanagedAIApiService;

public class UnmanagedAIApiControllerDocumentationTest {

	private UnmanagedAIApiController controller;
	private String internalDocsKey;
	private TestUserAuth testUserAuth;

	static class TestUserAuth extends controllers.auth.UserAuth {
		boolean authenticated = true;
		@Override
		public Optional<String> getUsername(Request request) {
			return authenticated ? Optional.of("testuser") : Optional.empty();
		}
	}

	static class TestableAiService extends UnmanagedAIApiService {
		services.api.remoting.RemoteApiRequest lastRequest;

		public TestableAiService(com.typesafe.config.Config config) {
			super(config != null ? config : ConfigFactory.empty(), null, null, null, null, null, null, null,
					new LocalModelMetadata(config != null ? config : ConfigFactory.empty()), null);
		}

		public TestableAiService() {
			this(ConfigFactory.empty());
		}

		@Override
		public java.util.concurrent.CompletableFuture<Void> submitApiRequest(services.api.remoting.RemoteApiRequest request) {
			this.lastRequest = request;
			request.setOutcome(services.api.remoting.RemoteApiRequest.Outcome.OK);
			request.setResult(Optional.of(play.libs.Json.newObject().put("result", "ok").toString()));
			return java.util.concurrent.CompletableFuture.completedFuture(null);
		}
	}

	@Before
	public void setUp() {
		controller = new UnmanagedAIApiController();
		TestableAiService aiService = new TestableAiService();
		controller.aiApiService = aiService;
		testUserAuth = new TestUserAuth();
		controller.userAuth = testUserAuth;
		internalDocsKey = aiService.getInternalDocumentationAPIKey();
	}

	@Test
	public void testDevelopmentLocalhostReferer() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "localhost:9000")
				.header("Referer", "http://localhost:9000/documentation/index.html")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "dummy_key");
		assertEquals("Should grant internal documentation key for localhost dev referer", internalDocsKey, resolvedKey);
	}

	@Test
	public void testProductionWafHttpsReferer() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.header("X-Forwarded-Proto", "https")
				.header("X-Forwarded-Port", "443")
				.header("Referer", "https://datafoundry.tue.nl/documentation/Guides/LocalAI/examples/example1.html")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "dummy_key");
		assertEquals("Should grant internal documentation key for production WAF referer", internalDocsKey, resolvedKey);
	}

	@Test
	public void testProductionWafWithXForwardedHost() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry-container:9000")
				.header("X-Forwarded-Host", "datafoundry.tue.nl")
				.header("X-Forwarded-Proto", "https")
				.header("Referer", "https://datafoundry.tue.nl/documentation/examples/example2.html")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "dummy_key");
		assertEquals("Should resolve public host via X-Forwarded-Host and grant documentation key", internalDocsKey, resolvedKey);
	}

	@Test
	public void testProductionWafWithMultipleXForwardedHosts() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry-container:9000")
				.header("X-Forwarded-Host", "datafoundry.tue.nl, proxy1.internal")
				.header("Referer", "https://datafoundry.tue.nl/documentation")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "dummy_key");
		assertEquals("Should use first entry in comma-separated X-Forwarded-Host", internalDocsKey, resolvedKey);
	}

	@Test
	public void testAuthorizeWithPlaceholderKeyFromDocumentation() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.header("Authorization", "Bearer YOUR_API_KEY")
				.header("Referer", "https://datafoundry.tue.nl/documentation/index.html")
				.build();

		Optional<ApiCall> call = controller.authorize(request);
		assertTrue("Authorize should succeed for documentation referer", call.isPresent());
		assertEquals(internalDocsKey, call.get().apiKey());
	}

	@Test
	public void testAuthorizeWithoutAuthorizationHeaderFromDocumentation() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.header("Referer", "https://datafoundry.tue.nl/documentation/index.html")
				.build();

		Optional<ApiCall> call = controller.authorize(request);
		assertTrue("Authorize should succeed even without Authorization header if from documentation", call.isPresent());
		assertEquals(internalDocsKey, call.get().apiKey());
	}

	@Test
	public void testAuthorizeWithEmptyBearerTokenFromDocumentation() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.header("Authorization", "Bearer ")
				.header("Referer", "https://datafoundry.tue.nl/documentation/index.html")
				.build();

		Optional<ApiCall> call = controller.authorize(request);
		assertTrue("Authorize should succeed with empty bearer token if from documentation", call.isPresent());
		assertEquals(internalDocsKey, call.get().apiKey());
	}

	@Test
	public void testRejectsForgedRefererFragmentH5() {
		// Finding H5 attack vector: fragment containing the target hostname
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.header("Referer", "https://evil.example/documentation#datafoundry.tue.nl")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "attacker_key");
		assertEquals("Must reject forged referer with hostname in fragment", "attacker_key", resolvedKey);
	}

	@Test
	public void testRejectsCrossDomainReferer() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.header("Referer", "https://attacker.com/documentation/malicious.html")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "attacker_key");
		assertEquals("Must reject cross-domain referer", "attacker_key", resolvedKey);
	}

	@Test
	public void testRejectsNonDocumentationPath() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.header("Referer", "https://datafoundry.tue.nl/profile")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "my_key");
		assertEquals("Must reject non-documentation path", "my_key", resolvedKey);
	}

	@Test
	public void testRejectsMissingReferer() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "my_key");
		assertEquals("Must keep original key when Referer header is missing", "my_key", resolvedKey);
	}

	@Test
	public void testAuthorizeRejectsMissingAuthWhenNotFromDocumentation() {
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.build();

		Optional<ApiCall> call = controller.authorize(request);
		assertFalse("Authorize must reject requests with no token and no documentation referer", call.isPresent());
	}

	@Test
	public void testRejectsDocumentationKeyWhenUnauthenticated() {
		testUserAuth.authenticated = false;
		Request request = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "datafoundry.tue.nl")
				.header("Referer", "https://datafoundry.tue.nl/documentation/index.html")
				.build();

		String resolvedKey = controller.checkDocumentationAPIKey(request, "dummy_key");
		assertEquals("Must not grant documentation key when unauthenticated", "dummy_key", resolvedKey);
	}

	@Test
	public void testChatCompletionModelPlaceholderResolution() throws Exception {
		com.typesafe.config.Config config = ConfigFactory.parseMap(java.util.Map.of(
				"df.processing.ai.models.default", "hermes-default-3-8b",
				"df.processing.ai.models.chat", "hermes-chat-3-8b",
				"df.processing.ai.models.vision", "llava-vision-v1",
				"df.processing.ai.models.coding", "qwen-coding-27b"
		));
		TestableAiService testAi = new TestableAiService(config);
		controller.aiApiService = testAi;

		// 1. "default" placeholder
		Request req1 = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "localhost:9000")
				.header("Referer", "http://localhost:9000/documentation/index.html")
				.bodyJson(play.libs.Json.newObject().put("model", "default"))
				.build();
		play.mvc.Result res1 = controller.chatCompletion(req1).toCompletableFuture().get();
		assertEquals(200, res1.status());
		assertEquals("hermes-default-3-8b", testAi.lastRequest.getModel());
		assertEquals("hermes-default-3-8b", testAi.lastRequest.getParams().get("model").asText());

		// 2. "vision" placeholder
		Request req2 = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "localhost:9000")
				.header("Referer", "http://localhost:9000/documentation/index.html")
				.bodyJson(play.libs.Json.newObject().put("model", "vision"))
				.build();
		play.mvc.Result res2 = controller.chatCompletion(req2).toCompletableFuture().get();
		assertEquals(200, res2.status());
		assertEquals("llava-vision-v1", testAi.lastRequest.getModel());
		assertEquals("llava-vision-v1", testAi.lastRequest.getParams().get("model").asText());

		// 3. "coding" placeholder
		Request req3 = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "localhost:9000")
				.header("Referer", "http://localhost:9000/documentation/index.html")
				.bodyJson(play.libs.Json.newObject().put("model", "coding"))
				.build();
		play.mvc.Result res3 = controller.chatCompletion(req3).toCompletableFuture().get();
		assertEquals(200, res3.status());
		assertEquals("qwen-coding-27b", testAi.lastRequest.getModel());

		// 4. Missing/omitted model parameter defaults to default placeholder
		Request req4 = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "localhost:9000")
				.header("Referer", "http://localhost:9000/documentation/index.html")
				.bodyJson(play.libs.Json.newObject())
				.build();
		play.mvc.Result res4 = controller.chatCompletion(req4).toCompletableFuture().get();
		assertEquals(200, res4.status());
		assertEquals("hermes-default-3-8b", testAi.lastRequest.getModel());

		// 5. Literal concrete model ID passes through
		Request req5 = new Http.RequestBuilder()
				.method("POST")
				.uri("/v1/chat/completions")
				.header("Host", "localhost:9000")
				.header("Referer", "http://localhost:9000/documentation/index.html")
				.bodyJson(play.libs.Json.newObject().put("model", "openai/gpt-4o"))
				.build();
		play.mvc.Result res5 = controller.chatCompletion(req5).toCompletableFuture().get();
		assertEquals(200, res5.status());
		assertEquals("openai/gpt-4o", testAi.lastRequest.getModel());
	}
}

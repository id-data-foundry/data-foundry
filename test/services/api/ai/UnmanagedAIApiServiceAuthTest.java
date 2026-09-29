package services.api.ai;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import play.libs.ws.WSClient;
import play.libs.ws.WSRequest;
import play.libs.ws.WSResponse;
import services.api.remoting.RemoteApiRequest;

public class UnmanagedAIApiServiceAuthTest {

	static class CapturingWSHandler implements InvocationHandler {
		String capturedUrl;
		Duration capturedTimeout;
		final Map<String, List<String>> capturedHeaders = new HashMap<>();
		int responseStatus = 200;
		String responseBody = "{}";

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			String name = method.getName();
			if (name.equals("url")) {
				this.capturedUrl = (String) args[0];
				return createRequestProxy();
			} else if (name.equals("setRequestTimeout")) {
				this.capturedTimeout = (Duration) args[0];
				return proxy;
			} else if (name.equals("addHeader")) {
				String hName = (String) args[0];
				String hVal = (String) args[1];
				capturedHeaders.computeIfAbsent(hName, k -> new ArrayList<>()).add(hVal);
				return proxy;
			} else if (name.equals("setMethod") || name.equals("setBody")) {
				return proxy;
			} else if (name.equals("get") || name.equals("post")) {
				return CompletableFuture.completedFuture(createResponseProxy());
			} else if (name.equals("close") || name.equals("hashCode") || name.equals("equals") || name.equals("toString")) {
				return null;
			}
			return null;
		}

		WSRequest createRequestProxy() {
			return (WSRequest) Proxy.newProxyInstance(
					WSRequest.class.getClassLoader(),
					new Class<?>[] { WSRequest.class },
					this);
		}

		WSResponse createResponseProxy() {
			return (WSResponse) Proxy.newProxyInstance(
					WSResponse.class.getClassLoader(),
					new Class<?>[] { WSResponse.class },
					(proxy, method, args) -> {
						if (method.getName().equals("getStatus")) {
							return responseStatus;
						} else if (method.getName().equals("getBody")) {
							return responseBody;
						}
						return null;
					});
		}

		WSClient createClientProxy() {
			return (WSClient) Proxy.newProxyInstance(
					WSClient.class.getClassLoader(),
					new Class<?>[] { WSClient.class },
					this);
		}
	}

	static class TestableUnmanagedAIApiService extends UnmanagedAIApiService {
		public TestableUnmanagedAIApiService(Config config, WSClient wsClient) {
			super(config, null, null, null, null, null, wsClient, null, new LocalModelMetadata(), null);
		}

		@Override
		public WSRequest prepareWSRequest(String path, Duration timeout) {
			return super.prepareWSRequest(path, timeout);
		}

		@Override
		public WSRequest prepareWSRemoteAPIRequest(RemoteApiRequest request) {
			return super.prepareWSRemoteAPIRequest(request);
		}

		@Override
		public CompletableFuture<Boolean> pingEndpoint(String path) {
			return super.pingEndpoint(path);
		}

		@Override
		public RemoteApiRequest createModelsRequestForRefresh() {
			return super.createModelsRequestForRefresh();
		}
	}

	@Test
	public void testPrepareWSRequestWithConfiguredKey() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.processing.ai.key", "my-secret-backend-key");
		Config config = ConfigFactory.parseMap(map);

		CapturingWSHandler handler = new CapturingWSHandler();
		WSClient wsClient = handler.createClientProxy();

		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, wsClient);
		WSRequest req = service.prepareWSRequest("/models", Duration.ofSeconds(5));

		assertNotNull(req);
		assertEquals("http://localhost:9191/v1/models", handler.capturedUrl);
		assertEquals(Duration.ofSeconds(5), handler.capturedTimeout);
		assertTrue(handler.capturedHeaders.containsKey("Authorization"));
		assertEquals(Collections.singletonList("Bearer my-secret-backend-key"), handler.capturedHeaders.get("Authorization"));
		assertEquals("my-secret-backend-key", service.getLocalAIAPIKey());
	}

	@Test
	public void testPrepareWSRequestWithoutKey() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.processing.ai.key", "");
		Config config = ConfigFactory.parseMap(map);

		CapturingWSHandler handler = new CapturingWSHandler();
		WSClient wsClient = handler.createClientProxy();

		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, wsClient);
		WSRequest req = service.prepareWSRequest("/models", Duration.ofSeconds(5));

		assertNotNull(req);
		assertEquals("http://localhost:9191/v1/models", handler.capturedUrl);
		assertFalse(handler.capturedHeaders.containsKey("Authorization"));
		assertEquals("", service.getLocalAIAPIKey());
	}

	@Test
	public void testPrepareWSRequestWithExistingBearerPrefix() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.processing.ai.key", "Bearer custom-bearer-token");
		Config config = ConfigFactory.parseMap(map);

		CapturingWSHandler handler = new CapturingWSHandler();
		WSClient wsClient = handler.createClientProxy();

		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, wsClient);
		WSRequest req = service.prepareWSRequest("/models", Duration.ofSeconds(5));

		assertNotNull(req);
		assertEquals(Collections.singletonList("Bearer custom-bearer-token"), handler.capturedHeaders.get("Authorization"));
	}

	@Test
	public void testPrepareWSRemoteAPIRequestIncludesModelAndAuth() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.processing.ai.key", "auth-token-123");
		Config config = ConfigFactory.parseMap(map);

		CapturingWSHandler handler = new CapturingWSHandler();
		WSClient wsClient = handler.createClientProxy();

		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, wsClient);
		RemoteApiRequest apiReq = new RemoteApiRequest("models", 5000, "SYSTEM", "user-key", -1L);
		apiReq.setModel("hermes-2-pro-llama-3-8b");

		WSRequest req = service.prepareWSRemoteAPIRequest(apiReq);
		assertNotNull(req);
		assertEquals(Collections.singletonList("Bearer auth-token-123"), handler.capturedHeaders.get("Authorization"));
		assertEquals(Collections.singletonList("hermes-2-pro-llama-3-8b"), handler.capturedHeaders.get("X-API-Model"));
	}

	@Test
	public void testPingEndpointAvailabilityChecks() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.processing.ai.key", "probe-key");
		Config config = ConfigFactory.parseMap(map);

		CapturingWSHandler handler = new CapturingWSHandler();
		WSClient wsClient = handler.createClientProxy();
		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, wsClient);

		// Status 200 -> available
		handler.responseStatus = 200;
		assertTrue(service.pingEndpoint("/chat/completions").get());
		assertEquals(Collections.singletonList("Bearer probe-key"), handler.capturedHeaders.get("Authorization"));

		// Status 405 (Method Not Allowed, typical for GET on POST endpoint) -> available
		handler.responseStatus = 405;
		assertTrue(service.pingEndpoint("/chat/completions").get());

		// Status 404 (Not Found) -> NOT available
		handler.responseStatus = 404;
		assertFalse(service.pingEndpoint("/chat/completions").get());

		// Status 401 (Unauthorized) -> NOT available
		handler.responseStatus = 401;
		assertFalse(service.pingEndpoint("/chat/completions").get());

		// Status 403 (Forbidden) -> NOT available
		handler.responseStatus = 403;
		assertFalse(service.pingEndpoint("/chat/completions").get());
	}

	@Test
	public void testRefreshInternalApiRequestInitialization() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		Config config = ConfigFactory.parseMap(map);

		CapturingWSHandler handler = new CapturingWSHandler();
		WSClient wsClient = handler.createClientProxy();
		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, wsClient);

		RemoteApiRequest req = service.createModelsRequestForRefresh();
		assertEquals("SYSTEM", req.getUsername());
		assertEquals(service.getInternalDocumentationAPIKey(), req.getUserApiKey());
		assertEquals("models", req.getType());
		assertEquals("/models", req.getPath());
	}
}

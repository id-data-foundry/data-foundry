package services.api.ai;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import play.libs.Json;
import services.api.ApiServiceConstants;
import services.api.remoting.RemoteApiRequest;

public class AbstractAIApiServiceMaxTokensTest {

	static class ConcreteAIApiService extends AbstractAIApiService {
		public ConcreteAIApiService(Config configuration) {
			super(configuration, null, null, null, new LocalModelMetadata());
		}

		public void testPreProcess(RemoteApiRequest request) {
			super.preProcessRequest(request);
		}
	}

	@Test
	public void testDefaultMaxTokensFallbackTo4096WhenUnconfigured() {
		Config config = ConfigFactory.empty();
		ConcreteAIApiService service = new ConcreteAIApiService(config);
		assertEquals(4096, service.getDefaultMaxTokens());

		ObjectNode params = Json.newObject().put("model", "test-model");
		RemoteApiRequest request = new RemoteApiRequest("chat", 1000, "user", "key", 1L, params);

		service.testPreProcess(request);

		assertTrue(params.has(ApiServiceConstants.REQUEST_MAX_TOKENS));
		assertEquals(4096, params.get(ApiServiceConstants.REQUEST_MAX_TOKENS).asInt());
	}

	@Test
	public void testCustomConfiguredDefaultMaxTokens() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.default_max_tokens", 8192);
		Config config = ConfigFactory.parseMap(map);

		ConcreteAIApiService service = new ConcreteAIApiService(config);
		assertEquals(8192, service.getDefaultMaxTokens());

		ObjectNode params = Json.newObject().put("model", "test-model");
		RemoteApiRequest request = new RemoteApiRequest("chat", 1000, "user", "key", 1L, params);

		service.testPreProcess(request);

		assertTrue(params.has(ApiServiceConstants.REQUEST_MAX_TOKENS));
		assertEquals(8192, params.get(ApiServiceConstants.REQUEST_MAX_TOKENS).asInt());
	}

	@Test
	public void testExplicitMaxTokensPreserved() {
		Config config = ConfigFactory.empty();
		ConcreteAIApiService service = new ConcreteAIApiService(config);

		ObjectNode params = Json.newObject().put("model", "test-model").put("max_tokens", 1000);
		RemoteApiRequest request = new RemoteApiRequest("chat", 1000, "user", "key", 1L, params);

		service.testPreProcess(request);

		assertEquals(1000, params.get(ApiServiceConstants.REQUEST_MAX_TOKENS).asInt());
	}

	@Test
	public void testMaxCompletionTokensPreventsMaxTokensInjection() {
		Config config = ConfigFactory.empty();
		ConcreteAIApiService service = new ConcreteAIApiService(config);

		ObjectNode params = Json.newObject().put("model", "test-model").put("max_completion_tokens", 2048);
		RemoteApiRequest request = new RemoteApiRequest("chat", 1000, "user", "key", 1L, params);

		service.testPreProcess(request);

		assertFalse(params.has(ApiServiceConstants.REQUEST_MAX_TOKENS));
		assertEquals(2048, params.get("max_completion_tokens").asInt());
	}

	@Test
	public void testDefaultMaxTokensZeroDisablesInjection() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.default_max_tokens", 0);
		Config config = ConfigFactory.parseMap(map);

		ConcreteAIApiService service = new ConcreteAIApiService(config);
		assertEquals(0, service.getDefaultMaxTokens());

		ObjectNode params = Json.newObject().put("model", "test-model");
		RemoteApiRequest request = new RemoteApiRequest("chat", 1000, "user", "key", 1L, params);

		service.testPreProcess(request);

		assertFalse(params.has(ApiServiceConstants.REQUEST_MAX_TOKENS));
	}
}

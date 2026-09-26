package utils.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import utils.conf.ConfigurationUtils;

public class TokenResolverUtilSecurityFallbackTest {

	@Test
	public void testFallbackToPlaySecretWhenProjectTokenMissing() {
		Map<String, Object> map = new HashMap<>();
		map.put("play.http.secret.key", "play-application-secret-key-1234567890-abcdef");
		Config config = ConfigFactory.parseMap(map);

		TokenResolverUtil util = new TokenResolverUtil(config);
		String token = util.getDatasetToken(1L);
		assertNotNull("Tokens should be generated successfully using play.http.secret.key fallback", token);
		assertFalse(token.isEmpty());
		Long resolvedId = util.getDatasetIdFromToken(token);
		assertEquals(Long.valueOf(1L), resolvedId);
	}

	@Test
	public void testFallbackToPlaySecretWhenProjectTokenEmpty() {
		Map<String, Object> map = new HashMap<>();
		map.put(ConfigurationUtils.DF_KEYS_PROJECT_TOKEN, "   ");
		map.put("play.http.secret.key", "play-application-secret-key-1234567890-abcdef");
		Config config = ConfigFactory.parseMap(map);

		TokenResolverUtil util = new TokenResolverUtil(config);
		String token = util.getDatasetToken(1L);
		assertNotNull("Tokens should be generated successfully when project_token is whitespace", token);
		assertFalse(token.isEmpty());
		Long resolvedId = util.getDatasetIdFromToken(token);
		assertEquals(Long.valueOf(1L), resolvedId);
	}

	@Test
	public void testCheckRegistrationAccessKeyRejectsEmptyAndBlankKeys() {
		Map<String, Object> map = new HashMap<>();
		map.put(ConfigurationUtils.DF_KEYS_PROJECT_TOKEN, "valid-secret-key-123456");
		map.put(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS, Arrays.asList("", "   ", "valid-code"));
		Config config = ConfigFactory.parseMap(map);

		TokenResolverUtil util = new TokenResolverUtil(config);
		assertFalse("Empty registration code must be rejected", util.checkRegistrationAccessKey(""));
		assertFalse("Whitespace registration code must be rejected", util.checkRegistrationAccessKey("   "));
		assertFalse("Null registration code must be rejected", util.checkRegistrationAccessKey(null));
		assertFalse("Non-existent code must be rejected", util.checkRegistrationAccessKey("wrong-code"));
		assertTrue("Configured non-empty code must be accepted", util.checkRegistrationAccessKey("valid-code"));
	}

	@Test
	public void testCheckRegistrationAccessKeyWhenOnlyEmptyInConfig() {
		Map<String, Object> map = new HashMap<>();
		map.put(ConfigurationUtils.DF_KEYS_PROJECT_TOKEN, "valid-secret-key-123456");
		map.put(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS, Collections.singletonList(""));
		Config config = ConfigFactory.parseMap(map);

		TokenResolverUtil util = new TokenResolverUtil(config);
		assertFalse("Empty string attempt on empty config must be rejected", util.checkRegistrationAccessKey(""));
		assertFalse("Arbitrary string attempt must be rejected", util.checkRegistrationAccessKey("anything"));
	}
}

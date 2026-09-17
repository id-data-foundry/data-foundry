package utils.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base32;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import utils.conf.ConfigurationUtils;

public class TokenResolverUtilBackwardsCompatibilityTest {

	private static final String MASTER_SECRET = "production-master-secret-key-98765";
	private TokenResolverUtil tokenResolverUtil;

	private String userKey;
	private String participationKey;
	private String reviewerKey;
	private String collaboratorKey;
	private String subscriberKey;
	private String datasetKey;
	private String emailResetKey;

	@Before
	public void setUp() {
		Map<String, Object> map = new HashMap<>();
		map.put(ConfigurationUtils.DF_KEYS_PROJECT_TOKEN, MASTER_SECRET);
		map.put(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS, Collections.singletonList("test-reg-key"));
		Config config = ConfigFactory.parseMap(map);
		tokenResolverUtil = new TokenResolverUtil(config);

		userKey = MASTER_SECRET + "_user";
		participationKey = MASTER_SECRET + "_participation";
		reviewerKey = MASTER_SECRET + "_reviewer";
		collaboratorKey = MASTER_SECRET + "_collaborator";
		subscriberKey = MASTER_SECRET + "_subscriber";
		datasetKey = MASTER_SECRET + "_dataset";
		emailResetKey = MASTER_SECRET + "_email";
	}

	/**
	 * Helper that generates a legacy token exactly as DataFoundry did prior to commit b7ea8e0.
	 * (AES-ECB with Base64 key derivation, no v2: prefix).
	 */
	private String generateLegacyToken(String payload, String secretKey) throws Exception {
		byte[] keyVal = Base64.getEncoder().encode(secretKey.getBytes(StandardCharsets.UTF_8));
		Key key = new SecretKeySpec(Arrays.copyOf(keyVal, 16), "AES");
		Cipher c = Cipher.getInstance("AES");
		c.init(Cipher.ENCRYPT_MODE, key);
		byte[] enc = c.doFinal(payload.getBytes(StandardCharsets.UTF_8));
		return Base64.getEncoder().encodeToString(enc);
	}

	@Test
	public void testLegacyParticipationTokenResolution() throws Exception {
		long projectId = 42L;
		long participantId = 777L;
		String payload = projectId + ":" + participantId + ":12345678";

		// Legacy token with outer Base64 URL encoding
		String legacyEncrypted = generateLegacyToken(payload, participationKey);
		String legacyToken = tokenResolverUtil.base64Encode(legacyEncrypted);

		// Must not have v2: prefix
		assertTrue(!legacyEncrypted.startsWith("v2:"));

		// Verify resolution via TokenResolverUtil
		Long resolvedProjectId = tokenResolverUtil.getProjectIdFromParticipationToken(legacyToken);
		Long resolvedParticipantId = tokenResolverUtil.getParticipantIdFromParticipationToken(legacyToken);

		assertEquals(Long.valueOf(projectId), resolvedProjectId);
		assertEquals(Long.valueOf(participantId), resolvedParticipantId);
	}

	@Test
	public void testLegacyBase32TelegramParticipationTokenResolution() throws Exception {
		long projectId = 88L;
		long participantId = 999L;
		String payload = projectId + ":" + participantId + ":87654321";

		// Legacy Base32 token as used in Telegram bot
		String legacyEncrypted = generateLegacyToken(payload, participationKey);
		String legacyBase32Token = tokenResolverUtil.base32Encode(legacyEncrypted).toLowerCase();

		Long resolvedProjectId = tokenResolverUtil.getProjectIdFromBase32ParticipationToken(legacyBase32Token);
		assertEquals(Long.valueOf(projectId), resolvedProjectId);
	}

	@Test
	public void testLegacyDatasetTokenResolution() throws Exception {
		long datasetId = 555L;
		String payload = datasetId + ":-:98765432";

		String legacyEncrypted = generateLegacyToken(payload, datasetKey);
		String legacyToken = tokenResolverUtil.base64Encode(legacyEncrypted);

		Long resolvedDatasetId = tokenResolverUtil.getDatasetIdFromToken(legacyToken);
		assertEquals(Long.valueOf(datasetId), resolvedDatasetId);
	}

	@Test
	public void testLegacyUserAccessTokenResolution() throws Exception {
		long userId = 1024L;
		long futureTimeout = System.currentTimeMillis() + 10000000L;
		String payload = userId + ":" + futureTimeout + ":11223344";

		String legacyEncrypted = generateLegacyToken(payload, userKey);
		String legacyToken = tokenResolverUtil.base64Encode(legacyEncrypted);

		Long resolvedUserId = tokenResolverUtil.retrieveUserIdFromUserAccessToken(legacyToken);
		Long resolvedTimeout = tokenResolverUtil.retrieveTimeoutFromUserAccessToken(legacyToken);

		assertEquals(Long.valueOf(userId), resolvedUserId);
		assertEquals(Long.valueOf(futureTimeout), resolvedTimeout);
	}

	@Test
	public void testLegacyCollaborationTokenResolution() throws Exception {
		long projectId = 12L;
		String email = "researcher@tue.nl";
		String payload = projectId + ":" + email + ":55443322";

		String legacyEncrypted = generateLegacyToken(payload, collaboratorKey);
		String legacyToken = tokenResolverUtil.base64Encode(legacyEncrypted);

		Long resolvedProjectId = tokenResolverUtil.getProjectIdFromCollaborationToken(legacyToken);
		String resolvedEmail = tokenResolverUtil.getCollaboratorEmailFromCollaborationToken(legacyToken);

		assertEquals(Long.valueOf(projectId), resolvedProjectId);
		assertEquals(email, resolvedEmail);
	}

	@Test
	public void testLegacySubscriptionTokenResolution() throws Exception {
		long projectId = 34L;
		long subscriberId = 890L;
		String payload = projectId + ":" + subscriberId + ":66778899";

		String legacyEncrypted = generateLegacyToken(payload, subscriberKey);
		String legacyToken = tokenResolverUtil.base64Encode(legacyEncrypted);

		Long resolvedProjectId = tokenResolverUtil.getProjectIdFromSubscriptionToken(legacyToken);
		Long resolvedSubscriberId = tokenResolverUtil.getSubscriberIdFromSubscriptionToken(legacyToken);

		assertEquals(Long.valueOf(projectId), resolvedProjectId);
		assertEquals(Long.valueOf(subscriberId), resolvedSubscriberId);
	}

	@Test
	public void testLegacyReviewerTokenResolution() throws Exception {
		long projectId = 99L;
		long reviewerId = 321L;
		String payload = projectId + ":" + reviewerId + ":99887766";

		String legacyEncrypted = generateLegacyToken(payload, reviewerKey);
		String legacyToken = tokenResolverUtil.base64Encode(legacyEncrypted);

		Long resolvedProjectId = tokenResolverUtil.getProjectIdFromReviewToken(legacyToken);
		Long resolvedReviewerId = tokenResolverUtil.getReviewerIdFromReviewToken(legacyToken);

		assertEquals(Long.valueOf(projectId), resolvedProjectId);
		assertEquals(Long.valueOf(reviewerId), resolvedReviewerId);
	}

	@Test
	public void testLegacyEmailResetTokenResolution() throws Exception {
		String username = "student@tue.nl";
		String legacyEncrypted = generateLegacyToken(username, emailResetKey);
		String legacyToken = tokenResolverUtil.base64Encode(legacyEncrypted);

		String resolvedUsername = tokenResolverUtil.retrieveUsernameFromEmailResetToken(legacyToken);
		assertEquals(username, resolvedUsername);
	}

	@Test
	public void testV2TokensInteroperability() {
		// Verify that newly generated v2 tokens resolve identically alongside legacy tokens
		long projectId = 100L;
		long participantId = 200L;
		String v2ParticipationToken = tokenResolverUtil.getParticipationToken(projectId, participantId);
		assertNotNull(v2ParticipationToken);

		assertEquals(Long.valueOf(projectId), tokenResolverUtil.getProjectIdFromParticipationToken(v2ParticipationToken));
		assertEquals(Long.valueOf(participantId), tokenResolverUtil.getParticipantIdFromParticipationToken(v2ParticipationToken));

		long datasetId = 300L;
		String v2DatasetToken = tokenResolverUtil.getDatasetToken(datasetId);
		assertEquals(Long.valueOf(datasetId), tokenResolverUtil.getDatasetIdFromToken(v2DatasetToken));

		long userId = 400L;
		long timeout = System.currentTimeMillis() + 5000000L;
		String v2UserToken = tokenResolverUtil.createUserAccessToken(userId, timeout);
		assertEquals(Long.valueOf(userId), tokenResolverUtil.retrieveUserIdFromUserAccessToken(v2UserToken));
		assertEquals(Long.valueOf(timeout), tokenResolverUtil.retrieveTimeoutFromUserAccessToken(v2UserToken));
	}
}

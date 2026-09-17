package utils.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URLDecoder;
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

	@Test
	public void testNewTokensUrlSafety() {
		long projectId = 123L;
		long participantId = 456L;
		long datasetId = 789L;
		long userId = 101L;
		long timeout = System.currentTimeMillis() + 1000000L;

		// 1. Participation Token
		String participationToken = tokenResolverUtil.getParticipationToken(projectId, participantId);
		assertUrlSafeToken(participationToken);

		// 2. Stable Participation Token
		String stableToken = tokenResolverUtil.getStableParticipationToken(projectId, participantId);
		assertUrlSafeToken(stableToken);

		// 3. Dataset Token (used in public CSV download URLs and API headers)
		String datasetToken = tokenResolverUtil.getDatasetToken(datasetId);
		assertUrlSafeToken(datasetToken);

		// 4. User Access Token
		String userToken = tokenResolverUtil.createUserAccessToken(userId, timeout);
		assertUrlSafeToken(userToken);

		// 5. Collaboration Invite Token (used in /projects/:id/collaborate/:token)
		String collabToken = tokenResolverUtil.getCollaborationToken(projectId, "researcher@example.com");
		assertUrlSafeToken(collabToken);

		// 6. Subscription Confirmation Token (used in /projects/:id/subscribe/:token)
		String subToken = tokenResolverUtil.getSubscriptionToken(projectId, 555L);
		assertUrlSafeToken(subToken);

		// 7. Reviewer Token (used in /review/:token)
		String reviewToken = tokenResolverUtil.getReviewToken(projectId, 666L);
		assertUrlSafeToken(reviewToken);

		// 8. Email Reset Token (used in /reset-password/:token)
		String resetToken = tokenResolverUtil.createEmailResetToken("user@example.com");
		assertUrlSafeToken(resetToken);

		// 9. Base32 Telegram Token (used in Telegram bot /start <token>)
		String base32Token = tokenResolverUtil.getBase32ParticipationToken(projectId, participantId);
		assertTrue("Base32 token must only contain a-z, 2-7, and =",
				base32Token.matches("^[a-z2-7=]+$"));
		URI telegramUri = URI.create("https://t.me/DataFoundryBot?start=" + base32Token);
		assertEquals("start=" + base32Token, telegramUri.getRawQuery());

		// Verify end-to-end token resolution after URL round-trip
		// A. Participation token transmitted in URL path
		String pPath = URI.create("https://data.id.tue.nl/participation/" + participationToken).getRawPath();
		String extractedPToken = pPath.substring("/participation/".length());
		assertEquals(Long.valueOf(projectId), tokenResolverUtil.getProjectIdFromParticipationToken(extractedPToken));
		assertEquals(Long.valueOf(participantId), tokenResolverUtil.getParticipantIdFromParticipationToken(extractedPToken));

		// B. Participation token with trailing '=' stripped (testing resilience against aggressive URL parsers)
		String unpaddedPToken = participationToken.replaceAll("=+$", "");
		assertEquals(Long.valueOf(projectId), tokenResolverUtil.getProjectIdFromParticipationToken(unpaddedPToken));
		assertEquals(Long.valueOf(participantId), tokenResolverUtil.getParticipantIdFromParticipationToken(unpaddedPToken));

		// C. Dataset token transmitted in public CSV download URL path
		String dsPath = URI.create("https://data.id.tue.nl/datasets/downloadPublic/" + datasetToken).getRawPath();
		String extractedDsToken = dsPath.substring("/datasets/downloadPublic/".length());
		assertEquals(Long.valueOf(datasetId), tokenResolverUtil.getDatasetIdFromToken(extractedDsToken));

		// D. User access token transmitted in query parameter
		String uQuery = URI.create("https://data.id.tue.nl/api/v2/data?token=" + userToken).getRawQuery();
		String extractedUToken = uQuery.substring("token=".length());
		assertEquals(Long.valueOf(userId), tokenResolverUtil.retrieveUserIdFromUserAccessToken(extractedUToken));
		assertEquals(Long.valueOf(timeout), tokenResolverUtil.retrieveTimeoutFromUserAccessToken(extractedUToken));

		// E. Collaboration token transmitted in URL path
		String collabPath = URI.create("https://data.id.tue.nl/projects/123/collaborate/" + collabToken).getRawPath();
		String extractedCollabToken = collabPath.substring("/projects/123/collaborate/".length());
		assertEquals(Long.valueOf(projectId), tokenResolverUtil.getProjectIdFromCollaborationToken(extractedCollabToken));
		assertEquals("researcher@example.com", tokenResolverUtil.getCollaboratorEmailFromCollaborationToken(extractedCollabToken));

		// F. Subscription token transmitted in URL path
		String subPath = URI.create("https://data.id.tue.nl/projects/123/subscribe/" + subToken).getRawPath();
		String extractedSubToken = subPath.substring("/projects/123/subscribe/".length());
		assertEquals(Long.valueOf(projectId), tokenResolverUtil.getProjectIdFromSubscriptionToken(extractedSubToken));
		assertEquals(Long.valueOf(555L), tokenResolverUtil.getSubscriberIdFromSubscriptionToken(extractedSubToken));

		// G. Review token transmitted in URL path
		String reviewPath = URI.create("https://data.id.tue.nl/review/" + reviewToken).getRawPath();
		String extractedReviewToken = reviewPath.substring("/review/".length());
		assertEquals(Long.valueOf(projectId), tokenResolverUtil.getProjectIdFromReviewToken(extractedReviewToken));
		assertEquals(Long.valueOf(666L), tokenResolverUtil.getReviewerIdFromReviewToken(extractedReviewToken));

		// H. Email reset token transmitted in URL path
		String resetPath = URI.create("https://data.id.tue.nl/users/resetPW/" + resetToken).getRawPath();
		String extractedResetToken = resetPath.substring("/users/resetPW/".length());
		assertEquals("user@example.com", tokenResolverUtil.retrieveUsernameFromEmailResetToken(extractedResetToken));

		// I. Base32 token transmitted in query parameter
		String b32Query = telegramUri.getRawQuery();
		String extractedB32Token = b32Query.substring("start=".length());
		assertEquals(Long.valueOf(projectId), tokenResolverUtil.getProjectIdFromBase32ParticipationToken(extractedB32Token));
	}

	private void assertUrlSafeToken(String token) {
		assertNotNull("Token must not be null", token);
		assertTrue("Token must not be empty", !token.isEmpty());

		// 1. Verify token characters strictly conform to RFC 3986 URL path segment and query string safety:
		// No whitespace, no slashes, no backslashes, no question marks, no hash, no ampersand, no colons, no plus
		assertTrue("Token must not contain spaces", !token.contains(" "));
		assertTrue("Token must not contain forward slashes (/)", !token.contains("/"));
		assertTrue("Token must not contain backslashes (\\)", !token.contains("\\"));
		assertTrue("Token must not contain question marks (?)", !token.contains("?"));
		assertTrue("Token must not contain hash (#)", !token.contains("#"));
		assertTrue("Token must not contain ampersand (&)", !token.contains("&"));
		assertTrue("Token must not contain colons (:)", !token.contains(":"));
		assertTrue("Token must not contain plus (+)", !token.contains("+"));

		// Must match Base64URL characters [A-Za-z0-9_-] and optional '=' padding
		assertTrue("Token must match Base64URL character set: " + token,
				token.matches("^[A-Za-z0-9_=-]+$"));

		// 2. Verify URI path creation and round-trip parsing
		URI pathUri = URI.create("https://data.id.tue.nl/participation/" + token);
		assertEquals("URI path must match exactly without escaping",
				"/participation/" + token, pathUri.getRawPath());

		// Verify that token forms a single clean path segment without introducing sub-paths
		String[] segments = pathUri.getPath().split("/");
		assertEquals("Token must be a single path segment", token, segments[segments.length - 1]);

		// 3. Verify URI query parameter creation and round-trip parsing
		URI queryUri = URI.create("https://data.id.tue.nl/api/v2/data?token=" + token);
		assertEquals("URI query must match exactly", "token=" + token, queryUri.getRawQuery());

		// 4. Verify URL-decoding invariance (URL-decoding must not alter the token or convert '+' to ' ')
		String decodedToken = URLDecoder.decode(token, StandardCharsets.UTF_8);
		assertEquals("Token must be unaffected by standard application/x-www-form-urlencoded decoding",
				token, decodedToken);
	}
}

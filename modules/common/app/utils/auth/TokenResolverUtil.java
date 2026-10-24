package utils.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import org.apache.commons.codec.binary.Base32;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import play.Logger;
import utils.conf.ConfigurationUtils;

@Singleton
public class TokenResolverUtil {

	private static final Logger.ALogger logger = Logger.of(TokenResolverUtil.class);

	private final String USER_TOKEN_KEY;
	private final String PROJECT_PARTICIPATION_TOKEN_KEY;
	private final String PROJECT_REVIEWER_TOKEN_KEY;
	private final String PROJECT_COLLABORATOR_TOKEN_KEY;
	private final String PROJECT_SUBSCRIBER_TOKEN_KEY;
	private final String PROJECT_DATASET_TOKEN_KEY;
	private final String PROJECT_VIEWER_TOKEN_KEY;
	private final String EMAIL_RESET_SEC_KEY;
	private final String USER_SIGN_UP_KEY;

	public final List<String> REGISTRATION_ACCESS_KEY;

	@Inject
	public TokenResolverUtil(Config config) {

		// symmetric encryption key from configuration
		String tokenKey = "";
		if (config.hasPath(ConfigurationUtils.DF_KEYS_PROJECT_TOKEN)) {
			tokenKey = config.getString(ConfigurationUtils.DF_KEYS_PROJECT_TOKEN).trim();
		}

		// fallback to play.http.secret.key to keep existing running instances operational
		if (tokenKey.isEmpty()) {
			if (config.hasPath("play.http.secret.key")) {
				String playSecret = config.getString("play.http.secret.key").trim();
				if (!playSecret.isEmpty()) {
					tokenKey = com.google.common.hash.Hashing.sha256()
							.hashString(playSecret, StandardCharsets.UTF_8).toString().substring(0, 32);
					logger.warn("'" + ConfigurationUtils.DF_KEYS_PROJECT_TOKEN
							+ "' is empty or not configured. Falling back to derived key from 'play.http.secret.key' to keep operational.");
				}
			}
		}

		if (!tokenKey.isEmpty()) {
			USER_TOKEN_KEY = tokenKey + "_user";
			PROJECT_PARTICIPATION_TOKEN_KEY = tokenKey + "_participation";
			PROJECT_REVIEWER_TOKEN_KEY = tokenKey + "_reviewer";
			PROJECT_COLLABORATOR_TOKEN_KEY = tokenKey + "_collaborator";
			PROJECT_SUBSCRIBER_TOKEN_KEY = tokenKey + "_subscriber";
			PROJECT_DATASET_TOKEN_KEY = tokenKey + "_dataset";
			PROJECT_VIEWER_TOKEN_KEY = tokenKey + "_viewer";
			EMAIL_RESET_SEC_KEY = tokenKey + "_email";
			USER_SIGN_UP_KEY = tokenKey + "_register";
		} else {
			USER_TOKEN_KEY = null;
			PROJECT_PARTICIPATION_TOKEN_KEY = null;
			PROJECT_REVIEWER_TOKEN_KEY = null;
			PROJECT_COLLABORATOR_TOKEN_KEY = null;
			PROJECT_SUBSCRIBER_TOKEN_KEY = null;
			PROJECT_DATASET_TOKEN_KEY = null;
			PROJECT_VIEWER_TOKEN_KEY = null;
			EMAIL_RESET_SEC_KEY = null;
			USER_SIGN_UP_KEY = null;
			logger.error("'" + ConfigurationUtils.DF_KEYS_PROJECT_TOKEN + "' is not defined and no 'play.http.secret.key' fallback found.");
			System.exit(1);
		}

		// registration access key from configuration
		if (config.hasPath(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS)) {
			REGISTRATION_ACCESS_KEY = config.getStringList(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS);
		} else {
			REGISTRATION_ACCESS_KEY = null;
			logger.error("'" + ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS + "' is not defined in configuration.");
		}

		boolean ssoActive = ConfigurationUtils.isSSO(config);
		boolean hasValidRegistrationKey = REGISTRATION_ACCESS_KEY != null
				&& REGISTRATION_ACCESS_KEY.stream().anyMatch(k -> k != null && !k.trim().isEmpty());

		if (!ssoActive && !hasValidRegistrationKey) {
			logger.warn("SECURITY WARNING: SSO is not active and no non-empty registration access key is configured in '"
					+ ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS + "'. Registration may be open to anyone!");
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * simple test whether the access key is included in the configuration-based registration access key string list
	 * 
	 * @param accessKey
	 * @return
	 */
	public boolean checkRegistrationAccessKey(String accessKey) {
		if (REGISTRATION_ACCESS_KEY == null || accessKey == null || accessKey.trim().isEmpty()) {
			return false;
		}

		return REGISTRATION_ACCESS_KEY.stream()
				.filter(k -> k != null && !k.trim().isEmpty())
				.anyMatch(k -> k.equals(accessKey));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get raw contents of decrypted collaboration token
	 * 
	 * @param token
	 * @return
	 */
	public String getRawCollabToken(String token) {
		String res = SymEncryption.decryptToken(token, PROJECT_COLLABORATOR_TOKEN_KEY);
		if (res == null && token != null) {
			res = SymEncryption.decryptToken(normalizeToken(token), PROJECT_COLLABORATOR_TOKEN_KEY);
		}
		if (res == null && PROJECT_COLLABORATOR_TOKEN_KEY != null && PROJECT_COLLABORATOR_TOKEN_KEY.contains("_")) {
			String legacySuffix = PROJECT_COLLABORATOR_TOKEN_KEY.substring(PROJECT_COLLABORATOR_TOKEN_KEY.indexOf('_'));
			if (!legacySuffix.equals(PROJECT_COLLABORATOR_TOKEN_KEY)) {
				res = SymEncryption.decryptToken(token, legacySuffix);
				if (res == null && token != null) {
					res = SymEncryption.decryptToken(normalizeToken(token), legacySuffix);
				}
			}
		}
		return res;
	}

	/**
	 * get raw contents of decrypted subscription token
	 * 
	 * @param token
	 * @return
	 */
	public String getRawSubscriptionToken(String token) {
		String res = SymEncryption.decryptToken(token, PROJECT_SUBSCRIBER_TOKEN_KEY);
		if (res == null && token != null) {
			res = SymEncryption.decryptToken(normalizeToken(token), PROJECT_SUBSCRIBER_TOKEN_KEY);
		}
		if (res == null && PROJECT_SUBSCRIBER_TOKEN_KEY != null && PROJECT_SUBSCRIBER_TOKEN_KEY.contains("_")) {
			String legacySuffix = PROJECT_SUBSCRIBER_TOKEN_KEY.substring(PROJECT_SUBSCRIBER_TOKEN_KEY.indexOf('_'));
			if (!legacySuffix.equals(PROJECT_SUBSCRIBER_TOKEN_KEY)) {
				res = SymEncryption.decryptToken(token, legacySuffix);
				if (res == null && token != null) {
					res = SymEncryption.decryptToken(normalizeToken(token), legacySuffix);
				}
			}
		}
		return res;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static final long EMAIL_RESET_TOKEN_VALIDITY_MS = 2 * 3600 * 1000L;

	/**
	 * create a Base64 encoded token of a username, for password resets
	 * 
	 * @param username
	 * @return
	 */
	public String createEmailResetToken(String username) {
		long expiry = System.currentTimeMillis() + EMAIL_RESET_TOKEN_VALIDITY_MS;
		String nonce = java.util.UUID.randomUUID().toString();
		return base64Encode(SymEncryption.encryptToken(username + ":" + expiry + ":" + nonce, EMAIL_RESET_SEC_KEY));
	}

	/**
	 * retrieve username from Base64 encoded token, for password reset
	 * 
	 * @param token
	 * @return
	 */
	public String retrieveUsernameFromEmailResetToken(String token) {
		String decrypted = SymEncryption.decryptToken(base64Decode(token), EMAIL_RESET_SEC_KEY);
		if (decrypted == null) {
			return null;
		}

		if (decrypted.contains(":")) {
			String[] parts = decrypted.split(":");
			if (parts.length >= 2) {
				try {
					long expiry = Long.parseLong(parts[1]);
					if (System.currentTimeMillis() > expiry) {
						logger.warn("Password reset token for user " + parts[0] + " has expired.");
						return null;
					}
					return parts[0];
				} catch (NumberFormatException e) {
					return parts[0];
				}
			}
		}

		return decrypted;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * create user registration token with a timeout
	 * 
	 * @param future point in time (in ms) when this user registration token expires
	 * @return
	 */
	public String createUserRegistrationToken(Long timeout) {
		return createToken(0L, timeout, USER_SIGN_UP_KEY);
	}

	/**
	 * check timeout (long) from user registration, time in ms
	 * 
	 * @param token
	 * @return
	 */
	public boolean checkTimeoutFromUserRegistrationToken(String token) {
		long timeout = retrieveSecondIdFromToken(token, USER_SIGN_UP_KEY);
		return System.currentTimeMillis() < timeout;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * create collaboration token for project id and collaborator id
	 * 
	 * @param userId user id
	 * @param future point in time (in ms) when this user access token expires
	 * @return
	 */
	public String createUserAccessToken(Long userId, Long timeout) {
		return createToken(userId, timeout, USER_TOKEN_KEY);
	}

	/**
	 * get user id from user access token
	 * 
	 * @param token
	 * @return
	 */
	public Long retrieveUserIdFromUserAccessToken(String token) {
		return retrieveFirstIdFromToken(token, USER_TOKEN_KEY);
	}

	/**
	 * get timeout (long) from user access, time in ms
	 * 
	 * @param token
	 * @return
	 */
	public Long retrieveTimeoutFromUserAccessToken(String token) {
		return retrieveSecondIdFromToken(token, USER_TOKEN_KEY);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get collaboration token for project id and collaborator id
	 * 
	 * @param projectId project id
	 * @param did       collaborator id
	 * @return
	 */
	public String getCollaborationToken(Long projectId, String collaboratorId) {
		return getToken(projectId, collaboratorId, PROJECT_COLLABORATOR_TOKEN_KEY);
	}

	/**
	 * get project id from collaboration token
	 * 
	 * @param token
	 * @return
	 */
	public Long getProjectIdFromCollaborationToken(String token) {
		return retrieveFirstIdFromToken(token, PROJECT_COLLABORATOR_TOKEN_KEY);
	}

	/**
	 * get string from collaboration token
	 * 
	 * @param token
	 * @return
	 */
	public String getCollaboratorEmailFromCollaborationToken(String token) {
		return getSecondStringFromToken(token, PROJECT_COLLABORATOR_TOKEN_KEY);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get subscription token for project id and subscriber id
	 * 
	 * @param projectId    project id
	 * @param subscriberId subscriber id
	 * @return
	 */
	public String getSubscriptionToken(Long projectId, Long subscriberId) {
		return createToken(projectId, subscriberId, PROJECT_SUBSCRIBER_TOKEN_KEY);
	}

	/**
	 * get project id from subscription token
	 * 
	 * @param token
	 * @return
	 */
	public Long getProjectIdFromSubscriptionToken(String token) {
		return retrieveFirstIdFromToken(token, PROJECT_SUBSCRIBER_TOKEN_KEY);
	}

	/**
	 * get subscriber id from subscription token
	 * 
	 * @param token
	 * @return
	 */
	public Long getSubscriberIdFromSubscriptionToken(String token) {
		return retrieveSecondIdFromToken(token, PROJECT_SUBSCRIBER_TOKEN_KEY);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get participation token for project id and participant id
	 * 
	 * @param projectId     project id
	 * @param participantId participant id
	 * @return
	 */
	public String getParticipationToken(Long projectId, Long participantId) {
		return createToken(projectId, participantId, PROJECT_PARTICIPATION_TOKEN_KEY);
	}

	/**
	 * get participation token for project id and participant id
	 * 
	 * @param projectId     project id
	 * @param participantId participant id
	 * @return
	 */
	public String getBase32ParticipationToken(Long projectId, Long participantId) {
		return createBase32Token(projectId, participantId, PROJECT_PARTICIPATION_TOKEN_KEY).toLowerCase();
	}

	/**
	 * get participation token for project id and participant id
	 * 
	 * @param projectId     project id
	 * @param participantId participant id
	 * @return
	 */
	public String getStableParticipationToken(Long projectId, Long participantId) {
		return createStableToken(projectId, participantId, PROJECT_PARTICIPATION_TOKEN_KEY);
	}

	/**
	 * get project id from participation token
	 * 
	 * @param token
	 * @return
	 */
	public Long getProjectIdFromParticipationToken(String token) {
		return retrieveFirstIdFromToken(token, PROJECT_PARTICIPATION_TOKEN_KEY);
	}

	/**
	 * get project id from base32 participation token
	 * 
	 * @param token
	 * @return
	 */
	public Long getProjectIdFromBase32ParticipationToken(String token) {
		return retrieveFirstIdFromBase32Token(token.toUpperCase(), PROJECT_PARTICIPATION_TOKEN_KEY);
	}

	/**
	 * get participant id from participation token
	 * 
	 * @param token
	 * @return
	 */
	public Long getParticipantIdFromParticipationToken(String token) {
		return retrieveSecondIdFromToken(token, PROJECT_PARTICIPATION_TOKEN_KEY);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get review token for project id and reviewer id
	 * 
	 * @param projectId  project id
	 * @param reviewerId reviewer id
	 * @return
	 */
	public String getReviewToken(Long projectId, Long reviewerId) {
		return createToken(projectId, reviewerId, PROJECT_REVIEWER_TOKEN_KEY);
	}

	/**
	 * get project id from review token
	 * 
	 * @param token
	 * @return
	 */
	public Long getProjectIdFromReviewToken(String token) {
		return retrieveFirstIdFromToken(token, PROJECT_REVIEWER_TOKEN_KEY);
	}

	/**
	 * get reviewer id from review token
	 * 
	 * @param token
	 * @return
	 */
	public Long getReviewerIdFromReviewToken(String token) {
		return retrieveSecondIdFromToken(token, PROJECT_REVIEWER_TOKEN_KEY);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * create dataset token
	 * 
	 * @param datasetId
	 * @return
	 */
	public String getDatasetToken(Long datasetId) {
		return getToken(datasetId, "-", PROJECT_DATASET_TOKEN_KEY);
	}

	/**
	 * get dataset id from dataset token
	 * 
	 * @param token
	 * @return
	 */
	public Long getDatasetIdFromToken(String token) {
		return retrieveFirstIdFromToken(token, PROJECT_DATASET_TOKEN_KEY);
	}

	public static final long VIEWER_TOKEN_VALIDITY_MS = 2 * 60 * 60 * 1000L; // 2 hours

	public static class ViewerTokenInfo {
		private final Long datasetId;
		private final long expiry;
		private final boolean expired;

		public ViewerTokenInfo(Long datasetId, long expiry, boolean expired) {
			this.datasetId = datasetId;
			this.expiry = expiry;
			this.expired = expired;
		}

		public Long getDatasetId() {
			return datasetId;
		}

		public long getExpiry() {
			return expiry;
		}

		public boolean isExpired() {
			return expired;
		}
	}

	/**
	 * create an ephemeral viewer token for dataset access with an expiry timestamp
	 * 
	 * @param datasetId
	 * @param expiryMs
	 * @return
	 */
	public String createViewerToken(Long datasetId, long expiryMs) {
		String nonce = java.util.UUID.randomUUID().toString().substring(0, 8);
		String payload = datasetId + ":" + expiryMs + ":" + nonce;
		return base64Encode(SymEncryption.encryptToken(payload, PROJECT_VIEWER_TOKEN_KEY));
	}

	/**
	 * resolve an ephemeral viewer token, returning dataset id and expiration state
	 * 
	 * @param token
	 * @return ViewerTokenInfo or null if token is invalid/corrupted
	 */
	public ViewerTokenInfo resolveViewerToken(String token) {
		if (token == null || token.isEmpty()) {
			return null;
		}
		String decrypted = SymEncryption.decryptToken(base64Decode(token), PROJECT_VIEWER_TOKEN_KEY);
		if (decrypted == null || !decrypted.contains(":")) {
			return null;
		}
		String[] parts = decrypted.split(":");
		if (parts.length < 2) {
			return null;
		}
		try {
			Long datasetId = Long.parseLong(parts[0]);
			long expiry = Long.parseLong(parts[1]);
			boolean expired = System.currentTimeMillis() > expiry;
			return new ViewerTokenInfo(datasetId, expiry, expired);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * create a token that is base64 url encoded for use in URLs
	 * 
	 * @param firstId
	 * @param secondId
	 * @param key
	 * @return
	 */
	private String createToken(Long firstId, Long secondId, String key) {
		return base64Encode(SymEncryption.getToken(firstId, secondId, key));
	}

	/**
	 * create a token that is base32 encoded for use with Telegram
	 * 
	 * @param firstId
	 * @param secondId
	 * @param key
	 * @return
	 */
	private String createBase32Token(Long firstId, Long secondId, String key) {
		return base32Encode(SymEncryption.getToken(firstId, secondId, key));
	}

	/**
	 * create a token that is base64 url encoded for use in URLs
	 * 
	 * @param firstId
	 * @param secondId
	 * @param key
	 * @return
	 */
	private String createStableToken(Long firstId, Long secondId, String key) {
		return base64Encode(SymEncryption.getStableToken(firstId, secondId, key));
	}

	/**
	 * create a token that is base64 url encoded for use in URLs
	 * 
	 * @param firstId
	 * @param secondId
	 * @param key
	 * @return
	 */
	private String getToken(Long firstId, String secondStr, String key) {
		return base64Encode(SymEncryption.getToken(firstId, secondStr, key));
	}

	/**
	 * retrieve first id part of a token that is base64 url encoded
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	private Long retrieveFirstIdFromToken(String token, String key) {
		if (token == null || token.trim().isEmpty()) {
			return -1L;
		}
		Long id = tryRetrieveFirstId(token, key);
		if (id != -1L) {
			return id;
		}
		String normalized = normalizeToken(token);
		if (normalized != null && !normalized.equals(token)) {
			id = tryRetrieveFirstId(normalized, key);
			if (id != -1L) {
				return id;
			}
		}
		// Fallback to legacy default key suffix (e.g., "_dataset")
		if (key != null && key.contains("_")) {
			String legacySuffixKey = key.substring(key.indexOf('_'));
			if (!legacySuffixKey.equals(key)) {
				id = tryRetrieveFirstId(token, legacySuffixKey);
				if (id != -1L) {
					return id;
				}
				if (normalized != null && !normalized.equals(token)) {
					id = tryRetrieveFirstId(normalized, legacySuffixKey);
					if (id != -1L) {
						return id;
					}
				}
			}
		}
		return -1L;
	}

	private Long tryRetrieveFirstId(String token, String key) {
		if (token == null || token.trim().isEmpty()) {
			return -1L;
		}
		// try with decode first (handles double-encoded tokens)
		String decoded = base64Decode(token);
		Long firstId = -1L;
		if (decoded != null && !decoded.equals("-- invalid token --")) {
			firstId = SymEncryption.getFirstIdFromToken(decoded, key);
		}
		if (firstId == -1L) {
			// if no result, try token directly
			firstId = SymEncryption.getFirstIdFromToken(token, key);
		}
		return firstId;
	}

	/**
	 * retrieve first id part of a token that is base32 encoded
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	private Long retrieveFirstIdFromBase32Token(String token, String key) {
		Long id = tryRetrieveFirstIdFromBase32(token, key);
		if (id != -1L) {
			return id;
		}
		if (key != null && key.contains("_")) {
			String legacySuffixKey = key.substring(key.indexOf('_'));
			if (!legacySuffixKey.equals(key)) {
				id = tryRetrieveFirstIdFromBase32(token, legacySuffixKey);
				if (id != -1L) {
					return id;
				}
			}
		}
		return -1L;
	}

	private Long tryRetrieveFirstIdFromBase32(String token, String key) {
		// try new token scheme with decode first
		Long firstIdFromToken = SymEncryption.getFirstIdFromToken(base32Decode(token), key);
		if (firstIdFromToken == -1l) {
			// if no result, try old scheme without decode
			firstIdFromToken = SymEncryption.getFirstIdFromToken(token, key);
		}
		return firstIdFromToken;
	}

	/**
	 * retrieve second id part of a token that is base64 url encoded
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	private Long retrieveSecondIdFromToken(String token, String key) {
		if (token == null || token.trim().isEmpty()) {
			return -1L;
		}
		Long id = tryRetrieveSecondId(token, key);
		if (id != -1L) {
			return id;
		}
		String normalized = normalizeToken(token);
		if (normalized != null && !normalized.equals(token)) {
			id = tryRetrieveSecondId(normalized, key);
			if (id != -1L) {
				return id;
			}
		}
		// Fallback to legacy default key suffix
		if (key != null && key.contains("_")) {
			String legacySuffixKey = key.substring(key.indexOf('_'));
			if (!legacySuffixKey.equals(key)) {
				id = tryRetrieveSecondId(token, legacySuffixKey);
				if (id != -1L) {
					return id;
				}
				if (normalized != null && !normalized.equals(token)) {
					id = tryRetrieveSecondId(normalized, legacySuffixKey);
					if (id != -1L) {
						return id;
					}
				}
			}
		}
		return -1L;
	}

	private Long tryRetrieveSecondId(String token, String key) {
		if (token == null || token.trim().isEmpty()) {
			return -1L;
		}
		String decoded = base64Decode(token);
		Long secondId = -1L;
		if (decoded != null && !decoded.equals("-- invalid token --")) {
			secondId = SymEncryption.getSecondIdFromToken(decoded, key);
		}
		if (secondId == -1L) {
			secondId = SymEncryption.getSecondIdFromToken(token, key);
		}
		return secondId;
	}

	/**
	 * retrieve second string part of a token that is base64 url encoded
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	private String getSecondStringFromToken(String token, String key) {
		if (token == null || token.trim().isEmpty()) {
			return null;
		}
		String val = tryGetSecondString(token, key);
		if (val != null) {
			return val;
		}
		String normalized = normalizeToken(token);
		if (normalized != null && !normalized.equals(token)) {
			val = tryGetSecondString(normalized, key);
			if (val != null) {
				return val;
			}
		}
		// Fallback to legacy default key suffix
		if (key != null && key.contains("_")) {
			String legacySuffixKey = key.substring(key.indexOf('_'));
			if (!legacySuffixKey.equals(key)) {
				val = tryGetSecondString(token, legacySuffixKey);
				if (val != null) {
					return val;
				}
				if (normalized != null && !normalized.equals(token)) {
					val = tryGetSecondString(normalized, legacySuffixKey);
					if (val != null) {
						return val;
					}
				}
			}
		}
		return null;
	}

	private String tryGetSecondString(String token, String key) {
		if (token == null || token.trim().isEmpty()) {
			return null;
		}
		String decoded = base64Decode(token);
		String str = null;
		if (decoded != null && !decoded.equals("-- invalid token --")) {
			str = SymEncryption.getSecondStringFromToken(decoded, key);
		}
		if (str == null) {
			str = SymEncryption.getSecondStringFromToken(token, key);
		}
		return str;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * base32 encoder helper
	 * 
	 * @param str
	 * @return
	 */
	public String base32Encode(String str) {
		return new Base32().encodeToString(str.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * base32 decoder helper
	 * 
	 * @param str
	 * @return
	 */
	public String base32Decode(String str) {
		return new String(new Base32().decode(str), StandardCharsets.UTF_8);
	}

	/**
	 * base64 url encoder helper
	 * 
	 * @param str
	 * @return
	 */
	public String base64Encode(String str) {
		return Base64.getUrlEncoder().encodeToString(str.getBytes());
	}

	/**
	 * base64 url decoder helper
	 * 
	 * @param str
	 * @return
	 */
	public String base64Decode(String str) {
		if (str == null) {
			return "-- invalid token --";
		}
		String s = normalizeToken(str);
		try {
			return new String(Base64.getUrlDecoder().decode(s), StandardCharsets.UTF_8);
		} catch (IllegalArgumentException e) {
			try {
				return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
			} catch (IllegalArgumentException e2) {
				return "-- invalid token --";
			}
		}
	}

	/**
	 * Normalize token by URL-decoding percent-encoded characters and restoring spaces to '+'.
	 * 
	 * @param token
	 * @return
	 */
	public String normalizeToken(String token) {
		if (token == null) {
			return null;
		}
		String normalized = token.trim();
		if (normalized.contains("%")) {
			try {
				normalized = java.net.URLDecoder.decode(normalized, StandardCharsets.UTF_8);
			} catch (Exception ignored) {
			}
		}
		if (normalized.contains(" ")) {
			normalized = normalized.replace(' ', '+');
		}
		return normalized;
	}

	/**
	 * Timing-safe token matching comparing candidate against configured token,
	 * accounting for URL percent-encoding, space-plus conversions, and single vs double base64 encoding.
	 * 
	 * @param candidate
	 * @param configured
	 * @return true if candidate matches configured token
	 */
	public boolean isTokenMatch(String candidate, String configured) {
		if (candidate == null || configured == null) {
			return false;
		}
		String cand = candidate.trim();
		String conf = configured.trim();
		if (cand.isEmpty() || conf.isEmpty()) {
			return false;
		}

		if (Hash.constantTimeEquals(cand, conf)) {
			return true;
		}

		String normCand = normalizeToken(cand);
		String normConf = normalizeToken(conf);
		if (Hash.constantTimeEquals(normCand, conf) || Hash.constantTimeEquals(cand, normConf)
				|| Hash.constantTimeEquals(normCand, normConf)) {
			return true;
		}

		// Check Base64 decoded forms
		String decCand = base64Decode(cand);
		if (!"-- invalid token --".equals(decCand)) {
			if (Hash.constantTimeEquals(decCand, conf) || Hash.constantTimeEquals(decCand, normConf)) {
				return true;
			}
		}
		String decNormCand = base64Decode(normCand);
		if (!"-- invalid token --".equals(decNormCand)) {
			if (Hash.constantTimeEquals(decNormCand, conf) || Hash.constantTimeEquals(decNormCand, normConf)) {
				return true;
			}
		}

		String decConf = base64Decode(conf);
		if (!"-- invalid token --".equals(decConf)) {
			if (Hash.constantTimeEquals(cand, decConf) || Hash.constantTimeEquals(normCand, decConf)) {
				return true;
			}
		}
		String decNormConf = base64Decode(normConf);
		if (!"-- invalid token --".equals(decNormConf)) {
			if (Hash.constantTimeEquals(cand, decNormConf) || Hash.constantTimeEquals(normCand, decNormConf)) {
				return true;
			}
		}

		// Check Base64 encoded forms
		String encCand = base64Encode(cand);
		if (Hash.constantTimeEquals(encCand, conf) || Hash.constantTimeEquals(encCand, normConf)) {
			return true;
		}
		String encNormCand = base64Encode(normCand);
		if (Hash.constantTimeEquals(encNormCand, conf) || Hash.constantTimeEquals(encNormCand, normConf)) {
			return true;
		}

		return false;
	}

}

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
	private final String EMAIL_RESET_SEC_KEY;
	private final String USER_SIGN_UP_KEY;

	public final List<String> REGISTRATION_ACCESS_KEY;

	@Inject
	public TokenResolverUtil(Config config) {

		// symmetric encryption key from configuration
		if (config.hasPath(ConfigurationUtils.DF_KEYS_PROJECT_TOKEN)) {
			final String tokenKey = config.getString(ConfigurationUtils.DF_KEYS_PROJECT_TOKEN);
			USER_TOKEN_KEY = tokenKey + "_user";
			PROJECT_PARTICIPATION_TOKEN_KEY = tokenKey + "_participation";
			PROJECT_REVIEWER_TOKEN_KEY = tokenKey + "_reviewer";
			PROJECT_COLLABORATOR_TOKEN_KEY = tokenKey + "_collaborator";
			PROJECT_SUBSCRIBER_TOKEN_KEY = tokenKey + "_subscriber";
			PROJECT_DATASET_TOKEN_KEY = tokenKey + "_dataset";
			EMAIL_RESET_SEC_KEY = tokenKey + "_email";
			USER_SIGN_UP_KEY = tokenKey + "_register";
		} else {
			USER_TOKEN_KEY = null;
			PROJECT_PARTICIPATION_TOKEN_KEY = null;
			PROJECT_REVIEWER_TOKEN_KEY = null;
			PROJECT_COLLABORATOR_TOKEN_KEY = null;
			PROJECT_SUBSCRIBER_TOKEN_KEY = null;
			PROJECT_DATASET_TOKEN_KEY = null;
			EMAIL_RESET_SEC_KEY = null;
			USER_SIGN_UP_KEY = null;
			logger.error("'" + ConfigurationUtils.DF_KEYS_PROJECT_TOKEN + "' is not defined in configuration.");
			System.exit(1);
		}

		// registration access key from configuration
		if (config.hasPath(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS)) {
			REGISTRATION_ACCESS_KEY = config.getStringList(ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS);
		} else {
			REGISTRATION_ACCESS_KEY = null;
			logger.error("'" + ConfigurationUtils.DF_KEYS_REGISTRATION_ACCESS + "' is not defined in configuration.");
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
		if (REGISTRATION_ACCESS_KEY == null || accessKey == null) {
			return false;
		}

		return REGISTRATION_ACCESS_KEY.contains(accessKey) ? true : false;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get raw contents of decrypted collaboration token
	 * 
	 * @param token
	 * @return
	 */
	public String getRawCollabToken(String token) {
		return SymEncryption.decryptToken(token, PROJECT_COLLABORATOR_TOKEN_KEY);
	}

	/**
	 * get raw contents of decrypted subscription token
	 * 
	 * @param token
	 * @return
	 */
	public String getRawSubscriptionToken(String token) {
		return SymEncryption.decryptToken(token, PROJECT_SUBSCRIBER_TOKEN_KEY);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * create a Base64 encoded token of a username, for password resets
	 * 
	 * @param username
	 * @return
	 */
	public String createEmailResetToken(String username) {
		return base64Encode(SymEncryption.encryptToken(username, EMAIL_RESET_SEC_KEY));
	}

	/**
	 * retrieve username from Base64 encoded token, for password reset
	 * 
	 * @param token
	 * @return
	 */
	public String retrieveUsernameFromEmailResetToken(String token) {
		return SymEncryption.decryptToken(base64Decode(token), EMAIL_RESET_SEC_KEY);
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
		// try new token scheme with decode first
		Long firstIdFromToken = SymEncryption.getFirstIdFromToken(base64Decode(token), key);
		if (firstIdFromToken == -1l) {
			// if no result, try old scheme without decode
			firstIdFromToken = SymEncryption.getFirstIdFromToken(token, key);
		}
		return firstIdFromToken;
	}

	/**
	 * retrieve first id part of a token that is base32 encoded
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	private Long retrieveFirstIdFromBase32Token(String token, String key) {
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
		// try new token scheme with decode first
		Long secondIdFromToken = SymEncryption.getSecondIdFromToken(base64Decode(token), key);
		if (secondIdFromToken == -1l) {
			// if no result, try old scheme without decode
			secondIdFromToken = SymEncryption.getSecondIdFromToken(token, key);
		}
		return secondIdFromToken;
	}

	/**
	 * retrieve second string part of a token that is base64 url encoded
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	private String getSecondStringFromToken(String token, String key) {
		// try new token scheme with decode first
		String secondStringFromToken = SymEncryption.getSecondStringFromToken(base64Decode(token), key);
		if (secondStringFromToken == null) {
			// if no result, try old scheme without decode
			secondStringFromToken = SymEncryption.getSecondStringFromToken(token, key);
		}
		return secondStringFromToken;
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
		try {
			return new String(Base64.getUrlDecoder().decode(str));
		} catch (IllegalArgumentException e) {
			return "-- invalid token --";
		}
	}

}

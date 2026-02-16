package controllers.auth;

import java.util.Optional;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import models.Person;
import play.Logger;
import play.mvc.Http.Request;
import play.mvc.Result;
import utils.auth.TokenResolverUtil;
import utils.conf.ConfigurationUtils;

/**
 * specific authentication for API v2 user project access
 * 
 * @author mathias
 *
 */
public class V2UserApiAuth extends AbstractApiAuth {

	public static final long API_TOKEN_VALIDITY = 1000 * 60 * 60 * 24 * 60l;
	public static final String X_API_TOKEN = "X-API-Token";

	private static final Logger.ALogger logger = Logger.of(V2UserApiAuth.class);

	private TokenResolverUtil tokenResolverUtil;

	@Inject
	public V2UserApiAuth(Config config, TokenResolverUtil tokenResolverUtil) {
		super(config, ConfigurationUtils.DF_KEYS_V2_USER_API);
		this.tokenResolverUtil = tokenResolverUtil;
	}

	@Override
	public Optional<String> getUsername(Request request) {

		// KEY -----------------------------------------------------------

		// first check the general API key authentication
		if (super.getUsername(request).isPresent()) {
			return request.headers().get(HEADER_API_KEY);
		}

		// TOKEN ---------------------------------------------------------

		// then check the specific token
		String apiToken = request.headers().get(X_API_TOKEN).orElse("");
		if (apiToken.length() == 0) {
			logger.error("API token auth failed (user access token is empty)");
			return Optional.empty();
		}

		Long userId = tokenResolverUtil.retrieveUserIdFromUserAccessToken(apiToken);
		Long tokenTimeout = tokenResolverUtil.retrieveTimeoutFromUserAccessToken(apiToken);
		if (userId == -1l || tokenTimeout < System.currentTimeMillis()) {
			logger.error("API token auth failed (user access token has timed out)");
			return Optional.empty();
		}

		// check if this token is still set as _the_ access token for the particular user
		Person user = Person.find.byId(userId);
		if (user == null || user.getAccesscode() == null || !apiToken.equals(user.getAccesscode())) {
			logger.error("API token auth failed (user access token not set in user entity)");
			return Optional.empty();
		}

		return Optional.of(apiToken);
	}

	@Override
	public Result onUnauthorized(Request request) {

		// KEY -----------------------------------------------------------

		// first check the general API key authentication
		if (super.getUsername(request).isEmpty()) {
			return forbidden(errorJSONResponseObject("The API-Key is not valid."));
		}

		// TOKEN ---------------------------------------------------------

		// then check the specific token
		String apiToken = request.headers().get(X_API_TOKEN).orElse("");
		if (apiToken.length() == 0) {
			return forbidden(errorJSONResponseObject("The API-Token is empty."));
		}

		Long userId = tokenResolverUtil.retrieveUserIdFromUserAccessToken(apiToken);
		Long tokenTimeout = tokenResolverUtil.retrieveTimeoutFromUserAccessToken(apiToken);
		if (userId == -1l || tokenTimeout < System.currentTimeMillis()) {
			return forbidden(errorJSONResponseObject("The API-Token is not valid anymore, generate a new one."));
		}

		// check if this token is still set as _the_ access token for the particular user
		Person user = Person.find.byId(userId);
		if (user == null || user.getAccesscode() == null || !apiToken.equals(user.getAccesscode())) {
			return forbidden(errorJSONResponseObject(
					"The API-Token was withdrawn from your user profile. Generate a new one if you want access again."));
		}

		return forbidden(errorJSONResponseObject("No valid user API-Key or API-Token given."));
	}
}

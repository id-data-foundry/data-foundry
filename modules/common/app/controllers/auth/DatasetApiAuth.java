package controllers.auth;

import java.util.Optional;

import com.google.inject.Inject;

import models.Dataset;
import play.Logger;
import play.mvc.Http.Request;
import play.mvc.Result;
import utils.auth.TokenResolverUtil;

/**
 * Specific authentication for datasets
 * 
 * @author mathias
 *
 */
public class DatasetApiAuth extends play.mvc.Security.Authenticator {

	private final TokenResolverUtil tokenResolverUtil;
	private static final Logger.ALogger logger = Logger.of(DatasetApiAuth.class);

	@Inject
	public DatasetApiAuth(TokenResolverUtil tokenResolverUtil) {
		this.tokenResolverUtil = tokenResolverUtil;
	}

	@Override
	public Optional<String> getUsername(Request request) {

		// check access
		String access_code = request.headers().get(Dataset.API_TOKEN).orElse("");
		if (access_code.isEmpty()) {
			logger.info("DatasetApiAuth: api_token empty");
			return Optional.empty();
		}

		// check dataset token
		Long id = tokenResolverUtil.getDatasetIdFromToken(access_code);
		if (id == -1l) {
			logger.info("DatasetApiAuth: api_token malformed");
			return Optional.empty();
		}

		// check dataset id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			logger.info("DatasetApiAuth: DS not found");
			return Optional.empty();
		}

		// check dataset configuration
		if (!access_code.equals(ds.getConfiguration().get(Dataset.API_TOKEN))) {
			logger.info("DatasetApiAuth: api_token not same as in DS configuration");
			return Optional.empty();
		}

		return Optional.of("ok");
	}

	@Override
	public Result onUnauthorized(Request req) {
		return forbidden("Access with api_token failed.");
	}
}

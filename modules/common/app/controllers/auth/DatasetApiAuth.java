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

		// If path contains a dataset id parameter, ensure it matches the authorized dataset id
		String[] segments = request.path().split("/");
		for (int i = 0; i < segments.length - 1; i++) {
			if ("datasets".equals(segments[i]) && i + 2 < segments.length) {
				try {
					long pathId = Long.parseLong(segments[i + 2]);
					if (pathId != ds.getId()) {
						logger.warn("DatasetApiAuth: token for dataset " + ds.getId() + " cannot access dataset " + pathId);
						return Optional.empty();
					}
				} catch (NumberFormatException ignored) {
				}
			}
		}

		return Optional.of(ds.getId().toString());
	}

	public static boolean isAuthorizedForDataset(Request request, Long datasetId) {
		if (datasetId == null || request == null) {
			return false;
		}
		String authDsId = request.attrs().getOptional(play.mvc.Security.USERNAME).orElse("");
		return String.valueOf(datasetId).equals(authDsId);
	}

	@Override
	public Result onUnauthorized(Request req) {
		return forbidden("Access with api_token failed.");
	}
}

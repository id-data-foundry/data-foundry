package controllers.auth;

import java.util.Arrays;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import play.Logger;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticator;

public class AbstractApiAuth extends Authenticator {

	protected static final String HEADER_API_KEY = "X-API-Key";
	private final String[] API_KEY;
	private static final Logger.ALogger logger = Logger.of(AbstractApiAuth.class);

	public AbstractApiAuth(Config config, String key) {

		// if not configuration property is given, abort application
		if (!config.hasPath(key)) {
			API_KEY = null;
			logger.error("'" + key + "' is not defined in configuration.");
			System.exit(1);
			return;
		}

		// configuration of API keys
		String[] temp = null;
		try {
			temp = new String[] { config.getString(key) };
		} catch (Exception e) {
		}
		if (temp == null) {
			try {
				temp = config.getStringList(key).toArray(new String[] {});
			} catch (Exception e1) {
			}
		}

		// check config keys
		if (temp == null) {
			logger.error("Authentication key has wrong type in configuration (must be String or StringList): " + key);
			API_KEY = new String[] {};
		} else {
			API_KEY = temp;
		}
	}

	@Override
	public Optional<String> getUsername(Request request) {
		String access_code = request.headers().get(HEADER_API_KEY).orElse("");
		if (Arrays.asList(API_KEY).stream().noneMatch(s -> s.equals(access_code))) {
			return Optional.empty();
		}
		return Optional.of("ok");
	}

	@Override
	public Result onUnauthorized(Request req) {
		return forbidden(errorJSONResponseObject("Wrong or missing API key (X-API-Key)."));
	}

	protected ObjectNode errorJSONResponseObject(String message) {
		ObjectNode on = Json.newObject();
		on.put("result", "error");
		on.put("message", message);
		return on;
	}

}

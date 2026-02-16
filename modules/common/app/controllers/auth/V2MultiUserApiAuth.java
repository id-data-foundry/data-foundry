package controllers.auth;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import utils.conf.ConfigurationUtils;

/**
 * specific authentication for API v2 multi-user access;
 * 
 * ATTENTION: this will only be accessible for known parties who are vetted
 * 
 * @author mathias
 *
 */
public class V2MultiUserApiAuth extends AbstractApiAuth {

	@Inject
	public V2MultiUserApiAuth(Config config) {
		super(config, ConfigurationUtils.DF_KEYS_V2_MULTI_API);
	}
}

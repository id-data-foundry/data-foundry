package utils.conf;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.typesafe.config.Config;

import play.Logger;

public class ConfigurationUtils {

	private static final Logger.ALogger logger = Logger.of(ConfigurationUtils.class);

	public static final String DF_BASEURL = "df.base_url";

	public static final String DF_UPLOAD_DIR = "df.upload_directory";
	public static final String DF_MAX_ACTIVE_PROJECTS = "df.max_active_projects";

	public static final String DF_SECURITYTXT_CONTACT = "df.securitytxt.contact";
	public static final String DF_SECURITYTXT_EXPIRES = "df.securitytxt.expires";
	public static final String DF_SECURITYTXT_LANG = "df.securitytxt.preferred-languages";

	public static final String DF_SSO_CLIENT = "df.sso.client";
	public static final String DF_SSO_SECRET = "df.sso.secret";
	public static final String DF_SSO_TENANT = "df.sso.tenant";
	public static final String DF_SSO_DISCOVERY = "df.sso.discovery";

	public static final String DF_MSGRAPH_FROM = "df.msgraph.from";
	public static final String DF_MSGRAPH_CLIENT = "df.msgraph.client";
	public static final String DF_MSGRAPH_SECRET = "df.msgraph.secret";
	public static final String DF_MSGRAPH_TENANT = "df.msgraph.tenant";
	public static final String DF_MSGRAPH_DISCOVERY = "df.msgraph.discovery";

	public static final String DF_USERS_ADMINS = "df.users.admins";
	public static final String DF_USERS_LIBRARIANS = "df.users.librarians";
	public static final String DF_USERS_MODERATORS = "df.users.moderators";
	public static final String DF_USERS_REVIEWERS = "df.users.reviewers";

	public static final String DF_KEYS_API = "df.keys.api";
	public static final String DF_KEYS_AUTH_API = "df.keys.auth.api";
	public static final String DF_KEYS_V2_MULTI_API = "df.keys.v2.multi.api";
	public static final String DF_KEYS_V2_USER_API = "df.keys.v2.user.api";
	public static final String DF_KEYS_PROJECT_TOKEN = "df.keys.project";
	public static final String DF_KEYS_REGISTRATION_ACCESS = "df.keys.registration";

	// ----------------------------------------------------------------------------------------------------------------

	public static final String DF_LINKS_ORGANIZATION = "df.links.organization";
	public static final String DF_LINKS_ABOUT = "df.links.about";
	public static final String DF_LINKS_CONTACT = "df.links.contact";
	public static final String DF_LINKS_PRIVACY = "df.links.privacy";
	public static final String DF_LINKS_SCIENTIFIC_INTEGRITY = "df.links.scientific_integrity";
	public static final String DF_LINKS_DATA_PROTECTION = "df.links.data_protection";
	public static final String DF_LINKS_ORG_LOGO = "df.links.organization_logo";
	public static final String DF_LINKS_TEAMS_COMMUNITY = "df.links.teams_community";

	// ----------------------------------------------------------------------------------------------------------------

	public static final String DF_OOCSI_SERVER = "df.oocsi_server";
	public static final String DF_TELEGRAM_BOTNAME = "df.telegram.bot_name";
	public static final String DF_TELEGRAM_BOTTOKEN = "df.telegram.bot_token";

	// ----------------------------------------------------------------------------------------------------------------

	public static final String DF_PROCESSING_PYTHON = "df.processing.python";

	// ----------------------------------------------------------------------------------------------------------------

	public static final String DF_LOCALAI_HOST = "df.processing.localai.host";

	// ----------------------------------------------------------------------------------------------------------------

	public static final String DF_FEEDBACK_LINK = "df.feedback_link";

	// ----------------------------------------------------------------------------------------------------------------

	public static final String DF_VENDOR_SLACK_CHANNEL = "df.vendor.slack.channel";

	public static final String DF_VENDOR_FITBIT_ID = "df.vendor.fitbit.id";
	public static final String DF_VENDOR_FITBIT_SECRET = "df.vendor.fitbit.secret";

	public static final String DF_VENDOR_GOOGLEFIT_ID = "df.vendor.googlefit.id";
	public static final String DF_VENDOR_GOOGLEFIT_SECRET = "df.vendor.googlefit.secret";

	public static final String DF_VENDOR_OPENAI = "df.vendor.openai.key";

	// ----------------------------------------------------------------------------------------------------------------

	public static final String DF_MAIL_FROM = "play.mailer.from";
	public static final String DF_MAIL_HOST = "play.mailer.host";
	public static final String DF_MAIL_USERNAME = "play.mailer.username";
	public static final String DF_MAIL_PASSWORD = "play.mailer.password";

	// ----------------------------------------------------------------------------------------------------------------

	// prepare map of all configuration items and their default value formats
	private static final Map<String, String> defaultValueFormat = new HashMap<>();
	static {
		// compile general paths
		defaultValueFormat.put(DF_BASEURL, "\"https://your-datafoundry-installation.org\"");
		defaultValueFormat.put(DF_UPLOAD_DIR, "\"data/\"");
		defaultValueFormat.put(DF_MAX_ACTIVE_PROJECTS, "20");

		defaultValueFormat.put(DF_SSO_TENANT, "\"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"");
		defaultValueFormat.put(DF_SSO_CLIENT, "\"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"");
		defaultValueFormat.put(DF_SSO_SECRET, "\"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"");
		defaultValueFormat.put(DF_SSO_DISCOVERY,
		        "\"https://auth-provider.com/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/v2.0/.well-known/...\"");

		defaultValueFormat.put(DF_MSGRAPH_TENANT, "\"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"");
		defaultValueFormat.put(DF_MSGRAPH_CLIENT, "\"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"");
		defaultValueFormat.put(DF_MSGRAPH_SECRET, "\"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"");
		defaultValueFormat.put(DF_MSGRAPH_DISCOVERY,
		        "\"https://auth-provider.com/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/v2.0/.well-known/...\"");

		defaultValueFormat.put(DF_USERS_ADMINS, "[\"admin1@example.com\",\"admin2@example.org\"]");
		defaultValueFormat.put(DF_USERS_MODERATORS, "[\"mod1@example.com\",\"mod2@example.org\"]");
		defaultValueFormat.put(DF_USERS_REVIEWERS, "[\"reviewer1@example.com\",\"reviewer2@example.org\"]");
//		defaultValueFormat.put(DF_USERS_LIBRARIANS, "[\"libra1@example.com\",\"libra2@example.org\"]");
		defaultValueFormat.put(DF_KEYS_API, "\"1234567890abcdef\"");
		defaultValueFormat.put(DF_KEYS_AUTH_API, "\"1234567890abcdef\"");
		defaultValueFormat.put(DF_KEYS_V2_MULTI_API, "\"1234567890abcdef\"");
		defaultValueFormat.put(DF_KEYS_V2_USER_API, "\"1234567890abcdef\"");
		defaultValueFormat.put(DF_KEYS_REGISTRATION_ACCESS, "\"1234567890abcdef\"");
		defaultValueFormat.put(DF_KEYS_PROJECT_TOKEN, "\"1234567890abcdef\"");

		defaultValueFormat.put(DF_OOCSI_SERVER, "\"oocsi.example.org\"");
		defaultValueFormat.put(DF_VENDOR_SLACK_CHANNEL,
		        "\"https://hooks.slack.com/services/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\"");

		defaultValueFormat.put(DF_LOCALAI_HOST, "\"http://localhost:9191\"");

		defaultValueFormat.put(DF_VENDOR_FITBIT_ID, "xxxxxxx");
		defaultValueFormat.put(DF_VENDOR_FITBIT_SECRET, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");

		defaultValueFormat.put(DF_VENDOR_GOOGLEFIT_ID,
		        "111111111111-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.apps.googleusercontent.com");
		defaultValueFormat.put(DF_VENDOR_GOOGLEFIT_SECRET, "xxxxx-xxxxx_xxxxxxxxx-xx");

		defaultValueFormat.put(DF_TELEGRAM_BOTNAME, "\"somethingTelegram_bot\"");
		defaultValueFormat.put(DF_TELEGRAM_BOTTOKEN, "\"1111111111:aaa_aaaaaaaaaaaaaaaaaa-aaaaaaaaaaa\"");

	}

	/**
	 * check the provided configuration for values, if some values are not present, this will print an error message
	 * with instructions
	 * 
	 * @param configurations
	 * @return
	 */
	public static boolean checkAllConfigurations(Config configurations) {
		List<String> sb = new LinkedList<>();
		checkGeneralConfig(configurations, sb);
		return hasErrors(sb);
	}

	/**
	 * check the general configuration with checkConfigurations()
	 * 
	 * @param configuration
	 * @return
	 */
	private static boolean checkGeneralConfig(Config configuration, List<String> sb) {
		return internalCheckConfiguration(new String[] { DF_BASEURL, DF_UPLOAD_DIR, DF_MAX_ACTIVE_PROJECTS,
		        DF_SSO_CLIENT, DF_SSO_SECRET, DF_SSO_DISCOVERY, DF_MSGRAPH_TENANT, DF_MSGRAPH_CLIENT, DF_MSGRAPH_SECRET,
		        DF_MSGRAPH_DISCOVERY, DF_USERS_ADMINS, DF_USERS_MODERATORS, DF_USERS_REVIEWERS, DF_KEYS_API,
		        DF_KEYS_AUTH_API, DF_KEYS_V2_MULTI_API, DF_KEYS_V2_USER_API, DF_KEYS_REGISTRATION_ACCESS,
		        DF_KEYS_PROJECT_TOKEN, DF_OOCSI_SERVER, DF_VENDOR_SLACK_CHANNEL, DF_LOCALAI_HOST }, configuration, sb);
	}

	/**
	 * check the Fitbit configurations with checkConfigurations()
	 * 
	 * @param configuration
	 * @return true if configured fully and correctly, false otherwise
	 */
	public static boolean checkFitbitConfig(Config configuration, List<String> sb) {
		return internalCheckConfiguration(new String[] { DF_VENDOR_FITBIT_ID, DF_VENDOR_FITBIT_SECRET }, configuration,
		        sb);
	}

	/**
	 * * check the GoogleFit configuration with checkConfigurations()
	 * 
	 * @param configuration
	 * @return true if configured fully and correctly, false otherwise
	 */
	public static boolean checkGoogleFitConfig(Config configuration, List<String> sb) {
		return internalCheckConfiguration(new String[] { DF_VENDOR_GOOGLEFIT_ID, DF_VENDOR_GOOGLEFIT_SECRET },
		        configuration, sb);
	}

	/**
	 * check the Telegram configuration with checkConfigurations()
	 * 
	 * @param configuration
	 * @return true if configured fully and correctly, false otherwise
	 */
	public static boolean checkTelegramConfig(Config configuration, List<String> sb) {
		return internalCheckConfiguration(new String[] { DF_TELEGRAM_BOTNAME, DF_TELEGRAM_BOTTOKEN }, configuration,
		        sb);
	}

	/**
	 * check the configuration for the given keys
	 * 
	 * @param key
	 * @param configuration
	 * @param sb
	 * @return
	 */
	private static boolean internalCheckConfiguration(String[] key, Config configuration, List<String> sb) {
		Map<String, String> pathsMessages = Arrays.stream(key)
		        .collect(Collectors.toMap(s -> s, s -> defaultValueFormat.get(s)));
		return internalCheckConfigurations(pathsMessages, configuration, sb);
	}

	/**
	 * check the provided configurations. if some values are not present, this will collect error messages
	 * 
	 * @param pathMsgs
	 * @param configuration
	 * @param target
	 * @return true if configured fully and correctly, false otherwise
	 */
	private static boolean internalCheckConfigurations(Map<String, String> pathMsgs, Config configuration,
	        List<String> sb) {
		boolean isConfigured = true;
		for (Map.Entry<String, String> entry : pathMsgs.entrySet()) {
			if (!configuration.hasPath(entry.getKey())) {
				sb.add(">  " + entry.getKey() + " = " + entry.getValue());
				isConfigured = false;
			}
		}
		return isConfigured;
	}

	/**
	 * output error messages / instructions if configuration is incomplete
	 * 
	 * @param sb
	 * @return true in case of no errors, false otherwise
	 */
	public static boolean hasErrors(List<String> sb) {
		if (sb.size() > 0) {
			logger.warn("-----------------------------------------------------------");
			logger.warn("Configuration incomplete:");
			for (String configurationError : sb) {
				logger.warn(configurationError);
			}
			logger.warn("");
			logger.warn("You can fix this in the application.conf.");
			logger.warn("-----------------------------------------------------------\n");

			return false;
		}

		return true;
	}

	/**
	 * check configuration, return true if all keys are present and not empty
	 * 
	 * @param configuration
	 * @param keys
	 * @return
	 */
	public static boolean checkConfiguration(Config configuration, String... keys) {
		for (String key : keys) {
			if (!configuration.hasPath(key) || configuration.getString(key).trim().isEmpty()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * extend the documentation text with template tags by data from the given configuration
	 * 
	 * @param configuration
	 * @param format
	 * @return
	 */
	public static String replaceConfigurationVars(Config configuration, String format) {
		// only replace allowed configuration keys
		final List<String> ALLOW_LIST = Arrays.asList(new String[] { "df.telegram.bot_name" });

		final String fieldStart = "\\{\\{";
		final String fieldEnd = "\\}\\}";
		final String regex = fieldStart + "([^}]+)" + fieldEnd;
		final Pattern pattern = Pattern.compile(regex);
		int cdlatch = 20;
		Matcher m = pattern.matcher(format);
		String result = format;
		while (m.find() && cdlatch-- > 0) {
			String found = m.group(1);
			if (ALLOW_LIST.contains(found) && configuration.hasPath(found)) {
				String newVal = configuration.getString(found);
				result = result.replaceFirst(regex, newVal);
			} else {
				result = result.replaceFirst(regex, "CONFIGURATION_ERROR");
			}
		}
		return result;
	}

	public static boolean isSSO(Config configuration) {
		return configuration.hasPath(ConfigurationUtils.DF_SSO_CLIENT)
		        && configuration.getString(ConfigurationUtils.DF_SSO_CLIENT).trim().length() > 0;
	}

	/**
	 * return String value at given path or default value
	 * 
	 * @param configuration
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static String configure(Config configuration, String key, String defaultValue) {
		return hasKeyConfiguration(configuration, key) ? configuration.getString(key) : defaultValue;
	}

	/**
	 * return int value at given path or default value
	 * 
	 * @param configuration
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static int configureInt(Config configuration, String key, int defaultValue) {
		return hasKeyConfiguration(configuration, key) ? configuration.getInt(key) : defaultValue;
	}

	/**
	 * return list value at given path or default value
	 * 
	 * @param configuration
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static List<String> configureList(Config configuration, String key) {
		return configuration.hasPath(key) ? configuration.getStringList(key) : Collections.emptyList();
	}

	/**
	 * configuration has given path and the string value is non-empty
	 * 
	 * @param configuration
	 * @param key
	 * @return
	 */
	public static boolean hasKeyConfiguration(Config configuration, String key) {
		return configuration.hasPath(key) && !configuration.getString(key).isEmpty();
	}

	/**
	 * retrieve default value format for a given key
	 * 
	 * @param configuration
	 * @param key
	 * @return
	 */
	public static String getKeyDefaultValueFormat(Config configuration, String key) {
		return defaultValueFormat.getOrDefault(key, "");
	}
}

package utils.conf;

import java.util.LinkedList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.typesafe.config.Config;

import play.Environment;

@Singleton
public class Configurator {

	private final Config configuration;

	private boolean isTelegramAvailable = true;
	private boolean isGoogleFitAvailable = true;
	private boolean isFitbitAvailable = true;

	@Inject
	public Configurator(Environment env, Config configuration) {
		this.configuration = configuration;

		// only check if not in test
		if (!env.isTest()) {
			List<String> sb = new LinkedList<>();
			isTelegramAvailable = ConfigurationUtils.checkTelegramConfig(configuration, sb);
			isFitbitAvailable = ConfigurationUtils.checkFitbitConfig(configuration, sb);
			isGoogleFitAvailable = ConfigurationUtils.checkGoogleFitConfig(configuration, sb);
			ConfigurationUtils.hasErrors(sb);
		}
	}

	public boolean isFitbitAvailable() {
		return isFitbitAvailable;
	}

	public boolean isGoogleFitAvailable() {
		return isGoogleFitAvailable;
	}

	public boolean isTelegramAvailable() {
		return isTelegramAvailable;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether the configuration option is defined and non-empty
	 * 
	 * @param key
	 * @return
	 */
	public boolean isDefined(String key) {
		return ConfigurationUtils.hasKeyConfiguration(configuration, key);
	}

	/**
	 * check whether the configuration option is defined and non-empty
	 * 
	 * @param key
	 * @return
	 */
	public boolean isListDefined(String key) {
		return configuration.hasPath(key) && !configuration.getList(key).isEmpty();
	}

	/**
	 * return the configuration value
	 * 
	 * @param key
	 * @return
	 */
	public String getString(String key) {
		return ConfigurationUtils.configure(configuration, key, "");
	}

	/**
	 * return the configuration value
	 * 
	 * @param key
	 * @return
	 */
	public List<String> getStringList(String key) {
		return ConfigurationUtils.configureList(configuration, key);
	}

	/**
	 * retrieve default value format for a given key
	 * 
	 * @param key
	 * @return
	 */
	public String getDefaultValueFormat(String key) {
		return ConfigurationUtils.getKeyDefaultValueFormat(configuration, key);
	}

	/**
	 * check the feature flag specified: if the given key is defined in the configuration the value is returned as
	 * Boolean, otherwise the given defaultValue is returned
	 * 
	 * @param ff
	 * @return
	 */
	public boolean isActive(FeatureFlag ff) {
		return configuration.hasPath(ff.key) ? configuration.getBoolean(ff.key) : ff.defaultValue;
	}

	/**
	 * Feature flag defined by key and default value
	 *
	 */
	static public class FeatureFlag {
		String key;
		boolean defaultValue;

		public FeatureFlag(String key, boolean defaultValue) {
			this.key = key;
			this.defaultValue = defaultValue;
		}
	}

}

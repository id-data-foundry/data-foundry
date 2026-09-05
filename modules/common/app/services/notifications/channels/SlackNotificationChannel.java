package services.notifications.channels;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

import com.typesafe.config.Config;

import play.Logger;
import services.notifications.NotificationChannel;
import services.notifications.NotificationLevel;
import services.notifications.NotificationMessage;
import services.slack.api.SlackApi;
import services.slack.api.SlackMessage;
import utils.conf.ConfigurationUtils;

/**
 * Slack notification channel leveraging the existing SlackApi and SlackMessage implementation.
 */
public class SlackNotificationChannel implements NotificationChannel {

	private static final Logger.ALogger logger = Logger.of(SlackNotificationChannel.class);
	private static final String STANDARD_SLACK_HOOK_BASE = "https://hooks.slack.com/services/";

	private final boolean enabled;
	private final String webhookUrl;
	private final SlackApi slackApi;
	private final String hostname;

	public SlackNotificationChannel(Config config) {
		String host;
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			host = "<server unknown>";
		}
		this.hostname = host;

		// 1. Resolve webhook URL or key
		String resolvedUrl = null;
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_SLACK_URL)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_SLACK_URL).trim().isEmpty()) {
			resolvedUrl = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_SLACK_URL).trim();
		} else if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_SLACK_KEY)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_SLACK_KEY).trim().isEmpty()) {
			String key = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_SLACK_KEY).trim();
			if (key.startsWith("http://") || key.startsWith("https://")) {
				resolvedUrl = key;
			} else {
				resolvedUrl = STANDARD_SLACK_HOOK_BASE + (key.startsWith("/") ? key.substring(1) : key);
			}
		} else if (config.hasPath(ConfigurationUtils.DF_VENDOR_SLACK_CHANNEL)
				&& !config.getString(ConfigurationUtils.DF_VENDOR_SLACK_CHANNEL).trim().isEmpty()) {
			resolvedUrl = config.getString(ConfigurationUtils.DF_VENDOR_SLACK_CHANNEL).trim();
		}

		// 2. Check if enabled
		boolean isGloballyEnabled = !config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_ENABLED)
				|| config.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_ENABLED);

		boolean isSlackExplicitlyEnabled = !config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_SLACK_ENABLED)
				|| config.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_SLACK_ENABLED);

		this.webhookUrl = resolvedUrl;
		this.enabled = isGloballyEnabled && isSlackExplicitlyEnabled && (this.webhookUrl != null && !this.webhookUrl.isEmpty());

		// 3. Initialize SlackApi
		SlackApi api = null;
		if (this.enabled) {
			try {
				api = new SlackApi(this.webhookUrl);
			} catch (Exception e) {
				logger.error("Failed to initialize SlackApi for webhook URL: " + e.getMessage());
			}
		}
		this.slackApi = api;
	}

	@Override
	public String getName() {
		return "slack";
	}

	@Override
	public boolean isEnabled() {
		return enabled && slackApi != null;
	}

	public String getWebhookUrl() {
		return webhookUrl;
	}

	@Override
	public CompletableFuture<Boolean> send(NotificationMessage message) {
		if (!isEnabled()) {
			return CompletableFuture.completedFuture(false);
		}

		return CompletableFuture.supplyAsync(() -> {
			try {
				String title = message.getTitle() != null ? message.getTitle() : "";
				String levelPrefix = "";
				if (message.getLevel() == NotificationLevel.CRITICAL) {
					levelPrefix = "🚨 [CRITICAL] ";
				} else if (message.getLevel() == NotificationLevel.ERROR) {
					levelPrefix = "❌ [ERROR] ";
				} else if (message.getLevel() == NotificationLevel.WARNING) {
					levelPrefix = "⚠️ [WARNING] ";
				} else if (message.getLevel() == NotificationLevel.TRACE || message.getLevel() == NotificationLevel.DEBUG) {
					levelPrefix = "[TRACE] ";
				}

				String fullTitle = hostname + ": " + levelPrefix + title;
				slackApi.call(new SlackMessage(fullTitle, message.getMessage()));
				return true;
			} catch (Exception e) {
				logger.error("Error sending Slack notification: " + e.getMessage());
				return false;
			}
		});
	}
}

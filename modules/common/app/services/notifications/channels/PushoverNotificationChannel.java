package services.notifications.channels;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import play.Logger;
import play.libs.Json;
import play.libs.ws.WSClient;
import play.libs.ws.WSRequest;
import services.notifications.NotificationChannel;
import services.notifications.NotificationLevel;
import services.notifications.NotificationMessage;
import utils.conf.ConfigurationUtils;

/**
 * Pushover notification channel (https://pushover.net).
 * As Pushover notifications are sent privately to authenticated user/group keys,
 * the server hostname is included in the notification title to identify the origin host.
 */
public class PushoverNotificationChannel implements NotificationChannel {

	private static final Logger.ALogger logger = Logger.of(PushoverNotificationChannel.class);
	public static final String DEFAULT_PUSHOVER_URL = "https://api.pushover.net/1/messages.json";

	private final WSClient wsClient;
	private final boolean enabled;
	private final String apiUrl;
	private final String token;
	private final String user;
	private final String device;
	private final int defaultPriority;
	private final String hostname;

	public PushoverNotificationChannel(Config config, WSClient wsClient) {
		this.wsClient = wsClient;

		// 1. Resolve hostname
		String host;
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			host = "<server unknown>";
		}
		this.hostname = host;

		// 2. Resolve API URL
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_URL)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_URL).trim().isEmpty()) {
			this.apiUrl = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_URL).trim();
		} else {
			this.apiUrl = DEFAULT_PUSHOVER_URL;
		}

		// 3. Resolve token and user key
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_TOKEN)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_TOKEN).trim().isEmpty()) {
			this.token = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_TOKEN).trim();
		} else {
			this.token = "";
		}

		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_USER)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_USER).trim().isEmpty()) {
			this.user = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_USER).trim();
		} else {
			this.user = "";
		}

		// 4. Resolve optional device
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_DEVICE)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_DEVICE).trim().isEmpty()) {
			this.device = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_DEVICE).trim();
		} else {
			this.device = "";
		}

		// 5. Resolve default priority
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_PRIORITY)) {
			this.defaultPriority = config.getInt(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_PRIORITY);
		} else {
			this.defaultPriority = 0;
		}

		// 6. Check if enabled
		boolean isGloballyEnabled = !config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_ENABLED)
				|| config.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_ENABLED);

		boolean isPushoverExplicitlyEnabled = config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_ENABLED)
				&& config.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_PUSHOVER_ENABLED);

		this.enabled = isGloballyEnabled && isPushoverExplicitlyEnabled && !this.token.isEmpty() && !this.user.isEmpty();
	}

	@Override
	public String getName() {
		return "pushover";
	}

	@Override
	public boolean isEnabled() {
		return enabled && wsClient != null;
	}

	public String getApiUrl() {
		return apiUrl;
	}

	public String getToken() {
		return token;
	}

	public String getUser() {
		return user;
	}

	public String getDevice() {
		return device;
	}

	public int getDefaultPriority() {
		return defaultPriority;
	}

	public String getHostname() {
		return hostname;
	}

	/**
	 * Map NotificationLevel to Pushover priority:
	 * - CRITICAL, HIGH, ERROR: 1 (high priority: sounds alert, bypasses quiet hours)
	 * - WARNING: 0 (normal priority)
	 * - INFO: defaultPriority (usually 0)
	 * - TRACE, DEBUG: -1 (low priority: no sound or vibration)
	 */
	public int resolvePriority(NotificationLevel level) {
		if (level == null) {
			return defaultPriority;
		}
		return switch (level) {
		case CRITICAL, HIGH, ERROR -> 1;
		case WARNING -> 0;
		case INFO -> defaultPriority;
		case DEBUG, TRACE -> -1;
		};
	}

	/**
	 * Formats notification title including origin hostname and level emoji prefix.
	 */
	public String formatTitle(NotificationMessage message) {
		String title = message.getTitle() != null ? message.getTitle().trim() : "";
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

		if (title.isEmpty()) {
			return hostname + (levelPrefix.isEmpty() ? "" : ": " + levelPrefix.trim());
		} else {
			return hostname + ": " + levelPrefix + title;
		}
	}

	@Override
	public CompletableFuture<Boolean> send(NotificationMessage message) {
		if (!isEnabled()) {
			return CompletableFuture.completedFuture(false);
		}

		try {
			WSRequest request = wsClient.url(apiUrl).setRequestTimeout(Duration.ofSeconds(5));
			request.setContentType("application/json");

			ObjectNode payload = Json.newObject();
			payload.put("token", token);
			payload.put("user", user);
			payload.put("title", formatTitle(message));
			payload.put("message", message.getMessage() != null ? message.getMessage() : "");
			payload.put("priority", resolvePriority(message.getLevel()));

			if (!device.isEmpty()) {
				payload.put("device", device);
			}

			if (message.getTimestamp() > 0) {
				payload.put("timestamp", message.getTimestamp() / 1000);
			}

			if (!message.getTags().isEmpty()) {
				payload.put("tags", String.join(",", message.getTags()));
			}

			return request.post(payload).thenApply(response -> {
				int status = response.getStatus();
				if (status >= 200 && status < 300) {
					return true;
				} else {
					logger.error("Pushover notification failed with HTTP status " + status + ": " + response.getBody());
					return false;
				}
			}).exceptionally(e -> {
				logger.error("Error sending Pushover notification: " + e.getMessage());
				return false;
			}).toCompletableFuture();

		} catch (Exception e) {
			logger.error("Error preparing Pushover notification request: " + e.getMessage());
			return CompletableFuture.completedFuture(false);
		}
	}
}

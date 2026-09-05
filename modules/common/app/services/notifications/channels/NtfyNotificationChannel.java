package services.notifications.channels;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.typesafe.config.Config;

import play.Logger;
import play.libs.ws.WSClient;
import play.libs.ws.WSRequest;
import services.notifications.NotificationChannel;
import services.notifications.NotificationLevel;
import services.notifications.NotificationMessage;
import utils.conf.ConfigurationUtils;

/**
 * Ntfy notification channel (ntfy.sh or self-hosted ntfy server).
 */
public class NtfyNotificationChannel implements NotificationChannel {

	private static final Logger.ALogger logger = Logger.of(NtfyNotificationChannel.class);
	private static final String DEFAULT_NTFY_SERVER = "https://ntfy.sh";

	private final WSClient wsClient;
	private final boolean enabled;
	private final String server;
	private final String topic;
	private final String token;
	private final int defaultPriority;
	private final String hostname;

	public NtfyNotificationChannel(Config config, WSClient wsClient) {
		this.wsClient = wsClient;

		String host;
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			host = "<server unknown>";
		}
		this.hostname = host;

		// 1. Resolve server
		String resolvedServer = DEFAULT_NTFY_SERVER;
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_SERVER)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_SERVER).trim().isEmpty()) {
			resolvedServer = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_SERVER).trim();
		}
		if (resolvedServer.endsWith("/")) {
			resolvedServer = resolvedServer.substring(0, resolvedServer.length() - 1);
		}
		this.server = resolvedServer;

		// 2. Resolve topic
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_TOPIC)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_TOPIC).trim().isEmpty()) {
			this.topic = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_TOPIC).trim();
		} else {
			this.topic = "";
		}

		// 3. Resolve optional token
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_TOKEN)
				&& !config.getString(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_TOKEN).trim().isEmpty()) {
			this.token = config.getString(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_TOKEN).trim();
		} else {
			this.token = "";
		}

		// 4. Resolve default priority
		if (config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_PRIORITY)) {
			this.defaultPriority = config.getInt(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_PRIORITY);
		} else {
			this.defaultPriority = 3;
		}

		// 5. Check if enabled
		boolean isGloballyEnabled = !config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_ENABLED)
				|| config.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_ENABLED);

		boolean isNtfyExplicitlyEnabled = config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_ENABLED)
				&& config.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_NTFY_ENABLED);

		this.enabled = isGloballyEnabled && isNtfyExplicitlyEnabled && !this.topic.isEmpty();
	}

	@Override
	public String getName() {
		return "ntfy";
	}

	@Override
	public boolean isEnabled() {
		return enabled && wsClient != null;
	}

	public String getServer() {
		return server;
	}

	public String getTopic() {
		return topic;
	}

	@Override
	public CompletableFuture<Boolean> send(NotificationMessage message) {
		if (!isEnabled()) {
			return CompletableFuture.completedFuture(false);
		}

		try {
			String url = server + "/" + topic;
			WSRequest request = wsClient.url(url).setRequestTimeout(Duration.ofSeconds(5));

			// Title
			String title = message.getTitle() != null ? message.getTitle() : "";
			request.addHeader("Title", hostname + ": " + title);

			// Priority mapping
			String priorityHeader;
			if (message.getLevel() == NotificationLevel.CRITICAL) {
				priorityHeader = "urgent";
			} else if (message.getLevel() == NotificationLevel.ERROR) {
				priorityHeader = "high";
			} else if (message.getLevel() == NotificationLevel.WARNING) {
				priorityHeader = "default";
			} else if (message.getLevel() == NotificationLevel.INFO) {
				priorityHeader = String.valueOf(defaultPriority);
			} else {
				priorityHeader = "min";
			}
			request.addHeader("Priority", priorityHeader);

			// Tags
			Set<String> tags = new LinkedHashSet<>(message.getTags());
			if (message.getLevel() == NotificationLevel.CRITICAL) {
				tags.add("rotating_light");
			} else if (message.getLevel() == NotificationLevel.ERROR) {
				tags.add("warning");
			}
			if (!tags.isEmpty()) {
				request.addHeader("Tags", String.join(",", tags));
			}

			// Auth token
			if (!token.isEmpty()) {
				request.addHeader("Authorization", "Bearer " + token);
			}

			return request.post(message.getMessage()).thenApply(response -> {
				int status = response.getStatus();
				if (status >= 200 && status < 300) {
					return true;
				} else {
					logger.error("Ntfy notification failed with HTTP status " + status + ": " + response.getBody());
					return false;
				}
			}).exceptionally(e -> {
				logger.error("Error sending Ntfy notification: " + e.getMessage());
				return false;
			}).toCompletableFuture();

		} catch (Exception e) {
			logger.error("Error preparing Ntfy notification request: " + e.getMessage());
			return CompletableFuture.completedFuture(false);
		}
	}
}

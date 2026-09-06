package services.notifications;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import play.Environment;
import play.Logger;
import play.libs.ws.WSClient;
import services.notifications.channels.NtfyNotificationChannel;
import services.notifications.channels.PushoverNotificationChannel;
import services.notifications.channels.SlackNotificationChannel;
import utils.conf.ConfigurationUtils;

/**
 * Main coordinator for system notifications and alerts across multiple providers.
 */
@Singleton
public class NotificationManager implements SystemNotificationService {

	private static final Logger.ALogger logger = Logger.of(NotificationManager.class);
	private static final int MAX_HISTORY_SIZE = 50;

	private static volatile NotificationManager instance;

	private final boolean globallyEnabled;
	private final List<NotificationChannel> channels = new ArrayList<>();
	private final ConcurrentLinkedDeque<NotificationRecord> history = new ConcurrentLinkedDeque<>();

	@Inject
	public NotificationManager(Config config, WSClient wsClient, Environment env) {
		this.globallyEnabled = !config.hasPath(ConfigurationUtils.DF_NOTIFICATIONS_ENABLED)
				|| config.getBoolean(ConfigurationUtils.DF_NOTIFICATIONS_ENABLED);

		// Register channels
		SlackNotificationChannel slackChannel = new SlackNotificationChannel(config);
		channels.add(slackChannel);

		NtfyNotificationChannel ntfyChannel = new NtfyNotificationChannel(config, wsClient);
		channels.add(ntfyChannel);

		PushoverNotificationChannel pushoverChannel = new PushoverNotificationChannel(config, wsClient);
		channels.add(pushoverChannel);

		// Set static reference for non-injected callers
		NotificationManager.instance = this;

		logger.info("NotificationManager initialized. Globally enabled: " + globallyEnabled
				+ ", Slack enabled: " + slackChannel.isEnabled()
				+ ", Ntfy enabled: " + ntfyChannel.isEnabled()
				+ ", Pushover enabled: " + pushoverChannel.isEnabled());
	}

	public static NotificationManager getInstance() {
		return instance;
	}

	@Override
	public boolean isEnabled() {
		return globallyEnabled;
	}

	@Override
	public List<NotificationChannel> getChannels() {
		return Collections.unmodifiableList(channels);
	}

	@Override
	public List<NotificationRecord> getRecentNotifications() {
		return new ArrayList<>(history);
	}

	@Override
	public CompletableFuture<Void> send(String title, String message) {
		NotificationLevel level = NotificationLevel.INFO;
		if (title != null && (title.toLowerCase().contains("exception") || title.toLowerCase().contains("error"))) {
			level = NotificationLevel.TRACE;
		}
		return send(level, title, message);
	}

	@Override
	public CompletableFuture<Void> send(NotificationLevel level, String title, String message) {
		return send(NotificationMessage.builder()
				.title(title)
				.message(message)
				.level(level)
				.build());
	}

	@Override
	public CompletableFuture<Void> send(NotificationMessage message) {
		if (message == null) {
			return CompletableFuture.completedFuture(null);
		}

		logger.info("[" + message.getLevel() + "] " + message.getTitle() + ": " + message.getMessage());

		if (!globallyEnabled) {
			addToHistory(new NotificationRecord(message.getLevel(), message.getTitle(), message.getMessage(),
					Collections.emptyList(), false));
			return CompletableFuture.completedFuture(null);
		}

		List<NotificationChannel> activeChannels = channels.stream()
				.filter(NotificationChannel::isEnabled)
				.collect(Collectors.toList());

		List<String> channelNames = activeChannels.stream()
				.map(NotificationChannel::getName)
				.collect(Collectors.toList());

		if (activeChannels.isEmpty()) {
			addToHistory(new NotificationRecord(message.getLevel(), message.getTitle(), message.getMessage(),
					Collections.emptyList(), false));
			return CompletableFuture.completedFuture(null);
		}

		List<CompletableFuture<Boolean>> futures = new ArrayList<>();
		for (NotificationChannel channel : activeChannels) {
			futures.add(channel.send(message));
		}

		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).handle((res, ex) -> {
			boolean anySuccess = false;
			for (CompletableFuture<Boolean> future : futures) {
				try {
					if (Boolean.TRUE.equals(future.getNow(false))) {
						anySuccess = true;
					}
				} catch (Exception e) {
					// ignore
				}
			}
			addToHistory(new NotificationRecord(message.getLevel(), message.getTitle(), message.getMessage(),
					channelNames, anySuccess));
			return null;
		});
	}

	private void addToHistory(NotificationRecord record) {
		history.addFirst(record);
		while (history.size() > MAX_HISTORY_SIZE) {
			history.pollLast();
		}
	}
}

package services.notifications;

import java.util.Collections;
import java.util.List;

import play.Logger;

/**
 * Static facade for dispatching system notifications and accessing notification history.
 * Ideal for Ebean models, static helpers, or non-injected contexts.
 */
public class Notifications {

	private static final Logger.ALogger logger = Logger.of(Notifications.class);

	public static void send(String title, String message) {
		NotificationLevel level = NotificationLevel.INFO;
		if (title != null && (title.toLowerCase().contains("exception") || title.toLowerCase().contains("error"))) {
			level = NotificationLevel.TRACE;
		}
		send(level, title, message);
	}

	public static void call(String title, String message) {
		send(title, message);
	}

	public static void info(String title, String message) {
		send(NotificationLevel.INFO, title, message);
	}

	public static void warn(String title, String message) {
		send(NotificationLevel.WARNING, title, message);
	}

	public static void error(String title, String message) {
		send(NotificationLevel.ERROR, title, message);
	}

	public static void critical(String title, String message) {
		send(NotificationLevel.CRITICAL, title, message);
	}

	public static void send(NotificationLevel level, String title, String message) {
		send(NotificationMessage.builder()
				.title(title)
				.message(message)
				.level(level)
				.build());
	}

	public static void send(NotificationMessage message) {
		NotificationManager manager = NotificationManager.getInstance();
		if (manager != null) {
			manager.send(message);
		} else {
			logger.info("[Pre-DI Notification - " + message.getLevel() + "] " + message.getTitle() + ": " + message.getMessage());
		}
	}

	public static List<NotificationRecord> getRecentNotifications() {
		NotificationManager manager = NotificationManager.getInstance();
		if (manager != null) {
			return manager.getRecentNotifications();
		}
		return Collections.emptyList();
	}
}

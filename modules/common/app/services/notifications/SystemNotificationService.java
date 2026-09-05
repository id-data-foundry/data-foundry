package services.notifications;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.google.inject.ImplementedBy;

/**
 * System notification service for dispatching alerts across configured channels (Slack, Ntfy, etc.).
 */
@ImplementedBy(NotificationManager.class)
public interface SystemNotificationService {

	/**
	 * Send a notification message across all enabled channels.
	 */
	CompletableFuture<Void> send(NotificationMessage message);

	/**
	 * Convenience method to send an INFO-level notification.
	 */
	CompletableFuture<Void> send(String title, String message);

	/**
	 * Convenience method to send a notification with a specific severity level.
	 */
	CompletableFuture<Void> send(NotificationLevel level, String title, String message);

	/**
	 * Retrieve all configured channels.
	 */
	List<NotificationChannel> getChannels();

	/**
	 * Retrieve recent notifications history kept in memory.
	 */
	List<NotificationRecord> getRecentNotifications();

	/**
	 * Whether system notifications are globally enabled.
	 */
	boolean isEnabled();
}

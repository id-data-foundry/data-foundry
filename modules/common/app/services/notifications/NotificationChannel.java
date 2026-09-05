package services.notifications;

import java.util.concurrent.CompletableFuture;

public interface NotificationChannel {

	/**
	 * Identifier name of the channel (e.g., "slack", "ntfy").
	 */
	String getName();

	/**
	 * Whether this channel is configured and enabled.
	 */
	boolean isEnabled();

	/**
	 * Dispatch a notification message asynchronously through this channel.
	 *
	 * @param message the notification message to send
	 * @return CompletableFuture completing with true if sent successfully, false otherwise
	 */
	CompletableFuture<Boolean> send(NotificationMessage message);
}

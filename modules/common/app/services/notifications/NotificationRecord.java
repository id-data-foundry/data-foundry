package services.notifications;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * In-memory representation of a dispatched notification for admin diagnostics.
 */
public class NotificationRecord {

	private final long timestamp;
	private final String formattedTime;
	private final NotificationLevel level;
	private final String title;
	private final String message;
	private final List<String> targetChannels;
	private final boolean success;

	public NotificationRecord(NotificationLevel level, String title, String message, List<String> targetChannels,
			boolean success) {
		this.timestamp = System.currentTimeMillis();
		this.formattedTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(this.timestamp));
		this.level = level != null ? level : NotificationLevel.INFO;
		this.title = title != null ? title : "";
		this.message = message != null ? message : "";
		this.targetChannels = targetChannels != null ? new ArrayList<>(targetChannels) : Collections.emptyList();
		this.success = success;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getFormattedTime() {
		return formattedTime;
	}

	public NotificationLevel getLevel() {
		return level;
	}

	public String getTitle() {
		return title;
	}

	public String getMessage() {
		return message;
	}

	public List<String> getTargetChannels() {
		return Collections.unmodifiableList(targetChannels);
	}

	public boolean isSuccess() {
		return success;
	}
}

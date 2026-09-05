package services.notifications;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class NotificationMessage {

	private final String title;
	private final String message;
	private final NotificationLevel level;
	private final Set<String> tags;
	private final long timestamp;

	private NotificationMessage(Builder builder) {
		this.title = builder.title != null ? builder.title : "";
		this.message = builder.message != null ? builder.message : "";
		this.level = builder.level != null ? builder.level : NotificationLevel.INFO;
		this.tags = Collections.unmodifiableSet(new LinkedHashSet<>(builder.tags));
		this.timestamp = builder.timestamp > 0 ? builder.timestamp : System.currentTimeMillis();
	}

	public String getTitle() {
		return title;
	}

	public String getMessage() {
		return message;
	}

	public NotificationLevel getLevel() {
		return level;
	}

	public Set<String> getTags() {
		return tags;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static NotificationMessage of(String title, String message) {
		return builder().title(title).message(message).level(NotificationLevel.INFO).build();
	}

	public static NotificationMessage info(String title, String message) {
		return builder().title(title).message(message).level(NotificationLevel.INFO).build();
	}

	public static NotificationMessage warn(String title, String message) {
		return builder().title(title).message(message).level(NotificationLevel.WARNING).build();
	}

	public static NotificationMessage error(String title, String message) {
		return builder().title(title).message(message).level(NotificationLevel.ERROR).build();
	}

	public static NotificationMessage critical(String title, String message) {
		return builder().title(title).message(message).level(NotificationLevel.CRITICAL).build();
	}

	public static class Builder {
		private String title;
		private String message;
		private NotificationLevel level = NotificationLevel.INFO;
		private final Set<String> tags = new LinkedHashSet<>();
		private long timestamp;

		public Builder title(String title) {
			this.title = title;
			return this;
		}

		public Builder message(String message) {
			this.message = message;
			return this;
		}

		public Builder level(NotificationLevel level) {
			this.level = level;
			return this;
		}

		public Builder tag(String tag) {
			if (tag != null && !tag.trim().isEmpty()) {
				this.tags.add(tag.trim());
			}
			return this;
		}

		public Builder tags(Set<String> tags) {
			if (tags != null) {
				for (String tag : tags) {
					tag(tag);
				}
			}
			return this;
		}

		public Builder timestamp(long timestamp) {
			this.timestamp = timestamp;
			return this;
		}

		public NotificationMessage build() {
			return new NotificationMessage(this);
		}
	}
}

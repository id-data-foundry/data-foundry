package services.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import play.Environment;
import services.notifications.channels.NtfyNotificationChannel;
import services.notifications.channels.SlackNotificationChannel;

public class NotificationManagerTest {

	@Test
	public void testNotificationMessageBuilder() {
		NotificationMessage msg = NotificationMessage.builder()
				.title("Test Alert")
				.message("Everything is ok")
				.level(NotificationLevel.WARNING)
				.tag("robot")
				.tag("warning")
				.build();

		assertEquals("Test Alert", msg.getTitle());
		assertEquals("Everything is ok", msg.getMessage());
		assertEquals(NotificationLevel.WARNING, msg.getLevel());
		assertTrue(msg.getTags().contains("robot"));
		assertTrue(msg.getTags().contains("warning"));
		assertTrue(msg.getTimestamp() > 0);
	}

	@Test
	public void testSlackNotificationChannelWithExplicitUrl() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.notifications.enabled", true);
		map.put("df.notifications.channels.slack.enabled", true);
		map.put("df.notifications.channels.slack.url", "https://hooks.slack.com/services/T11/B22/X33");

		Config config = ConfigFactory.parseMap(map);
		SlackNotificationChannel channel = new SlackNotificationChannel(config);

		assertEquals("slack", channel.getName());
		assertTrue(channel.isEnabled());
		assertEquals("https://hooks.slack.com/services/T11/B22/X33", channel.getWebhookUrl());
	}

	@Test
	public void testSlackNotificationChannelWithKeyOnly() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.notifications.enabled", true);
		map.put("df.notifications.channels.slack.enabled", true);
		map.put("df.notifications.channels.slack.key", "T111/B222/K333");

		Config config = ConfigFactory.parseMap(map);
		SlackNotificationChannel channel = new SlackNotificationChannel(config);

		assertEquals("slack", channel.getName());
		assertTrue(channel.isEnabled());
		assertEquals("https://hooks.slack.com/services/T111/B222/K333", channel.getWebhookUrl());
	}

	@Test
	public void testSlackNotificationChannelLegacyFallback() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.vendor.slack.channel", "https://hooks.slack.com/services/LEGACY/WEBHOOK/URL");

		Config config = ConfigFactory.parseMap(map);
		SlackNotificationChannel channel = new SlackNotificationChannel(config);

		assertEquals("slack", channel.getName());
		assertTrue(channel.isEnabled());
		assertEquals("https://hooks.slack.com/services/LEGACY/WEBHOOK/URL", channel.getWebhookUrl());
	}

	@Test
	public void testNtfyNotificationChannelConfiguration() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.notifications.enabled", true);
		map.put("df.notifications.channels.ntfy.enabled", true);
		map.put("df.notifications.channels.ntfy.server", "https://ntfy.example.com/");
		map.put("df.notifications.channels.ntfy.topic", "alerts-topic");
		map.put("df.notifications.channels.ntfy.token", "secret-token");

		Config config = ConfigFactory.parseMap(map);
		// With null WSClient, isEnabled() is false, but configuration properties can be checked
		NtfyNotificationChannel channel = new NtfyNotificationChannel(config, null);

		assertEquals("ntfy", channel.getName());
		assertEquals("https://ntfy.example.com", channel.getServer());
		assertEquals("alerts-topic", channel.getTopic());
		assertFalse(channel.isEnabled()); // null wsClient disables
	}

	@Test
	public void testNotificationHistoryBufferLimit() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.notifications.enabled", true);

		Config config = ConfigFactory.parseMap(map);
		NotificationManager manager = new NotificationManager(config, null, Environment.simple());

		// Send 60 messages
		for (int i = 1; i <= 60; i++) {
			manager.send("Title " + i, "Message " + i);
		}

		List<NotificationRecord> history = manager.getRecentNotifications();
		assertNotNull(history);
		// Should be capped at 50
		assertEquals(50, history.size());
		// Most recent notification should be at the front
		assertEquals("Title 60", history.get(0).getTitle());
	}

	@Test
	public void testNotificationsStaticFacadeAndLevelInference() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.notifications.enabled", true);

		Config config = ConfigFactory.parseMap(map);
		new NotificationManager(config, null, Environment.simple());

		// Send regular message
		Notifications.send("System", "Application started");
		// Send exception message
		Notifications.call("Exception occurred", "Connection refused");

		List<NotificationRecord> history = Notifications.getRecentNotifications();
		assertNotNull(history);
		assertTrue(history.size() >= 2);

		// The exception message should be at index 0 and have TRACE level
		NotificationRecord exceptionRecord = history.get(0);
		assertEquals("Exception occurred", exceptionRecord.getTitle());
		assertEquals(NotificationLevel.TRACE, exceptionRecord.getLevel());

		// The system message should be at index 1 and have INFO level
		NotificationRecord systemRecord = history.get(1);
		assertEquals("System", systemRecord.getTitle());
		assertEquals(NotificationLevel.INFO, systemRecord.getLevel());
	}
}

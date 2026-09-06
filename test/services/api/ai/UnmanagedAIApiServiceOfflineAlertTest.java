package services.api.ai;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import services.api.remoting.RemoteApiRequest;
import services.notifications.NotificationChannel;
import services.notifications.NotificationLevel;
import services.notifications.NotificationMessage;
import services.notifications.NotificationRecord;
import services.notifications.SystemNotificationService;

public class UnmanagedAIApiServiceOfflineAlertTest {

	static class RecordingNotificationService implements SystemNotificationService {
		final List<NotificationMessage> messages = new ArrayList<>();

		@Override
		public CompletableFuture<Void> send(NotificationMessage message) {
			messages.add(message);
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public CompletableFuture<Void> send(String title, String message) {
			return send(NotificationMessage.of(title, message));
		}

		@Override
		public CompletableFuture<Void> send(NotificationLevel level, String title, String message) {
			return send(NotificationMessage.builder().title(title).message(message).level(level).build());
		}

		@Override
		public List<NotificationChannel> getChannels() {
			return Collections.emptyList();
		}

		@Override
		public List<NotificationRecord> getRecentNotifications() {
			return Collections.emptyList();
		}

		@Override
		public boolean isEnabled() {
			return true;
		}
	}

	static class TestableUnmanagedAIApiService extends UnmanagedAIApiService {
		boolean shouldFail = true;
		String successResponse = "{\"data\": [{\"id\": \"test-model\", \"name\": \"Test Model\"}]}";
		String errorResponse = null;

		public TestableUnmanagedAIApiService(Config config, RecordingNotificationService notificationService) {
			super(config, null, null, null, null, null, null, null, new LocalModelMetadata(), notificationService);
		}

		@Override
		public Future<Void> submitApiRequest(RemoteApiRequest request) {
			if (shouldFail) {
				CompletableFuture<Void> failed = new CompletableFuture<>();
				failed.completeExceptionally(new RuntimeException("Connection refused to AI backend"));
				return failed;
			} else if (errorResponse != null) {
				request.setResult(Optional.of(errorResponse));
				return CompletableFuture.completedFuture(null);
			} else {
				request.setResult(Optional.of(successResponse));
				return CompletableFuture.completedFuture(null);
			}
		}
	}

	@Test
	public void testAiOfflineAndRecoveryAlertStateMachine() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.notifications.ai.consecutive_failures_threshold", 2);
		map.put("df.notifications.ai.alert_on_offline", true);
		map.put("df.notifications.ai.alert_on_recovery", true);

		Config config = ConfigFactory.parseMap(map);
		RecordingNotificationService notificationService = new RecordingNotificationService();
		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, notificationService);

		// Initial state
		assertTrue(service.isOnline());
		assertEquals(0, service.getConsecutiveFailures());
		assertEquals(0, notificationService.messages.size());

		// 1st failure: under threshold (threshold = 2) -> no alert yet
		service.shouldFail = true;
		service.refresh();
		assertEquals(1, service.getConsecutiveFailures());
		assertTrue(service.isOnline());
		assertEquals(0, notificationService.messages.size());

		// 2nd failure: threshold reached -> transitions to OFFLINE, sends CRITICAL alert
		service.refresh();
		assertEquals(2, service.getConsecutiveFailures());
		assertFalse(service.isOnline());
		assertEquals(1, notificationService.messages.size());
		assertEquals("AI Service Offline", notificationService.messages.get(0).getTitle());
		assertEquals(NotificationLevel.CRITICAL, notificationService.messages.get(0).getLevel());

		// 3rd failure: already OFFLINE -> no duplicate alert storm
		service.refresh();
		assertEquals(3, service.getConsecutiveFailures());
		assertFalse(service.isOnline());
		assertEquals(1, notificationService.messages.size());

		// Recovery: backend comes back online with valid model -> transitions to ONLINE, sends INFO alert
		service.shouldFail = false;
		service.refresh();
		assertEquals(0, service.getConsecutiveFailures());
		assertTrue(service.isOnline());
		assertEquals(2, notificationService.messages.size());
		assertEquals("AI Service Online", notificationService.messages.get(1).getTitle());
		assertEquals(NotificationLevel.INFO, notificationService.messages.get(1).getLevel());
	}

	@Test
	public void testAiOfflineOnEmptyStringResponse() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.notifications.ai.consecutive_failures_threshold", 2);
		map.put("df.notifications.ai.alert_on_offline", true);

		Config config = ConfigFactory.parseMap(map);
		RecordingNotificationService notificationService = new RecordingNotificationService();
		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, notificationService);

		service.shouldFail = false;
		service.successResponse = "";

		// 1st empty response
		service.refresh();
		assertEquals(1, service.getConsecutiveFailures());
		assertTrue(service.isOnline());
		assertEquals(0, notificationService.messages.size());

		// 2nd empty response -> threshold reached, triggers offline alert
		service.refresh();
		assertEquals(2, service.getConsecutiveFailures());
		assertFalse(service.isOnline());
		assertEquals(1, notificationService.messages.size());
		assertEquals("AI Service Offline", notificationService.messages.get(0).getTitle());
		assertEquals(NotificationLevel.CRITICAL, notificationService.messages.get(0).getLevel());
		assertTrue(notificationService.messages.get(0).getMessage().contains("Empty response from AI backend"));
	}

	@Test
	public void testAiOfflineOnEmptyDataArrayResponse() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.notifications.ai.consecutive_failures_threshold", 2);
		map.put("df.notifications.ai.alert_on_offline", true);

		Config config = ConfigFactory.parseMap(map);
		RecordingNotificationService notificationService = new RecordingNotificationService();
		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, notificationService);

		service.shouldFail = false;
		service.successResponse = "{\"data\": []}";

		// 1st empty models response
		service.refresh();
		assertEquals(1, service.getConsecutiveFailures());
		assertTrue(service.isOnline());
		assertEquals(0, notificationService.messages.size());

		// 2nd empty models response -> threshold reached, triggers offline alert
		service.refresh();
		assertEquals(2, service.getConsecutiveFailures());
		assertFalse(service.isOnline());
		assertEquals(1, notificationService.messages.size());
		assertEquals("AI Service Offline", notificationService.messages.get(0).getTitle());
		assertEquals(NotificationLevel.CRITICAL, notificationService.messages.get(0).getLevel());
		assertTrue(notificationService.messages.get(0).getMessage().contains("No models returned by AI backend"));
	}

	@Test
	public void testAiOfflineOnErrorResponse() {
		Map<String, Object> map = new HashMap<>();
		map.put("df.processing.ai.baseurl", "http://localhost:9191/v1");
		map.put("df.notifications.ai.consecutive_failures_threshold", 2);
		map.put("df.notifications.ai.alert_on_offline", true);

		Config config = ConfigFactory.parseMap(map);
		RecordingNotificationService notificationService = new RecordingNotificationService();
		TestableUnmanagedAIApiService service = new TestableUnmanagedAIApiService(config, notificationService);

		service.shouldFail = false;
		service.errorResponse = "{\"error\": {\"message\": \"API returned status 502: Bad Gateway\"}}";

		// 1st error response
		service.refresh();
		assertEquals(1, service.getConsecutiveFailures());
		assertTrue(service.isOnline());

		// 2nd error response -> triggers offline alert with extracted error message
		service.refresh();
		assertEquals(2, service.getConsecutiveFailures());
		assertFalse(service.isOnline());
		assertEquals(1, notificationService.messages.size());
		assertEquals("AI Service Offline", notificationService.messages.get(0).getTitle());
		assertEquals(NotificationLevel.CRITICAL, notificationService.messages.get(0).getLevel());
		assertTrue(notificationService.messages.get(0).getMessage().contains("API returned status 502: Bad Gateway"));
	}
}

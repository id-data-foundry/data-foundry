package utils.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChatbotMemoryUtilsTest {

	private File tempDir;

	@Before
	public void setUp() throws IOException {
		tempDir = Files.createTempDirectory("chatbot_memory_test").toFile();
	}

	@After
	public void tearDown() throws IOException {
		if (tempDir != null && tempDir.exists()) {
			FileUtils.deleteDirectory(tempDir);
		}
	}

	@Test
	public void testSanitizeUserId() {
		assertEquals("user_john_at_example_com", ChatbotMemoryUtils.sanitizeUserId("john@example.com"));
		assertEquals("user_jane_doe_at_tue_nl", ChatbotMemoryUtils.sanitizeUserId("Jane.Doe@tue.nl"));
		assertEquals("user_unknown", ChatbotMemoryUtils.sanitizeUserId(null));
		assertEquals("user_unknown", ChatbotMemoryUtils.sanitizeUserId("   "));
		assertEquals("user_user_123", ChatbotMemoryUtils.sanitizeUserId("user!123"));
	}

	@Test
	public void testIsValidUserFilename() {
		assertTrue(ChatbotMemoryUtils.isValidUserFilename("notes.md"));
		assertTrue(ChatbotMemoryUtils.isValidUserFilename("project_summary.md"));
		assertTrue(ChatbotMemoryUtils.isValidUserFilename("user-plan-2026.md"));

		// Prohibited files
		assertFalse(ChatbotMemoryUtils.isValidUserFilename("../notes.md"));
		assertFalse(ChatbotMemoryUtils.isValidUserFilename("sub/notes.md"));
		assertFalse(ChatbotMemoryUtils.isValidUserFilename("notes.txt"));
		assertFalse(ChatbotMemoryUtils.isValidUserFilename("script.sh"));
		assertFalse(ChatbotMemoryUtils.isValidUserFilename(""));
		assertFalse(ChatbotMemoryUtils.isValidUserFilename(null));
	}

	@Test
	public void testUserProfileOperations() {
		String userId = "student@example.com";

		// Initial profile should be empty
		String initial = ChatbotMemoryUtils.loadUserProfile(tempDir, userId);
		assertEquals("", initial);

		// Save first entry
		boolean saved1 = ChatbotMemoryUtils.saveUserProfileEntry(tempDir, userId, "thesis_topic",
				"AI-assisted design synthesis");
		assertTrue(saved1);

		String profile1 = ChatbotMemoryUtils.loadUserProfile(tempDir, userId);
		assertTrue(profile1.contains("- **thesis_topic**: AI-assisted design synthesis"));

		// Save second entry
		boolean saved2 = ChatbotMemoryUtils.saveUserProfileEntry(tempDir, userId, "coding_level",
				"Advanced Java / Python");
		assertTrue(saved2);

		String profile2 = ChatbotMemoryUtils.loadUserProfile(tempDir, userId);
		assertTrue(profile2.contains("- **thesis_topic**: AI-assisted design synthesis"));
		assertTrue(profile2.contains("- **coding_level**: Advanced Java / Python"));

		// Update existing topic (should update in place, not duplicate)
		boolean updated = ChatbotMemoryUtils.saveUserProfileEntry(tempDir, userId, "thesis_topic",
				"Generative design synthesis with Play framework");
		assertTrue(updated);

		String profile3 = ChatbotMemoryUtils.loadUserProfile(tempDir, userId);
		assertTrue(profile3.contains("Generative design synthesis with Play framework"));
		assertFalse(profile3.contains("AI-assisted design synthesis"));

		// Verify snapshot
		ChatbotMemoryUtils.UserMemorySnapshot snapshot = ChatbotMemoryUtils.getUserMemorySnapshot(tempDir, userId);
		assertNotNull(snapshot);
		assertEquals("Generative design synthesis with Play framework",
				snapshot.profileAttributes().get("thesis_topic").split(" \\(Updated:")[0]);
		assertEquals("Advanced Java / Python",
				snapshot.profileAttributes().get("coding_level").split(" \\(Updated:")[0]);
	}

	@Test
	public void testUserFilesAndSandboxing() {
		String userId = "researcher@example.com";

		// Write user note file
		String writeRes = ChatbotMemoryUtils.writeUserFile(tempDir, userId, "experiment_notes.md",
				"# Experiment 1\nResults were positive.");
		assertTrue(writeRes.startsWith("Success:"));

		// Read user note file
		String readRes = ChatbotMemoryUtils.readUserFile(tempDir, userId, "experiment_notes.md");
		assertEquals("# Experiment 1\nResults were positive.", readRes);

		// Reject traversal filename
		String traversalRes = ChatbotMemoryUtils.writeUserFile(tempDir, userId, "../hacked.md", "data");
		assertTrue(traversalRes.startsWith("Error:"));

		// Verify in snapshot
		ChatbotMemoryUtils.UserMemorySnapshot snapshot = ChatbotMemoryUtils.getUserMemorySnapshot(tempDir, userId);
		assertTrue(snapshot.userFiles().containsKey("experiment_notes.md"));

		// Reset memory
		boolean resetOk = ChatbotMemoryUtils.resetUserMemory(tempDir, userId);
		assertTrue(resetOk);

		// Notes and profile should now be gone
		assertEquals("", ChatbotMemoryUtils.loadUserProfile(tempDir, userId));
		assertTrue(ChatbotMemoryUtils.readUserFile(tempDir, userId, "experiment_notes.md").contains("does not exist"));
	}

	@Test
	public void testDeleteUserFile() {
		String userId = "student@tue.nl";

		// Write a file
		ChatbotMemoryUtils.writeUserFile(tempDir, userId, "draft.md", "# My Draft");
		assertEquals("# My Draft", ChatbotMemoryUtils.readUserFile(tempDir, userId, "draft.md"));

		// Delete the file
		String deleteRes = ChatbotMemoryUtils.deleteUserFile(tempDir, userId, "draft.md");
		assertTrue(deleteRes.startsWith("Success:"));

		// Verify it no longer exists
		assertTrue(ChatbotMemoryUtils.readUserFile(tempDir, userId, "draft.md").contains("does not exist"));

		// Cannot delete profile.md via deleteUserFile
		ChatbotMemoryUtils.saveUserProfileEntry(tempDir, userId, "topic", "note");
		String delProfileRes = ChatbotMemoryUtils.deleteUserFile(tempDir, userId, "profile.md");
		assertTrue(delProfileRes.startsWith("Error:"));

		// Path traversal rejection
		String delTraversal = ChatbotMemoryUtils.deleteUserFile(tempDir, userId, "../other.md");
		assertTrue(delTraversal.startsWith("Error:"));
	}

	@Test
	public void testDeleteUserProfileEntry() {
		String userId = "student@tue.nl";

		// Save two topics
		ChatbotMemoryUtils.saveUserProfileEntry(tempDir, userId, "thesis_topic", "Generative AI");
		ChatbotMemoryUtils.saveUserProfileEntry(tempDir, userId, "favorite_color", "Blue");

		String profile = ChatbotMemoryUtils.loadUserProfile(tempDir, userId);
		assertTrue(profile.contains("thesis_topic"));
		assertTrue(profile.contains("favorite_color"));

		// Delete one topic
		boolean deleted = ChatbotMemoryUtils.deleteUserProfileEntry(tempDir, userId, "favorite_color");
		assertTrue(deleted);

		String updatedProfile = ChatbotMemoryUtils.loadUserProfile(tempDir, userId);
		assertTrue(updatedProfile.contains("thesis_topic"));
		assertFalse(updatedProfile.contains("favorite_color"));

		// Delete remaining topic -> profile becomes empty / cleaned
		boolean deleted2 = ChatbotMemoryUtils.deleteUserProfileEntry(tempDir, userId, "thesis_topic");
		assertTrue(deleted2);
		assertEquals("", ChatbotMemoryUtils.loadUserProfile(tempDir, userId));
	}

	@Test
	public void testUserSessions() throws Exception {
		String userId = "designer@example.com";

		// Initially empty
		List<ChatbotMemoryUtils.UserSessionSummary> emptyList = ChatbotMemoryUtils.loadUserSessions(tempDir, userId);
		assertTrue(emptyList.isEmpty());

		// Save first session
		ChatbotMemoryUtils.saveUserSession(tempDir, userId, "sess-1", "Exploring 3D models");
		List<ChatbotMemoryUtils.UserSessionSummary> list1 = ChatbotMemoryUtils.loadUserSessions(tempDir, userId);
		assertEquals(1, list1.size());
		assertEquals("sess-1", list1.get(0).id());
		assertEquals("Exploring 3D models", list1.get(0).title());

		Thread.sleep(20);

		// Save second session
		ChatbotMemoryUtils.saveUserSession(tempDir, userId, "sess-2", "Debugging Play routes");
		List<ChatbotMemoryUtils.UserSessionSummary> list2 = ChatbotMemoryUtils.loadUserSessions(tempDir, userId);
		assertEquals(2, list2.size());
		assertEquals("sess-2", list2.get(0).id()); // most recent first

		Thread.sleep(20);

		// Update first session activity
		ChatbotMemoryUtils.saveUserSession(tempDir, userId, "sess-1", "Exploring 3D models");
		List<ChatbotMemoryUtils.UserSessionSummary> list3 = ChatbotMemoryUtils.loadUserSessions(tempDir, userId);
		assertEquals("sess-1", list3.get(0).id()); // sess-1 moved to top
	}

	@Test
	public void testDeleteUserSession() {
		String userId = "designer@example.com";
		ChatbotMemoryUtils.saveUserSession(tempDir, userId, "sess-1", "Session 1");
		ChatbotMemoryUtils.saveUserSession(tempDir, userId, "sess-2", "Session 2");
		assertEquals(2, ChatbotMemoryUtils.loadUserSessions(tempDir, userId).size());

		// Delete sess-1
		ChatbotMemoryUtils.deleteUserSession(tempDir, userId, "sess-1");
		List<ChatbotMemoryUtils.UserSessionSummary> remaining = ChatbotMemoryUtils.loadUserSessions(tempDir, userId);
		assertEquals(1, remaining.size());
		assertEquals("sess-2", remaining.get(0).id());

		// Delete non-existent session (no error)
		ChatbotMemoryUtils.deleteUserSession(tempDir, userId, "non-existent");
		assertEquals(1, ChatbotMemoryUtils.loadUserSessions(tempDir, userId).size());
	}
}

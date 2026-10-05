package utils.tools;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import play.Logger;
import play.libs.Json;

/**
 * Utility methods for managing sandboxed user memory, user-specific files, and user chat sessions for DataFoundry
 * custom chatbots.
 */
public class ChatbotMemoryUtils {

	private static final Logger.ALogger logger = Logger.of(ChatbotMemoryUtils.class);

	public static final String MEMORY_PROFILE_FILE = "profile.md";
	public static final String MEMORY_NOTES_FILE = "notes.md";
	public static final String SESSIONS_INDEX_FILE = "sessions.json";

	public record UserSessionSummary(String id, String title, long createdAt, long lastUpdatedAt) {
		public String formattedDate() {
			return new SimpleDateFormat("MMM d, HH:mm", Locale.ENGLISH).format(new Date(lastUpdatedAt));
		}
	}

	public record UserMemorySnapshot(java.util.Map<String, String> profileAttributes,
			java.util.Map<String, String> userFiles) {
	}

	/**
	 * Sanitize user ID (email or username) into a safe filesystem directory name.
	 *
	 * @param rawUserId user email or identifier
	 * @return clean directory name
	 */
	public static String sanitizeUserId(String rawUserId) {
		if (rawUserId == null || rawUserId.trim().isEmpty()) {
			return "user_unknown";
		}
		String clean = rawUserId.trim().toLowerCase(Locale.ROOT).replace("@", "_at_").replaceAll("[^a-z0-9_-]", "_");
		return "user_" + clean;
	}

	/**
	 * Validate a user-provided filename to prevent directory traversal and restrict to markdown files.
	 *
	 * @param filename user filename
	 * @return true if valid flat markdown filename
	 */
	public static boolean isValidUserFilename(String filename) {
		if (filename == null || filename.trim().isEmpty()) {
			return false;
		}
		return filename.matches("^[a-zA-Z0-9_-]+\\.md$");
	}

	/**
	 * Resolve the strictly sandboxed directory for a given user within a dataset's .agentscope directory.
	 *
	 * @param datasetFolder dataset directory on disk
	 * @param rawUserId     raw user ID/email
	 * @return verified user directory
	 */
	public static File getUserDirectory(File datasetFolder, String rawUserId) {
		File agentscopeDir = new File(datasetFolder, ".agentscope");
		File usersDir = new File(agentscopeDir, "users");
		File userDir = new File(usersDir, sanitizeUserId(rawUserId));
		if (!userDir.exists()) {
			userDir.mkdirs();
		}
		return userDir;
	}

	/**
	 * Read the user's persistent profile markdown if it exists.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @return profile markdown or empty string
	 */
	public static String loadUserProfile(File datasetFolder, String rawUserId) {
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File profileFile = new File(userDir, MEMORY_PROFILE_FILE);
		if (profileFile.exists() && profileFile.isFile()) {
			try {
				return FileUtils.readFileToString(profileFile, StandardCharsets.UTF_8).trim();
			} catch (IOException e) {
				logger.error("Error reading user profile for {}", rawUserId, e);
			}
		}
		return "";
	}

	/**
	 * Save or append a structured fact/topic entry to the user's profile.md.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @param topic         memory topic
	 * @param note          memory fact/note
	 * @return success boolean
	 */
	public static boolean saveUserProfileEntry(File datasetFolder, String rawUserId, String topic, String note) {
		if (topic == null || topic.trim().isEmpty() || note == null || note.trim().isEmpty()) {
			return false;
		}
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File profileFile = new File(userDir, MEMORY_PROFILE_FILE);

		String cleanTopic = topic.trim();
		String cleanNote = note.trim();
		String today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);

		try {
			String existing = profileFile.exists() ? FileUtils.readFileToString(profileFile, StandardCharsets.UTF_8)
					: "";
			String lineMarker = "- **" + cleanTopic + "**:";

			if (existing.contains(lineMarker)) {
				// Replace line
				String[] lines = existing.split("\n");
				StringBuilder sb = new StringBuilder();
				for (String line : lines) {
					if (line.trim().startsWith(lineMarker)) {
						sb.append("- **").append(cleanTopic).append("**: ").append(cleanNote).append(" (Updated: ")
								.append(today).append(")\n");
					} else {
						sb.append(line).append("\n");
					}
				}
				FileUtils.writeStringToFile(profileFile, sb.toString().trim() + "\n", StandardCharsets.UTF_8);
			} else {
				// Append entry
				String entry = (existing.isEmpty() ? "# User Profile\n" : "") + "- **" + cleanTopic + "**: " + cleanNote
						+ " (Updated: " + today + ")\n";
				FileUtils.writeStringToFile(profileFile, entry, StandardCharsets.UTF_8, true);
			}
			return true;
		} catch (IOException e) {
			logger.error("Error saving user profile entry for {}", rawUserId, e);
			return false;
		}
	}

	/**
	 * Safely write content to a file inside the user's sandboxed directory.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @param filename      target file name (must be flat markdown file)
	 * @param content       text content to write
	 * @return success or error message
	 */
	public static String writeUserFile(File datasetFolder, String rawUserId, String filename, String content) {
		if (!isValidUserFilename(filename)) {
			return "Error: Invalid filename. Only flat .md files allowed (e.g. 'notes.md').";
		}
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File targetFile = new File(userDir, filename);

		try {
			// Enforce path containment
			String canonicalUserDir = userDir.getCanonicalPath();
			String canonicalTarget = targetFile.getCanonicalPath();
			if (!canonicalTarget.startsWith(canonicalUserDir + File.separator)) {
				return "Error: Access denied (path traversal detected).";
			}

			FileUtils.writeStringToFile(targetFile, content != null ? content : "", StandardCharsets.UTF_8);
			return "Success: File '" + filename + "' updated.";
		} catch (IOException e) {
			logger.error("Error writing user file {} for {}", filename, rawUserId, e);
			return "Error: Could not write file: " + e.getMessage();
		}
	}

	/**
	 * Safely read a file from the user's sandboxed directory.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @param filename      filename to read
	 * @return file content or error message
	 */
	public static String readUserFile(File datasetFolder, String rawUserId, String filename) {
		if (!isValidUserFilename(filename)) {
			return "Error: Invalid filename.";
		}
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File targetFile = new File(userDir, filename);

		try {
			String canonicalUserDir = userDir.getCanonicalPath();
			String canonicalTarget = targetFile.getCanonicalPath();
			if (!canonicalTarget.startsWith(canonicalUserDir + File.separator)) {
				return "Error: Access denied.";
			}
			if (!targetFile.exists() || !targetFile.isFile()) {
				return "File '" + filename + "' does not exist.";
			}
			return FileUtils.readFileToString(targetFile, StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error reading user file {} for {}", filename, rawUserId, e);
			return "Error: Could not read file: " + e.getMessage();
		}
	}

	/**
	 * Safely delete an individual user file (e.g. 'notes.md'). Profile markdown is protected from deletion via this
	 * method.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @param filename      filename to delete
	 * @return success or error message
	 */
	public static String deleteUserFile(File datasetFolder, String rawUserId, String filename) {
		if (!isValidUserFilename(filename)) {
			return "Error: Invalid filename.";
		}
		if (MEMORY_PROFILE_FILE.equals(filename)) {
			return "Error: Cannot delete profile file via file deletion. Use profile entry deletion instead.";
		}
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File targetFile = new File(userDir, filename);

		try {
			String canonicalUserDir = userDir.getCanonicalPath();
			String canonicalTarget = targetFile.getCanonicalPath();
			if (!canonicalTarget.startsWith(canonicalUserDir + File.separator)) {
				return "Error: Access denied (path traversal detected).";
			}
			if (!targetFile.exists() || !targetFile.isFile()) {
				return "File '" + filename + "' does not exist.";
			}
			boolean deleted = targetFile.delete();
			return deleted ? "Success: File '" + filename + "' deleted."
					: "Error: Could not delete file '" + filename + "'.";
		} catch (IOException e) {
			logger.error("Error deleting user file {} for {}", filename, rawUserId, e);
			return "Error: Could not delete file: " + e.getMessage();
		}
	}

	/**
	 * Delete a specific topic/attribute from user's profile.md.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @param topic         memory topic to remove
	 * @return true if deleted or not found
	 */
	public static boolean deleteUserProfileEntry(File datasetFolder, String rawUserId, String topic) {
		if (topic == null || topic.trim().isEmpty()) {
			return false;
		}
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File profileFile = new File(userDir, MEMORY_PROFILE_FILE);
		if (!profileFile.exists() || !profileFile.isFile()) {
			return true;
		}

		String cleanTopic = topic.trim();
		String lineMarker = "- **" + cleanTopic + "**:";

		try {
			List<String> lines = FileUtils.readLines(profileFile, StandardCharsets.UTF_8);
			List<String> remaining = new ArrayList<>();
			boolean found = false;
			for (String line : lines) {
				if (line.trim().startsWith(lineMarker)) {
					found = true;
				} else {
					remaining.add(line);
				}
			}

			if (found) {
				boolean hasAttributes = remaining.stream()
						.anyMatch(l -> l.trim().startsWith("- **") && l.contains("**:"));
				if (!hasAttributes) {
					profileFile.delete();
				} else {
					FileUtils.writeLines(profileFile, StandardCharsets.UTF_8.name(), remaining);
				}
			}
			return true;
		} catch (IOException e) {
			logger.error("Error deleting user profile entry for {}", rawUserId, e);
			return false;
		}
	}

	/**
	 * Clear the user's profile and notes files (wipes memory while preserving session index).
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @return true if reset succeeded
	 */
	public static boolean resetUserMemory(File datasetFolder, String rawUserId) {
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File[] mdFiles = userDir.listFiles((dir, name) -> name.endsWith(".md"));
		boolean ok = true;
		if (mdFiles != null) {
			for (File f : mdFiles) {
				if (!f.delete()) {
					logger.warn("Could not delete file {} during user memory reset", f.getName());
					ok = false;
				}
			}
		}
		return ok;
	}

	/**
	 * Retrieve a complete snapshot of the user's profile attributes and custom notes.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @return memory snapshot
	 */
	public static UserMemorySnapshot getUserMemorySnapshot(File datasetFolder, String rawUserId) {
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		java.util.Map<String, String> profileAttrs = new java.util.LinkedHashMap<>();
		File profileFile = new File(userDir, MEMORY_PROFILE_FILE);
		if (profileFile.exists() && profileFile.isFile()) {
			try {
				List<String> lines = FileUtils.readLines(profileFile, StandardCharsets.UTF_8);
				for (String line : lines) {
					String trimmed = line.trim();
					if (trimmed.startsWith("- **") && trimmed.contains("**:")) {
						int endTopic = trimmed.indexOf("**:");
						String topic = trimmed.substring(4, endTopic).trim();
						String note = trimmed.substring(endTopic + 3).trim();
						profileAttrs.put(topic, note);
					}
				}
			} catch (IOException e) {
				logger.error("Error reading profile for snapshot", e);
			}
		}

		java.util.Map<String, String> userFiles = new java.util.LinkedHashMap<>();
		File[] files = userDir.listFiles((dir, name) -> name.endsWith(".md") && !name.equals(MEMORY_PROFILE_FILE));
		if (files != null) {
			for (File f : files) {
				try {
					userFiles.put(f.getName(), FileUtils.readFileToString(f, StandardCharsets.UTF_8));
				} catch (IOException e) {
					logger.error("Error reading file for snapshot: {}", f.getName(), e);
				}
			}
		}

		return new UserMemorySnapshot(profileAttrs, userFiles);
	}

	/**
	 * Load the user's active chat session summaries for this bot.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @return list of session summaries
	 */
	public static List<UserSessionSummary> loadUserSessions(File datasetFolder, String rawUserId) {
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File sessionsFile = new File(userDir, SESSIONS_INDEX_FILE);
		if (!sessionsFile.exists() || !sessionsFile.isFile()) {
			return Collections.emptyList();
		}

		try {
			JsonNode node = Json.parse(FileUtils.readFileToString(sessionsFile, StandardCharsets.UTF_8));
			if (node != null && node.isArray()) {
				List<UserSessionSummary> list = new ArrayList<>();
				for (JsonNode item : node) {
					String id = item.path("id").asText("");
					String title = item.path("title").asText("Chat Session");
					long createdAt = item.path("createdAt").asLong(System.currentTimeMillis());
					long lastUpdatedAt = item.path("lastUpdatedAt").asLong(createdAt);
					if (!id.isEmpty()) {
						list.add(new UserSessionSummary(id, title, createdAt, lastUpdatedAt));
					}
				}
				list.sort((a, b) -> Long.compare(b.lastUpdatedAt(), a.lastUpdatedAt()));
				return list;
			}
		} catch (Exception e) {
			logger.error("Error loading user sessions for {}", rawUserId, e);
		}
		return Collections.emptyList();
	}

	/**
	 * Save or update a session summary in the user's sessions.json index.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @param sessionId     session UUID
	 * @param sessionTitle  title (e.g. derived from first prompt)
	 */
	public static synchronized void saveUserSession(File datasetFolder, String rawUserId, String sessionId,
			String sessionTitle) {
		if (sessionId == null || sessionId.trim().isEmpty()) {
			return;
		}
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File sessionsFile = new File(userDir, SESSIONS_INDEX_FILE);

		List<UserSessionSummary> existing = new ArrayList<>(loadUserSessions(datasetFolder, rawUserId));
		long now = System.currentTimeMillis();
		String cleanTitle = sessionTitle != null && !sessionTitle.trim().isEmpty() ? sessionTitle.trim()
				: "Chat Session";
		if (cleanTitle.length() > 50) {
			cleanTitle = cleanTitle.substring(0, 47) + "...";
		}

		// Check if exists
		boolean found = false;
		List<UserSessionSummary> updated = new ArrayList<>();
		for (UserSessionSummary s : existing) {
			if (s.id().equals(sessionId)) {
				// Update title only if existing has default title and new has custom title
				String finalTitle = (!s.title().equals("Chat Session") && cleanTitle.equals("Chat Session")) ? s.title()
						: cleanTitle;
				updated.add(new UserSessionSummary(sessionId, finalTitle, s.createdAt(), now));
				found = true;
			} else {
				updated.add(s);
			}
		}
		if (!found) {
			updated.add(0, new UserSessionSummary(sessionId, cleanTitle, now, now));
		}

		updated.sort((a, b) -> Long.compare(b.lastUpdatedAt(), a.lastUpdatedAt()));

		// Write to JSON
		ArrayNode arr = Json.newArray();
		for (UserSessionSummary s : updated) {
			ObjectNode o = Json.newObject();
			o.put("id", s.id());
			o.put("title", s.title());
			o.put("createdAt", s.createdAt());
			o.put("lastUpdatedAt", s.lastUpdatedAt());
			arr.add(o);
		}

		try {
			FileUtils.writeStringToFile(sessionsFile, Json.stringify(arr), StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error saving user sessions index for {}", rawUserId, e);
		}
	}

	/**
	 * Remove a session entry from the user's sessions.json index.
	 *
	 * @param datasetFolder dataset directory
	 * @param rawUserId     user identifier
	 * @param sessionId     session UUID to remove
	 */
	public static synchronized void deleteUserSession(File datasetFolder, String rawUserId, String sessionId) {
		if (sessionId == null || sessionId.trim().isEmpty()) {
			return;
		}
		File userDir = getUserDirectory(datasetFolder, rawUserId);
		File sessionsFile = new File(userDir, SESSIONS_INDEX_FILE);
		if (!sessionsFile.exists() || !sessionsFile.isFile()) {
			return;
		}

		List<UserSessionSummary> existing = loadUserSessions(datasetFolder, rawUserId);
		List<UserSessionSummary> updated = existing.stream()
				.filter(s -> !s.id().equals(sessionId))
				.collect(Collectors.toList());

		ArrayNode arr = Json.newArray();
		for (UserSessionSummary s : updated) {
			ObjectNode o = Json.newObject();
			o.put("id", s.id());
			o.put("title", s.title());
			o.put("createdAt", s.createdAt());
			o.put("lastUpdatedAt", s.lastUpdatedAt());
			arr.add(o);
		}

		try {
			FileUtils.writeStringToFile(sessionsFile, Json.stringify(arr), StandardCharsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error updating user sessions index for {}", rawUserId, e);
		}
	}
}

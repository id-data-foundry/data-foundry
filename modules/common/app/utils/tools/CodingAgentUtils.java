package utils.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.node.ObjectNode;

import models.DatasetType;
import play.Logger;
import play.libs.Json;

public class CodingAgentUtils {

	private static final Logger.ALogger logger = Logger.of(CodingAgentUtils.class);

	public static final String BOT_NAME = "boter";
	public static final String BOT_SUMMON_TAG = "@" + BOT_NAME;
	public static final String BOT_DISPLAY_NAME = "@" + BOT_NAME + " 🧈";

	public static final String SUBAGENT_NAME = "comrade roomboter";

	private static final String[] SUBAGENT_START_MESSAGES = {
			"🤖 " + SUBAGENT_NAME + " is rolling up their sleeves to build your changes... 🚀",
			"⚙️ Handing over to " + SUBAGENT_NAME + " to work on your files... 🛠️",
			"💻 " + SUBAGENT_NAME + " is diving into the codebase to implement your request... ✨",
			"🚀 " + SUBAGENT_NAME + " activated! Writing and refining code... ⚡",
			"🛠️ " + SUBAGENT_NAME + " is on it! Modifying files now... 🎨",
			"🔍 " + SUBAGENT_NAME + " is crafting changes in the workspace... Hang tight! 🔨" };

	private static final String[] SUBAGENT_COMPLETION_MESSAGES = {
			"✨ " + SUBAGENT_NAME + " finished updating your files!",
			"🎉 All done! " + SUBAGENT_NAME + " has completed the requested updates. 🚀",
			"✅ " + SUBAGENT_NAME + " finished executing the task successfully! 💻",
			"🙌 Changes applied! " + SUBAGENT_NAME + " handed control back to the agent. 🛠️",
			"🎯 Implementation complete! " + SUBAGENT_NAME + " updated your files. ✨",
			"⚡ Workspace updated successfully by " + SUBAGENT_NAME + "!" };

	/**
	 * Get a random start notification message for the sub-agent.
	 *
	 * @return randomized start notification
	 */
	public static String getRandomSubAgentStartMessage() {
		return SUBAGENT_START_MESSAGES[ThreadLocalRandom.current().nextInt(SUBAGENT_START_MESSAGES.length)];
	}

	/**
	 * Get a random completion notification message for the sub-agent.
	 *
	 * @return randomized completion notification
	 */
	public static String getRandomSubAgentCompletionMessage() {
		return SUBAGENT_COMPLETION_MESSAGES[ThreadLocalRandom.current().nextInt(SUBAGENT_COMPLETION_MESSAGES.length)];
	}

	/**
	 * Check whether a message contains a summoning tag for the agent (e.g. "@bot").
	 *
	 * @param message the chat message to inspect
	 * @return true if the agent is summoned, false otherwise
	 */
	public static boolean isAgentSummoned(String message) {
		if (message == null || message.trim().isEmpty()) {
			return false;
		}
		String tag = BOT_SUMMON_TAG.toLowerCase();
		return Pattern.compile("(?i)(^|\\s)" + Pattern.quote(tag) + "\\b").matcher(message).find()
				|| message.toLowerCase().contains(tag);
	}

	/**
	 * Format a user message with the sender's username for multi-user chat context in the agent memory.
	 *
	 * @param username the username of the sender
	 * @param message  the text message
	 * @return formatted string including sender identity
	 */
	public static String formatUserMessageForAgent(String username, String message) {
		if (username == null || username.trim().isEmpty()) {
			return message != null ? message : "";
		}
		return username + ": " + (message != null ? message : "");
	}

	/**
	 * Remove <think>...</think> tags and unclosed thinking traces from LLM output.
	 *
	 * @param text the raw LLM output
	 * @return cleaned text without thinking traces
	 */
	public static String cleanThinkingTags(String text) {
		if (text == null) {
			return "";
		}
		String cleaned = text;
		if (cleaned.contains("<think>")) {
			// Remove complete <think>...</think> blocks
			cleaned = cleaned.replaceAll("(?s)<think>.*?</think>", "");
			// Remove unclosed <think>... to end of string
			cleaned = cleaned.replaceAll("(?s)<think>.*$", "");
		} else if (cleaned.contains("</think>")) {
			// Model omitted opening <think> tag, remove thinking trace from start of string to first </think>
			cleaned = cleaned.replaceAll("(?s)^.*?</think>", "");
		}
		return cleaned.trim();
	}

	/**
	 * Filter out dataset and agent workspace filesystem paths from the text,
	 * replacing them with a generic "PATH".
	 *
	 * @param text          the text containing possible filesystem paths
	 * @param datasetFolder the base File representing the dataset directory
	 * @return text with dataset and workspace paths replaced by "PATH"
	 */
	public static String filterDatasetPath(String text, File datasetFolder) {
		if (text == null || text.isEmpty()) {
			return text;
		}
		try {
			Set<String> pathVariants = new LinkedHashSet<>();

			if (datasetFolder != null) {
				File agentscopeFolder = new File(datasetFolder, ".agentscope");
				collectPathVariants(pathVariants, agentscopeFolder);
				collectPathVariants(pathVariants, datasetFolder);
				collectPathVariants(pathVariants, datasetFolder.getParentFile());
			}

			// Add application / project root directory
			try {
				collectPathVariants(pathVariants, new File(""));
			} catch (Exception ignored) {
			}
			try {
				collectPathVariants(pathVariants, new File("."));
			} catch (Exception ignored) {
			}
			try {
				String userDir = System.getProperty("user.dir");
				if (userDir != null && !userDir.isEmpty()) {
					collectPathVariants(pathVariants, new File(userDir));
				}
			} catch (Exception ignored) {
			}

			List<String> sortedVariants = new ArrayList<>(pathVariants);
			sortedVariants.sort((a, b) -> Integer.compare(b.length(), a.length()));

			String result = text;
			for (String path : sortedVariants) {
				if (path != null && path.trim().length() > 3 && !path.equals("/") && !path.equals("\\")) {
					result = result.replace(path, "PATH");
				}
			}
			return result;
		} catch (Exception e) {
			logger.error("Error filtering dataset path from text", e);
			return text;
		}
	}

	private static void collectPathVariants(Set<String> variants, File folder) {
		if (folder == null) {
			return;
		}
		try {
			addCleanVariant(variants, folder.getAbsolutePath());
		} catch (Exception ignored) {
		}
		try {
			addCleanVariant(variants, folder.getCanonicalPath());
		} catch (Exception ignored) {
		}
		try {
			addCleanVariant(variants, folder.getPath());
		} catch (Exception ignored) {
		}
		try {
			addCleanVariant(variants, folder.toURI().getPath());
		} catch (Exception ignored) {
		}
	}

	private static void addCleanVariant(Set<String> variants, String rawPath) {
		if (rawPath == null) {
			return;
		}
		String p = rawPath.trim();
		while (p.endsWith("/.") || p.endsWith("\\.")) {
			p = p.substring(0, p.length() - 2);
		}
		while (p.endsWith("/") || p.endsWith("\\")) {
			if (p.length() <= 1) {
				break;
			}
			p = p.substring(0, p.length() - 1);
		}
		if (p.length() > 3 && !p.equals("/") && !p.equals("\\")) {
			variants.add(p);
			variants.add(p.replace('\\', '/'));
			variants.add(p.replace('/', '\\'));
		}
	}

	/**
	 * Get the standardized string representation of a dataset type for coding agents and metadata.
	 * COMPLETE dataset types are named as EXISTING to align with the DataFoundry UI.
	 *
	 * @param type the dataset type
	 * @return string representation of dataset type (e.g. EXISTING, IOT, ENTITY, etc.)
	 */
	public static String getDatasetType(DatasetType type) {
		if (type == null) {
			return "";
		}
		return type.toUIString();
	}

	/**
	 * Generate the map of API routes for a dataset based on its type and dataset ID.
	 *
	 * @param type      the dataset type
	 * @param datasetId the dataset ID
	 * @return ObjectNode mapping operation names to HTTP methods and route paths
	 */
	public static ObjectNode getDatasetApiRoutes(DatasetType type, Long datasetId) {
		ObjectNode routes = Json.newObject();
		if (type == null || datasetId == null) {
			return routes;
		}
		switch (type) {
		case IOT:
		case TIMESERIES:
			routes.put("log", "POST /api/v1/datasets/ts/" + datasetId);
			routes.put("downloadJson", "GET /datasets/download/json/" + datasetId);
			routes.put("downloadCsv", "GET /datasets/download/" + datasetId);
			break;
		case ENTITY:
			routes.put("getItem", "GET /api/v1/datasets/entity/" + datasetId);
			routes.put("addItem", "POST /api/v1/datasets/entity/" + datasetId);
			routes.put("updateItem", "PUT /api/v1/datasets/entity/" + datasetId);
			routes.put("deleteItem", "DELETE /api/v1/datasets/entity/" + datasetId);
			routes.put("downloadJson", "GET /datasets/download/json/" + datasetId);
			routes.put("downloadCsv", "GET /datasets/download/" + datasetId);
			break;
		case MEDIA:
			routes.put("uploadMedia", "POST /api/v1/datasets/media/" + datasetId);
			routes.put("getMedia", "GET /api/v1/datasets/media/" + datasetId + "/{filename}");
			routes.put("updateMedia", "PUT /api/v1/datasets/media/" + datasetId + "/{itemId}");
			routes.put("downloadJson", "GET /datasets/download/json/" + datasetId);
			routes.put("downloadCsv", "GET /datasets/download/" + datasetId);
			break;
		case COMPLETE:
			routes.put("uploadFile", "POST /api/v1/datasets/existing/" + datasetId);
			routes.put("downloadLatestFile", "GET /datasets/existing/downloadLatest/" + datasetId + "/{fileName}");
			routes.put("downloadFile", "GET /datasets/existing/download/" + datasetId + "/{fileId}");
			routes.put("web", "GET /datasets/web/" + datasetId + "/{filepath}");
			break;
		case ANNOTATION:
			routes.put("addRecord", "POST /api/v2/datasets/annotation/" + datasetId);
			routes.put("downloadJson", "GET /datasets/download/json/" + datasetId);
			routes.put("downloadCsv", "GET /datasets/download/" + datasetId);
			break;
		case DIARY:
			routes.put("addRecord", "POST /api/v2/datasets/diary/" + datasetId + "/{participant_id}");
			routes.put("downloadJson", "GET /datasets/download/json/" + datasetId);
			routes.put("downloadCsv", "GET /datasets/download/" + datasetId);
			break;
		case FORM:
			routes.put("record", "POST /datasets/form/record/" + datasetId);
			routes.put("downloadCsv", "GET /datasets/form/raw/" + datasetId + ".csv");
			routes.put("downloadJson", "GET /datasets/download/json/" + datasetId);
			break;
		case SURVEY:
			routes.put("record", "POST /datasets/survey/record/" + datasetId + "/{invite_token}");
			routes.put("downloadCsv", "GET /datasets/survey/raw/" + datasetId + ".csv");
			routes.put("downloadJson", "GET /datasets/download/json/" + datasetId);
			break;
		case MOVEMENT:
		case ES:
			routes.put("uploadFile", "POST /api/v2/datasets/upload/" + datasetId);
			routes.put("downloadJson", "GET /api/v2/datasets/download/" + datasetId + ".json");
			routes.put("downloadCsv", "GET /api/v2/datasets/download/" + datasetId + ".csv");
			break;
		case FITBIT:
			routes.put("heartrate", "GET /datasets/fitbit/heartrate/" + datasetId);
			routes.put("downloadJson", "GET /api/v2/datasets/download/" + datasetId + ".json");
			routes.put("downloadCsv", "GET /api/v2/datasets/download/" + datasetId + ".csv");
			break;
		case GOOGLEFIT:
			routes.put("heartrate", "GET /datasets/googlefit/heartrate/" + datasetId);
			routes.put("downloadJson", "GET /api/v2/datasets/download/" + datasetId + ".json");
			routes.put("downloadCsv", "GET /api/v2/datasets/download/" + datasetId + ".csv");
			break;
		case LINKED:
		default:
			routes.put("downloadJson", "GET /datasets/download/json/" + datasetId);
			routes.put("downloadCsv", "GET /datasets/download/" + datasetId);
			break;
		}
		return routes;
	}
}

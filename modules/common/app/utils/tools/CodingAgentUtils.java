package utils.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import play.Logger;

public class CodingAgentUtils {

	private static final Logger.ALogger logger = Logger.of(CodingAgentUtils.class);

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
}

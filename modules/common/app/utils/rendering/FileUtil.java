package utils.rendering;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import play.Environment;

public class FileUtil {

	private static final Set<String> ALLOWED_ENVIRONMENT_FOLDERS = Collections
			.unmodifiableSet(new HashSet<>(Arrays.asList("documentation", "content", "templates", "announcements", "domain", "id")));

	/**
	 * Retrieve a safe environment folder (e.g. dist/documentation, dist/templates, etc.)
	 *
	 * @param environment Play environment
	 * @param folderName the base folder name (e.g., "documentation", "templates")
	 * @return Optional containing the resolved directory if valid and safe
	 */
	public static Optional<File> getEnvironmentFolder(Environment environment, String folderName) {
		if (environment == null || folderName == null || folderName.trim().isEmpty()) {
			return Optional.empty();
		}

		String cleanFolderName = folderName.trim();
		while (cleanFolderName.endsWith("/")) {
			cleanFolderName = cleanFolderName.substring(0, cleanFolderName.length() - 1);
		}
		while (cleanFolderName.startsWith("/")) {
			cleanFolderName = cleanFolderName.substring(1);
		}

		if (!ALLOWED_ENVIRONMENT_FOLDERS.contains(cleanFolderName)) {
			return Optional.empty();
		}

		File root = environment.rootPath();
		File canonicalRoot;
		File parent;
		String rootCanon;
		String parentCanon;
		try {
			canonicalRoot = root.getCanonicalFile();
			parent = canonicalRoot.getParentFile();
			rootCanon = canonicalRoot.getCanonicalPath();
			if (!rootCanon.endsWith(File.separator)) {
				rootCanon += File.separator;
			}
			if (parent != null) {
				parentCanon = parent.getCanonicalPath();
				if (!parentCanon.endsWith(File.separator)) {
					parentCanon += File.separator;
				}
			} else {
				parentCanon = rootCanon;
			}
		} catch (IOException e) {
			return Optional.empty();
		}

		List<File> candidates = Arrays.asList(
				new File(canonicalRoot, "dist/" + cleanFolderName),
				new File(canonicalRoot, "DataFoundry/dist/" + cleanFolderName),
				parent != null ? new File(parent, "dist/" + cleanFolderName) : null,
				parent != null ? new File(parent, "DataFoundry/dist/" + cleanFolderName) : null,
				new File(canonicalRoot, cleanFolderName),
				parent != null ? new File(parent, cleanFolderName) : null
		);

		for (File candidate : candidates) {
			if (candidate != null && candidate.exists() && candidate.isDirectory()) {
				try {
					String folderCanon = candidate.getCanonicalPath();
					if (!folderCanon.endsWith(File.separator)) {
						folderCanon += File.separator;
					}
					// Verify folder is within root workspace or its parent
					if (folderCanon.startsWith(rootCanon) || folderCanon.startsWith(parentCanon)) {
						return Optional.of(candidate);
					}
				} catch (IOException e) {
					// continue searching candidates
				}
			}
		}

		return Optional.empty();
	}

	/**
	 * Retrieve a safe file or directory within a folder, verifying canonical path containment.
	 *
	 * @param folder the base folder
	 * @param relativePath the relative file/directory path
	 * @return Optional containing the file or directory if it exists and is contained within folder
	 */
	public static Optional<File> getSafePathInFolder(File folder, String relativePath) {
		if (folder == null || relativePath == null || relativePath.trim().isEmpty()) {
			return Optional.empty();
		}

		File target = new File(folder, relativePath.trim());
		try {
			String canonicalDir = folder.getCanonicalPath();
			if (!canonicalDir.endsWith(File.separator)) {
				canonicalDir += File.separator;
			}
			String canonicalTarget = target.getCanonicalPath();
			if (canonicalTarget.startsWith(canonicalDir) || canonicalTarget.equals(folder.getCanonicalPath())) {
				if (target.exists()) {
					return Optional.of(target);
				}
			}
		} catch (IOException e) {
			// ignore and return empty
		}

		return Optional.empty();
	}

	/**
	 * check whether a file within a folder is safe
	 * 
	 * @param folder
	 * @param fileName
	 * @return
	 */
	public static boolean isFileInFolderSafe(File folder, String fileName) {
		// check inputs
		if (folder == null || fileName == null || fileName.isEmpty()) {
			return false;
		}

		File file = new File(folder, fileName);
		boolean isSafe = false;
		try {
			String canonicalDir = folder.getCanonicalPath();
			if (!canonicalDir.endsWith(File.separator)) {
				canonicalDir += File.separator;
			}
			if (file.getCanonicalPath().startsWith(canonicalDir)) {
				isSafe = true;
			}
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}

		return isSafe && file.exists() && file.isFile();
	}

	/**
	 * retrieve file in folder if safe
	 * 
	 * @param folder
	 * @param fileName
	 * @return
	 */
	public static Optional<File> getSafeFileInFolder(File folder, String fileName) {
		// check inputs
		if (folder == null || fileName == null || fileName.isEmpty()) {
			return Optional.empty();
		}

		File file = new File(folder, fileName);
		boolean isSafe = false;
		try {
			String canonicalDir = folder.getCanonicalPath();
			if (!canonicalDir.endsWith(File.separator)) {
				canonicalDir += File.separator;
			}
			if (file.getCanonicalPath().startsWith(canonicalDir)) {
				isSafe = true;
			}
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}

		return isSafe && file.exists() && file.isFile() ? Optional.of(file) : Optional.empty();
	}

	/**
	 * check whether a folder within a folder is safe
	 * 
	 * @param folder
	 * @param folderName
	 * @return
	 */
	public static boolean isFolderInFolderSafe(File folder, String folderName) {
		// check inputs
		if (folder == null || folderName == null || folderName.isEmpty()) {
			return false;
		}

		File folderInFolder = new File(folder, folderName);
		boolean isSafe = false;
		try {
			String canonicalDir = folder.getCanonicalPath();
			if (!canonicalDir.endsWith(File.separator)) {
				canonicalDir += File.separator;
			}
			if (folderInFolder.getCanonicalPath().startsWith(canonicalDir)) {
				isSafe = true;
			}
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}

		return isSafe && folderInFolder.exists() && folderInFolder.isDirectory();
	}

}

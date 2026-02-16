package utils.rendering;

import java.io.File;
import java.util.Optional;

public class FileUtil {

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

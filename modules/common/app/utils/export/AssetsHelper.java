package utils.export;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import play.Logger;

public class AssetsHelper {

	private static final Logger.ALogger logger = Logger.of(AssetsHelper.class);

	public static void zipString(ZipOutputStream zipOut, String entryName, String contentToZip) {
		ZipEntry zipEntry = new ZipEntry(entryName);
		try {
			zipOut.putNextEntry(zipEntry);
			zipOut.write(contentToZip.getBytes(Charset.defaultCharset()));
			zipOut.closeEntry();
		} catch (IOException e) {
			logger.error("Error zipping string content for entry " + entryName, e);
		}
	}

	public static void zipFile(ZipOutputStream zipOut, String entryName, Optional<File> contentToZip) {
		// only zip if file is present
		if (!contentToZip.isPresent()) {
			return;
		}

		zipFile(zipOut, entryName, contentToZip.get());
	}

	public static void zipFile(ZipOutputStream zipOut, String entryName, File contentToZip) {
		if (contentToZip == null || !contentToZip.exists()) {
			return;
		}

		ZipEntry zipEntry = new ZipEntry(entryName);
		try {
			zipOut.putNextEntry(zipEntry);
			java.nio.file.Files.copy(contentToZip.toPath(), zipOut);
			zipOut.closeEntry();
		} catch (IOException e) {
			logger.error("Error zipping file " + contentToZip.getAbsolutePath() + " for entry " + entryName, e);
		}
	}

}

package utils.validators;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.tika.Tika;

import com.google.common.io.Files;

import play.Logger;
import play.libs.Files.TemporaryFile;
import play.mvc.Http;

/**
 * Helper for robust, content-based file type detection and validation.
 *
 * - Uses Apache Tika for content-based MIME detection (preferred). - Falls back to a few lightweight magic-byte checks
 * for common types. - Maintains internal allowlists for common categories (IMAGE, AUDIO, VIDEO, DOCUMENT, CSV). -
 * Provides helpers to validate a MultipartFile.FilePart (TemporaryFile) and to log mismatches.
 *
 * Usage: - FileTypeUtils.isAllowed(tempFile.path().toFile(), FileTypeUtils.FileCategory.IMAGE) -
 * FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.IMAGE)
 *
 */
public final class FileTypeUtils {

	private static final Logger.ALogger logger = Logger.of(FileTypeUtils.class);

	private static final Tika tika = new Tika();

	public enum FileCategory {
		IMAGE, AUDIO, VIDEO, DOCUMENT, CSV, XML, PDF, ANY
	}

	// Internal allowlists (can be adjusted centrally here)
	private static final Set<String> IMAGE_MIMES;
	private static final Set<String> AUDIO_MIMES;
	private static final Set<String> VIDEO_MIMES;
	private static final Set<String> DOCUMENT_MIMES;
	private static final Set<String> CSV_MIMES;
	private static final Set<String> XML_MIMES;
	private static final Set<String> PDF_MIMES;

	// Map category -> set for quick lookup
	private static final Map<FileCategory, Set<String>> CATEGORY_MAP;

	static {
		Set<String> images = new HashSet<>();
		images.add("image/jpeg");
		images.add("image/png");
		images.add("image/apng");
		images.add("image/gif");
		images.add("image/webp");
		images.add("image/bmp");
		images.add("image/svg+xml");
		images.add("image/avif");
		// keep a generic entry used by isAllowedMime check
		images.add("image/*");

		Set<String> audio = new HashSet<>();
		audio.add("audio/mpeg");
		audio.add("audio/mp3");
		audio.add("audio/wav");
		audio.add("audio/ogg");
		audio.add("audio/webm");
		audio.add("audio/*");

		Set<String> video = new HashSet<>();
		video.add("video/mp4");
		video.add("video/webm");
		video.add("video/ogg");
		video.add("video/*");

		Set<String> docs = new HashSet<>();
		docs.add("application/pdf");
		docs.add("text/html");
		docs.add("application/xhtml+xml");
		docs.add("application/xml");
		docs.add("text/xml");
		docs.add("text/calendar");
		docs.add("text/css");
		docs.add("text/javascript");
		docs.add("application/json");
		docs.add("application/ld+json");
		docs.add("text/markdown");
		docs.add("text/plain");
		docs.add("text/*");

		Set<String> csvs = new HashSet<>();
		csvs.add("text/csv");
		csvs.add("application/csv");
		csvs.add("text/plain"); // permissive fallback for csv-like content

		Set<String> xmls = new HashSet<>();
		xmls.add("text/xml");
		xmls.add("application/xml");
		xmls.add("application/gpx+xml");
		xmls.add("text/plain"); // permissive fallback for xml-like content

		Set<String> pdfs = new HashSet<>();
		pdfs.add("application/pdf");

		IMAGE_MIMES = Collections.unmodifiableSet(images);
		AUDIO_MIMES = Collections.unmodifiableSet(audio);
		VIDEO_MIMES = Collections.unmodifiableSet(video);
		DOCUMENT_MIMES = Collections.unmodifiableSet(docs);
		CSV_MIMES = Collections.unmodifiableSet(csvs);
		XML_MIMES = Collections.unmodifiableSet(xmls);
		PDF_MIMES = Collections.unmodifiableSet(pdfs);

		Map<FileCategory, Set<String>> m = new HashMap<>();
		m.put(FileCategory.IMAGE, IMAGE_MIMES);
		m.put(FileCategory.AUDIO, AUDIO_MIMES);
		m.put(FileCategory.VIDEO, VIDEO_MIMES);
		m.put(FileCategory.DOCUMENT, DOCUMENT_MIMES);
		m.put(FileCategory.CSV, CSV_MIMES);
		m.put(FileCategory.XML, XML_MIMES);
		m.put(FileCategory.PDF, PDF_MIMES);
		m.put(FileCategory.ANY, Collections.emptySet());
		CATEGORY_MAP = Collections.unmodifiableMap(m);
	}

	private FileTypeUtils() {
		// static utility
	}

	/**
	 * Detect MIME type using Apache Tika (content-based). If Tika fails, fall back to magic number checks for some
	 * common formats. Never trust filename extension or multipart header.
	 *
	 * @param f file to inspect
	 * @return detected MIME string or "application/octet-stream" on unknown
	 */
	public static String detectMime(File f) {
		if (f == null || !f.exists()) {
			return "application/octet-stream";
		}

		// Try Tika first
		try {
			String detected = tika.detect(f);
			if (detected != null && !detected.isEmpty()) {
				return detected;
			}
		} catch (Throwable t) {
			// don't fail hard, fall back to magic checks below
			logger.warn("Tika detection failed: " + t.getMessage());
		}

//		// Fallback quick magic checks (read small header)
//		try (FileInputStream is = new FileInputStream(f)) {
//			byte[] header = new byte[16];
//			int read = is.read(header);
//			if (read >= 4) {
//				// PNG: 89 50 4E 47
//				if ((header[0] & 0xFF) == 0x89 && header[1] == 0x50 && header[2] == 0x4E && header[3] == 0x47) {
//					return "image/png";
//				}
//				// JPG: FF D8 FF
//				if ((header[0] & 0xFF) == 0xFF && (header[1] & 0xFF) == 0xD8 && (header[2] & 0xFF) == 0xFF) {
//					return "image/jpeg";
//				}
//				// GIF: "GIF8"
//				if (header[0] == 'G' && header[1] == 'I' && header[2] == 'F') {
//					return "image/gif";
//				}
//				// WebP: "RIFF" .... "WEBP"
//				if (read >= 12 && header[0] == 'R' && header[1] == 'I' && header[2] == 'F' && header[3] == 'F'
//				        && header[8] == 'W' && header[9] == 'E' && header[10] == 'B' && header[11] == 'P') {
//					return "image/webp";
//				}
//				// PDF: "%PDF"
//				if (header[0] == '%' && header[1] == 'P' && header[2] == 'D' && header[3] == 'F') {
//					return "application/pdf";
//				}
//				// ZIP (might be docx, xlsx, general zip): "PK\003\004"
//				if ((header[0] & 0xFF) == 0x50 && (header[1] & 0xFF) == 0x4B && (header[2] & 0xFF) == 0x03
//				        && (header[3] & 0xFF) == 0x04) {
//					return "application/zip";
//				}
//			}
//		} catch (IOException e) {
//			logger.warn("Magic detection failed: " + e.getMessage());
//		}

		return "application/octet-stream";
	}

	/**
	 * Convenience: detect MIME for file and check against category allowlist.
	 *
	 * @param file     file on disk
	 * @param category category to allow
	 * @return true if the content-based mime is allowed
	 */
	public static boolean isAllowed(File file, FileCategory category) {
		String mime = detectMime(file);
		boolean ok = isAllowedMime(mime, category);
		if (!ok) {
			logger.warn("File rejected by content-based check. Detected MIME=" + mime + " for file=" + file.getName()
					+ " (category=" + category + ")");
		} else {
			logger.debug("File accepted by content-based check. Detected MIME=" + mime + " for file=" + file.getName()
					+ " (category=" + category + ")");
		}
		return ok;
	}

	/**
	 * Check whether the detected MIME for a file belongs to the allowlist for the given category. If category == ANY,
	 * always returns true.
	 *
	 * @param mime     detected mime
	 * @param category category to check
	 * @return true if allowed
	 */
	public static boolean isAllowedMime(String mime, FileCategory category) {
		if (category == FileCategory.ANY) {
			return true;
		}
		if (mime == null || mime.isEmpty()) {
			return false;
		}
		Set<String> allow = CATEGORY_MAP.getOrDefault(category, Collections.emptySet());
		if (allow.isEmpty()) {
			return false;
		}
		if (allow.contains(mime)) {
			return true;
		}
		// permit wildcard like image/*
		if (mime.contains("/") && allow.contains(mime.split("/")[0] + "/*")) {
			return true;
		}
		return false;
	}

	/**
	 * Expose the internal allowed mimetypes for a category (read-only).
	 */
	public static Set<String> getAllowedMimes(FileCategory category) {
		return CATEGORY_MAP.getOrDefault(category, Collections.emptySet());
	}

	/**
	 * Validate a Multipart FilePart<TemporaryFile> and log useful information.
	 *
	 * - Uses the stored temporary file to detect mime (trusted). - Compares with declared content type from the
	 * multipart part (not trusted). - Logs mismatch and returns false if detected mime is not allowed for the category.
	 *
	 * @param fp       MultipartFile part (may be null)
	 * @param category expected category
	 * @return true if the file is allowed (and logged)
	 */
	public static boolean validateAndLog(Http.MultipartFormData.FilePart<TemporaryFile> fp, FileCategory category) {
		if (fp == null) {
			logger.warn("validateAndLog: file part is null");
			return false;
		}
		TemporaryFile tmp = fp.getRef();
		if (tmp == null) {
			logger.warn("validateAndLog: temporary file missing for " + fp.getFilename());
			return false;
		}

		File f = tmp.path().toFile();
		String detected = detectMime(f);
		String declared = fp.getContentType();

		if (declared == null) {
			declared = "";
		}

		// Log both declared and detected, keep logging for audit
		if (!declared.isEmpty() && !declared.equals(detected)) {
			logger.warn("Content-Type DECLARED=" + declared + " but DETECTED=" + detected + " for file="
					+ fp.getFilename());
		} else {
			logger.debug(
					"Content-Type declared=" + declared + " detected=" + detected + " for file=" + fp.getFilename());
		}

		boolean allowed = isAllowedMime(detected, category);
		if (!allowed) {
			logger.error("Upload rejected: file " + fp.getFilename() + " detected as " + detected
					+ " which is not allowed for category " + category);
		}
		return allowed;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether this filename seems to be an executable file just from its file extension.
	 * 
	 * note: excluding .js files which are very useful in a Complete DS and also cannot be prevented anyway if one
	 * includes the JS in an HTML document
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeExecutableFile(String fileName) {
		String[] executableFileExtensions = { "exe", "bat", "com", "cmd", "inf", "ipa", "osx", "pif", "run", "wsf",
				"wsh", "app", "bin", "cpl", "gadget", "ins", "msi", "msp", "mst", "reg", "scr", "vb", "vbs", "workflow",
				"sh", "ksh", "pyc", "pyo", "elf", "out", "run", "jsx", "ahk", "awk", "chm", "jse", "kix", "mcr", "mel",
				"rbx", "sca", "scb", "udf", "upx", "url", "vpm", "wcm", "wiz", "wpm", "xap", "xbap", "xlam", "xltm",
				"zlx", "jar", "class", "php", "php3", "php4", "phar", "phtml", "inc", "ps1", "docx", "xlsx", "pl" };

		String extension = FileTypeUtils.getExtension(fileName).toLowerCase();
		return Arrays.stream(executableFileExtensions).anyMatch(ext -> extension.equalsIgnoreCase(ext));
	}

	/**
	 * check whether this file seems to be a file for a machine in the lab
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeLabMachineFile(String fileName) {
		String[] documentFileExtensions = { //
				// image manipulation / adobe tools
				"png", "ai", "psd", "eps",
				// knit
				"shp", "stp",
				// embroidery
				"pes", "svg", "emf", // + ai
				// vinyl cutter
				"cst" // + ai
		};

		String extension = FileTypeUtils.getExtension(fileName).toLowerCase();
		return Arrays.stream(documentFileExtensions).anyMatch(ext -> extension.equalsIgnoreCase(ext));
	}

	/**
	 * check whether this file seems to be a CSV file
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeCSVFile(String fileName) {
		fileName = fileName.trim().toLowerCase();
		return fileName.endsWith(".csv");
	}

	/**
	 * check whether this file seems editable in a DF text editor or Starboard or TwineJS
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeEditableFile(String fileName) {

		// check for text file
		if (FileTypeUtils.lookLikeTextFile(fileName)) {
			return true;
		}

		// otherwise it might still be a Starboard or TwineJS file
		String[] documentFileExtensions = { "gg", "tw", "twee" };

		String extension = FileTypeUtils.getExtension(fileName).toLowerCase();
		return Arrays.stream(documentFileExtensions).anyMatch(ext -> extension.equalsIgnoreCase(ext));
	}

	/**
	 * check whether this file seems to be a document file
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeDocumentFile(String fileName) {
		String[] documentFileExtensions = { "pdf", "txt", "rtf", "md" };

		String extension = FileTypeUtils.getExtension(fileName).toLowerCase();
		return Arrays.stream(documentFileExtensions).anyMatch(ext -> extension.equalsIgnoreCase(ext));
	}

	/**
	 * check whether this file seems to be an audio file
	 * 
	 * @param file
	 * @return
	 */
	public static boolean looksLikeAudioFile(File file) {
		String fileName = file.getName().trim().toLowerCase();
		return fileName.endsWith(".mp3") || fileName.endsWith(".ogg");
	}

	/**
	 * check whether this file seems to be an movie file
	 * 
	 * @param file
	 * @return
	 */
	public static boolean looksLikeVideoFile(File file) {
		String fileName = file.getName().trim().toLowerCase();
		return fileName.endsWith(".mp4");
	}

	/**
	 * check whether this file seems to be an movie file
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeVideoFile(String fileName) {
		fileName = fileName.trim().toLowerCase();
		return fileName.endsWith(".mp4");
	}

	/**
	 * check whether this file seems to be an image file
	 * 
	 * @param file
	 * @return
	 */
	public static boolean looksLikeImageFile(File file) {
		String fileName = file.getName().trim().toLowerCase();
		return fileName.endsWith(".png") || fileName.endsWith(".jpg") || fileName.endsWith(".jpeg")
				|| fileName.endsWith(".gif");
	}

	/**
	 * check whether this file seems to be an image file
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeImageFile(String fileName) {
		fileName = fileName.trim().toLowerCase();
		return fileName.endsWith(".png") || fileName.endsWith(".jpg") || fileName.endsWith(".jpeg")
				|| fileName.endsWith(".gif");
	}

	/**
	 * check whether this file seems to be an audio file
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeAudioFile(String fileName) {
		fileName = fileName.trim().toLowerCase();
		return fileName.endsWith(".mp3") || fileName.endsWith(".ogg");
	}

	/**
	 * check whether this file seems to be a text file (that can be edited in a text editor)
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean lookLikeTextFile(String fileName) {
		String[] documentFileExtensions = { "txt", "md", "html", "css", "js", "json" };

		String extension = FileTypeUtils.getExtension(fileName).toLowerCase();
		return Arrays.stream(documentFileExtensions).anyMatch(ext -> extension.equalsIgnoreCase(ext));
	}

	/**
	 * check whether this file seems to be a movement data file
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean looksLikeMovementDataFile(String fileName) {
		fileName = fileName.trim().toLowerCase();
		return fileName.endsWith(".gpx") || fileName.endsWith(".xml");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * sanitize filenames: first replace all illegal chars, then collapse potential double underscores
	 * 
	 * @param fileName
	 * @return
	 */
	public static String sanitizeFilename(String fileName) {
		return fileName.replaceAll("[^\\p{L}\\p{N}._-]", "_").replace("__", "_");
	}

	/**
	 * shorten length file name to <code>maxLength</code>: shorten the file name before extension, then append extension
	 * 
	 * @param fileName
	 * @param maxLength
	 * @return
	 */
	public static String shortenFilename(String fileName, int maxLength) {
		// check filename length and shorten if needed
		if (fileName.length() > maxLength) {
			String extension = fileName.substring(fileName.lastIndexOf("."));
			fileName = fileName.substring(0, Math.min(fileName.lastIndexOf("."), maxLength - (extension.length() + 1)))
					+ extension;
		}

		return fileName;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * retrieve the extension of the given filename
	 * 
	 * @param fileName
	 * @return
	 */
	public static String getExtension(String fileName) {
		return Files.getFileExtension(fileName).toLowerCase();
	}

	/**
	 * retrieve the name of the given filename without extension
	 * 
	 * @param fileName
	 * @return
	 */
	public static String getNameWithoutExtension(String fileName) {
		return Files.getNameWithoutExtension(fileName);
	}
}
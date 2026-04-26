package services.processing;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.opendataloader.pdf.api.OpenDataLoaderPDF;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import play.Environment;
import play.Logger;
import play.cache.SyncCacheApi;
import utils.conf.ConfigurationUtils;

@Singleton
public class MediaProcessingService {

	public static final int EXPIRATION_TIMEOUT_SECS = 60 * 60 * 12;

	private final String PYTHON_INTERPRETER;

	private static final Logger.ALogger logger = Logger.of(MediaProcessingService.class);

	private final int STT_PROCESSING_JOBS = 20;
	private final Semaphore jobTickets = new Semaphore(STT_PROCESSING_JOBS);

	// we use a single thread executor to ensure that STT jobs will not run in parallel
	private final Executor executor = Executors.newSingleThreadExecutor();
	private final boolean isProduction;
	private final SyncCacheApi cache;

	@Inject
	public MediaProcessingService(Environment env, Config configuration, SyncCacheApi cache) {
		this.isProduction = env.isProd();
		this.cache = cache;
		if (configuration.hasPath(ConfigurationUtils.DF_PROCESSING_PYTHON)) {
			PYTHON_INTERPRETER = configuration.getString(ConfigurationUtils.DF_PROCESSING_PYTHON);
		} else {
			PYTHON_INTERPRETER = "python3";
		}
	}

	public int getEnqueuedJobs() {
		return STT_PROCESSING_JOBS - jobTickets.availablePermits();
	}

	public CompletionStage<String> scheduleMediaToTextProcess(File inputFile, String language, String type, String user,
			String internalToken) {
		return CompletableFuture.supplyAsync(() -> run(inputFile, language, type, user, internalToken), executor);
	}

	/**
	 * run the processing request
	 * 
	 * @param inputFile
	 * @param language
	 * @param type
	 * @param user
	 * @param internalToken
	 * @return
	 */
	private String run(File inputFile, String language, String type, String user, String internalToken) {

		// set the cache with a waiting message
		cache.set(internalToken, "waiting for data...", EXPIRATION_TIMEOUT_SECS);

		// wait for an empty processing slot
		try {
			jobTickets.acquire();
		} catch (InterruptedException e) {
		}

		if (!inputFile.exists()) {
			return "[ERROR] File missing.";
		}

		// check file type and route request
		String result = "";
		if (type.startsWith("image/")) {
			// process image
			result = processImage(inputFile, language, user, internalToken);
		} else if (type.startsWith("application/pdf")) {
			// process PDF file
			result = processPDF(inputFile, user, internalToken);
		} else if (type.startsWith("text/")) {
			// process text or markdown file
			result = processText(inputFile, user, internalToken);
		} else {
			// now we know that the engine can work with the files
			result = processAudio(inputFile, language, user, internalToken);
		}

		jobTickets.release();

		return result;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * process the input file as an image and use OCR to extra the text
	 * 
	 * @param inputFile
	 * @param language
	 * @param user
	 * @param internalToken
	 * @return
	 */
	private String processImage(File inputFile, String language, String user, String internalToken) {

		// check if script is available
		String script = language.equals("nl") ? "process_img_nl.py" : "process_img.py";
		if (new File("../processing/stt/" + script).exists()) {
			script = "../processing/stt/" + script;
		} else {
			if (isProduction) {
				script = "processing/stt/" + script;
			} else {
				script = "dist/processing/stt/" + script;
			}
		}

		final String[] command = new String[] { PYTHON_INTERPRETER, script, inputFile.getAbsolutePath() };

		StringBuilder sb = new StringBuilder();
		ProcessBuilder pb = new ProcessBuilder(command);
		logger.info("OCR calling: " + pb.command().stream().collect(Collectors.joining(" ")) + " in directory "
				+ (pb.directory() != null ? pb.directory().getAbsolutePath() : "?") + "...");
		try {
			Process p = pb.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");

				// update cache for independent retrieval
				cache.set(internalToken, sb.toString(), EXPIRATION_TIMEOUT_SECS);
			}
			p.waitFor();
		} catch (IOException | InterruptedException e) {
			logger.error("OCR problem I/O or Interrupted Exception", e);
			sb.append("The OCR engine reported an I/O error.");
		} catch (Exception e) {
			logger.error("OCR problem Exception", e);
			sb.append("The OCR engine reported a general error.");
		} finally {
			// clean up the input and audio files
			try {
				inputFile.delete();
			} catch (Exception e) {
				// do nothing, just close
			}
		}

		String output = sb.toString();
		if (output.length() == 0) {
			output = "No text was detected in the uploaded image file.";
		}

		cache.set(internalToken, output + " [END]", EXPIRATION_TIMEOUT_SECS);
		return output;
	}

	/**
	 * process the input file as a text or markdown file
	 *
	 * @param inputFile
	 * @param user
	 * @param internalToken
	 * @return
	 */
	private String processText(File inputFile, String user, String internalToken) {

		String output = "";
		try {
			output = java.nio.file.Files.readString(inputFile.toPath());
		} catch (Exception e) {
			logger.error("Text processing error", e);
		} finally {
			// delete input file
			inputFile.delete();
		}

		// update cache for independent retrieval
		cache.set(internalToken, output, EXPIRATION_TIMEOUT_SECS);

		return output;
	}

	/**
	 * process the input file as a PDF file
	 * 
	 * @param inputFile
	 * @param user
	 * @param internalToken
	 * @return
	 */
	private String processPDF(File inputFile, String user, String internalToken) {

		File removeFolder = null;

		String output = "";
		try {
			// create working folder for PDF operations
			File workingFolder = new File(inputFile.getParent(), "pdfprocessing" + System.currentTimeMillis());
			workingFolder.mkdir();

			// check folder
			if (!workingFolder.exists()) {
				return "";
			}

			// mark folder for removal
			removeFolder = workingFolder;

			// configuration of PDF worker
			org.opendataloader.pdf.api.Config config = new org.opendataloader.pdf.api.Config();
			config.setOutputFolder(workingFolder.getAbsolutePath());
			config.setAddImageToMarkdown(false);
			config.setUseHTMLInMarkdown(false);
			config.setGenerateMarkdown(true);
			config.setGeneratePDF(false);
			config.setGenerateText(false);
			config.setGenerateJSON(false);
			config.setGenerateHtml(false);
			config.setKeepLineBreaks(true);
			config.setReplaceInvalidChars("?");

			// run PDF processing
			OpenDataLoaderPDF.processFile(inputFile.getAbsolutePath(), config);

			// read from output markdown file and delete it afterwards
			File[] outputFiles = workingFolder.listFiles();
			for (File file : outputFiles) {
				if (file.isFile()) {
					output = Files.readString(file.toPath());
					file.delete();
				} else {
					Arrays.stream(file.listFiles()).forEach(f -> f.delete());
				}
				file.delete();
			}

			// delete dir
			workingFolder.delete();
		} catch (Exception e) {
			// do nothing, just close
			logger.error("PDF processing error", e);
		} finally {

			// delete input file
			inputFile.delete();

			// remove processing folder
			if (removeFolder != null && removeFolder.exists()) {
				removeFolder.delete();
			}
		}

		// remove markdown image links
		output = output.replaceAll("!\\[[^\\]]*\\]\\([^)]*\\)", "");

		// remove excessive blank lines
		output = output.replaceAll("(?m)(^[ \t]*\\r?\\n){3,}", "\n\n");

		// update cache for independent retrieval
		cache.set(internalToken, output, EXPIRATION_TIMEOUT_SECS);

		return output;
	}

	/**
	 * process the input file as a media file. first extract audio track, convert sampling rate, then run speech
	 * recognition with the given language (relevant for model selection)
	 * 
	 * @param inputFile
	 * @param language
	 * @param user
	 * @param internalToken
	 * @return
	 */
	private String processAudio(File inputFile, String language, String user, String internalToken) {

		// output string
		StringBuilder sb = new StringBuilder();

		// pre-process and check the input file
		final File canonicalWavFile = preProcessFile(inputFile);
		if (!canonicalWavFile.exists() || !canonicalWavFile.canRead() || canonicalWavFile.length() < 100) {
			// abort if no suitable media
			sb.append(
					"[ERROR] Media stream processing error. Please check that the uploaded file contains an audio stream.");
		} else {
			// language / model selection
			String script;
			if (language.equals("nl")) {
				script = "process_nl.py";
			} else if (language.equals("en_sm")) {
				script = "process_sm.py";
			} else {
				script = "process.py";
			}

			// check if script is available
			if (new File("../processing/stt/" + script).exists()) {
				script = "../processing/stt/" + script;
			} else {
				if (isProduction) {
					script = "processing/stt/process.py";
				} else {
					script = "dist/processing/stt/process.py";
				}
			}
			final String[] command = new String[] { PYTHON_INTERPRETER, script, canonicalWavFile.getAbsolutePath() };

			ProcessBuilder pb = new ProcessBuilder(command);
			logger.info("SpeechToText calling (" + language + "): "
					+ pb.command().stream().collect(Collectors.joining(" ")) + " in directory "
					+ (pb.directory() != null ? pb.directory().getAbsolutePath() : "?") + "...");
			try {
				Process p = pb.start();
				BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String line;
				while ((line = reader.readLine()) != null) {
					sb.append(line + "\n");

					// update cache for independent retrieval
					cache.set(internalToken, sb.toString(), EXPIRATION_TIMEOUT_SECS);
				}
				p.waitFor();
			} catch (IOException | InterruptedException e) {
				logger.error("SpeechToText problem I/O or Interrupted Exception", e);
				sb.append("The speech detection engine reported an I/O error.");
			} catch (Exception e) {
				logger.error("SpeechToText problem Exception", e);
				sb.append("The speech detection engine reported a general error.");
			}
		}

		// clean up the input and audio files
		try {
			inputFile.delete();
		} catch (Exception e) {
			// do nothing, just close
		}
		try {
			canonicalWavFile.delete();
		} catch (Exception e) {
			// do nothing, just close
		}

		String output = sb.toString();
		if (output.length() == 0) {
			output = "No speech was detected in the uploaded media file. Please check that the speech is clear and that there is only little background noise.";
		}

		cache.set(internalToken, output + " [END]", EXPIRATION_TIMEOUT_SECS);

		return output;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * process the input text and generate a speech file; assumes sanitized text input and language arguments
	 * 
	 * @param input
	 * @param outputFile
	 * @param user
	 * @throws Exception
	 */
	public synchronized void processText(String input, File outputFile, String language, String user) throws Exception {
		// check if script is available
		final String[] command = new String[] { "espeak", "\"" + input + "\"", "-v" + language, "-s160", "-w",
				outputFile.getAbsolutePath() };

		ProcessBuilder pb = new ProcessBuilder(command);
		logger.info("Speech generation calling: " + pb.command().stream().collect(Collectors.joining(" "))
				+ " in directory " + (pb.directory() != null ? pb.directory().getAbsolutePath() : "?") + "...");
		try {
			Process p = pb.start();
			p.waitFor();
		} catch (IOException | InterruptedException e) {
			logger.error("Speech generation problem I/O or Interrupted Exception", e);
			throw e;
		} catch (Exception e) {
			logger.error("Speech generation problem exception", e);
			throw e;
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * preprocess the input file with ffmpeg so that we have a new file with the right sample rate and channels
	 * 
	 * @param inputFile
	 * @return
	 */
	private File preProcessFile(File inputFile) {

		File resultFileName = new File(
				inputFile.getParentFile().getAbsolutePath() + File.separator + System.currentTimeMillis() + ".wav");

		String[] command = new String[] { "ffmpeg", "-i", inputFile.getAbsolutePath(), "-ar", "16000", "-ac", "1",
				resultFileName.getAbsolutePath() };

		ProcessBuilder pb = new ProcessBuilder(command);

		logger.info("SpeechToText calling: " + pb.command().stream().collect(Collectors.joining(" ")) + " in directory "
				+ (pb.directory() != null ? pb.directory().getAbsolutePath() : "?") + "...");

		try {
			Process p = pb.start();
			p.waitFor();
		} catch (IOException | InterruptedException e) {
			logger.error("SpeechToText ffmpeg problem I/O or Interrupted Exception", e);
		} catch (Exception e) {
			logger.error("SpeechToText ffmpeg problem Exception", e);
		}

		return resultFileName;
	}

}

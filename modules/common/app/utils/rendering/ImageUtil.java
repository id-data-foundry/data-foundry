package utils.rendering;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

import javax.inject.Singleton;

import play.Logger;
import utils.validators.FileTypeUtils;

@Singleton
public class ImageUtil {

	// "convert" is deprecated since IM7, so we just use "magick"
	private static String CONVERT_TOOL = "magick";

	private static final Logger.ALogger logger = Logger.of(ImageUtil.class);

	/**
	 * create thumbnails for the given input file
	 * 
	 * @param originalImageFile
	 */
	public void generateThumbnails(File originalImageFile) {
		generateThumbnail(originalImageFile, 120);
		generateThumbnail(originalImageFile, 240);
	}

	/**
	 * generate two thumbnails of the given inputFile with the given width in png and avif formats
	 * 
	 * @param originalImageFile
	 * @param width
	 */
	private void generateThumbnail(File originalImageFile, int width) {
		{
			// construct new filename for scaled image in PNG format
			File scaledImageFile = new File(originalImageFile.getParent() + File.separator
			        + FileTypeUtils.getNameWithoutExtension(originalImageFile.getName()) + "-" + width + ".png");
			runConvert(originalImageFile, scaledImageFile, width, width, 90);
		}
		{
			// construct new filename for scaled image in AVIF format
			File scaledImageFile = new File(originalImageFile.getParent() + File.separator
			        + FileTypeUtils.getNameWithoutExtension(originalImageFile.getName()) + "-" + width + ".avif");
			runConvert(originalImageFile, scaledImageFile, width, width, 85);
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * handle the file delivery for scaled files
	 * 
	 * @param request
	 * @param originalImageFile
	 * @param scale
	 * @param format
	 * @return
	 */
	public File deliverScaledFile(File originalImageFile, String scale, String format) {
		// no scaling needed --> return original
		if (scale.isEmpty() || scale.equals("original") || FileTypeUtils.looksLikeVideoFile(originalImageFile)
		        || FileTypeUtils.looksLikeAudioFile(originalImageFile)) {
			return originalImageFile;
		}

		// check scale
		final int maxWidth;
		final int maxHeight;
		if (scale.equals("thumb")) {
			// scale image to thumb size: 100x100 (fixed width, height depending on aspect ratio)
			maxWidth = 120;
			maxHeight = 120;
		} else if (scale.equals("profile")) {
			// scale image to thumb size: 100x100 (fixed width, height depending on aspect ratio)
			maxWidth = 240;
			maxHeight = 240;
		} else if (scale.equals("card")) {
			// scale image to card size: 460x460 (fixed width, height depending on aspect ratio)
			maxWidth = 480;
			maxHeight = 480;
		} else if (scale.equals("poster")) {
			// scale image to thumb size: 1920x1080 (fixed width, height depending on aspect ratio)
			maxWidth = 1920;
			maxHeight = 1080;
		} else {
			// scale image to default scaled width: 800
			maxWidth = 800;
			maxHeight = 600;
		}

		// check format
		if (format.equals("png")) {
			final File scaledImageFile = new File(originalImageFile.getParent() + File.separator
			        + FileTypeUtils.getNameWithoutExtension(originalImageFile.getName()) + "-" + maxWidth + ".png");

			// no scaling necessary if file exists
			if (!scaledImageFile.exists()) {
				runConvert(originalImageFile, scaledImageFile, maxWidth, maxHeight, 90);
			}

			if (!scaledImageFile.exists()) {
				return originalImageFile;
			}

			return scaledImageFile;
		} else {
			final File scaledAvifImageFile = new File(originalImageFile.getParent() + File.separator
			        + FileTypeUtils.getNameWithoutExtension(originalImageFile.getName()) + "-" + maxWidth + ".avif");

			// no scaling necessary if file exists
			if (!scaledAvifImageFile.exists()) {
				runConvert(originalImageFile, scaledAvifImageFile, maxWidth, maxHeight, 90);
			}

			if (!scaledAvifImageFile.exists()) {
				return originalImageFile;
			}

			return scaledAvifImageFile;
		}
	}

	private boolean runConvert(File source, File destination, int width, int height, float quality) {
		final String[] command = new String[] { //
		        CONVERT_TOOL, //
		        source.getAbsolutePath(), //
		        "-resize", width + "x" + height + ">", //
		        "-quality", quality + "%", //
		        destination.getAbsolutePath() //
		};
		ProcessBuilder pb = new ProcessBuilder(command);
		logger.info("Image conversion > " + pb.command().stream().collect(Collectors.joining(" ")) + "...");
		try {
			Process p = pb.start();
			p.waitFor();
			return true;
		} catch (IOException | InterruptedException e) {
			logger.error("Image conversion problem: I/O or interrupted exception", e);
		} catch (Exception e) {
			logger.error("Image conversion problem: general exception", e);
		}

		return false;
	}

}

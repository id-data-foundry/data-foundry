package utils.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Optional;

import models.Dataset;
import models.ds.CompleteDS;
import play.Logger;
import utils.validators.FileTypeUtils;

public class NarrativeSurveyUtils {

	private static final Logger.ALogger logger = Logger.of(NarrativeSurveyUtils.class);

	/**
	 * create empty survey in the dataset under the given file name, pre-initialized with some scaffolding
	 * 
	 * @param tempFile
	 * @param fileName
	 * @return Optional with correct fileName if all went fine, otherwise empty Optional
	 */
	public static Optional<String> createEmptySurveyFile(CompleteDS cdsWeb, Dataset cdsData, String fileName,
	        Date timestamp) {
		try {
			// check folder first
			final File theFolder = cdsWeb.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			// clean up path components from filename
			fileName = FileTypeUtils.sanitizeFilename(fileName);
			fileName = FileTypeUtils.shortenFilename(fileName, 60);

			// copy the file to final destination
			Path target = new File(theFolder, fileName).toPath();
			Files.writeString(target,
			        views.html.tools.twine.survey_template.render(cdsData.getProject().getId(), cdsData).body());

			return Optional.of(fileName);
		} catch (IOException e) {
			logger.error("Error in storing a file in dataset table and on disk.", e);
			return Optional.empty();
		}
	}

	/**
	 * create empty survey in the dataset under the given file name, pre-initialized with some scaffolding
	 * 
	 * @param tempFile
	 * @param fileName
	 * @return Optional with correct fileName if all went fine, otherwise empty Optional
	 */
	public static Optional<String> createEmptyHTMLFile(CompleteDS cdsWeb, Dataset cdsData, String fileName,
	        Date timestamp) {
		try {
			// check folder first
			final File theFolder = cdsWeb.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			// clean up path components from filename
			fileName = FileTypeUtils.sanitizeFilename(fileName);
			fileName = FileTypeUtils.shortenFilename(fileName, 60);

			// copy the file to final destination
			Path target = new File(theFolder, fileName).toPath();
			Files.writeString(target, views.html.tools.twine.html_template.render().body());

			return Optional.of(fileName);
		} catch (IOException e) {
			logger.error("Error in storing a file in dataset table and on disk.", e);
			return Optional.empty();
		}
	}

}

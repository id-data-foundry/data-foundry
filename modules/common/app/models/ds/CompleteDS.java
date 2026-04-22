package models.ds;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import io.ebean.DB;
import io.ebean.Transaction;
import models.Dataset;
import models.sr.Cluster;
import models.vm.TimedMedia;
import play.Logger;
import play.libs.Json;
import play.mvc.Http.Request;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;
import utils.conf.ConfigurationUtils;
import utils.validators.FileTypeUtils;

/**
 * 
 */
public class CompleteDS extends LinkedDS {

	private static final Logger.ALogger logger = Logger.of(CompleteDS.class);

	protected static final String UPLOADS_DATASETS = "uploads/datasets/";
	protected final String UPLOAD_DIR_PARENT;

	public CompleteDS(Dataset dataset, Config config) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_cp";

		// configuration of file upload directory
		if (config != null && config.hasPath(ConfigurationUtils.DF_UPLOAD_DIR)) {
			UPLOAD_DIR_PARENT = config.getString(ConfigurationUtils.DF_UPLOAD_DIR);
		} else {
			UPLOAD_DIR_PARENT = "public/";
		}
	}

	@Override
	public void createInstance() {

		// id bigint auto_increment not null,
		// file_name varchar(255),
		// description varchar(255),
		// dataset_id bigint,
		// ts timestamp

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			connection.createStatement()
					.execute("CREATE TABLE IF NOT EXISTS " + dataTableName + " ( id bigint auto_increment not null,"
							+ "file_name varchar(255)," + "description varchar(255)," + "dataset_id bigint,"
							+ "ts timestamp," + "PRIMARY KEY (id) );");

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating the dataset table in DB.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "file_name", "description", "dataset_id", "ts" };
	}

	public void addRecord(String fileName, String description, Date ts) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
						+ " ( file_name, description, dataset_id, ts )" + " VALUES (?, ?, ?, ?);");) {

			stmt.setString(1, nss(fileName, 255));
			stmt.setString(2, nss(description, 255));
			stmt.setLong(3, dataset.getId());
			stmt.setTimestamp(4, new Timestamp(ts.getTime()));
			stmt.executeUpdate();
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in inserting a record in dataset table.", e);
			Slack.call("Exception", e.getLocalizedMessage());

			// post update on OOCSI
			oocsiStreaming.datasetUpdate(dataset, OOCSIStreamOutService.map().put("operation", "add")
					.put("filename", nss(fileName, 255)).put("description", nss(description, 255)).build());
		}
	}

	public void updateRecord(long fileId, String description) {

		// update record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("UPDATE " + dataTableName + " SET description = ? WHERE id = ?;");) {

			stmt.setString(1, description);
			stmt.setLong(2, fileId);
			stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in updating a record in dataset table.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	public void deleteRecord(String fileName) {

		// delete record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("DELETE FROM " + dataTableName + " WHERE file_name LIKE ?;");) {

			stmt.setString(1, fileName);
			stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in deleting a record from dataset table.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	public void deleteRecord(Long fileId) {

		// delete record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("DELETE FROM " + dataTableName + " WHERE id = ?;");) {

			stmt.setLong(1, fileId);
			stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in deleting a record from dataset table.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	public void deleteAllFiles() {
		final File theFolder = this.getFolder();
		if (theFolder.exists()) {
			for (File file : theFolder.listFiles()) {
				file.delete();
			}
		}
	}

	/**
	 * store the file in the dataset under the given file name
	 * 
	 * @param tempFile
	 * @param fileName
	 * @return Optional with correct fileName if the file did not exist before, otherwise empty Optional
	 */
	public Optional<String> storeFile(File tempFile, String fileName) {
		try {
			// check folder first
			final File theFolder = this.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			// clean up path components from filename, then shorten it
			fileName = FileTypeUtils.sanitizeFilename(fileName);
			fileName = FileTypeUtils.shortenFilename(fileName, 60);

			// copy file to final destination
			Path source = tempFile.toPath();
			File file = new File(getFolder(), fileName);

			// copy, i.e., overwrite potentially existing file
			Path target = file.toPath();
			Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

			return Optional.of(fileName);
		} catch (IOException e) {
			logger.error("Error in storing a file in dataset table and on disk.", e);
			return Optional.empty();
		}
	}

	/**
	 * create notebook file in the dataset under the given file name, pre-initialized with the some notebook scaffolding
	 * 
	 * @param fileName
	 * @param timestamp
	 * @param notebookLines
	 * @return Optional with correct fileName if all went fine, otherwise empty Optional
	 */
	public Optional<String> createNotebookFile(String fileName, Date timestamp, String[] notebookLines) {
		try {
			// check folder first
			final File theFolder = this.getFolder();
			if (!theFolder.exists()) {
				theFolder.mkdirs();
			}

			// clean up path components from filename, then shorten it
			fileName = FileTypeUtils.sanitizeFilename(fileName);
			fileName = FileTypeUtils.shortenFilename(fileName, 60);

			// create file in final destination
			File destination = new File(theFolder, fileName);
			Path target = destination.toPath();

			if (notebookLines == null || notebookLines.length == 0) {
				String[] defaultLines = { "# %% [javascript]", "let what = {", "	test: true", "}", "",
						"console.log(what)", "# %% [python]", "globalWhat = {}", "", "print(globalWhat)" };
				Files.write(target, Arrays.asList(defaultLines));
			} else {
				Files.write(target, Arrays.asList(notebookLines));
			}

			return Optional.of(fileName);
		} catch (IOException e) {
			logger.error("Error in storing a file in dataset table and on disk.", e);
			return Optional.empty();
		}
	}

	@Override
	public void resetDataset() {

		// delete all files in dataset folder
		File[] listFiles = getFolder().listFiles();
		if (listFiles != null) {
			Arrays.stream(listFiles).forEach(f -> f.delete());
		}

		// delete data in database
		super.resetDataset();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get folder for dataset
	 * 
	 * @return
	 */
	public File getFolder() {
		File theFolder = new File(
				UPLOAD_DIR_PARENT + UPLOADS_DATASETS + dataset.getProject().getRefId() + "__" + dataset.getRefId());

		// ensure the folder is available
		if (!theFolder.exists()) {
			theFolder.mkdirs();
		}

		return theFolder;
	}

	/**
	 * get specified file from the dataset
	 * 
	 * @param fileName
	 * @return
	 */
	public Optional<File> getFile(String fileName) {

		// first just decode from URL
		String decodedFilename = URLDecoder.decode(fileName, StandardCharsets.UTF_8);
		File f = new File(getFolder(), decodedFilename);

		if (!f.exists()) {
			// MODERATE: sanitize filename then find
			String sanitizedFileName = FileTypeUtils.sanitizeFilename(decodedFilename);
			f = new File(getFolder(), sanitizedFileName);

			if (!f.exists()) {
				// EXPENSIVE: try potential matches on disk in old formats
				Optional<String> possibleMatch = Arrays.stream(getFolder().list())
						.filter(sf -> FileTypeUtils.sanitizeFilename(sf).equals(sanitizedFileName)).findAny();
				if (possibleMatch.isPresent()) {
					f = new File(getFolder(), possibleMatch.get());
				}
			}
		}

		// final check
		return f.exists() ? Optional.of(f) : Optional.empty();
	}

	/**
	 * get specified file from the dataset; note that this is just the handle, the file might not exist (yet)
	 * 
	 * @param fileName
	 * @return
	 */
	public Optional<File> getFileTemp(String fileName) {

		// sanitize beforehand
		fileName = FileTypeUtils.sanitizeFilename(fileName);

		File f = new File(getFolder(), fileName);
		return Optional.of(f);
	}

	/**
	 * get specified file from the dataset
	 * 
	 * @param fileId
	 * @return
	 */
	public Optional<File> getFile(Long fileId) {
		return getFileInternal(fileId, dataTableName);
	}

	/**
	 * get specified file from the dataset, with the given table
	 * 
	 * @param fileId
	 * @param dataTableName
	 * @return
	 */
	protected Optional<File> getFileInternal(Long fileId, String dataTableName) {

		// delete record
		Optional<File> result = Optional.empty();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT ts, file_name FROM " + dataTableName + " WHERE id = ?;");) {

			stmt.setLong(1, fileId);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String fileName = rs.getString("file_name");
				Date timestamp = rs.getTimestamp("ts");

				// try just the filename
				result = this.getFile(fileName);

				// try filename with timestamp
				if (result.isEmpty()) {
					result = getFile(timestamp.getTime() + "_" + fileName);
				}
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in retrieving a file.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	/**
	 * get specified file id from the dataset
	 * 
	 * @param filename
	 * @return
	 */
	public Optional<Long> getLatestFileVersionId(String filename) {
		Optional<Long> id = Optional.empty();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT MAX(id) FROM " + dataTableName + " WHERE file_name LIKE ?;")) {

			stmt.setString(1, filename);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				id = Optional.of(rs.getLong(1));
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("File access problem", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
		return id;
	}

	/**
	 * return file list with raw file name
	 * 
	 * @return
	 */
	public List<TimedMedia> getFiles() {
		return getFiles(Optional.empty());
	}

	/**
	 * return file list with raw file name filtered by pattern
	 * 
	 * @return
	 */
	public List<TimedMedia> getFiles(Optional<String> pattern) {
		final String fullpath = UPLOAD_DIR_PARENT + UPLOADS_DATASETS + dataset.getProject().getRefId() + "__"
				+ dataset.getRefId() + "/";

		List<TimedMedia> result = new LinkedList<TimedMedia>();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement("SELECT id, file_name, ts, description FROM "
						+ maxIdJoinExpression(dataTableName) + " ORDER BY file_name ASC");
				ResultSet rs = stmt.executeQuery()) {

			while (rs.next()) {
				Long id = rs.getLong("id");
				String filename = rs.getString("file_name");
				// check if either no pattern was provided, or the pattern matches the filename
				if (!pattern.isPresent() || filename.matches(pattern.get())) {
					Date timestamp = rs.getTimestamp("ts");
					Optional<File> fileOpt = getFile(filename);

					// check timestamped version
					if (fileOpt.isEmpty()) {
						fileOpt = getFile(timestamp.getTime() + "_" + filename);
					}

					// add to result list of file exists
					if (fileOpt.isPresent()) {
						TimedMedia tm = new TimedMedia(id, timestamp, filename, "", rs.getString("description"), null);

						// retrieve on-disk timestamp for file
						File timestampedFile = new File(
								fullpath + timestamp.getTime() + "_" + filename.replace("..", ""));
						if (timestampedFile.exists()) {
							tm.timestamp = new Date(timestampedFile.lastModified());
						}

						result.add(tm);
					}
				}
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in retrieving all files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		export(queue, Function.identity(), limit, start, end);
	}

	/**
	 * generate csv file with file links of all files
	 * 
	 * @param queue
	 * @param linkMapper
	 * @param start
	 * @param end
	 */
	public void export(SourceQueueWithComplete<ByteString> queue, Function<String, String> linkMapper, long limit,
			long start, long end) {
		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, dataset_id, ts, file_name, description FROM "
								+ maxIdJoinExpression(dataTableName) + timeFilterWhereClause(start, end)
								+ " ORDER BY id ASC " + limitExpression(limit) + ";");
				ResultSet rs = stmt.executeQuery();) {

			// header
			queue.offer(ByteString.fromString("id,dataset_id,ts,link,description\n")).toCompletableFuture().get();

			// data
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong("id") + ",");
				sb.append(cf(rs.getString("dataset_id")) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp("ts")) + ",");
				sb.append(cf(linkMapper.apply(nss(rs.getString("file_name"), 1000))) + ",");
				sb.append(cf(rs.getString("description")));
				sb.append("\n");

				queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		queue.complete();
	}

	@Override
	public ArrayNode retrieveProjected(Cluster cluster, long limit, long start, long end) {

		List<ObjectNode> objects = new LinkedList<ObjectNode>();
		// export the data
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement(
						"SELECT id, ts, file_name, description FROM " + maxIdJoinExpression(dataTableName)
								+ timeFilterWhereClause(start, end) + " ORDER BY id DESC LIMIT " + limit + ";");
				ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {

				// ObjectNode on = result.addObject();
				ObjectNode on = Json.newObject();
				objects.add(on);

				// StringBuffer sb = new StringBuffer();
				on.put("id", rs.getLong(1));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(2)));
				on.put("file_name", rs.getString(3));
				on.put("description", rs.getString(4));
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		ArrayNode result = Json.newArray();

		// ensure right order comes back to caller
		Collections.reverse(objects);
		objects.stream().forEach(o -> result.add(o));

		return result;
	}

	public Function<Long, String> getLinks(Request request, Dataset ds) {
		return fileId -> controllers.api.routes.CompleteDSController.downloadFile(ds.getId(), fileId)
				.absoluteURL(request, true);
	}

	protected String maxIdJoinExpression(String tableName) {
		return """
				   (
				       SELECT
				       	   MAX(id) AS mid,
				           file_name as gfn
				       FROM %s
				       GROUP BY gfn
				   ) AS d1
				   JOIN %s as d2
				   ON d1.mid = d2.id
				""".formatted(tableName, tableName);
	}

	@Override
	public void lastUpdatedSource(Map<Long, Long> sourceUpdates) {
		// not implemented on purpose
	}
}

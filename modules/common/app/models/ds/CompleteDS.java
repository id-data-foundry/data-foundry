package models.ds;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
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
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
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
import play.cache.SyncCacheApi;
import play.libs.Json;
import play.mvc.Http.Request;
import services.outlets.OOCSIStreamOutService;
import services.notifications.Notifications;
import utils.conf.ConfigurationUtils;
import utils.rendering.FileUtil;
import utils.validators.FileTypeUtils;

/**
 * 
 */
public class CompleteDS extends LinkedDS {

	private static final Logger.ALogger logger = Logger.of(CompleteDS.class);

	public static final String CACHE_FILES = "CP_DS_FILES_";
	public static final String CACHE_CONTENT_PREFIX = "CP_DS_CONTENT_";

	protected static final String UPLOADS_DATASETS = "uploads/datasets/";
	protected final String UPLOAD_DIR_PARENT;
	private final SyncCacheApi cache;

	public CompleteDS(Dataset dataset, Config config) {
		this(dataset, config, null);
	}

	public CompleteDS(Dataset dataset, Config config, SyncCacheApi cache) {
		super(dataset);
		this.cache = cache;
		this.dataTableName = "ds_" + dataset.getRefId() + "_cp";

		// configuration of file upload directory
		if (config != null && config.hasPath(ConfigurationUtils.DF_UPLOAD_DIR)) {
			UPLOAD_DIR_PARENT = config.getString(ConfigurationUtils.DF_UPLOAD_DIR);
		} else {
			UPLOAD_DIR_PARENT = "public/";
		}
	}

	public void invalidateCache() {
		if (cache != null && dataset != null) {
			cache.remove(CACHE_FILES + dataset.getId());
		}
	}

	public void invalidateContentCache(Long fileId) {
		if (cache != null && dataset != null && fileId != null) {
			cache.remove(CACHE_CONTENT_PREFIX + dataset.getId() + "_" + fileId);
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
			connection.createStatement()
					.execute("CREATE INDEX IF NOT EXISTS " + dataTableName + "_fn_idx ON " + dataTableName
							+ " (file_name);");

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating the dataset table in DB.", e);
			Notifications.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public void migrateDatasetSchema() {
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			connection.createStatement()
					.execute("CREATE INDEX IF NOT EXISTS " + dataTableName + "_fn_idx ON " + dataTableName
							+ " (file_name);");
			transaction.commit();
		} catch (Exception e) {
			// index might already exist or not supported, ignore safely
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

			invalidateCache();

			// post update on OOCSI
			oocsiStreaming.datasetUpdate(dataset, OOCSIStreamOutService.map().put("operation", "add")
					.put("filename", nss(fileName, 255)).put("description", nss(description, 255)).build());
		} catch (Exception e) {
			logger.error("Error in inserting a record in dataset table.", e);
			Notifications.call("Exception", e.getLocalizedMessage());
		}
	}

	public void updateRecord(long fileId, String description) {

		// update record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("UPDATE " + dataTableName + " SET description = ? WHERE id = ?;");) {

			stmt.setString(1, nss(description, 255));
			stmt.setLong(2, fileId);
			stmt.executeUpdate();

			transaction.commit();

			invalidateCache();
		} catch (Exception e) {
			logger.error("Error in updating a record in dataset table.", e);
			Notifications.call("Exception", e.getLocalizedMessage());
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

			invalidateCache();
		} catch (Exception e) {
			logger.error("Error in deleting a record from dataset table.", e);
			Notifications.call("Exception", e.getLocalizedMessage());
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

			invalidateCache();
			invalidateContentCache(fileId);
		} catch (Exception e) {
			logger.error("Error in deleting a record from dataset table.", e);
			Notifications.call("Exception", e.getLocalizedMessage());
		}
	}

	/**
	 * return the file name for the given record id
	 * 
	 * @param fileId
	 * @return
	 */
	public Optional<String> getFileName(Long fileId) {
		if (fileId == null || fileId <= 0) {
			return Optional.empty();
		}
		// Fast path: cached file list lookup
		List<TimedMedia> files = getFiles();
		for (TimedMedia tm : files) {
			if (fileId.equals(tm.getId())) {
				return Optional.of(tm.getLink());
			}
		}

		Optional<String> result = Optional.empty();
		try (Connection connection = DB.getDefault().dataSource().getConnection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT file_name FROM " + dataTableName + " WHERE id = ?;")) {

			stmt.setLong(1, fileId);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					result = Optional.of(rs.getString("file_name"));
				}
			}
		} catch (Exception e) {
			logger.error("Error in retrieving file name by id.", e);
			Notifications.call("Exception", e.getLocalizedMessage());
		}
		return result;
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

			invalidateCache();

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

			invalidateCache();

			return Optional.of(fileName);
		} catch (IOException e) {
			logger.error("Error in storing a file in dataset table and on disk.", e);
			return Optional.empty();
		}
	}

	@Override
	public void resetDataset() {

		// delete all files, but just on disk
		deleteAllFiles();

		// delete data in database
		super.resetDataset();

		invalidateCache();
	}

	/**
	 * delete all files in this dataset, but just on disk
	 * 
	 * 
	 */
	public void deleteAllFiles() {
		// delete all files in dataset folder
		File[] listFiles = getFolder().listFiles();
		if (listFiles != null) {
			Arrays.stream(listFiles).forEach(f -> f.delete());
		}
		invalidateCache();
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
		if (fileName == null || fileName.trim().isEmpty()) {
			return Optional.empty();
		}

		final File theFolder = getFolder();
		if (!theFolder.exists() || !theFolder.isDirectory()) {
			return Optional.empty();
		}

		// 1. Direct safe lookup in folder (canonical path containment + regular file check)
		Optional<File> safeFile = FileUtil.getSafeFileInFolder(theFolder, fileName);
		if (safeFile.isPresent()) {
			return safeFile;
		}

		// 2. MODERATE: sanitize filename then find safely
		String sanitizedFileName = FileTypeUtils.sanitizeFilename(fileName);
		safeFile = FileUtil.getSafeFileInFolder(theFolder, sanitizedFileName);
		if (safeFile.isPresent()) {
			return safeFile;
		}

		// 3. EXPENSIVE: try potential matches on disk in old formats
		String[] fileList = theFolder.list();
		if (fileList != null) {
			Optional<String> possibleMatch = Arrays.stream(fileList)
					.filter(sf -> FileTypeUtils.sanitizeFilename(sf).equals(sanitizedFileName)).findAny();
			if (possibleMatch.isPresent()) {
				return FileUtil.getSafeFileInFolder(theFolder, possibleMatch.get());
			}
		}

		return Optional.empty();
	}

	/**
	 * get specified file from the dataset; note that this is just the handle, the file might not exist (yet)
	 * 
	 * @param fileName
	 * @return
	 */
	public Optional<File> getFileTemp(String fileName) {
		if (fileName == null || fileName.trim().isEmpty() || fileName.contains("..") || fileName.contains("/")
				|| fileName.contains("\\")) {
			return Optional.empty();
		}

		// sanitize beforehand
		fileName = FileTypeUtils.sanitizeFilename(fileName);

		File folder = getFolder();
		File f = new File(folder, fileName);
		try {
			String canonicalDir = folder.getCanonicalPath();
			if (!canonicalDir.endsWith(File.separator)) {
				canonicalDir += File.separator;
			}
			if (!f.getCanonicalPath().startsWith(canonicalDir)) {
				return Optional.empty();
			}
		} catch (IOException e) {
			return Optional.empty();
		}

		return Optional.of(f);
	}

	/**
	 * get specified file from the dataset
	 * 
	 * @param fileId
	 * @return
	 */
	public Optional<File> getFile(Long fileId) {
		if (fileId == null || fileId <= 0) {
			return Optional.empty();
		}
		// Fast path: find in cached file list without database query
		List<TimedMedia> files = getFiles();
		for (TimedMedia tm : files) {
			if (fileId.equals(tm.getId())) {
				return getFile(tm.getLink());
			}
		}

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
		Optional<File> result = Optional.empty();
		String fileName = null;
		Date timestamp = null;

		try (Connection connection = DB.getDefault().dataSource().getConnection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT ts, file_name FROM " + dataTableName + " WHERE id = ?;")) {

			stmt.setLong(1, fileId);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					fileName = rs.getString("file_name");
					timestamp = rs.getTimestamp("ts");
				}
			}
		} catch (Exception e) {
			logger.error("Error in retrieving file metadata.", e);
			Notifications.call("Exception", e.getLocalizedMessage());
		}

		if (fileName != null) {
			result = this.getFile(fileName);
			if (result.isEmpty() && timestamp != null) {
				result = getFile(timestamp.getTime() + "_" + fileName);
			}
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
		if (filename == null || filename.isEmpty()) {
			return Optional.empty();
		}
		// Fast path: cached file list lookup
		List<TimedMedia> files = getFiles();
		for (TimedMedia tm : files) {
			if (filename.equals(tm.getLink())) {
				return Optional.of(tm.getId());
			}
		}

		Optional<Long> id = Optional.empty();
		try (Connection connection = DB.getDefault().dataSource().getConnection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT MAX(id) FROM " + dataTableName + " WHERE file_name LIKE ?;")) {

			stmt.setString(1, filename);
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					long val = rs.getLong(1);
					if (!rs.wasNull() && val > 0) {
						id = Optional.of(val);
					}
				}
			}
		} catch (Exception e) {
			logger.error("File access problem", e);
			Notifications.call("Exception", e.getLocalizedMessage());
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

	private record FileRow(Long id, String fileName, Date timestamp, String description) {
	}

	/**
	 * return file list with raw file name filtered by pattern
	 * 
	 * @param pattern
	 * @return
	 */
	public List<TimedMedia> getFiles(Optional<String> pattern) {
		List<TimedMedia> allFiles;
		if (cache != null && dataset != null) {
			allFiles = cache.getOrElseUpdate(CACHE_FILES + dataset.getId(), () -> fetchFilesFromDBAndDisk(), 300);
		} else {
			allFiles = fetchFilesFromDBAndDisk();
		}

		if (pattern == null || pattern.isEmpty()) {
			return new LinkedList<>(allFiles);
		}
		return allFiles.stream().filter(tm -> tm.getLink().matches(pattern.get()))
				.collect(Collectors.toCollection(LinkedList::new));
	}

	private List<TimedMedia> fetchFilesFromDBAndDisk() {
		List<FileRow> rows = new LinkedList<>();
		try (Connection connection = DB.getDefault().dataSource().getConnection();
				PreparedStatement stmt = connection.prepareStatement("SELECT id, file_name, ts, description FROM "
						+ maxIdJoinExpression(dataTableName) + " ORDER BY file_name ASC");
				ResultSet rs = stmt.executeQuery()) {

			while (rs.next()) {
				rows.add(new FileRow(rs.getLong("id"), rs.getString("file_name"), rs.getTimestamp("ts"),
						rs.getString("description")));
			}
		} catch (Exception e) {
			logger.error("Error in retrieving file metadata from dataset table.", e);
			Notifications.call("Exception", e.getLocalizedMessage());
			return Collections.emptyList();
		}

		List<TimedMedia> result = new LinkedList<>();
		for (FileRow row : rows) {
			Optional<File> fileOpt = getFile(row.fileName());

			// check timestamped version
			if (fileOpt.isEmpty() && row.timestamp() != null) {
				fileOpt = getFile(row.timestamp().getTime() + "_" + row.fileName());
			}

			// add to result list if file exists
			if (fileOpt.isPresent()) {
				TimedMedia tm = new TimedMedia(row.id(), new Date(fileOpt.get().lastModified()), row.fileName(), "",
						row.description(), null);
				result.add(tm);
			}
		}

		return result;
	}

	/**
	 * retrieve file text content with caching
	 * 
	 * @param fileId
	 * @return
	 */
	public Optional<String> getFileContent(Long fileId) {
		if (cache != null && dataset != null && fileId != null && fileId > 0) {
			return cache.getOrElseUpdate(CACHE_CONTENT_PREFIX + dataset.getId() + "_" + fileId,
					() -> readFileContentFromDisk(fileId), 300);
		}
		return readFileContentFromDisk(fileId);
	}

	private Optional<String> readFileContentFromDisk(Long fileId) {
		Optional<File> fileOpt = getFile(fileId);
		if (fileOpt.isPresent()) {
			try {
				return Optional.of(FileUtils.readFileToString(fileOpt.get(), Charset.defaultCharset()));
			} catch (Exception e) {
				logger.error("Error reading file content from disk", e);
			}
		}
		return Optional.empty();
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
			Notifications.call("Exception", e.getLocalizedMessage());
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
			Notifications.call("Exception", e.getLocalizedMessage());
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

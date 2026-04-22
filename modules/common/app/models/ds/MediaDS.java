package models.ds;

import java.io.File;
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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import io.ebean.DB;
import io.ebean.Transaction;
import models.Dataset;
import models.sr.Cluster;
import models.sr.Participant;
import models.vm.TimedAnnotatedMedia;
import models.vm.TimedMedia;
import play.Logger;
import play.libs.Json;
import play.mvc.Http.Request;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;
import utils.DataUtils;

public class MediaDS extends CompleteDS {

	private static final Logger.ALogger logger = Logger.of(MediaDS.class);
	private final String dataTableNameRawFiles;

	public MediaDS(Dataset dataset, Config config) {
		super(dataset, config);
		this.dataTableName = "ds_" + dataset.getRefId() + "_md";
		this.dataTableNameRawFiles = "ds_" + dataset.getRefId() + "_mdrf";
	}

	@Override
	public void createInstance() {

		// raw data files: _mdrf

		// id bigint auto_increment not null,
		// participant_id bigint,
		// file_name varchar(255),
		// description varchar(255),
		// status varchar(20),
		// ts timestamp

		// create the database for the raw data files
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			// use raw JDBC
			connection.createStatement().execute("CREATE TABLE IF NOT EXISTS " + dataTableNameRawFiles
					+ " ( id bigint auto_increment not null," + "participant_id bigint," + "file_name varchar(255),"
					+ "description varchar(255)," + "status varchar(20)," + "ts timestamp," + "PRIMARY KEY (id) );");
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset table in DB.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		// structured data: _md

		// id bigint auto_increment not null,
		// participant_id bigint,
		// ts timestamp,
		// link varchar(255),
		// mediatype varchar(31),
		// description varchar(255),
		// pp1 varchar(255),
		// pp2 varchar(255),
		// pp3 varchar(255)

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			// use raw JDBC
			connection.createStatement()
					.execute("CREATE TABLE IF NOT EXISTS " + dataTableName + " ( id bigint auto_increment not null,"
							+ "participant_id bigint," + "ts timestamp," + "link varchar(255),"
							+ "mediatype varchar(31)," + "description varchar(255)," + "pp1 varchar(255),"
							+ "pp2 varchar(255)," + "pp3 varchar(255)," + "PRIMARY KEY (id) );");

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset table in DB.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "participant_id", "ts", "link", "mediatype", "description", "pp1", "pp2", "pp3" };
	}

	public void addRecord(Participant participant, String fileName, String description, Date ts, String status) {
		if (participant == null) {
			participant = Participant.EMPTY_PARTICIPANT;
		}

		// insert record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableNameRawFiles
						+ " ( participant_id, file_name, description, status, ts )" + " VALUES (?, ?, ?, ?, ?);");) {

			stmt.setLong(1, participant.getId());
			stmt.setString(2, nss(fileName, 255));
			stmt.setString(3, nss(description, 255));
			stmt.setString(4, nss(status, 20));
			stmt.setTimestamp(5, new Timestamp(ts.getTime()));
			stmt.executeUpdate();
			transaction.commit();

			// post update on OOCSI
			oocsiStreaming.datasetUpdate(dataset,
					OOCSIStreamOutService.map().put("operation", "add").put("filename", nss(fileName, 255))
							.put("description", nss(description, 255))
							.put("participant_id", nss(participant.getRefId(), 32)).build());
		} catch (Exception e) {
			logger.error("Error in inserting record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	public void importFileContents(Participant participant, String link, String mediatype, String description,
			Date ts) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
						+ " ( participant_id, ts, link, mediatype, description, pp1, pp2, pp3 )"
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?);");) {

			stmt.setLong(1, participant != null ? participant.getId() : -1);
			stmt.setTimestamp(2, new Timestamp(ts.getTime()));
			stmt.setString(3, nss(link, 255));
			stmt.setString(4, nss(mediatype, 31));
			stmt.setString(5, nss(description, 255));
			stmt.setString(6, nss(participant != null ? participant.getPublicParameter1() : "", 255));
			stmt.setString(7, nss(participant != null ? participant.getPublicParameter2() : "", 255));
			stmt.setString(8, nss(participant != null ? participant.getPublicParameter3() : "", 255));
			stmt.executeUpdate();
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in importing file contents.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	public void importFileContents(Participant participant, Dataset ds, String fileName, Request url, String mediatype,
			String description, Date ts) {

		String link = controllers.api.routes.MediaDSController.image(ds.getId(), fileName).absoluteURL(url, true);

		// insert record
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {

			// use raw JDBC
			PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
					+ " ( participant_id, ts, link, mediatype, description, pp1, pp2, pp3 )"
					+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?);");
			stmt.setLong(1, participant != null ? participant.getId() : -1);
			stmt.setTimestamp(2, new Timestamp(ts.getTime()));
			stmt.setString(3, nss(link, 255));
			stmt.setString(4, nss(mediatype, 31));
			stmt.setString(5, nss(description, 255));
			stmt.setString(6, nss(participant != null ? participant.getPublicParameter1() : "", 255));
			stmt.setString(7, nss(participant != null ? participant.getPublicParameter2() : "", 255));
			stmt.setString(8, nss(participant != null ? participant.getPublicParameter3() : "", 255));
			stmt.executeUpdate();
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in importing file contents.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public void deleteRecord(String file_name) {
		// do nothing
	}

	@Override
	public void deleteRecord(Long fileId) {

		// delete record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("DELETE FROM " + dataTableNameRawFiles + " WHERE id = ?;");) {

			stmt.setLong(1, fileId);
			stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in deleting a record from dataset table.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public void resetDataset() {

		// delete all files in dataset folder
		File[] listFiles = getFolder().listFiles();
		if (listFiles != null) {
			Arrays.stream(listFiles).forEach(f -> f.delete());
		}

		// delete all data
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement(
						"DELETE FROM " + dataTableName + "; DELETE FROM " + dataTableNameRawFiles + ";");) {
			stmt.execute();
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in resetting dataset table.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	/**
	 * return file list with raw file name
	 * 
	 * @return
	 */
	public long checkParticipantByFilename(String fileName) {

		long participantId = -2l;
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement("SELECT participant_id FROM "
						+ dataTableNameRawFiles + " WHERE file_name LIKE ? ORDER BY ts DESC;");) {
			stmt.setString(1, fileName);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				participantId = rs.getLong("participant_id");
			}

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in checking participant by filename.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in checking participant by filename.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return participantId;
	}

	/**
	 * get specified file from the dataset for a participant
	 * 
	 * @param filename
	 * @return
	 */
	public Optional<File> getLatestFileVersionForParticipant(Participant p, String filename) {
		Optional<Long> id = getLatestFileVersionForParticipantInternal(p, filename, dataTableNameRawFiles);

		Optional<File> result = Optional.empty();
		if (id.isPresent()) {
			result = this.getFileInternal(id.get(), dataTableName);
		}
		return result;
	}

	/**
	 * get id for specified file from the dataset for a participant
	 * 
	 * @param p
	 * @param filename
	 * @param dataTableName
	 * @return
	 */
	private Optional<Long> getLatestFileVersionForParticipantInternal(Participant p, String filename,
			String dataTableName) {

		Optional<Long> id = Optional.empty();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement(
						"SELECT MAX(id) FROM " + dataTableName + " WHERE participant_id = ? AND file_name LIKE ?;")) {

			stmt.setLong(1, p.getId());
			stmt.setString(2, filename);
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
	 * return the first file in this media dataset as list of TimedMedia objects
	 * 
	 */
	public List<TimedMedia> getFirstFile() {
		return getNFilesByStatus(1, "cover");
	}

	/**
	 * get specified file from the dataset as TimedMedia object
	 * 
	 * @param itemId
	 * @return
	 */
	public Optional<TimedMedia> getItem(long itemId) {
		Optional<TimedMedia> result = Optional.empty();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, ts, file_name, description, status, participant_id FROM "
								+ dataTableNameRawFiles + " WHERE id = ?;")) {
			stmt.setLong(1, itemId);
			ResultSet rs = stmt.executeQuery();

			if (rs.next()) {
				String filename = rs.getString("file_name");
				Timestamp timestamp = rs.getTimestamp("ts");

				String participantId = rs.getString("participant_id");
				long pid = DataUtils.parseLong(participantId);
				Participant participant = Participant.find.byId(pid);
				TimedMedia tm = new TimedMedia(rs.getLong("id"), timestamp, filename, rs.getString("status"),
						rs.getString("description"), participant);
				result = Optional.of(tm);
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in retrieving file by id.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in retrieving file by id.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	/**
	 * update record in dataset
	 * 
	 * @param itemId
	 * @param description
	 */
	public void updateItemRecord(long itemId, String description) {
		Optional<TimedMedia> item = getItem(itemId);
		if (item.isPresent()) {
			updateItemRecord(itemId, item.get().participant, item.get().link, item.get().link, description);
		}
	}

	/**
	 * update record in dataset
	 * 
	 * @param itemId
	 * @param participant
	 * @param fileName
	 * @param link
	 * @param description
	 */
	public void updateItemRecord(long itemId, Participant participant, String fileName, String link,
			String description) {

		// get the file name first
		Optional<TimedMedia> item = getItem(itemId);
		if (item.isEmpty()) {
			return;
		}

		String oldFileName = item.get().link;

		// update record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmtRaw = connection.prepareStatement("UPDATE " + dataTableNameRawFiles
						+ " SET participant_id = ?, file_name = ?, description = ? WHERE id = ?;");
				PreparedStatement stmtFull = connection.prepareStatement("UPDATE " + dataTableName
						+ " SET participant_id = ?, link = ?, description = ?, pp1 = ?, pp2 = ?, pp3 = ? WHERE link LIKE ?;");) {

			// update raw files table
			stmtRaw.setLong(1, participant != null ? participant.getId() : -1);
			stmtRaw.setString(2, nss(fileName, 255));
			stmtRaw.setString(3, nss(description, 255));
			stmtRaw.setLong(4, itemId);
			stmtRaw.executeUpdate();

			// update structured data table (match by filename in link)
			stmtFull.setLong(1, participant != null ? participant.getId() : -1);
			stmtFull.setString(2, nss(link, 255));
			stmtFull.setString(3, nss(description, 255));
			stmtFull.setString(4, nss(participant != null ? participant.getPublicParameter1() : "", 255));
			stmtFull.setString(5, nss(participant != null ? participant.getPublicParameter2() : "", 255));
			stmtFull.setString(6, nss(participant != null ? participant.getPublicParameter3() : "", 255));
			stmtFull.setString(7, "%" + oldFileName);
			stmtFull.executeUpdate();

			transaction.commit();

			// post update on OOCSI
			oocsiStreaming.datasetUpdate(dataset,
					OOCSIStreamOutService.map().put("operation", "update").put("filename", nss(fileName, 255))
							.put("description", nss(description, 255))
							.put("participant_id", nss(participant != null ? participant.getRefId() : "", 32)).build());
		} catch (Exception e) {
			logger.error("Error in updating record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	/**
	 * return all files in this media dataset as list of TimedMedia objects (soft limit to 1000)
	 * 
	 */
	public List<TimedMedia> getFiles() {
		return getNFiles(1000);
	}

	/**
	 * return the first n files in this media dataset as list of TimedMedia objects
	 * 
	 * @param n
	 * @return
	 */
	public List<TimedMedia> getNFiles(int n) {
		// no files requested
		if (n <= 0) {
			return Collections.emptyList();
		}

		final List<TimedMedia> fileList = new LinkedList<TimedMedia>();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, ts, file_name, description, status, participant_id FROM "
								+ maxIdJoinExpression(dataTableNameRawFiles) + " ORDER BY ts DESC LIMIT " + n + ";");
				ResultSet rs = stmt.executeQuery()) {

			while (rs.next()) {
				String filename = rs.getString("file_name");
				Timestamp timestamp = rs.getTimestamp("ts");
				Optional<File> fileOpt = getFile(filename);
				if (fileOpt.isEmpty()) {
					filename = timestamp.getTime() + "_" + filename;
					fileOpt = getFile(filename);
				}

				if (fileOpt.isPresent()) {
					String participantId = rs.getString("participant_id");
					long pid = DataUtils.parseLong(participantId);
					Participant participant = Participant.find.byId(pid);
					// if (participant != null) {
					// participantName = participant.getName();
					// }
					TimedMedia tm = new TimedMedia(rs.getLong("id"), timestamp, filename, rs.getString("status"),
							rs.getString("description"), participant);
					fileList.add(tm);
				}
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in retrieving files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in retrieving files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return fileList;
	}

	/**
	 * return the first n files in this media dataset as list of TimedMedia objects filtered by status
	 * 
	 * @param n
	 * @return
	 */
	public List<TimedMedia> getNFilesByStatus(int n, String status) {
		// no files requested
		if (n <= 0) {
			return Collections.emptyList();
		}

		final List<TimedMedia> fileList = new LinkedList<TimedMedia>();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, ts, file_name, description, status, participant_id FROM "
								+ maxIdJoinExpression(dataTableNameRawFiles)
								+ " WHERE status = ? ORDER BY ts DESC LIMIT ?;")) {
			stmt.setString(1, status);
			stmt.setInt(2, n);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				String filename = rs.getString("file_name");
				Timestamp timestamp = rs.getTimestamp("ts");

				Optional<File> fileOpt = getFile(filename);
				if (fileOpt.isEmpty()) {
					fileOpt = getFile(timestamp.getTime() + "_" + filename);
				}

				if (fileOpt.isPresent()) {
					String participantId = rs.getString("participant_id");
					long pid = DataUtils.parseLong(participantId);
					Participant participant = Participant.find.byId(pid);
					TimedMedia tm = new TimedMedia(rs.getLong("id"), timestamp, filename, rs.getString("status"),
							rs.getString("description"), participant);
					fileList.add(tm);
				}
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in retrieving files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in retrieving files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return fileList;
	}

	/**
	 * return all files in this media dataset as list of TimedMedia objects
	 * 
	 * @param participants
	 * @return
	 */
	public List<TimedMedia> getFiles(List<Participant> participants) {
		if (participants.isEmpty()) {
			return Collections.emptyList();
		}

		final String whereClause = " WHERE participant_id IN ("
				+ participants.stream().map(p -> p.getId().toString()).collect(Collectors.joining(",")) + ")";

		final List<TimedMedia> fileList = new LinkedList<TimedMedia>();

		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, ts, file_name, description, status, participant_id FROM "
								+ maxIdJoinExpression(dataTableNameRawFiles) + whereClause + " ORDER BY ts DESC;");
				ResultSet rs = stmt.executeQuery()) {

			while (rs.next()) {
				String filename = rs.getString("file_name");
				Timestamp timestamp = rs.getTimestamp("ts");
				Optional<File> fileOpt = getFile(filename);
				if (fileOpt.isEmpty()) {
					fileOpt = getFile(timestamp.getTime() + "_" + filename);
				}

				if (fileOpt.isPresent()) {
					String participantId = rs.getString("participant_id");
					long pid = DataUtils.parseLong(participantId);
					Participant participant = Participant.find.byId(pid);
					TimedMedia tm = new TimedMedia(rs.getLong("id"), timestamp, filename, "",
							rs.getString("description"), participant);
					fileList.add(tm);
				}
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in retrieving files by participant.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in retrieving files by participant.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return fileList;
	}

	/**
	 * return a list of timedannotatedmedia for the given participant
	 * 
	 * @param id
	 * @return
	 */
	public List<TimedAnnotatedMedia> getMediaForParticipant(Long id) {
		final List<TimedAnnotatedMedia> result = new LinkedList<TimedAnnotatedMedia>();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement("SELECT id, ts, link, mediatype, description FROM "
						+ dataTableName + " WHERE participant_id = ? ORDER BY ts DESC;");) {
			stmt.setLong(1, id);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				TimedAnnotatedMedia am = new TimedAnnotatedMedia(rs.getLong("id"), rs.getTimestamp("ts"),
						rs.getString("link"), rs.getString("mediatype"), rs.getString("description"));
				result.add(am);
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in retrieving media by participant.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	/**
	 * get specified file from the dataset
	 * 
	 * @param fileId
	 * @return
	 */
	@Override
	public Optional<File> getFile(Long fileId) {
		return getFileInternal(fileId, dataTableNameRawFiles);
	}

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		export(queue, Function.identity(), cluster.getParticipantList(), limit, start, end);
	}

	public void export(SourceQueueWithComplete<ByteString> queue,
			Function<String, String> filePathTransformationFunction, List<Long> participantIds, long limit, long start,
			long end) {
		final String whereClause;
		if (participantIds.isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE participant_id IN ("
					+ participantIds.stream().map(l -> l.toString()).collect(Collectors.joining(",")) + ") "
					+ timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}
		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement(
						"SELECT id, participant_id, ts, link, mediatype, description, pp1, pp2, pp3 FROM "
								+ dataTableName + whereClause + " ORDER BY id ASC " + limitExpression(limit) + ";");
				ResultSet rs = stmt.executeQuery();) {

			// header
			queue.offer(ByteString.fromString("id,participant_id,ts,link,mediatype,description,pp1,pp2,pp3\n"))
					.toCompletableFuture().get();

			// data
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong("id") + ",");
				sb.append(rs.getLong("participant_id") + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp("ts")) + ",");
				sb.append(cf(filePathTransformationFunction.apply(nss(rs.getString("link"), 1000))) + ",");
				sb.append(cf(rs.getString("mediatype")) + ",");
				sb.append(cf(rs.getString("description")) + ",");
				sb.append(cf(rs.getString("pp1")) + ",");
				sb.append(cf(rs.getString("pp2")) + ",");
				sb.append(cf(rs.getString("pp3")));
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

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public ArrayNode retrieveProjected(Cluster cluster, long limit, long start, long end) {
		return retrieveProjected(Function.identity(), cluster, limit, start, end);
	}

	public ArrayNode retrieveProjected(Function<String, String> linkMapper, Cluster cluster, long limit, long start,
			long end) {

		final String whereClause;
		if (cluster.getParticipants().isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE participant_id IN ("
					+ cluster.getParticipants().stream().map(p -> p.getId().toString()).collect(Collectors.joining(","))
					+ ") " + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		List<ObjectNode> objects = new LinkedList<ObjectNode>();
		// export the data
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement(
						"SELECT id, participant_id, ts, link, mediatype, description, pp1, pp2, pp3 FROM "
								+ dataTableName + whereClause + " ORDER BY id DESC LIMIT " + limit + ";");
				ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {

				// ObjectNode on = result.addObject();
				ObjectNode on = Json.newObject();
				objects.add(on);

				// StringBuffer sb = new StringBuffer();
				on.put("id", rs.getLong(1));
				on.put("participant_id", rs.getLong(2));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				on.put("link", linkMapper.apply(nss(rs.getString(4), 1000)));
				on.put("mediatype", nss(rs.getString(5), 255));
				on.put("description", nss(rs.getString(6), 255));
				on.put("pp1", nss(rs.getString(7), 255));
				on.put("pp2", nss(rs.getString(8), 255));
				on.put("pp3", nss(rs.getString(9), 255));
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
}

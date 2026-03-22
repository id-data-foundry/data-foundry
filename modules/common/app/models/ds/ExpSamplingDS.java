package models.ds;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;

import io.ebean.DB;
import io.ebean.Transaction;
import models.Dataset;
import models.sr.Cluster;
import models.sr.Participant;
import models.vm.TimedMedia;
import play.Logger;
import play.libs.Json;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;
import utils.DataUtils;
import utils.conf.ConfigurationUtils;

public class ExpSamplingDS extends LinkedDS {

	private static final String UPLOADS_DATASETS = "uploads/datasets/";
	private final String UPLOAD_DIR_PARENT;
	private final String dataTableNameRawFiles;

	private static final Logger.ALogger logger = Logger.of(ExpSamplingDS.class);

	public ExpSamplingDS(Dataset dataset, Config config) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_es";
		this.dataTableNameRawFiles = "ds_" + dataset.getRefId() + "_esrf";

		// configuration of file upload directory
		if (config != null && config.hasPath(ConfigurationUtils.DF_UPLOAD_DIR)) {
			UPLOAD_DIR_PARENT = config.getString(ConfigurationUtils.DF_UPLOAD_DIR);
		} else {
			UPLOAD_DIR_PARENT = "public/";
		}
	}

	@Override
	public void createInstance() {

		// raw data files: _mvrf

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

		// structured data: _mv

		// id bigint auto_increment not null,
		// participant_id bigint,
		// ts timestamp,
		// ts_done timestamp,
		// pp1 varchar(255),
		// pp2 varchar(255),
		// pp3 varchar(255),
		// data varchar(1024),

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			// use raw JDBC
			connection.createStatement()
					.execute("CREATE TABLE IF NOT EXISTS " + dataTableName + " ( id bigint auto_increment not null,"
							+ "participant_id bigint," + "ts timestamp," + "ts_done timestamp," + "pp1 varchar(255),"
							+ "pp2 varchar(255)," + "pp3 varchar(255)," + "data varchar(1024),"
							+ "PRIMARY KEY (id) );");

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset table in DB.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "participant_id", "ts", "ts_done", "pp1", "pp2", "pp3", "data" };
	}

	public void addRecord(long participantId, String fileName, String description, Date ts, String status) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableNameRawFiles
						+ " ( participant_id, file_name, description, status, ts )" + " VALUES (?, ?, ?, ?, ?);");) {

			stmt.setLong(1, participantId);
			stmt.setString(2, nss(fileName, 255));
			stmt.setString(3, nss(description, 255));
			stmt.setString(4, nss(status, 20));
			stmt.setTimestamp(5, new Timestamp(ts.getTime()));
			stmt.executeUpdate();
			transaction.commit();

			// post update on OOCSI
			oocsiStreaming.datasetUpdate(dataset,
					OOCSIStreamOutService.map().put("operation", "add").put("filename", nss(fileName, 255))
							.put("description", nss(description, 255)).put("participant_id", participantId).build());
		} catch (SQLException e) {
			logger.error("Error in inserting record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	public boolean importFileContents(File dataFile, Participant participant) {
		try (FileReader reader = new FileReader(dataFile.getAbsoluteFile());
				CSVReader csvReader = new CSVReader(reader);) {
			try (Transaction transaction = DB.beginTransaction();
					Connection connection = transaction.connection();
					PreparedStatement stmt = connection.prepareStatement(
							"INSERT INTO " + dataTableName + " ( participant_id, ts, ts_done, pp1, pp2, pp3, data )"
									+ " VALUES (?, ?, ?, ?, ?, ?, ?);");) {

				// read first 4 lines
				csvReader.readNext();
				csvReader.readNext();
				csvReader.readNext();
				csvReader.readNext();

				// read header
				String[] data;
				while ((data = csvReader.readNext()) != null) {

					// read data
					data = csvReader.readNext();
					if (data == null) {
						break;
					}

					// parse data
					String timestamp1 = data[0];
					Date d1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(timestamp1);
					String timestamp2 = data[1];
					Date d2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(timestamp2);

					String rest = Arrays.stream(data).skip(2).collect(Collectors.joining(","));

					// store data
					stmt.setLong(1, participant.getId());
					stmt.setTimestamp(2, new Timestamp(d1.getTime()));
					stmt.setTimestamp(3, new Timestamp(d2.getTime()));
					stmt.setString(4, participant.getPublicParameter1());
					stmt.setString(5, participant.getPublicParameter2());
					stmt.setString(6, participant.getPublicParameter3());
					stmt.setString(7, rest);
					stmt.executeUpdate();

					// update projection
					dataset.getConfiguration().put("projection", IntStream.range(1, data.length - 1).boxed()
							.map(i -> "item" + i).collect(Collectors.joining(",")));

					// clean timing row
					csvReader.readNext();
				}

				transaction.commit();
			} catch (Exception e) {
				logger.error("Error in inserting file record in dataset.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}

		} catch (FileNotFoundException e) {
			logger.error("Error in retrieving uploaded file from request.", e);
			Slack.call("Exception", e.getLocalizedMessage());
			return false;
		} catch (IOException e) {
			logger.error("Error in retrieving uploaded file from request.", e);
			Slack.call("Exception", e.getLocalizedMessage());
			return false;
		}

		dataset.update();

		// everything fine
		return true;
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
			logger.error("Error in resetting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	public File getFolder() {
		return new File(
				UPLOAD_DIR_PARENT + UPLOADS_DATASETS + dataset.getProject().getRefId() + "__" + dataset.getRefId());
	}

	public File getFile(String filename) {
		return new File(UPLOAD_DIR_PARENT + UPLOADS_DATASETS + dataset.getProject().getRefId() + "__"
				+ dataset.getRefId() + "/" + filename.replace("..", ""));
	}

	public List<TimedMedia> getFiles() {

		List<TimedMedia> result = new LinkedList<TimedMedia>();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, ts, file_name, description, status, participant_id FROM "
								+ dataTableNameRawFiles + " ORDER BY ts DESC;");
				ResultSet rs = stmt.executeQuery()) {

			while (rs.next()) {
				String filename = rs.getString("file_name");
				if (getFile(filename).exists()) {

					String participantName = rs.getString("participant_id");
					long pid = DataUtils.parseLong(participantName);
					Participant participant = Participant.find.byId(pid);
					if (participant != null) {
						participantName = participant.getName();
					}

					TimedMedia tm = new TimedMedia(rs.getLong("id"), rs.getTimestamp("ts"), filename,
							rs.getString("description"), rs.getString("description"), participant);
					result.add(tm);
				}
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in retrieving all files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in retrieving all files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	public List<TimedMedia> getFiles(List<Participant> participants) {
		if (participants.isEmpty()) {
			return Collections.emptyList();
		}

		final String whereClause = " WHERE participant_id IN ("
				+ participants.stream().map(p -> p.getId().toString()).collect(Collectors.joining(",")) + ")";

		List<TimedMedia> result = new LinkedList<TimedMedia>();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, ts, file_name, description, status, participant_id FROM "
								+ dataTableNameRawFiles + whereClause + " ORDER BY ts DESC;");
				ResultSet rs = stmt.executeQuery()) {

			while (rs.next()) {
				String filename = rs.getString("file_name");
				if (getFile(filename).exists()) {
					String participantName = rs.getString("participant_id");
					long pid = DataUtils.parseLong(participantName);
					Participant participant = Participant.find.byId(pid);
					TimedMedia tm = new TimedMedia(rs.getLong("id"), rs.getTimestamp("ts"), filename, "",
							rs.getString("description"), participant);
					result.add(tm);
				}
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in retrieving participant files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in retrieving participant files.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		exportProjected(queue, cluster.getParticipantList(), limit, start, end);
	}

	public void exportProjected(SourceQueueWithComplete<ByteString> queue, List<Long> participantIds, long limit,
			long start, long end) {

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
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, participant_id, ts, ts_done, pp1, pp2, pp3, data FROM "
								+ dataTableName + whereClause + " ORDER BY id ASC " + limitExpression(limit) + ";");
				ResultSet rs = stmt.executeQuery();) {

			final String[] projection = dataset.getConfiguration().getOrDefault(Dataset.DATA_PROJECTION, "").split(",");
			queue.offer(ByteString
					.fromString("id,participant_id,ts,ts_done,pp1,pp2,pp3," + String.join(",", projection) + "\n"))
					.toCompletableFuture().get();
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong("id") + ",");
				sb.append(rs.getLong("participant_id") + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp("ts")) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp("ts_done")) + ",");
				sb.append(cf(rs.getString("pp1")) + ",");
				sb.append(cf(rs.getString("pp2")) + ",");
				sb.append(cf(rs.getString("pp3")) + ",");

				// parse data as JSON
				String data = rs.getString("data");
				JsonNode jn = Json.parse(data);
				final JsonNode on = (jn == null || !jn.isObject()) ? Json.newObject() : jn;

				// iterate through values
				String values = Arrays.stream(projection).map(key -> {
					JsonNode value = on.get(key);
					if (value != null) {
						return value.toString();
					} else {
						return "";
					}
				}).collect(Collectors.joining(","));
				sb.append(values);
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
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, participant_id, ts, ts_done, pp1, pp2, pp3, data FROM "
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
				on.put("ts_end", tsExportFormatter.format(rs.getTimestamp(4)));
				on.put("pp1", rs.getString(5));
				on.put("pp2", rs.getString(6));
				on.put("pp3", rs.getString(7));

				// parse data as JSON
				String data = rs.getString(8);
				try {
					JsonNode jn = data != null ? Json.parse(data) : Json.newObject();
					jn.fields().forEachRemaining(e -> on.set(e.getKey(), e.getValue()));
				} catch (Exception e) {
					// add data verbatim
					on.put("data", data);

					// log potential parsing exceptions
					logger.error("Error in parsing.", e);
				}
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

	@Override
	public void lastUpdatedSource(Map<Long, Long> sourceUpdates) {
		super.lastUpdatedSource(sourceUpdates, "participant_id");
	}
}

package models.ds;

import java.io.File;
import java.io.IOException;
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
import java.util.stream.Collectors;

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import io.ebean.DB;
import io.ebean.Transaction;
import io.jenetics.jpx.Extension;
import io.jenetics.jpx.GPX;
import io.jenetics.jpx.GPX.Reader.Mode;
import io.jenetics.jpx.TrackSegment;
import models.Dataset;
import models.sr.Cluster;
import models.sr.Participant;
import models.vm.TimedMedia;
import play.Logger;
import play.libs.Json;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;
import utils.DataUtils;

public class MovementDS extends CompleteDS {

	private static final Logger.ALogger logger = Logger.of(MovementDS.class);
	private final String dataTableNameRawFiles;

	public MovementDS(Dataset dataset, Config config) {
		super(dataset, config);
		this.dataTableName = "ds_" + dataset.getRefId() + "_mv";
		this.dataTableNameRawFiles = "ds_" + dataset.getRefId() + "_mvrf";

		// DATA_PROJECTION for movement dataset
		dataset.getConfiguration().put(Dataset.DATA_PROJECTION, "participant_id,x,y,z,track,data");
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
		// x bigint,
		// y bigint,
		// z bigint,
		// track varchar(255),
		// pp1 varchar(255),
		// pp2 varchar(255),
		// pp3 varchar(255),
		// data varchar(1024),

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			// use raw JDBC
			connection.createStatement()
					.execute("CREATE TABLE IF NOT EXISTS " + dataTableName + " ( id bigint auto_increment not null,"
							+ "participant_id bigint," + "ts timestamp," + "x real," + "y real," + "z real,"
							+ "track varchar(255)," + "pp1 varchar(255)," + "pp2 varchar(255)," + "pp3 varchar(255),"
							+ "data varchar(1024)," + "PRIMARY KEY (id) );");

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset table in DB.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "participant_id", "ts", "x", "y", "z", "track", "pp1", "pp2", "pp3", "data" };
	}

	public void addRecord(Participant participant, String fileName, String description, Date ts, String status) {

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

	public boolean importFileContents(File gpxDataFile, Participant participant) {
		try {
			GPX gpx = GPX.reader(Mode.LENIENT).read(gpxDataFile.toPath());

			try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
				gpx.tracks().forEach(t -> {

					final String track = t.getName().orElse("") + " (" + t.getType().orElse("") + ")";

					// use raw JDBC
					try (PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
							+ " ( participant_id, ts, x, y, z, track, pp1, pp2, pp3, data )"
							+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");) {
						t.segments().flatMap(TrackSegment::points).forEach(p -> {

							String data = ",,";
							if (p.getExtension().isPresent()) {
								Extension extension = p.getExtension().get();
								data = extension.get_atemp() + "," + extension.get_hr() + "," + extension.get_cad();
							}

							try {
								stmt.setLong(1, participant.getId());
								stmt.setTimestamp(2, new Timestamp(p.getTime().get().toInstant().toEpochMilli()));
								stmt.setFloat(3, p.getLongitude().floatValue());
								stmt.setFloat(4, p.getLatitude().floatValue());
								stmt.setFloat(5, p.getElevation().get().floatValue());
								stmt.setString(6, nss(track, 255));
								stmt.setString(7, nss(participant.getPublicParameter1(), 255));
								stmt.setString(8, nss(participant.getPublicParameter2(), 255));
								stmt.setString(9, nss(participant.getPublicParameter3(), 255));
								stmt.setString(10, nss(data, 1024));
								stmt.executeUpdate();
							} catch (SQLException e) {
								logger.error("Error in importing file line by line.", e);
								Slack.call("Exception", e.getLocalizedMessage());
							}
						});
					} catch (SQLException e) {
						logger.error("Error in importing movement data into dataset.", e);
						Slack.call("Exception", e.getLocalizedMessage());
					}
				});
				transaction.commit();
			} catch (SQLException e) {
				logger.error("Error in important file contents.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		} catch (IOException e) {
			logger.error("Error in important file contents and reading file.", e);
			Slack.call("Exception", e.getLocalizedMessage());

			return false;
		}

		// everything fine
		return true;
	}

	public boolean addMovement(Participant participant, float longitude, float latitude, float elevation) {
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement(
						"INSERT INTO " + dataTableName + " ( participant_id, ts, x, y, z, track, pp1, pp2, pp3, data )"
								+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");) {
			final String track = "from Telegram";

			// data should be a JSON object, so ",," will cause JSON parsing error
			String summary = "{}";

			stmt.setLong(1, participant.getId());
			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			stmt.setFloat(3, longitude);
			stmt.setFloat(4, latitude);
			stmt.setFloat(5, elevation);
			stmt.setString(6, nss(track, 255));
			stmt.setString(7, nss(participant.getPublicParameter1(), 255));
			stmt.setString(8, nss(participant.getPublicParameter2(), 255));
			stmt.setString(9, nss(participant.getPublicParameter3(), 255));
			stmt.setString(10, nss(summary, 1024));
			stmt.executeUpdate();
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in add movement to dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
			return false;
		}

		// everything fine
		return true;
	}

	@Override
	public void deleteRecord(String file_name) {
		// do nothing
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

	public List<TimedMedia> getFiles() {
		List<TimedMedia> result = new LinkedList<TimedMedia>();
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, ts, file_name, description, status, participant_id FROM "
								+ maxIdJoinExpression(dataTableNameRawFiles) + " ORDER BY ts DESC;");
				ResultSet rs = stmt.executeQuery()) {

			while (rs.next()) {
				String filename = rs.getString("file_name");
				Timestamp timestamp = rs.getTimestamp("ts");
				Optional<File> file = getFile(filename);
				if (file.isPresent()) {
					String participantName = rs.getString("participant_id");
					long pid = DataUtils.parseLong(participantName);
					Participant participant = Participant.find.byId(pid);

					TimedMedia tm = new TimedMedia(rs.getLong("id"), timestamp, filename, "",
							rs.getString("description"), participant);
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
					String participantName = rs.getString("participant_id");
					long pid = DataUtils.parseLong(participantName);
					Participant participant = Participant.find.byId(pid);
					TimedMedia tm = new TimedMedia(rs.getLong("id"), timestamp, filename, "",
							rs.getString("description"), participant);
					result.add(tm);
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

		return result;
	}

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		export(queue, cluster.getParticipantList(), limit, start, end);
	}

	public void export(SourceQueueWithComplete<ByteString> queue, List<Long> participantIds, long limit, long start,
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
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, participant_id, ts, x, y, z, track, pp1, pp2, pp3, data FROM "
								+ dataTableName + whereClause + " ORDER BY id ASC " + limitExpression(limit) + ";");
				ResultSet rs = stmt.executeQuery();) {

			// header
			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			queue.offer(ByteString.fromString("id,participant_id,ts,lng,lat,ele,track,pp1,pp2,pp3,atemp,hr,cad\n"))
					.toCompletableFuture().get();

			// data
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong("id") + ",");
				sb.append(rs.getLong("participant_id") + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp("ts")) + ",");
				sb.append(rs.getFloat("x") + ",");
				sb.append(rs.getFloat("y") + ",");
				sb.append(rs.getFloat("z") + ",");
				sb.append(cf(rs.getString("track")) + ",");
				sb.append(cf(rs.getString("pp1")) + ",");
				sb.append(cf(rs.getString("pp2")) + ",");
				sb.append(cf(rs.getString("pp3")) + ",");

				// parse data as JSON
				String data = rs.getString("data");
				try {
					JsonNode jn = Json.parse(data);
					final JsonNode on = (jn == null || !jn.isObject()) ? Json.newObject() : jn;

					// iterate through values
					String values = Arrays.stream(new String[] { "atemp", "hr", "cad" }).map(key -> {
						JsonNode value = on.get(key);
						if (value != null) {
							return value.toString();
						} else {
							return "";
						}
					}).collect(Collectors.joining(","));
					sb.append(values);
				} catch (Exception e) {
					// logger.error("Error parsing movement data", e);
					sb.append(data);
				}
				sb.append("\n");

				queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
			}

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
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
						.prepareStatement("SELECT id, participant_id, ts, x, y, z, track, pp1, pp2, pp3, data FROM "
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
				on.put("x", rs.getFloat(4));
				on.put("y", rs.getFloat(5));
				on.put("z", rs.getFloat(6));
				on.put("track", rs.getString(7));
				on.put("pp1", rs.getString(8));
				on.put("pp2", rs.getString(9));
				on.put("pp3", rs.getString(10));

				// parse data as JSON
				String data = rs.getString(11);
				try {
					JsonNode jn = data != null ? Json.parse(data) : Json.newObject();
					jn.fields().forEachRemaining(e -> on.set(e.getKey(), e.getValue()));
				} catch (Exception e) {
					// add data verbatim
					on.put("data", data);

					// log potential parsing exceptions
					logger.error("Error in exporting dataset.", e);
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
}

package models.ds;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import java.util.stream.Collectors;

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.ebean.DB;
import io.ebean.Transaction;
import models.Dataset;
import models.sr.Cluster;
import models.sr.Participant;
import play.Logger;
import play.libs.Json;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;

public class SurveyDS extends LinkedDS {

	private static final Logger.ALogger logger = Logger.of(SurveyDS.class);

	public SurveyDS(Dataset dataset) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_sv";
	}

	/**
	 * create the actual database for the data
	 *
	 */
	@Override
	public void createInstance() {
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			connection.createStatement().execute("CREATE TABLE IF NOT EXISTS " + dataTableName + " ( " //
			        + "id bigint auto_increment not null," //
                    + "participant_id bigint," //
			        + "ts timestamp," //
			        + "data TEXT," //
			        + "PRIMARY KEY (id) );");
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset table in DB.", e);

		}
	}

	@Override
	public void migrateDatasetSchema() {
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			DatabaseMetaData dmd = connection.getMetaData();
			ResultSet res = dmd.getColumns(null, null, dataTableName.toUpperCase(), "DATA");
			if (res.next() && TEXT.equals(res.getString(TYPE_NAME))) {
				// schema is ok, do nothing
			} else {
				connection.createStatement().execute("ALTER TABLE IF EXISTS " + dataTableName + " " //
				        + "ALTER COLUMN data TEXT;");
				logger.info("Dataset table " + dataTableName + " migrated.");
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in migrating dataset table in DB.", e);

		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "participant_id", "ts", "data" };
	}

	public void addRecord(Participant participant, Date ts, String data) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("INSERT INTO " + dataTableName + " (participant_id, ts, data )" + " VALUES (?, ?, ?);");) {

            stmt.setLong(1, participant.getId());
			stmt.setTimestamp(2, new Timestamp(ts.getTime()));
			stmt.setString(3, nss(data));

			int i = stmt.executeUpdate();
			transaction.commit();

			// post update on OOCSI and log
			oocsiStreaming.datasetUpdate(dataset,
			        OOCSIStreamOutService.map().put("operation", "add").put("data", nss(data)).put("participant_id", participant.getId()).build());
			logger.trace(i + " records inserted into " + dataTableName);
		} catch (SQLException e) {
			logger.error("Error in inserting record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
        // export with participant filter
        exportProjected(queue, cluster.getParticipantList(), limit, start, end);
	}

	public void export(SourceQueueWithComplete<ByteString> queue, List<Long> participantIds, long limit, long start, long end) {

        final String whereClause;
		if (participantIds == null || participantIds.isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE participant_id IN ("
			        + participantIds.stream().map(l -> l.toString()).collect(Collectors.joining(",")) + ") "
			        + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT id, participant_id, ts, data FROM " + dataTableName
		                + whereClause + " ORDER BY id ASC " + limitExpression(limit) + ";");
		        ResultSet rs = stmt.executeQuery();) {

			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			queue.offer(ByteString.fromString("id,participant_id,ts,data\n")).toCompletableFuture().get();
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
                sb.append(rs.getLong(2) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append("\"" + rs.getString(4) + "\"\n");

				queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		queue.complete();
	}

	public void exportProjected(SourceQueueWithComplete<ByteString> queue, List<Long> participantIds, long limit, long start, long end) {

		// check whether there is a projection given
		if (!dataset.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			export(queue, participantIds, limit, start, end);
			return;
		}

        final String whereClause;
		if (participantIds == null || participantIds.isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE participant_id IN ("
			        + participantIds.stream().map(l -> l.toString()).collect(Collectors.joining(",")) + ") "
			        + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		// retrieve and process projection
		String projectionStr = dataset.getConfiguration().getOrDefault(Dataset.DATA_PROJECTION, "");
		final String[] projection = projectionStr.split(",");

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT id, participant_id, ts, data FROM " + dataTableName
		                + whereClause + " ORDER BY id ASC;");
		        ResultSet rs = stmt.executeQuery();) {

			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			queue.offer(ByteString.fromString("id,participant_id,ts," + projectionStr + "\n")).toCompletableFuture().get();
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
                sb.append(rs.getLong(2) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");

				// parse data as JSON
				String data = rs.getString(4);
				JsonNode jn = Json.parse(data);
				final JsonNode on = (jn == null || !jn.isObject()) ? Json.newObject() : jn;

				// iterate through values
				String values = Arrays.stream(projection).map(key -> {
					JsonNode value = on.get(key);
					if (value != null) {
						if (key.contains("choice") || key.contains("text")) {
							return value.toString();
						} else {
							return value.asInt() + "";
						}
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
		        PreparedStatement stmt = connection.prepareStatement("SELECT id, participant_id, ts, data FROM " + dataTableName
		                + whereClause + " ORDER BY id DESC LIMIT " + limit + ";");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {
				// ObjectNode on = result.addObject();
				ObjectNode on = Json.newObject();
				objects.add(on);

				// StringBuffer sb = new StringBuffer();
				on.put("id", rs.getLong(1));
                on.put("participant_id", rs.getLong(2));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));

				// parse data as JSON
				String data = rs.getString(4);
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
		} catch (SQLException e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
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
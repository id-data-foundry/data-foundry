package models.ds;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.ebean.DB;
import io.ebean.Transaction;
import models.Dataset;
import models.sr.Cluster;
import models.sr.Participant;
import models.sr.Wearable;
import play.Logger;
import play.libs.Json;
import services.slack.Slack;

public class GoogleFitDS extends LinkedDS {

	private static final Logger.ALogger logger = Logger.of(GoogleFitDS.class);

	public GoogleFitDS(Dataset dataset) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_gfds";
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
			        + "wearable_id bigint not null," //
			        + "user_id varchar(50) not null," //
			        + "ts timestamp not null," //
			        + "pp1 varchar(255)," //
			        + "pp2 varchar(255)," //
			        + "pp3 varchar(255)," //
			        + "data_date bigint," //
			        + "activity int," //
			        + "calories float," //
			        + "speed float," //
			        + "heart_rate float," //
			        + "step_count int," //
			        + "distance float," //
			        + "weight float," //
			        + "sleep int," //
			        + "PRIMARY KEY (id) );");

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset table in DB.", e);
		}
	}

	/**
	 * perform database table migration: add sleep column create migration statement
	 * 
	 */
	@Override
	public void migrateDatasetSchema() {
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			DatabaseMetaData dmd = connection.getMetaData();
			ResultSet res = dmd.getColumns(null, null, dataTableName.toUpperCase(), "SLEEP");
			if (res.next()) {
				// scheme is ok, do nothing
			} else {
				connection.createStatement()
				        .execute("ALTER TABLE " + dataTableName + " ADD COLUMN IF NOT EXISTS sleep int;");
				logger.info("Applied db table migration for " + dataTableName + ": added column 'SLEEP'.");
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in migrating dataset table.", e);
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "wearable_id", "user_id", "ts", "pp1", "pp2", "pp3", "data_date", "activity",
		        "calories", "speed", "heart_rate", "step_count", "distance", "weight", "sleep" };
	}

	/**
	 * add a new record
	 * 
	 * @param wearable
	 * @param scopeList
	 * @param dataDate
	 * @param ts
	 */
	public void addRecord(Wearable wearable, String[][] scopeList, long data_ts) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(
		                "INSERT INTO " + dataTableName + " (wearable_id, user_id, ts, pp1, pp2, pp3, data_date,"
		                        + " activity, calories, speed, heart_rate, step_count, distance, weight, sleep)"
		                        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");) {
			Participant participant = wearable.getClusterParticipant();
			if (participant != null) {
				stmt.setString(2, participant.getId() + "");
				stmt.setString(4,
				        wearable.getPublicParameter1() == null
				                ? (participant.getPublicParameter1() == null ? "" : participant.getPublicParameter1())
				                : wearable.getPublicParameter1());
				stmt.setString(5,
				        wearable.getPublicParameter2() == null
				                ? (participant.getPublicParameter2() == null ? "" : participant.getPublicParameter2())
				                : wearable.getPublicParameter2());
				stmt.setString(6,
				        wearable.getPublicParameter3() == null
				                ? (participant.getPublicParameter3() == null ? "" : participant.getPublicParameter3())
				                : wearable.getPublicParameter3());
			} else {
				stmt.setString(2, wearable.getUserId());
				stmt.setString(4, wearable.getPublicParameter1() == null ? "" : wearable.getPublicParameter1());
				stmt.setString(5, wearable.getPublicParameter2() == null ? "" : wearable.getPublicParameter2());
				stmt.setString(6, wearable.getPublicParameter3() == null ? "" : wearable.getPublicParameter3());
			}

			stmt.setLong(1, wearable.getId());
			stmt.setTimestamp(3, new Timestamp(new Date().getTime()));
			stmt.setString(7, Long.toString(data_ts));

			for (int i = 0; i < scopeList.length; i++) {
				stmt.setString((i + 8), scopeList[i][1]);
			}

			// stmt.setString(8, scope.equals("activity") ? jn.toString() : "");
			// stmt.setString(9, scope.equals("calories") ? jn.toString() : "");
			// stmt.setString(10, scope.equals("speed") ? jn.toString() : "");
			// stmt.setString(11, scope.equals("heart_rate") ? jn.toString() : "");
			// stmt.setString(12, scope.equals("step_count") ? jn.toString() : "");
			// stmt.setString(13, scope.equals("distance") ? jn.toString() : "");
			// stmt.setString(14, scope.equals("weight") ? jn.toString() : "");
			// stmt.setString(15, scope.equals("sleep") ? jn.toString() : "");
			stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in inserting record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	/**
	 * update sleep data
	 * 
	 * @param wearable
	 * @param scope
	 * @param jn
	 * @param dataDate
	 * @param ts
	 */
	public void updateSleepRecord(Wearable wearable, long dataDate, String data) {
		int rs = 0;

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("UPDATE " + dataTableName + " SET "
		                + "sleep = ?, ts = ? WHERE wearable_id = ? AND data_date = ?;");) {

			stmt.setString(1, data);
			stmt.setTimestamp(2, new Timestamp(new Date().getTime()));
			stmt.setLong(3, wearable.getId());
			stmt.setLong(4, dataDate);
			rs = stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in updating sleep record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());

			// no such record, add a new one
			if (rs <= 0) {
				String[][] scopeList = { { "activity", "0" }, { "calories", "0" }, { "speed", "0" },
				        { "heart_rate", "0" }, { "step_count", "0" }, { "distance", "0" }, { "weight", "0" },
				        { "sleep", data } };

				addRecord(wearable, scopeList, dataDate);
			}
		}
	}

	/**
	 * update data of whole row, if the record is not existed, then add a new record
	 * 
	 * @param wearable
	 * @param scopeList
	 * @param dataDate
	 * @param ts
	 */
	public void updateRecord(Wearable wearable, String[][] scopeList, long ts) {
		int rs = 0;

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("UPDATE " + dataTableName + " SET"
		                + " activity = ?, calories = ?, speed = ?, heart_rate = ?, step_count = ?, distance = ?,"
		                + " weight = ?, sleep = ?, ts = ? WHERE wearable_id = ? AND data_date = ?;");) {

			for (int i = 0; i < scopeList.length; i++) {
				if (i == 0 || i == 4 || i == 8) {
					stmt.setLong((i + 1), Long.parseLong(scopeList[i][1]));
				} else {
					stmt.setFloat((i + 1), Float.parseFloat(scopeList[i][1]));
				}
			}

			stmt.setTimestamp(9, new Timestamp(new Date().getTime()));
			stmt.setLong(10, wearable.getId());
			stmt.setLong(11, ts);
			rs = stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in updating record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());

			if (rs <= 0) {
				addRecord(wearable, scopeList, ts);
			}
		}
	}

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		export(queue, limit, start, end);
	}

	/**
	 * data download
	 * 
	 * @param end
	 * @param start
	 */
	public void export(SourceQueueWithComplete<ByteString> queue, long limit, long start, long end) {

		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + dataTableName
		                + timeFilterWhereClause(start, end) + " ORDER BY id ASC " + limitExpression(limit) + ";");
		        ResultSet rs = stmt.executeQuery();) {

			queue.offer(ByteString.fromString("id,wearable_id,user_id,ts,pp1,pp2,pp3,data_date,activity,calories,speed"
			        + ",heart_rate,step_count,distance,weight,sleep\n")).toCompletableFuture().get();

			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(rs.getLong(2) + ",");
				sb.append(rs.getLong(3) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(4)) + ",");
				sb.append(rs.getString(5) + ",");
				sb.append(rs.getString(6) + ",");
				sb.append(rs.getString(7) + ",");
				sb.append(rs.getLong(8) + ","); // data_date
				sb.append(rs.getInt(9) + ",");
				sb.append(rs.getFloat(10) + ",");
				sb.append(rs.getFloat(11) + ",");
				sb.append(rs.getFloat(12) + ",");
				sb.append(rs.getInt(13) + ",");
				sb.append(rs.getFloat(14) + ",");
				sb.append(rs.getFloat(15) + ",");
				sb.append(rs.getInt(16));
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
		if (cluster.getWearables().isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE wearable_id IN ("
			        + cluster.getWearables().stream().map(w -> w.getId().toString()).collect(Collectors.joining(","))
			        + ") " + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		List<ObjectNode> objects = new LinkedList<ObjectNode>();
		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(
		                "SELECT id, wearable_id, ts, activity, calories, speed, heart_rate, step_count, distance, weight, sleep, pp1, pp2, pp3 FROM "
		                        + dataTableName + whereClause + " ORDER BY id DESC LIMIT " + limit + ";");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {

				// ObjectNode on = result.addObject();
				ObjectNode on = Json.newObject();
				objects.add(on);

				// StringBuffer sb = new StringBuffer();
				on.put("id", rs.getLong(1));
				on.put("wearable_id", rs.getLong(2));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				on.put("activity", rs.getString(4));
				on.put("calories", rs.getFloat(5));
				on.put("speed", rs.getFloat(6));
				on.put("heart_rate", rs.getFloat(7));
				on.put("step_count", rs.getInt(8));
				on.put("distance", rs.getFloat(9));
				on.put("weight", rs.getFloat(10));
				on.put("sleep", rs.getInt(11));
				on.put("pp1", rs.getString(12));
				on.put("pp2", rs.getString(13));
				on.put("pp3", rs.getString(14));
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

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * checking data of specific data_date
	 * 
	 * @param wearable
	 * @param dataDate
	 * @return
	 */
	public boolean hasRecord(Wearable wearable, long dataDate) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(
		                "SELECT id FROM " + dataTableName + " WHERE wearable_id = ? AND data_date = ?;");) {

			stmt.setLong(1, wearable.getId());
			stmt.setLong(2, dataDate);
			ResultSet rs = stmt.executeQuery();

			boolean hasRecord = rs.next() ? true : false;
			transaction.commit();

			return hasRecord;
			// return true;
		} catch (Exception e) {
			logger.error("Error in checking record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return false;
	}

	@Override
	public void lastUpdatedSource(Map<Long, Long> sourceUpdates) {
		super.lastUpdatedSource(sourceUpdates, "wearable_id");
	}
}

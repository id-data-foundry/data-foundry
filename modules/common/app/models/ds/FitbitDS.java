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

import com.fasterxml.jackson.databind.JsonNode;
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

public class FitbitDS extends LinkedDS {

	private static final Logger.ALogger logger = Logger.of(FitbitDS.class);

	public FitbitDS(Dataset dataset) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_fbds";
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
			        + "heartrate int," //
			        + "activity varchar(16383)," //
			        + "calories float," //
			        + "steps int," //
			        + "distance float," //
			        + "floors int," //
			        + "elevation float," //
			        + "sleep varchar(10)," //
			        + "weight float," //
			        + "bmi float," //
			        + "fat float," //
			        + "PRIMARY KEY (id) );");

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset table in DB.", e);
		}
	}

	/**
	 * perform database table migration: add bmi and fat columns
	 * 
	 */
	@Override
	public void migrateDatasetSchema() {
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			DatabaseMetaData dmd = connection.getMetaData();
			ResultSet res = dmd.getColumns(null, null, dataTableName.toUpperCase(), "BMI");
			if (res.next()) {
				// scheme is ok, do nothing
			} else {
				connection.createStatement()
				        .execute("ALTER TABLE " + dataTableName + " ADD COLUMN IF NOT EXISTS bmi float; "
				                + "ALTER TABLE " + dataTableName + " ADD COLUMN IF NOT EXISTS fat float;");
				logger.info("Applied db table migration for " + dataTableName + ": added column 'BMI' and 'FAT'.");
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in migrating dataset table.", e);
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "wearable_id", "user_id", "ts", "pp1", "pp2", "pp3", "data_date", "heartrate",
		        "activity", "calories", "steps", "distance", "floors", "elevation", "sleep", "weight", "bmi", "fat" };
	}

	/**
	 * write data to Fitbit dataset table
	 * 
	 * @param wearable
	 * @param scope
	 * @param jn
	 * @param dataDate
	 */
	public void addRecord(Wearable wearable, String scope, JsonNode jn, long dataDate) {
		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
		                + " (wearable_id, user_id, ts, pp1, pp2, pp3, data_date, heartrate, activity, calories, steps,"
		                + " distance, floors, elevation, sleep, weight, bmi, fat )"
		                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");) {

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
			stmt.setString(7, Long.toString(dataDate));
			stmt.setString(8, scope.equals("heartrate") ? jn.get("value").toString() : "0");
			stmt.setString(9, scope.equals("activity") ? jn.toString() : "");
			stmt.setString(10, scope.equals("calories") ? jn.get("value").toString() : "0");
			stmt.setString(11, scope.equals("steps") ? jn.get("value").toString() : "0");
			stmt.setString(12, scope.equals("distance") ? jn.get("value").toString() : "0");
			stmt.setString(13, scope.equals("floors") ? jn.get("value").toString() : "0");
			stmt.setString(14, scope.equals("elevation") ? jn.get("value").toString() : "0");
			stmt.setString(15, scope.equals("sleep") ? jn.get("level").toString() : "");
			stmt.setString(16, scope.equals("weight") ? jn.get("weight").toString() : "0");
			stmt.setString(17, scope.equals("bmi") ? jn.get("bmi").toString() : "0");
			stmt.setString(18, scope.equals("fat") ? jn.get("fat").toString() : "0");
			stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in inserting record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	/**
	 * write data to Fitbit dataset table
	 * 
	 * @param wearable
	 * @param scope
	 * @param jn
	 * @param dataDate
	 */
	public void addEmptyRecord(Wearable wearable, String scope, String emptyContent, long dataDate) {
		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
		                + " (wearable_id, user_id, ts, pp1, pp2, pp3, data_date, heartrate, activity, calories, steps,"
		                + " distance, floors, elevation, sleep, weight, bmi, fat )"
		                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");) {

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
			stmt.setString(7, Long.toString(dataDate));
			stmt.setString(8, scope.equals("heartrate") ? emptyContent : "0");
			stmt.setString(9, scope.equals("activity") ? emptyContent : "");
			stmt.setString(10, scope.equals("calories") ? emptyContent : "0");
			stmt.setString(11, scope.equals("steps") ? emptyContent : "0");
			stmt.setString(12, scope.equals("distance") ? emptyContent : "0");
			stmt.setString(13, scope.equals("floors") ? emptyContent : "0");
			stmt.setString(14, scope.equals("elevation") ? emptyContent : "0");
			stmt.setString(15, scope.equals("sleep") ? emptyContent : "");
			stmt.setString(16, scope.equals("weight") ? emptyContent : "0");
			stmt.setString(17, scope.equals("bmi") ? emptyContent : "0");
			stmt.setString(18, scope.equals("fat") ? emptyContent : "0");
			stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in inserting empty record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	/**
	 * update a record based on new data from the fitbit service
	 * 
	 * @param wearable
	 * @param scope
	 * @param jn
	 * @param dataDate
	 * @param ts
	 */
	public void updateRecord(Wearable wearable, String scope, JsonNode jn, long dataDate) {
		if (scope == null || !scope.matches("^[a-zA-Z0-9_]+$")) {
			return;
		}

		// check against schema (whitelist validation)
		boolean found = false;
		for (String col : getSchema()) {
			if (col.equals(scope)) {
				found = true;
				break;
			}
		}

		if (!found) {
			return;
		}

		int rs = 0;
		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("UPDATE " + dataTableName + " SET " + scope
		                + " = ?, ts = ? WHERE wearable_id = ? AND data_date = ?;");) {

			switch (scope) {
			case "sleep":
				stmt.setString(1, jn.get("level").toString());
				break;
			case "activity":
				stmt.setString(1, jn.toString());
				break;
			case "weight":
				stmt.setString(1, jn.get("weight").toString());
				break;
			case "bmi":
				stmt.setString(1, jn.get("bmi").toString());
				break;
			case "fat":
				stmt.setString(1, jn.get("fat").toString());
				break;
			default:
				stmt.setString(1, jn.get("value").toString());
			}
			stmt.setTimestamp(2, new Timestamp(new Date().getTime()));
			stmt.setLong(3, wearable.getId());
			stmt.setLong(4, dataDate);
			rs = stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in updating record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());

			if (rs <= 0) {
				addRecord(wearable, scope, jn, dataDate);
			}
		}
	}

	/**
	 * update a record based on new data from the fitbit service
	 * 
	 * @param wearable
	 * @param scope
	 * @param jn
	 * @param dataDate
	 * @param ts
	 */
	public void updateEmptyRecord(Wearable wearable, String scope, String emptyContent, long dataDate) {
		if (scope == null || !scope.matches("^[a-zA-Z0-9_]+$")) {
			return;
		}

		// check against schema (whitelist validation)
		boolean found = false;
		for (String col : getSchema()) {
			if (col.equals(scope)) {
				found = true;
				break;
			}
		}

		if (!found) {
			return;
		}

		int rs = 0;
		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("UPDATE " + dataTableName + " SET " + scope
		                + " = ?, ts = ? WHERE wearable_id = ? AND data_date = ?;");) {

			stmt.setString(1, emptyContent);
			stmt.setTimestamp(2, new Timestamp(new Date().getTime()));
			stmt.setLong(3, wearable.getId());
			stmt.setLong(4, dataDate);
			rs = stmt.executeUpdate();

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in updating empty record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());

			if (rs <= 0) {
				addEmptyRecord(wearable, scope, emptyContent, dataDate);
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
	 * @param queue
	 * @param end
	 * @param start
	 */
	public void export(SourceQueueWithComplete<ByteString> queue, long limit, long start, long end) {
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + dataTableName
		                + timeFilterWhereClause(start, end) + " ORDER BY id ASC " + limitExpression(limit) + ";");
		        ResultSet rs = stmt.executeQuery();) {

			queue.offer(ByteString.fromString("id,wearable_id,user_id,ts,pp1,pp2,pp3,data_date,heartrate,activity"
			        + ",calories,steps,distance,floors,elevation,sleep,weight,bmi,fat\n")).toCompletableFuture().get();

			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(rs.getLong(2) + ",");
				sb.append(rs.getLong(3) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(4)) + ",");
				sb.append(rs.getString(5) + ",");
				sb.append(rs.getString(6) + ",");
				sb.append(rs.getString(7) + ",");
				sb.append(rs.getString(8) + ",");
				sb.append(rs.getInt(9) + ",");
				sb.append(rs.getString(10) + ",");
				sb.append(rs.getFloat(11) + ",");
				sb.append(rs.getInt(12) + ",");
				sb.append(rs.getFloat(13) + ",");
				sb.append(rs.getInt(14) + ",");
				sb.append(rs.getInt(15) + ",");
				sb.append(rs.getString(16) + ",");
				sb.append(rs.getFloat(17) + ",");
				sb.append(rs.getFloat(18) + ",");
				sb.append(rs.getFloat(19));
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
		                "SELECT id, wearable_id, ts, activity, calories, heartrate, steps, distance, floors, elevation, weight, bmi, fat, sleep, pp1, pp2, pp3 FROM "
		                        + dataTableName + whereClause + " ORDER BY id DESC LIMIT " + limit + ";");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {

				// ObjectNode on = result.addObject();
				ObjectNode on = Json.newObject();
				objects.add(on);

				on.put("id", rs.getLong(1));
				on.put("wearable_id", rs.getLong(2));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				on.put("activity", rs.getString(4));
				on.put("calories", rs.getFloat(5));
				on.put("heart_rate", rs.getInt(6));
				on.put("step_count", rs.getInt(7));
				on.put("distance", rs.getFloat(8));
				on.put("floors", rs.getInt(9));
				on.put("elevation", rs.getInt(10));
				on.put("weight", rs.getFloat(11));
				on.put("bmi", rs.getFloat(12));
				on.put("fat", rs.getFloat(13));
				on.put("sleep", rs.getString(14));
				on.put("pp1", rs.getString(15));
				on.put("pp2", rs.getString(16));
				on.put("pp3", rs.getString(17));
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

	public boolean hasRecord(Wearable wearable, long timeStart) {
		long timeEnd = timeStart + 86400000l;

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT id FROM " + dataTableName
		                + " WHERE wearable_id = ? AND data_date >= ? AND data_date <= ?;");) {

			stmt.setLong(1, wearable.getId());
			stmt.setLong(2, timeStart);
			stmt.setLong(3, timeEnd);
			ResultSet rs = stmt.executeQuery();

			boolean hasRecord = rs.next() ? true : false;
			transaction.commit();
			return hasRecord;
		} catch (Exception e) {
			logger.error("Error in checking for record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
			return false;
		}
	}

	@Override
	public void lastUpdatedSource(Map<Long, Long> sourceUpdates) {
		super.lastUpdatedSource(sourceUpdates, "wearable_id");
	}
}

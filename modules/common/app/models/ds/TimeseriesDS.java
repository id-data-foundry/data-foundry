package models.ds;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
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
import models.sr.Device;
import play.Logger;
import play.libs.Json;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;

public class TimeseriesDS extends LinkedDS {

	static final Logger.ALogger logger = Logger.of(TimeseriesDS.class);

	private static final List<String> EXCLUSION = new ArrayList<String>(
	        Arrays.asList("device_id", "activity", "timestamp"));

	public TimeseriesDS(Dataset dataset) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_ts";
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
			        + "device_id bigint," //
			        + "ts timestamp," //
			        + "activity varchar(255),"//
			        + "pp1 varchar(255)," //
			        + "pp2 varchar(255)," //
			        + "pp3 varchar(255)," //
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
		return new String[] { "id", "device_id", "ts", "activity", "pp1", "pp2", "pp3", "data" };
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * add a sample to the database for an open participation device
	 * 
	 * @param device_name
	 * @param ts
	 * @param activity
	 * @param data
	 */
	public void addRecord(String device_name, Date ts, String activity, String data) {
		ObjectNode on;
		try {
			JsonNode element = Json.parse(data);
			if (element != null && element.isObject()) {
				on = (ObjectNode) element;
			} else {
				on = Json.newObject().put("data", data);
			}
		} catch (Exception e) {
			on = Json.newObject().put("data", data);
		}

		addRecord(device_name, ts, activity, on);
	}

	/**
	 * will attempt to parse the <code>data</code> attribute before adding to the database
	 * 
	 * @param device
	 * @param ts
	 * @param activity
	 * @param data
	 */
	public void addRecord(Device device, Date ts, String activity, String data) {
		ObjectNode on;
		try {
			JsonNode element = Json.parse(data);
			if (element != null && element.isObject()) {
				on = (ObjectNode) element;
			} else {
				on = Json.newObject().put("data", data);
			}
		} catch (Exception e) {
			on = Json.newObject().put("data", data);
		}

		addRecord(device, ts, activity, on);
	}

	/**
	 * add a sample to the database for an open participation device
	 * 
	 * @param device_name
	 * @param ts
	 * @param activity
	 * @param data
	 */
	public void addRecord(String device_name, Date ts, String activity, ObjectNode data) {
		// create empty device
		final Device device = new Device();
		device.setId(-1l);
		device.setRefId(device_name);
		device.setPublicParameter1(device_name);

		addRecord(device, ts, activity, data);
	}

	/**
	 * add a sample to the database for a given registered device
	 * 
	 * @param device
	 * @param ts
	 * @param activity
	 * @param data
	 */
	public void addRecord(Device device, Date ts, String activity, ObjectNode data) {

		// try to extract the timestamp from the data
		long timestamp = 0;
		if (data.has("timestamp")) {
			timestamp = data.get("timestamp").asLong(timestamp);

			// check seconds or milliseconds
			if (timestamp < 1500000000000L) {
				timestamp *= 1000;
			}
		}

		// add record
		internalAddRecord(device, timestamp > 0 ? new Date(timestamp) : ts, activity, data.toString());

		// post update on OOCSI
		oocsiStreaming.datasetUpdate(dataset,
		        OOCSIStreamOutService.map().put("operation", "add").put("activity", nss(activity, 255))
		                .put("data", data).put("device_id", nss(device.getRefId(), 32)).build());

		// check whether projection update is necessary
		updateProjection(data);
	}

	/**
	 * internal operation to add a sample to the database for any device
	 * 
	 * @param device
	 * @param ts
	 * @param activity
	 * @param data
	 */
	public void internalAddRecord(Device device, Date ts, String activity, String data) {
		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
		                + " (device_id, ts, activity, pp1, pp2, pp3, data )" + " VALUES (?, ?, ?, ?, ?, ?, ?);");) {
			stmt.setLong(1, device.getId());
			stmt.setTimestamp(2, new Timestamp(ts.getTime()));
			stmt.setString(3, nss(activity, 255));
			stmt.setString(4, nss(device.getPublicParameter1(), 255));
			stmt.setString(5, nss(device.getPublicParameter2(), 255));
			stmt.setString(6, nss(device.getPublicParameter3(), 255));
			stmt.setString(7, nss(data));
			stmt.executeUpdate();
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in inserting record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	/**
	 * internal operation to add a sample to the database for any device
	 * 
	 * @param deviceId
	 * @param ts
	 * @param activity
	 * @param data
	 */
	public void internalAddRecord(String device_id, String pp1, String pp2, String pp3, Date ts, String activity,
	        ObjectNode data) {

		// create synthetic public parameter 1 for open participation devices
		final String spp = device_id;

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
		                + " (device_id, ts, activity, pp1, pp2, pp3, data )" + " VALUES (?, ?, ?, ?, ?, ?, ?);");) {

			stmt.setLong(1, 0);
			stmt.setTimestamp(2, new Timestamp(ts.getTime()));
			stmt.setString(3, nss(activity, 255));
			stmt.setString(4, nss(pp1 == null ? spp : pp1, 255));
			stmt.setString(5, nss(pp2, 255));
			stmt.setString(6, nss(pp3, 255));
			stmt.setString(7, nss(data.toString()));
			stmt.executeUpdate();
			transaction.commit();

			// note: don't post to OOCSI here because this is a batch operation
		} catch (Exception e) {
			logger.error("Error in inserting record in dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		exportProjected(queue, cluster.getDeviceList(), limit, start, end);
	}

	public void export(SourceQueueWithComplete<ByteString> destinationQueue, List<Long> deviceIds, long limit,
	        long start, long end) {

		final String whereClause;
		if (deviceIds.isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE device_id IN ("
			        + deviceIds.stream().map(l -> l.toString()).collect(Collectors.joining(",")) + ") "
			        + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, device_id, ts, activity, pp1, pp2, pp3, data FROM "
		                        + dataTableName + whereClause + " ORDER BY ts ASC " + limitExpression(limit) + ";");
		        ResultSet rs = stmt.executeQuery();) {

			destinationQueue.offer(ByteString.fromString("id,device_id,ts,activity,pp1,pp2,pp3,data\n"))
			        .toCompletableFuture().get();
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(rs.getLong(2) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append(rs.getString(4) + ",");
				sb.append(rs.getString(5) + ",");
				sb.append(rs.getString(6) + ",");
				sb.append(rs.getString(7) + ",");
				sb.append(rs.getString(8));
				sb.append("\n");

				destinationQueue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
			}

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		destinationQueue.complete();
	}

	public void exportProjected(SourceQueueWithComplete<ByteString> destinationQueue, List<Long> deviceIds, long limit,
	        long start, long end) {

		// check whether there is a projection given
		if (!dataset.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			export(destinationQueue, deviceIds, limit, start, end);
			return;
		}

		final String whereClause;
		if (deviceIds.isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE device_id IN ("
			        + deviceIds.stream().map(l -> l.toString()).collect(Collectors.joining(",")) + ") "
			        + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		// retrieve and process projection
		final String[] projection = dataset.getConfiguration().getOrDefault(Dataset.DATA_PROJECTION, "").split(",");

		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, device_id, ts, activity, pp1, pp2, pp3, data FROM "
		                        + dataTableName + whereClause + " ORDER BY id ASC " + limitExpression(limit) + ";");
		        ResultSet rs = stmt.executeQuery();) {

			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			destinationQueue
			        .offer(ByteString
			                .fromString("id,device_id,ts,activity,pp1,pp2,pp3," + String.join(",", projection) + "\n"))
			        .toCompletableFuture().get();

			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(rs.getLong(2) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append(rs.getString(4) + ",");
				sb.append(rs.getString(5) + ",");
				sb.append(rs.getString(6) + ",");
				sb.append(rs.getString(7) + ",");

				// parse data as JSON
				String data = rs.getString(8);
				try {
					JsonNode jn = data != null ? Json.parse(data) : Json.newObject();
					final JsonNode on = (jn == null || !jn.isObject()) ? Json.newObject() : jn;

					// iterate through values
					String values = Arrays.stream(projection).map(key -> {
						final JsonNode value = on.get(key);
						if (value != null) {
							String valueStr = value.toString();
							return valueStr.equals("null") ? "" : valueStr;
						} else {
							return "";
						}
					}).collect(Collectors.joining(","));
					sb.append(values);
					sb.append("\n");

				} catch (Exception e) {
					// log potential parsing exceptions
					logger.error("Error in parsing Json in dataset record to export.", e);

					// log the problematic line still
					sb.append(data);
					sb.append("\n");
				}

				destinationQueue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());

		}

		destinationQueue.complete();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * export the dataset as JSON array to the given file
	 *
	 *
	 */
	@Override
	public void exportProjectedToFile(FileWriter fileWriter, Cluster cluster, long limit, long start, long end) {
		final String whereClause;
		if (cluster.getDevices().isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE device_id IN ("
			        + cluster.getDevices().stream().map(d -> d.getId().toString()).collect(Collectors.joining(","))
			        + ") " + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		// start JSON array
		try {
			fileWriter.append("[");
		} catch (IOException e) {
		}

		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, device_id, ts, activity, pp1, pp2, pp3, data FROM "
		                        + dataTableName + whereClause + " ORDER BY id ASC LIMIT " + limit + ";");
		        ResultSet rs = stmt.executeQuery();) {

			boolean firstRow = true;
			while (rs.next()) {
				ObjectNode on = Json.newObject();

				// StringBuffer sb = new StringBuffer();
				on.put("id", rs.getLong(1));
				on.put("device_id", rs.getLong(2));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				on.put("activity", rs.getString(4));
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
					// note: it's not worth logging these issues, there are just too many ways this goes wrong
				}

				// write serialized object
				try {
					if (firstRow) {
						fileWriter.append(on.toString());
						firstRow = false;
					} else {
						fileWriter.append("," + on.toString());
					}
				} catch (Exception e) {
				}
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in exporting dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		try {
			fileWriter.append("]");
			fileWriter.close();
		} catch (IOException e) {
		}
	}

	public ArrayNode retrieveProjected(Cluster cluster, long limit, long start, long end) {

		final String whereClause;
		if (cluster.getDevices().isEmpty()) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE device_id IN ("
			        + cluster.getDevices().stream().map(d -> d.getId().toString()).collect(Collectors.joining(","))
			        + ") " + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		// set a max limit for the result as defined in constant MAX_ROWS_RETRIEVE_PROJECTED
		if (limit < 0 || limit > MAX_ROWS_EXPORT) {
			limit = MAX_ROWS_EXPORT;
		}

		List<ObjectNode> objects = new LinkedList<ObjectNode>();
		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, device_id, ts, activity, pp1, pp2, pp3, data FROM "
		                        + dataTableName + whereClause + " ORDER BY id DESC LIMIT " + limit + ";");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {
				// ObjectNode on = result.addObject();
				ObjectNode on = Json.newObject();
				objects.add(on);

				// StringBuffer sb = new StringBuffer();
				on.put("id", rs.getLong(1));
				on.put("device_id", rs.getLong(2));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				on.put("activity", rs.getString(4));
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
					// note: it's not worth logging these issues, there are just too many ways this goes wrong
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

	public ArrayNode getItemsNested() {

		final ArrayNode result = Json.newArray();

		// export the data
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT id, device_id, ts, activity, pp1, pp2, pp3, data FROM " + dataTableName
								+ " WHERE id IN " + "(SELECT MAX(id) FROM " + dataTableName
								+ " GROUP BY device_id, pp1) ORDER BY ts DESC LIMIT 1000;");
				ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {
				final ObjectNode item = result.addObject();

				item.put("id", rs.getLong(1));
				long deviceId = rs.getLong(2);
				item.put("device_id", deviceId);
				item.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				item.put("activity", rs.getString(4));
				item.put("pp1", nss(rs.getString(5), 255));
				item.put("pp2", nss(rs.getString(6), 255));
				item.put("pp3", nss(rs.getString(7), 255));

				// Set resource_id for the view
				if (deviceId > 0) {
					item.put("resource_id", "Device " + deviceId);
				} else {
					item.put("resource_id", nss(rs.getString(5), 255));
				}

				// parse data as JSON
				try {
					JsonNode jn = Json.parse(rs.getString(8));
					JsonNode on = (jn == null || !jn.isObject()) ? Json.newObject() : jn;
					item.set("data", on);
				} catch (Exception e) {
					// catch parsing exceptions
				}
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("TimeseriesDS getItemsNested general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check if a projection setting exists, and if not, creates it
	 * 
	 * @param ds
	 * @param tssc
	 * @param data
	 */
	public void updateProjection(ObjectNode data) {
		updateProjection(retrieveProjection(data));
	}

	/**
	 * check if a projection setting exists, and if not, creates it
	 * 
	 * @param projection
	 */
	public void updateProjection(final List<String> projection) {
		String updatedProjection;
		// existing --> compare before store
		if (dataset.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			boolean changed = false;
			updatedProjection = dataset.getConfiguration().get(Dataset.DATA_PROJECTION);
			List<String> existingProjection = new ArrayList<String>(Arrays.asList(updatedProjection.split(",")));
			for (String column : projection) {
				if (!existingProjection.contains(column)) {
					existingProjection.add(column);
					changed = true;
				}
			}

			// update only on change
			if (changed) {
				existingProjection.sort(String.CASE_INSENSITIVE_ORDER);
				updatedProjection = existingProjection.stream().collect(Collectors.joining(","));
			}
		} else {
			// not existing? --> store it in configuration directly
			updatedProjection = projection.stream().collect(Collectors.joining(","));
		}

		// store projection
		dataset.getConfiguration().put(Dataset.DATA_PROJECTION, updatedProjection);
		dataset.update();
	}

	/**
	 * retrieve the new projection from a newly added record's data payload
	 * 
	 * @param data in JSON format
	 * @return
	 */
	private static List<String> retrieveProjection(ObjectNode data) {
		List<String> projection = new LinkedList<String>();

		// iterate through values
		data.fields().forEachRemaining(e -> {
			// if value is not on exclusion list, include it
			String column = e.getKey();
			if (!EXCLUSION.contains(column)) {
				projection.add(column);
			}
		});

		// sort abc order
		projection.sort(String.CASE_INSENSITIVE_ORDER);

		return projection;
	}

	@Override
	public void lastUpdatedSource(Map<Long, Long> sourceUpdates) {
		super.lastUpdatedSource(sourceUpdates, "device_id");
	}
}

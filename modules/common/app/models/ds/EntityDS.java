package models.ds;

import java.sql.Connection;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
import models.sr.DataResource;
import play.Logger;
import play.libs.Json;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;

public class EntityDS extends LinkedDS {

	private static final List<String> EXCLUSION = new ArrayList<String>(Arrays.asList("resource_id", "timestamp"));
	private static final Logger.ALogger logger = Logger.of(EntityDS.class);

	public EntityDS(Dataset dataset) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_et";
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
			        + "resource_id varchar(63)," //
			        + "token varchar(63)," //
			        + "ts timestamp," //
			        + "pp1 varchar(255)," //
			        + "pp2 varchar(255)," //
			        + "pp3 varchar(255)," //
			        + "data TEXT," //
			        + "PRIMARY KEY (id) );");
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset table in DB.", e);
		}

		// create an index for the table
		migrateDatasetSchema();
	}

	@Override
	public void migrateDatasetSchema() {
		// add index for the resource id
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			connection.createStatement().execute("CREATE INDEX IF NOT EXISTS ix_" + dataTableName + "_resource_id on "
			        + dataTableName + " (resource_id);");
			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS add index problem.", e);
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "resource_id", "ts", "pp1", "pp2", "pp3", "data" };
	}

	/**
	 * add item to entity store
	 * 
	 * @param resource_id
	 * @param token
	 * @param data
	 * @return
	 */
	public Optional<ObjectNode> addItem(String resource_id, Optional<String> token, String data) {
		try {
			JsonNode jn = Json.parse(data);
			return addItem(resource_id, token, jn);
		} catch (Exception e) {
			logger.error("EntityDS addItem parsing problem", e);
		}

		return Optional.empty();
	}

	/**
	 * add item to entity store, token provided or empty Optional for internal use (= no token check)
	 * 
	 * @param resource_id
	 * @param token
	 * @param jn
	 * @return
	 */
	public Optional<ObjectNode> addItem(String resource_id, Optional<String> token, JsonNode jn) {

		// never store invalid JSON data in an Entity dataset
		if (jn == null || !jn.isObject()) {
			return Optional.empty();
		}

		final ObjectNode on = (ObjectNode) jn;
		internalAddItem(resource_id, token, new Date(), on);

		// check JSON structure and whether projection update is necessary
		updateProjection(dataset, on);

		// post update on OOCSI
		oocsiStreaming.datasetUpdate(dataset,
		        OOCSIStreamOutService.map().put("operation", "add").put("resource_id", resource_id).build());

		return Optional.of(on);
	}

	/**
	 * retrieve an item from the entity store. note that the retrieved item might be an empty JSON object.
	 * 
	 * @param resource_id
	 * @param token
	 * @return
	 */
	public Optional<ObjectNode> getItem(String resource_id, Optional<String> token) {
		return internalGetItem(resource_id, token);
	}

	/**
	 * update item in entity store, token provided or empty Optional for internal use (= no token check)
	 * 
	 * @param resource_id
	 * @param token
	 * @param data
	 * @return
	 */
	public Optional<ObjectNode> updateItem(String resource_id, Optional<String> token, String data) {
		try {
			JsonNode jn = Json.parse(data);

			// post update on OOCSI
			oocsiStreaming.datasetUpdate(dataset,
			        OOCSIStreamOutService.map().put("operation", "update").put("resource_id", resource_id).build());

			return updateItem(resource_id, token, jn);
		} catch (Exception e) {
			// ignore
		}

		return Optional.empty();
	}

	/**
	 * update item in entity store, token provided or empty Optional for internal use (= no token check)
	 * 
	 * @param resource_id
	 * @param token
	 * @param jn
	 * @return
	 */
	public Optional<ObjectNode> updateItem(String resource_id, Optional<String> token, JsonNode jn) {

		// no json data given --> replace all, save
		if (jn == null || !jn.isObject()) {
			return internalAddItem(resource_id, token, new Date(), Json.newObject());
		}

		// json data given --> check internal data
		final ObjectNode newData = (ObjectNode) jn;
		final ObjectNode existingData = internalGetItemWithPublicParameters(resource_id, token)
		        .orElse(Json.newObject());

		final Optional<ObjectNode> result;
		// check whether the item does not exist
		if (existingData.isEmpty()) {
			// add new data
			result = internalAddItem(resource_id, token, new Date(), newData);
		} else {
			// add or update new data in existing data
			newData.fields().forEachRemaining(e -> {
				existingData.set(e.getKey(), e.getValue());
			});

			// update existing data
			result = internalUpdateItem(resource_id, token, existingData.path("pp1").asText(),
			        existingData.path("pp2").asText(), existingData.path("pp2").asText(), new Date(), existingData);
		}

		// check whether projection update is necessary
		updateProjection(dataset, newData);

		// clean the entity table asynchronously
		CompletableFuture.runAsync(() -> internalDeleteObsoleteItems(resource_id));

		return result;
	}

	/**
	 * delete item from entity store, token provided or empty Optional for internal use (= no token check)
	 * 
	 * @param resource_id
	 * @param token
	 * @return
	 */
	public Optional<ObjectNode> deleteItem(String resource_id, Optional<String> token) {
		final Optional<ObjectNode> result = internalDeleteItem(resource_id, token);

		// post update on OOCSI
		oocsiStreaming.datasetUpdate(dataset,
		        OOCSIStreamOutService.map().put("operation", "delete").put("resource_id", resource_id).build());

		return result;
	}

	/**
	 * add a resource item directly without token check because this can only by triggered internally by project owner
	 * 
	 * @param resource
	 * @param ts
	 * @param data
	 */
	public void addResourceItem(DataResource resource, Date ts, ObjectNode jo) {
		internalAddItem(resource.getRefId(), Optional.empty(), resource.getPublicParameter1(),
		        resource.getPublicParameter2(), resource.getPublicParameter3(), ts, jo);

		// check whether projection update is necessary
		updateProjection(dataset, jo);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private Optional<ObjectNode> internalAddItem(String resource_id, Optional<String> token, Date ts, ObjectNode jo) {
		// check permissions for existing items -- if they exist, because we don't want to overwrite items with a new or
		// empty token
		Optional<String> internalItemToken = internalGetItemToken(resource_id);
		if (internalItemToken.isPresent()) {
			// we have a token set, is the request token set AND same as the internal one?
			if (token.isEmpty() || !token.get().equals(internalItemToken.get())) {
				return Optional.empty();
			}
		}

		return internalAddItem(resource_id, token, "", "", "", ts, jo);
	}

	private Optional<ObjectNode> internalUpdateItem(String resource_id, Optional<String> token, String pp1, String pp2,
	        String pp3, Date ts, ObjectNode jo) {

		// check permissions for existing items
		if (internalGetItem(resource_id, token).isEmpty()) {
			// do not add if resource_id and token to not match
			return Optional.empty();
		}

		return internalAddItem(resource_id, token, pp1, pp2, pp3, ts, jo);
	}

	/**
	 * add an item to the dataset without checking overwrite
	 * 
	 * @param resource_id
	 * @param token
	 * @param pp1
	 * @param pp2
	 * @param pp3
	 * @param ts
	 * @param jo
	 * @return
	 */
	private Optional<ObjectNode> internalAddItem(String resource_id, Optional<String> token, String pp1, String pp2,
	        String pp3, Date ts, ObjectNode jo) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
		                + " (resource_id, token, ts, pp1, pp2, pp3, data )" + " VALUES (?, ?, ?, ?, ?, ?, ?);");) {

			stmt.setString(1, resource_id);
			stmt.setString(2, token.orElse(""));
			stmt.setTimestamp(3, new Timestamp(ts.getTime()));
			stmt.setString(4, pp1);
			stmt.setString(5, pp2);
			stmt.setString(6, pp3);
			stmt.setString(7, jo == null ? "" : jo.toString());

			int i = stmt.executeUpdate();
			logger.trace(i + " records inserted into " + dataTableName);

			transaction.commit();
		} catch (SQLException e) {
			logger.error("SQL exception", e);
			Slack.call("Exception", e.getLocalizedMessage());
			return Optional.empty();
		} catch (Exception e) {
			logger.error("General exception", e);
			Slack.call("Exception", e.getLocalizedMessage());
			return Optional.empty();
		}

		return Optional.of(jo);
	}

	public Optional<String> internalGetItemToken(String resource_id) {

		String result = null;

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(
		                "SELECT token FROM " + dataTableName + " WHERE resource_id = ? ORDER BY ts DESC LIMIT 1;");) {
			stmt.setString(1, resource_id);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getString("token");
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS getItemToken general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return Optional.ofNullable(result);
	}

	private Optional<ObjectNode> internalGetItem(String resource_id, Optional<String> token) {

		String result = "";

		// faster SQL for just getting the last item
		String sql = "SELECT data FROM " + dataTableName + " WHERE id = (SELECT max(id) FROM " + dataTableName
		        + " WHERE resource_id = ?" + (token.isPresent() ? " AND token = ?" : "") + ");";

		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(sql);) {
			stmt.setString(1, resource_id);
			if (token.isPresent()) {
				stmt.setString(2, token.get());
			}
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getString("data");
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS getItem general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		JsonNode jn = Json.parse(result);
		return jn.isObject() ? Optional.of((ObjectNode) jn) : Optional.empty();
	}

	private Optional<ObjectNode> internalGetItemWithPublicParameters(String resource_id, Optional<String> token) {

		String jsonData = "";
		String pp1 = "", pp2 = "", pp3 = "";

		// faster SQL for just getting the last item
		String sql = "SELECT pp1, pp2, pp3, data FROM " + dataTableName + " WHERE id = (SELECT max(id) FROM "
		        + dataTableName + " WHERE resource_id = ?" + (token.isPresent() ? " AND token = ?" : "") + ");";

		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(sql);) {
			stmt.setString(1, resource_id);
			if (token.isPresent()) {
				stmt.setString(2, token.get());
			}
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				jsonData = rs.getString("data");
				pp1 = nss(rs.getString("pp1"), 255);
				pp2 = nss(rs.getString("pp2"), 255);
				pp3 = nss(rs.getString("pp3"), 255);
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS getItem general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		JsonNode jn = Json.parse(jsonData);
		if (jn.isObject()) {
			ObjectNode on = (ObjectNode) jn;
			on.put("pp1", pp1);
			on.put("pp2", pp2);
			on.put("pp3", pp3);
			return Optional.of(on);
		} else {
			return Optional.empty();
		}
	}

	public ArrayNode getItems() {

		final ArrayNode result = Json.newArray();

		// check whether there is a projection given
		if (!dataset.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			return result;
		}

		// retrieve and process projection
		final String[] projection = dataset.getConfiguration().get(Dataset.DATA_PROJECTION).split(",");

		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(
		                "SELECT id, resource_id, ts, pp1, pp2, pp3, data FROM " + dataTableName + " WHERE id IN "
		                        + "(SELECT MAX(id) FROM " + dataTableName + " GROUP BY resource_id) ORDER BY ts ASC;");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {
				final ObjectNode item = result.addObject();

				item.put("id", rs.getLong(1));
				item.put("resource_id", nss(rs.getString(2), 63));
				item.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				item.put("pp1", nss(rs.getString(4), 255));
				item.put("pp2", nss(rs.getString(5), 255));
				item.put("pp3", nss(rs.getString(6), 255));

				// parse data as JSON
				try {
					JsonNode jn = Json.parse(rs.getString("data"));
					final JsonNode on = (jn == null || !jn.isObject()) ? Json.newObject() : jn;

					// iterate through values
					Arrays.stream(projection).forEach(key -> {
						JsonNode value = on.get(key);
						if (value != null) {
							item.set(key, value);
						}
					});
				} catch (Exception e) {
					// catch parsing exceptions
				}
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS getItems general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	public ArrayNode getItemsNested() {

		final ArrayNode result = Json.newArray();

		// check whether there is a projection given
		if (!dataset.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			return result;
		}

		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, resource_id, ts, pp1, pp2, pp3, data FROM " + dataTableName
		                        + " WHERE id IN " + "(SELECT MAX(id) FROM " + dataTableName
		                        + " GROUP BY resource_id) ORDER BY ts ASC LIMIT 1000;");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {
				final ObjectNode item = result.addObject();

				item.put("id", rs.getLong(1));
				item.put("resource_id", nss(rs.getString(2), 63));
				item.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				item.put("pp1", nss(rs.getString(4), 255));
				item.put("pp2", nss(rs.getString(5), 255));
				item.put("pp3", nss(rs.getString(6), 255));

				// parse data as JSON
				try {
					JsonNode jn = Json.parse(rs.getString("data"));
					JsonNode on = (jn == null || !jn.isObject()) ? Json.newObject() : jn;
					item.set("data", on);
				} catch (Exception e) {
					// catch parsing exceptions
				}
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS getItemsNested general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	public ArrayNode getItemsMatching(String resource_id, Optional<String> token) {

		final ArrayNode result = Json.newArray();

		// check whether there is a projection given
		if (!dataset.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			return result;
		}

		// retrieve and process projection
		final String[] projection = dataset.getConfiguration().get(Dataset.DATA_PROJECTION).split(",");

		// faster SQL for just getting the last item
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, resource_id, ts, pp1, pp2, pp3, data FROM " + dataTableName
		                        + " WHERE id IN (SELECT max(id) FROM " + dataTableName + " WHERE resource_id LIKE ?"
		                        + (token.isPresent() ? " AND token = ?" : "") + " GROUP BY resource_id);");) {
			stmt.setString(1, resource_id + "%");
			if (token.isPresent()) {
				stmt.setString(2, token.get());
			}
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				final ObjectNode item = result.addObject();

				item.put("id", rs.getLong(1));
				item.put("resource_id", nss(rs.getString(2), 63));
				item.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				item.put("pp1", nss(rs.getString(4), 255));
				item.put("pp2", nss(rs.getString(5), 255));
				item.put("pp3", nss(rs.getString(6), 255));

				// parse data as JSON
				try {
					JsonNode jn = Json.parse(rs.getString("data"));
					final JsonNode on = (jn == null || !jn.isObject()) ? Json.newObject() : jn;

					// iterate through values
					Arrays.stream(projection).forEach(key -> {
						JsonNode value = on.get(key);
						if (value != null) {
							item.set(key, value);
						}
					});
				} catch (Exception e) {
					// catch parsing exceptions
				}
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS getItem general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public List<String> getResources() {

		final List<String> result = new LinkedList<String>();

		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT id, resource_id FROM " + dataTableName
		                + " WHERE id IN " + "(SELECT MAX(id) FROM " + dataTableName + " GROUP BY resource_id);");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {
				result.add(nss(rs.getString(2), 63));
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS getItems general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	private Optional<ObjectNode> internalDeleteItem(String resource_id, Optional<String> token) {

		boolean deleteSuccessful = false;

		// delete record
		String sql = "DELETE FROM " + dataTableName + " WHERE resource_id = ?"
		        + (token.isPresent() ? " AND token = ?" : "") + ";";
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(sql);) {
			stmt.setString(1, nss(resource_id, 63));
			if (token.isPresent()) {
				stmt.setString(2, nss(token.get(), 63));
			}

			deleteSuccessful = stmt.executeUpdate() > 0;

			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS deleteItems general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return deleteSuccessful ? Optional.of(Json.newObject()) : Optional.empty();
	}

	private boolean internalDeleteObsoleteItems(String resource_id) {

		boolean deleteSuccessful = false;

		// delete record
		String sql = "DELETE FROM " + dataTableName
		        + " WHERE resource_id = ? AND id < (SELECT MIN(id) FROM (SELECT id FROM " + dataTableName
		        + " WHERE resource_id = ? ORDER BY id DESC LIMIT 100));";
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(sql);) {
			stmt.setString(1, nss(resource_id, 63));
			stmt.setString(2, nss(resource_id, 63));

			int updateCount = stmt.executeUpdate();
			deleteSuccessful = updateCount > 0;
			if (updateCount > 10) {
				dataset.getProject().refresh();
				logger.info("Removed " + updateCount + "obsolete items from Entity dataset: " + dataset.getId() + " ("
				        + dataset.getProject().getOwner().getName() + ")");
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("EntityDS deleteItems general ex: ", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return deleteSuccessful;
	}

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		exportProjected(queue, start, end);
	}

	public void export(SourceQueueWithComplete<ByteString> queue, long start, long end) {

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(
		                "SELECT id, resource_id, ts, pp1, pp2, pp3, data FROM " + dataTableName + " WHERE id IN "
		                        + "(SELECT MAX(id) FROM " + dataTableName + " GROUP BY resource_id) ORDER BY ts ASC;");
		        ResultSet rs = stmt.executeQuery();) {

			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			queue.offer(ByteString.fromString("id,resource_id,ts,pp1,pp2,pp3,data\n")).toCompletableFuture().get();
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(nss(rs.getString(2), 63) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append(nss(rs.getString(4), 255) + ",");
				sb.append(nss(rs.getString(5), 255) + ",");
				sb.append(nss(rs.getString(6), 255) + ",");
				sb.append(nss(rs.getString(7), 10000));
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

	public void exportLog(SourceQueueWithComplete<ByteString> queue, long start, long end) {

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, resource_id, ts, pp1, pp2, pp3, data FROM " + dataTableName
		                        + timeFilterWhereClause(start, end) + " ORDER BY id ASC;");
		        ResultSet rs = stmt.executeQuery();) {

			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			queue.offer(ByteString.fromString("id,resource_id,ts,pp1,pp2,pp3,data\n")).toCompletableFuture().get();
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(nss(rs.getString(2), 63) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append(nss(rs.getString(4), 255) + ",");
				sb.append(nss(rs.getString(5), 255) + ",");
				sb.append(nss(rs.getString(6), 255) + ",");
				sb.append(nss(rs.getString(7), 10000));
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

	public void exportProjected(SourceQueueWithComplete<ByteString> queue, long start, long end) {

		// check whether there is a projection given
		if (!dataset.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			export(queue, start, end);
			return;
		}

		// retrieve and process projection
		final String[] projection = dataset.getConfiguration().getOrDefault(Dataset.DATA_PROJECTION, "").split(",");

		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, resource_id, ts, pp1, pp2, pp3, data FROM " + dataTableName
		                        + " WHERE id IN " + "(SELECT MAX(id) FROM " + dataTableName + " GROUP BY resource_id) "
		                        + timeFilterWhereClause(start, end).replace("WHERE", "AND") + " ORDER BY ts ASC;");
		        ResultSet rs = stmt.executeQuery();) {

			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			queue.offer(ByteString.fromString("id,resource_id,ts,pp1,pp2,pp3," + String.join(",", projection) + "\n"))
			        .toCompletableFuture().get();

			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(nss(rs.getString(2), 63) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append(nss(rs.getString(4), 255) + ",");
				sb.append(nss(rs.getString(5), 255) + ",");
				sb.append(nss(rs.getString(6), 255) + ",");

				// parse data as JSON
				String data = rs.getString(7);
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

	public void exportLogProjected(SourceQueueWithComplete<ByteString> queue, long start, long end) {

		// check whether there is a projection given
		if (!dataset.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			export(queue, start, end);
			return;
		}

		// retrieve and process projection
		final String[] projection = dataset.getConfiguration().get(Dataset.DATA_PROJECTION).split(",");

		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, resource_id, ts, pp1, pp2, pp3, data FROM " + dataTableName
		                        + timeFilterWhereClause(start, end) + " ORDER BY id ASC;");
		        ResultSet rs = stmt.executeQuery();) {

			queue.offer(ByteString.fromString("id,resource_id,ts,pp1,pp2,pp3," + String.join(",", projection) + "\n"))
			        .toCompletableFuture().get();

			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(nss(rs.getString(2), 63) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append(nss(rs.getString(4), 255) + ",");
				sb.append(nss(rs.getString(5), 255) + ",");
				sb.append(nss(rs.getString(6), 255) + ",");

				// parse data as JSON
				String data = rs.getString(7);
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

		List<ObjectNode> objects = new LinkedList<ObjectNode>();
		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection
		                .prepareStatement("SELECT id, resource_id, ts, pp1, pp2, pp3, data FROM " + dataTableName
		                        + " WHERE id IN " + "(SELECT MAX(id) FROM " + dataTableName + " GROUP BY resource_id) "
		                        + timeFilterWhereClause(start, end).replace("WHERE", "AND") + " ORDER BY ts ASC;");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {

				// ObjectNode on = result.addObject();
				ObjectNode on = Json.newObject();
				objects.add(on);

				// StringBuffer sb = new StringBuffer();
				on.put("id", rs.getLong(1));
				on.put("resource_id", rs.getString(2));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				on.put("pp1", rs.getString(4));
				on.put("pp2", rs.getString(5));
				on.put("pp3", rs.getString(6));

				// parse data as JSON
				String data = rs.getString(7);
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

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check if a projection setting exists, and if not, creates it
	 * 
	 * @param ds
	 * @param tssc
	 * @param data
	 */
	private static void updateProjection(Dataset ds, ObjectNode data) {

		// retrieve the projection
		final List<String> projection = retrieveProjection(data);

		// not existing? --> store it in configuration directly
		String updatedProjection = projection.stream().collect(Collectors.joining(","));

		// existing --> compare before store
		if (ds.getConfiguration().containsKey(Dataset.DATA_PROJECTION)) {
			boolean changed = false;
			updatedProjection = ds.getConfiguration().get(Dataset.DATA_PROJECTION);
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
		}

		// store projection
		ds.getConfiguration().put(Dataset.DATA_PROJECTION, updatedProjection);
		ds.update();
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
		super.lastUpdatedSource(sourceUpdates, "resource_id");
	}
}

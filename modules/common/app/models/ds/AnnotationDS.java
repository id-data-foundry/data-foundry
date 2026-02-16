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

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.ebean.DB;
import io.ebean.Transaction;
import models.Dataset;
import models.sr.Cluster;
import play.Logger;
import play.libs.Json;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;

public class AnnotationDS extends LinkedDS {

	private static final Logger.ALogger logger = Logger.of(AnnotationDS.class);

	public AnnotationDS(Dataset dataset) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_an";
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
			        + "cluster_id bigint," //
			        + "ts timestamp," //
			        + "title varchar(255)," //
			        + "text TEXT," //
			        + "PRIMARY KEY (id) );");
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in creating dataset DB table.", e);
		}
	}

	@Override
	public void migrateDatasetSchema() {
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			DatabaseMetaData dmd = connection.getMetaData();
			ResultSet res = dmd.getColumns(null, null, dataTableName.toUpperCase(), "TEXT");
			if (res.next() && TEXT.equals(res.getString(TYPE_NAME))) {
				// schema is ok, do nothing
			} else {
				connection.createStatement().execute("ALTER TABLE IF EXISTS " + dataTableName + " " //
				        + "ALTER COLUMN text TEXT;");
				logger.info("Dataset table " + dataTableName + " migrated.");
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in migrating dataset table in DB.", e);
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "cluster_id", "ts", "title", "text" };
	}

	public void addRecord(Cluster cluster, Date ts, String title, String text) {
		addRecord(cluster != null ? cluster.getId() : -1, ts, title, text);
	}

	public void addRecord(Long cluster_id, Date ts, String title, String text) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
		                + " (cluster_id, ts, title, text )" + " VALUES (?, ?, ?, ?);");) {

			// use raw JDBC
			stmt.setLong(1, cluster_id);
			stmt.setTimestamp(2, new Timestamp(ts.getTime()));
			stmt.setString(3, nss(title, 255));
			stmt.setString(4, nss(text));

			int i = stmt.executeUpdate();
			logger.trace(i + " records inserted into " + dataTableName);
			transaction.commit();

			// post update on OOCSI
			oocsiStreaming.datasetUpdate(dataset, OOCSIStreamOutService.map().put("operation", "add")
			        .put("title", nss(title, 255)).put("text", nss(text)).put("cluster", cluster_id).build());
		} catch (SQLException e) {
			logger.error("Error in writing record to table.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		export(queue, cluster.getId(), limit, start, end);
	}

	public void export(SourceQueueWithComplete<ByteString> queue, long cluster_id, long limit, long start, long end) {

		// create the actual database for the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT id, cluster_id, ts, title, text FROM "
		                + dataTableName + timeFilterWhereClause(start, end) + " ORDER BY id ASC "
		                + limitExpression(limit) + ";");
		        ResultSet rs = stmt.executeQuery();) {

			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			queue.offer(ByteString.fromString("id,cluster_id,ts,title,text\n")).toCompletableFuture().get();
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(rs.getLong(2) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append("\"" + rs.getString(4) + "\",");
				sb.append("\"" + rs.getString(5) + "\"");
				sb.append("\n");

				queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
			}

			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in exporting from DB.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		queue.complete();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public ArrayNode retrieveProjected(Cluster cluster, long limit, long start, long end) {

		final String whereClause;
		if (cluster.getId() == -1l) {
			whereClause = timeFilterWhereClause(start, end);
		} else {
			whereClause = " WHERE cluster_id IN (" + cluster.getId() + ") "
			        + timeFilterWhereClause(start, end).replace("WHERE", "AND");
		}

		List<ObjectNode> objects = new LinkedList<ObjectNode>();
		// export the data
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT id, cluster_id, ts, title, text FROM "
		                + dataTableName + whereClause + " ORDER BY id DESC LIMIT " + limit + ";");
		        ResultSet rs = stmt.executeQuery();) {

			while (rs.next()) {
				// ObjectNode on = result.addObject();
				ObjectNode on = Json.newObject();
				objects.add(on);

				// StringBuffer sb = new StringBuffer();
				on.put("id", rs.getLong(1));
				on.put("cluster_id", rs.getLong(2));
				on.put("ts", tsExportFormatter.format(rs.getTimestamp(3)));
				on.put("title", rs.getString(4));
				on.put("text", rs.getString(5));
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in exporting from DB.", e);
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
		super.lastUpdatedSource(sourceUpdates, "cluster_id");
	}
}

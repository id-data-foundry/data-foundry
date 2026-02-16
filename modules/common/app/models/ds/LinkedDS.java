package models.ds;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.apache.pekko.Done;
import org.apache.pekko.stream.QueueOfferResult;
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
import utils.DateUtils;

public abstract class LinkedDS {

	protected final Dataset dataset;
	protected final DateFormat tsExportFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private final SimpleDateFormat sqlDateTimeParsingFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private static final Logger.ALogger logger = Logger.of(LinkedDS.class);

	protected String dataTableName;
	// column metadata address
	protected static final String TYPE_NAME = "TYPE_NAME";
	// column type
	protected static final String TEXT = "CLOB";
	// limit for simple in-memory generated exports
	protected static final int MAX_ROWS_EXPORT = 10_000;

	protected OOCSIStreamOutService oocsiStreaming;

	public LinkedDS(Dataset ds) {
		this.dataset = ds;
	}

	/**
	 * creates the actual table that holds the data of this dataset
	 * 
	 */
	public abstract void createInstance();

	/**
	 * apply all migrations that this DS needs to align with emerging changes and fixes to the schema
	 * 
	 */
	public void migrateDatasetSchema() {
		// default: do nothing.
	}

	/**
	 * set the streaming out provider
	 * 
	 * @param oocsiStreaming
	 */
	public void setStreamoutService(OOCSIStreamOutService oocsiStreaming) {
		this.oocsiStreaming = oocsiStreaming;
	}

	/**
	 * check active dates for the data set and decide whether this API can be opened or requests can be let pass
	 * 
	 * @return
	 */
	public boolean isActive() {
		return dataset.isActive();
	}

	/**
	 * returns the concrete table name for this dataset
	 * 
	 * @return
	 */
	public String getDataTableName() {
		return dataTableName;
	}

	/**
	 * returns the columns of this datatable as String array
	 * 
	 * @return
	 */
	abstract public String[] getSchema();

	/**
	 * retrieve the dataset data as Json ArrayNode
	 * 
	 * @param cluster
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	abstract public ArrayNode retrieveProjected(Cluster cluster, long limit, long start, long end);

	/**
	 * export the dataset data as JSON array to a given file writer;
	 * 
	 * NOTE: this is supposed to be overridden by dataset types with potentially a LOT of data, i.e., the TimeseriesDS
	 * 
	 * @param fileWriter
	 * @param cluster
	 * @param limit
	 * @param start
	 * @param end
	 */
	public void exportProjectedToFile(FileWriter fileWriter, Cluster cluster, long limit, long start, long end) {
		try {
			fileWriter.write(retrieveProjected(cluster, limit, start, end).toString());
			fileWriter.close();
		} catch (IOException e) {
		}
	}

	/**
	 * export the dataset data as CSV to a given file writer
	 * 
	 * @param fileWriter
	 * @param cluster
	 * @param limit
	 * @param start
	 * @param end
	 */
	public void exportToFile(FileWriter fileWriter, Cluster cluster, long limit, long start, long end) {
		export(new SourceQueueWithComplete<ByteString>() {

			@Override
			public CompletionStage<QueueOfferResult> offer(ByteString elem) {
				try {
					fileWriter.write(new String(elem.toArray()));
				} catch (IOException e) {
				}
				return CompletableFuture.completedFuture(QueueOfferResult.enqueued());
			}

			@Override
			public CompletionStage<Done> watchCompletion() {
				return CompletableFuture.completedFuture(Done.done());
			}

			@Override
			public void fail(Throwable ex) {
			}

			@Override
			public void complete() {
			}
		}, cluster, limit, start, end);

		try {
			fileWriter.close();
		} catch (IOException e) {
		}
	}

	/**
	 * export CSV data from this dataset
	 * 
	 * @param queue
	 * @param cluster_id
	 * @param limit
	 * @param start
	 * @param end
	 */
	abstract public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start,
	        long end);

	/**
	 * retrieve a data for the dataset activity to allows for a timeseries visualisation
	 * 
	 * @param sourceActor
	 */
	public void timeseries(SourceQueueWithComplete<ByteString> queue) {

		// export timeseries data
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {

			if (dataset.getCreation().toInstant().plus(30, ChronoUnit.DAYS).isBefore(Instant.now())) {
				// use day resolution for timeseries output
				PreparedStatement stmt = connection
				        .prepareStatement("SELECT TRUNCATE(ts) AS day, count(*) as events FROM " + dataTableName
				                + " GROUP BY day ORDER BY day ASC;");
				ResultSet rs = stmt.executeQuery();
				queue.offer(ByteString.fromString("day,events,\n")).toCompletableFuture().get();
				while (rs.next()) {
					StringBuffer sb = new StringBuffer();
					sb.append(rs.getTimestamp(1).toString().substring(0, 10) + ",");
					sb.append(rs.getLong(2) + ",");
					sb.append("\n");

					queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
				}
			} else {
				// use hour resolution for timeseries output
				PreparedStatement stmt = connection.prepareStatement(
				        "SELECT PARSEDATETIME(FORMATDATETIME(ts, 'yyyy-MM-dd HH'), 'yyyy-MM-dd HH') AS day, count(*) as events FROM "
				                + dataTableName + " GROUP BY day ORDER BY day ASC LIMIT 200;");
				ResultSet rs = stmt.executeQuery();
				queue.offer(ByteString.fromString("day,events,\n")).toCompletableFuture().get();
				while (rs.next()) {
					StringBuffer sb = new StringBuffer();
					sb.append(tsExportFormatter.format(rs.getTimestamp(1)) + ",");
					sb.append(rs.getLong(2) + ",");
					sb.append("\n");

					queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
				}
			}

			transaction.commit();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Error in exporting timeseries.", e);
		} catch (Exception e) {
			logger.error("Error in exporting timeseries.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		queue.complete();
	}

	public void dropTable() {
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			PreparedStatement stmt = connection.prepareStatement("DROP TABLE IF EXISTS " + dataTableName + ";");
			stmt.execute();
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in dropping database table.", e);
		}
	}

	/**
	 * return stats for this DS connector (for administration)
	 * 
	 * @return
	 */
	public ObjectNode stats() {

		ObjectNode result = Json.newObject();
		result.put("id", dataset.getId());
		result.put("name", dataset.getName());
		result.put("type", dataset.getDsType().toString());
		result.put("color", dataset.getDsType().toColor());
		result.put("refId", dataset.getRefId());

		// export timeseries data
		long totalItemCount = 0;
		long lastWeekItemCount = 0;
		long thisWeekItemCount = 0;
		String lastItem = "";
		if (dataTableName != null && dataset.getRefId() != null) {
			// all items
			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection
			                .prepareStatement("SELECT count(*) as items FROM " + dataTableName + ";");
			        ResultSet rs = stmt.executeQuery();) {

				if (rs.next()) {
					totalItemCount = rs.getLong(1);
				}
				transaction.commit();
			} catch (Exception e) {
				logger.error("Error in retrieving stats.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}

			// last week items
			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection.prepareStatement(
			                "SELECT count(*) as items FROM " + dataTableName + " WHERE ts >= ? AND ts < ?;");) {
				stmt.setTimestamp(1, new Timestamp(DateUtils.moveDays(new Date(), -14).getTime()));
				stmt.setTimestamp(2, new Timestamp(DateUtils.moveDays(new Date(), -7).getTime()));
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					lastWeekItemCount = rs.getLong(1);
				}
				transaction.commit();
			} catch (Exception e) {
				logger.error("Error in retrieving stats.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}

			// this week items
			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection.prepareStatement(
			                "SELECT MAX(ts) AS ts, count(*) as items FROM " + dataTableName + " WHERE ts >= ?;");) {
				stmt.setTimestamp(1, new Timestamp(DateUtils.moveDays(new Date(), -7).getTime()));
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					Timestamp timestamp = rs.getTimestamp(1);
					if (timestamp != null) {
						lastItem = tsExportFormatter.format(timestamp);
					}
					thisWeekItemCount = rs.getLong(2);
				}
				transaction.commit();
			} catch (Exception e) {
				logger.error("Error in retrieving stats.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		}

		result.put("items", totalItemCount);
		result.put("thisweek", thisWeekItemCount);
		result.put("lastweek", lastWeekItemCount);
		result.put("lastItem", lastItem);

		return result;
	}

	/**
	 * reset the dataset
	 * 
	 * @return
	 */
	public void resetDataset() {
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("DELETE FROM " + dataTableName + ";");) {
			stmt.execute();
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in resetting dataset table.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	protected final String timeFilterWhereClause(long start, long end) {
		if (start > -1l && end > -1l) {
			return " WHERE ts >= " + tlsf(start) + " AND ts < " + tlsf(end);
		} else if (start > -1l) {
			return " WHERE ts >= " + tlsf(start);
		} else if (end > -1l) {
			return " WHERE ts < " + tlsf(end);
		} else {
			return "";
		}
	}

	protected final String limitExpression(long limit) {
		return limit > -1L ? (" LIMIT " + limit + " ") : "";
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private final String tlsf(long epochMillis) {
		return "PARSEDATETIME('" + sqlDateTimeParsingFormat.format(new Date(epochMillis)) + "','"
		        + sqlDateTimeParsingFormat.toPattern() + "')";
	}

	/**
	 * format / escape string for CSV output
	 */
	protected final String cf(String s) {
		return s != null ? "\"" + s + "\"" : "\"\"";
	}

	/**
	 * null safe string with maximum length restriction
	 */
	protected final String nss(String s, int maxLength) {
		String string = s != null ? s : "";
		if (string.length() > maxLength) {
			string = string.substring(0, maxLength);
		}
		return string;
	}

	/**
	 * null safe string
	 */
	protected final String nss(String s) {
		return s != null ? s : "";
	}

	abstract public void lastUpdatedSource(Map<Long, Long> sourceUpdates);

	protected void lastUpdatedSource(Map<Long, Long> sourceUpdates, String resourceColumnName) {
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT " + resourceColumnName + ", MAX(ts) FROM "
		                + dataTableName + " GROUP BY " + resourceColumnName + ";");
		        ResultSet rs = stmt.executeQuery();) {
			while (rs.next()) {
				Long source_id = rs.getLong(1);
				if (sourceUpdates.containsKey(source_id)) {
					long newValue = rs.getTimestamp(2).getTime();
					sourceUpdates.compute(source_id, (k, v) -> v < newValue ? newValue : v);
				}
			}

			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in retrieving last updates from dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Error in exporting last updates from dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}
}

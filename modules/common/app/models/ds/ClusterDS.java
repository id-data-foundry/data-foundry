package models.ds;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import datasets.DatasetConnector;
import io.ebean.DB;
import io.ebean.Transaction;
import models.DatasetType;
import models.Project;
import models.sr.Cluster;
import play.Logger;
import services.slack.Slack;

public class ClusterDS {

	protected final DatasetConnector datasetConnector;
	protected final DateFormat tsShortFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	private static final Logger.ALogger logger = Logger.of(ClusterDS.class);

	@Inject
	public ClusterDS(DatasetConnector datasetConnector) {
		this.datasetConnector = datasetConnector;
	}

	public void downloadTS(Cluster cluster, SourceQueueWithComplete<ByteString> queue) {
		// gather the SQL sub queries
		final Project project = cluster.getProject();
		project.refresh();

		// get list of device Ids
		String deviceIds = cluster.getDevices().stream().map(d -> d.getId().toString())
		        .collect(Collectors.joining(","));

		// use day resolution for timeseries output
		if (project.getCreation().toInstant().plus(30, ChronoUnit.DAYS).isBefore(Instant.now())) {
			String query = project.getDatasets().stream().filter(ds -> ds.getDsType() == DatasetType.IOT).map(ds -> {
				if (ds.getDsType() == DatasetType.IOT) {
					String id = datasetConnector.getDatasetDS(ds).getDataTableName();
					String name = ds.getName().replace("'", "");
					return "SELECT TRUNCATE(ts) AS day, count(*) as events, '" + name + "' FROM " + id
					        + " WHERE device_id IN (" + deviceIds + ") GROUP BY day ";
				}

				return null;
			}).filter(s -> s != null).collect(Collectors.joining(" UNION "));

			String sortingClause = ";";
			if (query.contains("UNION")) {
				sortingClause = " ORDER BY day ASC;";
			}

			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection.prepareStatement(query + sortingClause);
			        ResultSet rs = stmt.executeQuery();) {

				queue.offer(ByteString.fromString("day,events,dataset\n")).toCompletableFuture().get();
				while (rs.next()) {
					StringBuffer sb = new StringBuffer();
					sb.append(rs.getTimestamp(1).toString().substring(0, 10) + ",");
					sb.append(rs.getLong(2) + ",");
					sb.append("\"" + rs.getString(3) + "\"\n");

					queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
				}
				transaction.commit();
			} catch (Exception e) {
				logger.error("Error in exporting a timeseries dataset.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		}
		// use hour resolution for timeseries output
		else {
			String query = project.getDatasets().stream().filter(ds -> ds.getDsType() == DatasetType.IOT).map(ds -> {
				if (ds.getDsType() == DatasetType.IOT) {
					String id = datasetConnector.getDatasetDS(ds).getDataTableName();
					String name = ds.getName().replace("'", "");
					return "SELECT PARSEDATETIME(FORMATDATETIME(ts, 'yyyy-MM-dd HH'), 'yyyy-MM-dd HH') AS day, count(*) as events, '"
					        + name + "' FROM " + id + " WHERE device_id IN (" + deviceIds + ") GROUP BY day ";
				}

				return null;
			}).filter(s -> s != null).collect(Collectors.joining(" UNION "));

			String sortingClause = ";";
			if (query.contains("UNION")) {
				sortingClause = " ORDER BY day ASC;";
			}

			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection.prepareStatement(query + sortingClause);
			        ResultSet rs = stmt.executeQuery();) {

				queue.offer(ByteString.fromString("day,events,dataset\n")).toCompletableFuture().get();
				while (rs.next()) {
					StringBuffer sb = new StringBuffer();
					sb.append(tsShortFormatter.format(rs.getTimestamp(1)) + ",");
					sb.append(rs.getLong(2) + ",");
					sb.append("\"" + rs.getString(3) + "\"\n");

					queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
				}
				transaction.commit();
			} catch (Exception e) {
				logger.error("Error in exporting a timeseries dataset.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		}

		queue.complete();
	}

	/**
	 * timeseries data overview of a project
	 * 
	 * @param project
	 * @param sourceActor
	 */
	public void timeseries(SourceQueueWithComplete<ByteString> queue, Project project) {
		if (project.getCreation().toInstant().plus(30, ChronoUnit.DAYS).isBefore(Instant.now())) {
			String query = project.getDatasets().stream().map(ds -> {
				String id = datasetConnector.getDatasetDS(ds).getDataTableName();
				String name = ds.getName().replace("'", "");
				return "SELECT TRUNCATE(ts) AS day, count(*) as events, '" + name + "' FROM " + id + " GROUP BY day";
			}).collect(Collectors.joining(" UNION "));

			// use day resolution for timeseries output
			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection.prepareStatement(query + " ORDER BY day ASC;");
			        ResultSet rs = stmt.executeQuery();) {

				queue.offer(ByteString.fromString("day,events,dataset\n")).toCompletableFuture().get();
				while (rs.next()) {
					StringBuffer sb = new StringBuffer();
					sb.append(rs.getTimestamp(1).toString().substring(0, 10) + ",");
					sb.append(rs.getLong(2) + ",");
					sb.append("\"" + rs.getString(3) + "\"\n");

					queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
				}
				transaction.commit();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Error in retrieving a timeseries.", e);
			} catch (Exception e) {
				logger.error("Error in retrieving a timeseries.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		}
		// use hour resolution for timeseries output
		else {
			String query = project.getDatasets().stream().map(ds -> {
				String id = datasetConnector.getDatasetDS(ds).getDataTableName();
				String name = ds.getName().replace("'", "");
				return "SELECT PARSEDATETIME(FORMATDATETIME(ts, 'yyyy-MM-dd HH'), 'yyyy-MM-dd HH') AS day, count(*) as events, '"
				        + name + "' FROM " + id + " GROUP BY day";
			}).collect(Collectors.joining(" UNION "));

			// use day resolution for timeseries output
			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection.prepareStatement(query + " ORDER BY day ASC;");
			        ResultSet rs = stmt.executeQuery();) {

				queue.offer(ByteString.fromString("day,events,dataset\n")).toCompletableFuture().get();
				while (rs.next()) {
					StringBuffer sb = new StringBuffer();
					sb.append(tsShortFormatter.format(rs.getTimestamp(1)) + ",");
					sb.append(rs.getLong(2) + ",");
					sb.append("\"" + rs.getString(3) + "\"\n");

					queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
				}
				transaction.commit();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Error in retrieving a timeseries.", e);
			} catch (Exception e) {
				logger.error("Error in retrieving a timeseries.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		}

		queue.complete();
	}

	/**
	 * timeseries data overview of a cluster
	 * 
	 * @param cluster
	 * @param sourceActor
	 */
	public void timeseries(SourceQueueWithComplete<ByteString> queue, final Cluster cluster) {

		// gather the SQL sub queries
		final Project project = cluster.getProject();
		project.refresh();

		// get list of device Ids
		String deviceIds = cluster.getDevices().stream().map(d -> d.getId().toString())
		        .collect(Collectors.joining(","));
		String participantIds = cluster.getParticipants().stream().map(d -> d.getId().toString())
		        .collect(Collectors.joining(","));
		// List<Long> wearableIds = c.wearables.stream().map(d -> d.id).collect(Collectors.toList());

		// use day resolution for timeseries output
		if (project.getCreation().toInstant().plus(30, ChronoUnit.DAYS).isBefore(Instant.now())) {
			String query = project.getDatasets().stream().filter(ds -> (ds.getDsType() == DatasetType.ANNOTATION
			        || ds.getDsType() == DatasetType.IOT || ds.getDsType() == DatasetType.DIARY)).map(ds -> {

				        if (ds.getDsType() == DatasetType.ANNOTATION) {
					        String id = datasetConnector.getDatasetDS(ds).getDataTableName();
					        String name = ds.getName().replace("'", "");
					        return "SELECT TRUNCATE(ts) AS day, count(*) as events, '" + name
					                + "' FROM " + id + " GROUP BY day ";
				        }
				        if (ds.getDsType() == DatasetType.IOT) {
					        String id = datasetConnector.getDatasetDS(ds).getDataTableName();
					        String name = ds.getName().replace("'", "");
					        return "SELECT TRUNCATE(ts) AS day, count(*) as events, '" + name
					                + "' FROM " + id + " WHERE device_id IN (" + deviceIds + ") GROUP BY day ";
				        }
				        if (ds.getDsType() == DatasetType.DIARY) {
					        String id = datasetConnector.getDatasetDS(ds).getDataTableName();
					        String name = ds.getName().replace("'", "");
					        return "SELECT TRUNCATE(ts) AS day, count(*) as events, '" + name
					                + "' FROM " + id + " WHERE participant_id IN (" + participantIds
					                + ") GROUP BY day ";
				        }

				        return null;
			        }).filter(s -> s != null).collect(Collectors.joining(" UNION "));

			String sortingClause = ";";
			if (query.contains("UNION")) {
				sortingClause = " ORDER BY day ASC;";
			}

			// nothing to query?
			if (query.length() == 0) {
				try {
					queue.offer(ByteString.fromString("day,events,dataset\n")).toCompletableFuture().get();
				} catch (InterruptedException | ExecutionException e) {
					logger.error("Error in exporting a timeseries.", e);
				}
				queue.complete();
				return;
			}

			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection.prepareStatement(query + sortingClause);
			        ResultSet rs = stmt.executeQuery();) {

				queue.offer(ByteString.fromString("day,events,dataset\n")).toCompletableFuture().get();
				while (rs.next()) {
					StringBuffer sb = new StringBuffer();
					sb.append(rs.getTimestamp(1).toString().substring(0, 10) + ",");
					sb.append(rs.getLong(2) + ",");
					sb.append("\"" + rs.getString(3) + "\"\n");

					queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
				}
				transaction.commit();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Error in exporting a timeseries.", e);
			} catch (Exception e) {
				logger.error("Error in exporting a timeseries.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		}
		// use hour resolution for timeseries output
		else {
			String query = project.getDatasets().stream().filter(ds -> (ds.getDsType() == DatasetType.ANNOTATION
			        || ds.getDsType() == DatasetType.IOT || ds.getDsType() == DatasetType.DIARY)).map(ds -> {

				        if (ds.getDsType() == DatasetType.ANNOTATION) {
					        String id = datasetConnector.getDatasetDS(ds).getDataTableName();
					        String name = ds.getName().replace("'", "");
					        return "SELECT PARSEDATETIME(FORMATDATETIME(ts, 'yyyy-MM-dd HH'), 'yyyy-MM-dd HH') AS day, count(*) as events, '"
					                + name + "' FROM " + id + " GROUP BY day ";
				        }
				        if (ds.getDsType() == DatasetType.IOT) {
					        String id = datasetConnector.getDatasetDS(ds).getDataTableName();
					        String name = ds.getName().replace("'", "");
					        return "SELECT PARSEDATETIME(FORMATDATETIME(ts, 'yyyy-MM-dd HH'), 'yyyy-MM-dd HH') AS day, count(*) as events, '"
					                + name + "' FROM " + id + " WHERE device_id IN (" + deviceIds + ") GROUP BY day ";
				        }
				        if (ds.getDsType() == DatasetType.DIARY) {
					        String id = datasetConnector.getDatasetDS(ds).getDataTableName();
					        String name = ds.getName().replace("'", "");
					        return "SELECT PARSEDATETIME(FORMATDATETIME(ts, 'yyyy-MM-dd HH'), 'yyyy-MM-dd HH') AS day, count(*) as events, '"
					                + name + "' FROM " + id + " WHERE participant_id IN (" + participantIds
					                + ") GROUP BY day ";
				        }

				        return null;
			        }).filter(s -> s != null).collect(Collectors.joining(" UNION "));

			String sortingClause = ";";
			if (query.contains("UNION")) {
				sortingClause = " ORDER BY day ASC;";
			}

			// nothing to query?
			if (query.length() == 0) {
				try {
					queue.offer(ByteString.fromString("day,events,dataset\n")).toCompletableFuture().get();
				} catch (InterruptedException | ExecutionException e) {
					logger.error("Error in exporting a timeseries.", e);
				}
				queue.complete();
				return;
			}

			try (Transaction transaction = DB.beginTransaction();
			        Connection connection = transaction.connection();
			        PreparedStatement stmt = connection.prepareStatement(query + sortingClause);
			        ResultSet rs = stmt.executeQuery();) {

				queue.offer(ByteString.fromString("day,events,dataset\n")).toCompletableFuture().get();
				while (rs.next()) {
					StringBuffer sb = new StringBuffer();
					sb.append(tsShortFormatter.format(rs.getTimestamp(1)) + ",");
					sb.append(rs.getLong(2) + ",");
					sb.append("\"" + rs.getString(3) + "\"\n");

					queue.offer(ByteString.fromString(sb.toString())).toCompletableFuture().get();
				}
				transaction.commit();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Error in exporting a timeseries.", e);
			} catch (Exception e) {
				logger.error("Error in exporting a timeseries.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}
		}

		queue.complete();
	}

}

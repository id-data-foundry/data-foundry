package models.ds;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import models.vm.TimedText;
import play.Logger;
import play.libs.Json;
import services.outlets.OOCSIStreamOutService;
import services.slack.Slack;

public class DiaryDS extends LinkedDS {

	private static final Logger.ALogger logger = Logger.of(DiaryDS.class);

	public DiaryDS(Dataset dataset) {
		super(dataset);
		this.dataTableName = "ds_" + dataset.getRefId() + "_dy";
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
			        + "pp1 varchar(255)," //
			        + "pp2 varchar(255),"//
			        + "pp3 varchar(255)," //
			        + "title varchar(255)," //
			        + "text TEXT," + "PRIMARY KEY (id) );");
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in creating dataset table in DB.", e);
		}
	}

	/**
	 * perform database table migration: modify data column to fit longer data
	 * 
	 */
	@Override
	public void migrateDatasetSchema() {
		try (Transaction transaction = DB.beginTransaction(); Connection connection = transaction.connection();) {
			DatabaseMetaData dmd = connection.getMetaData();
			ResultSet res = dmd.getColumns(null, null, dataTableName.toUpperCase(), "TEXT");
			if (res.next() && TEXT.equals(res.getString(TYPE_NAME))) {
				// scheme is ok, do nothing
			} else {
				connection.createStatement().execute("ALTER TABLE " + dataTableName + " " //
				        + "ALTER COLUMN text TEXT;");
				logger.info("Applied db table migration for " + dataTableName + ": extended column 'text'.");
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in preparing the dataset via migration.", e);
		}
	}

	@Override
	public String[] getSchema() {
		return new String[] { "id", "participant_id", "ts", "pp1", "pp2", "pp3", "title", "text" };
	}

	public void addRecord(String participantRefId, Date ts, String title, String text) {
		Participant p = new Participant(participantRefId);
		p.setId(-1L);
		p.setRefId(participantRefId);
		p.setPublicParameter1(participantRefId);

		addRecord(p, ts, title, text);
	}

	public void addRecord(Participant participant, Date ts, String title, String text) {

		// insert record
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + dataTableName
		                + " (participant_id, ts, pp1, pp2, pp3, title, text)" + " VALUES (?, ?, ?, ?, ?, ?, ?);");) {

			stmt.setLong(1, participant.getId());
			stmt.setTimestamp(2, new Timestamp(ts.getTime()));
			stmt.setString(3, nss(participant.getPublicParameter1(), 255));
			stmt.setString(4, nss(participant.getPublicParameter2(), 255));
			stmt.setString(5, nss(participant.getPublicParameter3(), 255));
			stmt.setString(6, nss(title, 255));
			stmt.setString(7, nss(text, 10000));

			int i = stmt.executeUpdate();
			logger.trace(i + " records inserted into " + dataTableName);
			transaction.commit();

			// post update on OOCSI
			oocsiStreaming.datasetUpdate(dataset,
			        OOCSIStreamOutService.map().put("operation", "add").put("title", nss(title, 255))
			                .put("text", nss(text, 10000)).put("participant_id", nss(participant.getRefId(), 32))
			                .build());
		} catch (Exception e) {
			logger.error("Error in inserting a record in dataset table.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	public List<TimedText> getDiary() {
		final List<TimedText> result = new LinkedList<TimedText>();
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement(
		                "SELECT participant_id, ts, title, text FROM " + dataTableName + " ORDER BY ts DESC;");) {
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				TimedText tt = new TimedText(rs.getLong("participant_id"), rs.getTimestamp("ts"), rs.getString("title"),
				        rs.getString("text"));
				result.add(tt);
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in retrieving a diary for a participant.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	public List<TimedText> getDiaryForParticipant(Long id) {
		final List<TimedText> result = new LinkedList<TimedText>();
		try (Transaction transaction = DB.beginTransaction();
		        Connection connection = transaction.connection();
		        PreparedStatement stmt = connection.prepareStatement("SELECT ts, title, text FROM " + dataTableName
		                + " WHERE participant_id = ? ORDER BY ts DESC;");) {
			stmt.setLong(1, id);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				TimedText tt = new TimedText(id, rs.getTimestamp("ts"), rs.getString("title"), rs.getString("text"));
				result.add(tt);
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error in retrieving a diary for a participant.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return result;
	}

	@Override
	public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start, long end) {
		export(queue, cluster.getParticipantList(), start, end);
	}

	public void export(SourceQueueWithComplete<ByteString> queue, List<Long> participantIds, long start, long end) {

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
		                .prepareStatement("SELECT id, participant_id, ts, pp1, pp2, pp3, title, text FROM "
		                        + dataTableName + whereClause + " ORDER BY id ASC;");
		        ResultSet rs = stmt.executeQuery();) {

			// header
			// sourceActor.tell(ByteString.fromString("# dataset export created on " + new Date() + "\n"), null);
			queue.offer(ByteString.fromString("id,participant_id,ts,pp1,pp2,pp3,title,entry\n")).toCompletableFuture()
			        .get();

			// data
			while (rs.next()) {
				StringBuffer sb = new StringBuffer();
				sb.append(rs.getLong(1) + ",");
				sb.append(rs.getLong(2) + ",");
				sb.append(tsExportFormatter.format(rs.getTimestamp(3)) + ",");
				sb.append("\"" + rs.getString(4) + "\",");
				sb.append("\"" + rs.getString(5) + "\",");
				sb.append("\"" + rs.getString(6) + "\",");
				sb.append("\"" + rs.getString(7) + "\",");
				sb.append("\"" + rs.getString(8) + "\"\n");

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
		                .prepareStatement("SELECT id, participant_id, ts, pp1, pp2, pp3, title, text FROM "
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
				on.put("pp1", rs.getString(4));
				on.put("pp2", rs.getString(5));
				on.put("pp3", rs.getString(6));
				on.put("title", rs.getString(7));
				on.put("text", rs.getString(8));
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

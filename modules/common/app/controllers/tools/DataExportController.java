package controllers.tools;

import java.sql.Clob;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import io.ebean.DB;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import play.Logger;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;

public class DataExportController extends AbstractAsyncController {

	private static final long EXTRACTION_LIMIT = 300_000l;
	private final DatasetConnector datasetConnector;

	private static final Logger.ALogger logger = Logger.of(DataExportController.class);

	@Inject
	public DataExportController(DatasetConnector datasetConnector) {
		this.datasetConnector = datasetConnector;
	}

	@Authenticated(UserAuth.class)
	public Result index(Request request, Long id) {
		Person user = getAuthenticatedUserOrReturn(request,
		        redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		ArrayNode metaData = Json.newArray();

		// find all owned projects
		List<Project> projects = user.projects();
		projects.stream().forEach(p -> {
			p.getDatasets().forEach(ds -> {
				ds.refresh();
				metaData.add(ds.getMetaDataProjectionDescriptionsJSON().put("dataset", ds.getId()));
			});
		});

		// find all collaboration projects
		List<Project> collaborations = user.collaborations();
		collaborations.stream().forEach(p -> {
			p.refresh();
			p.getDatasets().forEach(ds -> {
				ds.refresh();
				metaData.add(ds.getMetaDataProjectionDescriptionsJSON().put("dataset", ds.getId()));
			});
		});

		// find all subscription projects
		List<Project> subscriptions = user.subscriptions();
		subscriptions.stream().forEach(p -> {
			p.refresh();
			p.getDatasets().forEach(ds -> {
				ds.refresh();
				metaData.add(ds.getMetaDataProjectionDescriptionsJSON().put("dataset", ds.getId()));
			});
		});

		// check the dataset
		if (id == -1) {
			return ok(views.html.tools.export.index.render(user, projects, collaborations, subscriptions,
			        metaData.toString(), null, "[]", "[]"));
		}

		// Dataset dataset = null;
		Dataset dataset = Dataset.find.byId(id);
		if (id > -1 && (dataset == null || !dataset.visibleFor(user))) {
			// select projects / datasets within projects
			return ok(views.html.tools.export.index.render(user, projects, collaborations, subscriptions,
			        metaData.toString(), null, "[]", "[]")).addingToSession(request, "error",
			                "Selected saved query is not available.");

		}

		if (projects.size() + collaborations.size() + subscriptions.size() == 0) {
			return redirect(controllers.routes.ProjectsController.index()).addingToSession(request, "message",
			        "No project data to export.");
		}

		String json = dataset.getConfiguration().get(Dataset.SAVED_EXPORT);
		String datasets = dataset.getConfiguration().get(Dataset.SAVED_EXPORT_DATASETS);

		// select projects / datasets within projects
		return

		ok(views.html.tools.export.index.render(user, projects, collaborations, subscriptions, metaData.toString(),
		        dataset.getName(), datasets, json));
	}

	@Authenticated(UserAuth.class)
	public CompletableFuture<Result> data(Request request, Long limit, Integer samplingFactor, Long start, Long end,
	        Long datasetId) {
		String username = getAuthenticatedUserNameOrReturn(request,
		        redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		if (samplingFactor > 1) {
			limit = EXTRACTION_LIMIT;
		}

		// collect data from request
		final ArrayNode jn = (ArrayNode) request.body().asJson();
		if (jn == null || jn.size() == 0) {
			return CompletableFuture.completedFuture(ok());
		}

		final Map<String, List<String[]>> dats = new LinkedHashMap<String, List<String[]>>();
		final Map<String, List<String[]>> cols = new LinkedHashMap<String, List<String[]>>();
		final Map<String, String> datasetTables = new LinkedHashMap<String, String>();
		final Map<String, String[]> datasetSchemas = new LinkedHashMap<String, String[]>();
		final List<String> projection = new LinkedList<String>();

		for (JsonNode jsonNode : jn) {
			if (jsonNode.isObject()) {
				final String id = ((ObjectNode) jsonNode).get("id").asText();
				final String[] idComp = id.substring("column_".length()).split("_", 2);
				final String dsId = idComp[0];
				final String colName = idComp[1];

				// check the contents of the dsId and colName for SQL injections
				if (!dsId.matches("\\d+") || !colName.matches("[0-9a-zA-Z_\\-]+")) {
					continue;
				}

				final Dataset ds = Dataset.find.byId(Long.parseLong(dsId));
				if (ds == null || !ds.visibleFor(username)) {
					continue;
				}

				if (!datasetTables.containsKey(dsId)) {
					datasetTables.put(dsId, datasetConnector.getDatasetDS(ds).getDataTableName());
				}
				if (!datasetSchemas.containsKey(dsId)) {
					datasetSchemas.put(dsId, datasetConnector.getDatasetDS(ds).getSchema());
				}

				if (!dats.containsKey(dsId)) {
					dats.put(dsId, new LinkedList<String[]>());
				}
				dats.get(dsId).add(idComp);

				// check col for being part of the direct schema
				if (Arrays.stream(datasetSchemas.get(dsId)).anyMatch(s -> s.equals(colName))) {
					if (!cols.containsKey(colName)) {
						cols.put(colName, new LinkedList<String[]>());
					}
					cols.get(colName).add(idComp);
				} else {
					if (!cols.containsKey("data")) {
						cols.put("data", new LinkedList<String[]>());
					}
					cols.get("data").add(new String[] { idComp[0], "data" });

					// add column name to projection
					if (!projection.contains(colName)) {
						projection.add(colName);
					}
				}
			}
		}

		// only allow for up to 3 datasets to be processed
		if (datasetTables.size() > 3) {
			return CompletableFuture.supplyAsync(() -> badRequest());
		}

		// build SQL query
		final StringBuffer outerSelect = new StringBuffer();
		outerSelect.append("SELECT ");
		outerSelect.append(cols.entrySet().stream().map(e -> {
			return e.getKey();
		}).collect(Collectors.joining(", ")));

		final List<String> datasetTableNames = new ArrayList<String>(datasetTables.values());
		final StringBuffer sql = new StringBuffer();

		// 1. ts column is present --> we do a full outer join on the ts column
		if (cols.containsKey("ts")) {

			if (datasetTables.size() == 1) {
				final String select = twoWaySelect(cols, datasetTables, Arrays.asList(datasetTableNames.get(0)));
				final String where = whereClause("", start, end, samplingFactor);

				sql.append(select);
				sql.append(" FROM ");
				sql.append(datasetTableNames.get(0));
				sql.append(where + " ORDER BY ts ASC LIMIT " + limit + ";");
			} else if (datasetTables.size() == 2) {
				final String select = twoWaySelect(cols, datasetTables,
				        Arrays.asList(datasetTableNames.get(0), datasetTableNames.get(1)));
				final String where = whereClause("", start, end, samplingFactor);

				sql.append(outerSelect);
				sql.append(" FROM (");

				// left outer join
				sql.append(select);
				sql.append(" FROM ");
				sql.append(datasetTableNames.get(0) + " LEFT OUTER JOIN " + datasetTableNames.get(1));
				sql.append(" ON " + datasetTableNames.get(0) + ".ts = " + datasetTableNames.get(1) + ".ts ");

				sql.append(" UNION ");

				// right outer join
				sql.append(select);
				sql.append(" FROM ");
				sql.append(datasetTableNames.get(0) + " RIGHT OUTER JOIN " + datasetTableNames.get(1));
				sql.append(" ON " + datasetTableNames.get(0) + ".ts = " + datasetTableNames.get(1) + ".ts ");

				sql.append(")" + where + " ORDER BY ts ASC LIMIT " + limit + ";");
			} else if (datasetTables.size() == 3) {
				final String where = whereClause("", start, end, 1);

				sql.append(outerSelect);
				sql.append(" FROM (");

				{
					// left outer join 1
					final String select = twoWaySelect(cols, datasetTables,
					        Arrays.asList(datasetTableNames.get(0), datasetTableNames.get(1)));
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(0) + " LEFT OUTER JOIN " + datasetTableNames.get(1));
					sql.append(" ON " + datasetTableNames.get(0) + ".ts = " + datasetTableNames.get(1) + ".ts ");
					sql.append(whereClause(datasetTableNames.get(0), start, end, samplingFactor));

					sql.append(" UNION ");

					// right outer join 1
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(0) + " RIGHT OUTER JOIN " + datasetTableNames.get(1));
					sql.append(" ON " + datasetTableNames.get(0) + ".ts = " + datasetTableNames.get(1) + ".ts ");
					sql.append(whereClause(datasetTableNames.get(0), start, end, samplingFactor));
				}

				sql.append(" UNION ");

				{
					// left outer join 2
					final String select = twoWaySelect(cols, datasetTables,
					        Arrays.asList(datasetTableNames.get(1), datasetTableNames.get(2)));
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(1) + " LEFT OUTER JOIN " + datasetTableNames.get(2));
					sql.append(" ON " + datasetTableNames.get(1) + ".ts = " + datasetTableNames.get(2) + ".ts ");
					sql.append(whereClause(datasetTableNames.get(1), start, end, samplingFactor));

					sql.append(" UNION ");

					// right outer join 2
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(1) + " RIGHT OUTER JOIN " + datasetTableNames.get(2));
					sql.append(" ON " + datasetTableNames.get(1) + ".ts = " + datasetTableNames.get(2) + ".ts ");
					sql.append(whereClause(datasetTableNames.get(1), start, end, samplingFactor));
				}

				sql.append(" UNION ");

				{
					// left outer join 3
					final String select = twoWaySelect(cols, datasetTables,
					        Arrays.asList(datasetTableNames.get(2), datasetTableNames.get(0)));
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(2) + " LEFT OUTER JOIN " + datasetTableNames.get(0));
					sql.append(" ON " + datasetTableNames.get(2) + ".ts = " + datasetTableNames.get(0) + ".ts ");
					sql.append(whereClause(datasetTableNames.get(2), start, end, samplingFactor));

					sql.append(" UNION ");

					// right outer join 3
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(2) + " RIGHT OUTER JOIN " + datasetTableNames.get(0));
					sql.append(" ON " + datasetTableNames.get(2) + ".ts = " + datasetTableNames.get(0) + ".ts ");
					sql.append(whereClause(datasetTableNames.get(2), start, end, samplingFactor));
				}

				// where clause optional
				sql.append(")" + where + " ORDER BY ts ASC LIMIT " + limit + ";");
			}
		}
		// 2. no ts column is present, then we don't order and join on the TS column
		else {
			if (datasetTables.size() == 1) {
				final String select = twoWaySelect(cols, datasetTables, Arrays.asList(datasetTableNames.get(0)));

				sql.append(select);
				sql.append(" FROM ");
				sql.append(datasetTableNames.get(0));

				// where clause optional
				sql.append(" LIMIT " + limit + ";");
			} else if (datasetTables.size() == 2) {

				sql.append(outerSelect);
				sql.append(" FROM (");

				{
					// table 1
					final String select = twoWaySelect(cols, datasetTables, Arrays.asList(datasetTableNames.get(0)));
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(0));
				}

				sql.append(" UNION ");

				{
					// table 2
					final String select = twoWaySelect(cols, datasetTables, Arrays.asList(datasetTableNames.get(1)));
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(1));
				}

				// where clause optional
				sql.append(" LIMIT " + limit + ");");
			} else if (datasetTables.size() == 3) {

				sql.append(outerSelect);
				sql.append(" FROM (");

				{
					// table 1
					final String select = twoWaySelect(cols, datasetTables, Arrays.asList(datasetTableNames.get(0)));
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(0));
				}

				sql.append(" UNION ");

				{
					// table 2
					final String select = twoWaySelect(cols, datasetTables, Arrays.asList(datasetTableNames.get(1)));
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(1));
				}

				sql.append(" UNION ");

				{
					// table 3
					final String select = twoWaySelect(cols, datasetTables, Arrays.asList(datasetTableNames.get(2)));
					sql.append(select);
					sql.append(" FROM ");
					sql.append(datasetTableNames.get(2));
				}

				// where clause optional
				sql.append(" LIMIT " + limit + ");");
			}
		}

		return CompletableFuture.supplyAsync(() -> internalExport(sql.toString(), samplingFactor, projection))
		        .thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	@Authenticated(UserAuth.class)
	public Result save(Request request, String name, Integer samplingFactor) {
		String username = getAuthenticatedUserNameOrReturn(request,
		        redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		// collect data from request
		ArrayNode jn = (ArrayNode) request.body().asJson();
		if (jn == null || jn.size() == 0 || name == null || name.isEmpty()) {
			return badRequest();
		}

		// let's create a new dataset for this

		// enumerate datasets
		Set<Dataset> datasets = new HashSet<Dataset>();

		// find a project to add this to
		long projectId = -1;
		for (JsonNode jsonNode : jn) {
			if (jsonNode.isObject()) {
				String id = ((ObjectNode) jsonNode).get("id").asText();
				String[] idComp = id.substring("column_".length()).split("_", 2);
				String dsId = idComp[0];

				// check the contents of the dsId and colName for SQL injections
				if (!dsId.matches("\\d+")) {
					continue;
				}

				Dataset ds = Dataset.find.byId(Long.parseLong(dsId));
				if (ds == null || !ds.visibleFor(username)) {
					continue;
				}

				datasets.add(ds);

				if (projectId == -1) {
					projectId = ds.getProject().getId();
				}
			}
		}

		// check ownership
		Project project = Project.find.byId(projectId);
		if (project == null || (!project.editableBy(username))) {
			return forbidden();
		}

		// create dataset json array
		ArrayNode dsn = Json.newArray();
		datasets.stream().forEach(ds -> dsn.add(Json.newObject().put("id", ds.getId()).put("name", ds.getName())));

		// add dataset into the project of choice
		Dataset selds = datasetConnector.create(name, DatasetType.COMPLETE, project,
		        "This dataset represents a saved export.", "", null, null);
		selds.setCollectorType(Dataset.SAVED_EXPORT);
		selds.getConfiguration().put(Dataset.SAVED_EXPORT, Json.stringify(jn));
		selds.getConfiguration().put(Dataset.SAVED_EXPORT_LIMIT, "" + EXTRACTION_LIMIT);
		selds.getConfiguration().put(Dataset.SAVED_EXPORT_SAMPLING, "" + samplingFactor);
		selds.getConfiguration().put(Dataset.SAVED_EXPORT_DATASETS, Json.stringify(dsn));
		selds.save();

		selds.setTargetObject(
		        controllers.tools.routes.DataExportController.index(selds.getId()).absoluteURL(request, true));
		selds.update();

		return ok();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * @param cols
	 * @param datasetTables
	 * @return
	 */
	private String twoWaySelect(final Map<String, List<String[]>> cols, final Map<String, String> datasetTables,
	        final List<String> datasetTableNames) {
		StringBuffer select = new StringBuffer();
		select.append("SELECT ");
		select.append(cols.entrySet().stream().map(e -> {
			String s = e.getValue().stream().map(l -> {
				if (datasetTableNames.contains(datasetTables.get(l[0]))) {
					return datasetTables.get(l[0]) + "." + l[1];
				} else {
					return "NULL";
				}
			}).collect(Collectors.joining(","));
			return "COALESCE(" + s + ") as " + e.getKey();
		}).collect(Collectors.joining(", ")));

		return select.toString();
	}

	/**
	 * generate a WHERE clause that can be made specific to the datasetName for ambiguous ts column names
	 * 
	 * @param datasetName
	 * @param start
	 * @param end
	 * @param samplingFactor
	 * @return
	 */
	private String whereClause(String datasetName, Long start, Long end, int samplingFactor) {
		String where = "";

		final String ts;
		if (datasetName != null && !datasetName.isEmpty()) {
			ts = datasetName + ".ts";
		} else {
			ts = "ts";
		}

		if (start > -1 && end > -1) {
			where = " WHERE " + ts + " >= TIMESTAMP '" + new Timestamp(start).toString() + "' AND " + ts
			        + " < TIMESTAMP '" + new Timestamp(end).toString() + "'";
			if (samplingFactor != 1) {
				float percent = 1 - 1.0f / samplingFactor;
				where += " AND RAND() >= " + percent + " ";
			}
		} else {
			if (samplingFactor != 1) {
				float percent = 1 - 1.0f / samplingFactor;
				where += " WHERE RAND() >= " + percent + " ";
			}
		}
		return where;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * internal export just the raw data
	 * 
	 * @param ds
	 * @return
	 */
	private Source<ByteString, ?> internalExport(String sql, int samplingFactor, List<String> projection) {

		final DateFormat tsExportFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		// create the stream
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> {

				try {
					SqlQuery createSqlQuery = DB.sqlQuery(sql.toString());
					List<SqlRow> lsr = createSqlQuery.findList();

					StringBuffer csv = new StringBuffer();
					if (lsr.size() > 0) {
						// header
						for (String key : lsr.get(0).keySet()) {
							if (key.equals("data")) {
								csv.append(projection.stream().collect(Collectors.joining(",")) + ",");
							} else {
								csv.append(key + ",");
							}
						}
						sourceActor.offer(ByteString.fromString(csv.toString() + "\n")).toCompletableFuture().get();

						// loop through results
						for (SqlRow sqlRow : lsr) {
							csv = new StringBuffer();
							for (String key : sqlRow.keySet()) {
								// timestamp data needs datetime format
								if (key.equals("ts")) {
									Timestamp timestamp = sqlRow.getTimestamp(key);
									if (timestamp != null) {
										csv.append(tsExportFormatter.format(timestamp) + ",");
									} else {
										csv.append(",");
									}
								}
								// textual data needs quotes
								else if (key.equals("title") || key.equals("text") || key.equals("description")) {
									// parse data as JSON
									Object o = sqlRow.get(key);

									// check object for Clob
									final String data;
									if (o instanceof Clob) {
										Clob clob = (Clob) o;
										data = clob.getSubString(1, (int) clob.length());
									} else if (o != null) {
										data = o.toString();
									} else {
										data = sqlRow.getString(key);
									}
									csv.append(cf(data) + ",");
								}
								// JSON data needs unpacking
								else if (key.equals("data")) {
									// parse data as JSON
									Object o = sqlRow.get(key);

									// check object
									final String data;
									if (o instanceof Clob) {
										Clob clob = (Clob) o;
										data = clob.getSubString(1, (int) clob.length());
									} else if (o != null) {
										data = o.toString();
									} else {
										data = "{}";
									}

									JsonNode tempObject;
									try {
										JsonNode jn = Json.parse(data);
										tempObject = (jn == null || !jn.isObject()) ? Json.newObject() : jn;
									} catch (Exception e) {
										// something went wrong with the parsing of "data" data
										tempObject = Json.newObject();
									}

									final JsonNode on = tempObject;
									// iterate through values
									String values = projection.stream().map(s -> {
										JsonNode value = on.get(s);
										if (value != null) {
											return value.toString();
										} else {
											return "";
										}
									}).collect(Collectors.joining(","));
									csv.append(values);
									csv.append(",");
								}
								// other data just falls into place (no "null" string though)
								else {
									String obj = sqlRow.getString(key);
									csv.append((obj != null ? obj : "") + ",");
								}
							}
							sourceActor.offer(ByteString.fromString(csv.toString() + "\n")).toCompletableFuture().get();
						}
					}
				} catch (Exception e) {
					logger.error("Data export error", e);
				} finally {
					sourceActor.complete();
				}
			});
			return sourceActor;
		});
	}

}

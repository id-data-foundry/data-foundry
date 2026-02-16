package controllers.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import jakarta.inject.Inject;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.TimeseriesDS;
import models.sr.Device;
import play.Environment;
import play.Logger;
import play.cache.SyncCacheApi;
import play.filters.csrf.AddCSRFToken;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http.MultipartFormData;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.StringUtils;
import utils.auth.TokenResolverUtil;

public class DataLogger extends AbstractAsyncController {

	@Inject
	TokenResolverUtil tokenResolver;

	@Inject
	Environment environment;

	@Inject
	SyncCacheApi cache;

	@Inject
	DatasetConnector datasetConnector;

	private static final Logger.ALogger logger = Logger.of(DataLogger.class);

	@Authenticated(UserAuth.class)
	public Result index(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(HOME));

		ObjectNode projectsAndDatasets = Json.newObject();
		user.getOwnAndCollabProjects().stream().forEach(p -> {
			List<Dataset> datasetList = p.getIoTDatasets().stream()
			        .filter(ds -> ds.isActive() && !ds.configuration(Dataset.API_TOKEN, "").isEmpty())
			        .collect(Collectors.toList());
			if (datasetList.isEmpty()) {
				return;
			}

			ObjectNode pon = projectsAndDatasets.putObject(p.getId() + "");
			pon.put("id", p.getId());
			pon.put("name", p.getName());

			ObjectNode datasets = pon.putObject("datasets");
			datasetList.stream().filter(ds -> ds.isActive())
			        .forEach(ds -> datasets.putObject(ds.getId() + "").put("id", ds.getId()).put("name", ds.getName())
			                .put("open", ds.isOpenParticipation())
			                .put("token", ds.configuration(Dataset.API_TOKEN, "")));

			ArrayNode devices = pon.putArray("devices");
			p.getDevices().stream()
			        .forEach(dev -> devices.addObject().put("id", dev.getRefId()).put("name", dev.getName()));
		});

		// just render the tools page
		return ok(views.html.tools.datalogger.index.render(projectsAndDatasets,
		        routes.DataLogger.datalogger(1000000, 2000000, "DATASET", "DEVICE")
		                .absoluteURL(request, !environment.isDev()).toString(),
		        routes.DataLogger.log(2000000, "DATASET", "DEVICE").absoluteURL(request, !environment.isDev())
		                .toString(),
		        routes.DataLogger.importLog(2000000, "DATASET", "DEVICE").absoluteURL(request, !environment.isDev())
		                .toString()));
	}

	public Result datalogger(Request request, long projectId, long datasetId, String datasetToken, String deviceId) {

		// check datasetToken against project
		Project project = Project.find.byId(projectId);

		Dataset ds = Dataset.find.byId(datasetId);

		// check whether dataset exists, belongs to project and is active
		if (ds == null || !ds.getProject().getId().equals(project.getId()) || !ds.isActive()
		        || !datasetToken.equals(ds.configuration(Dataset.API_TOKEN, ""))) {
			return redirect(HOME);
		}

		// check whether device Id belongs to project
		if (!ds.isOpenParticipation()
		        && project.getDevices().stream().noneMatch(dev -> dev.getRefId().equals(deviceId))) {
			return redirect(HOME);
		}

		String datasetRecordUrl = controllers.api.routes.TimeseriesDSController
		        .record(ds.getId(), ds.configuration(Dataset.API_TOKEN, "")).absoluteURL(request, !environment.isDev());

		// render the data logger interface
		return ok(views.html.tools.datalogger.datalogger.render(project.getName(), datasetRecordUrl, deviceId));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * show import page
	 * 
	 * @param request
	 * @param id
	 * @param token
	 * @param deviceId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result importLog(Request request, Long id, String token, String deviceId) {
		getAuthenticatedUserOrReturn(request, redirect(HOME));

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound("Dataset not found");
		} else if (!ds.canAppend()) {
			return forbidden("Dataset not accessible");
		}

		// check API token (both internal and configuration)
		String checkToken = token;
		String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
		if (checkToken == null || (!ds.getApiToken().equals(checkToken) && !checkToken.equals(internalToken))) {
			return forbidden("Api token is not correct");
		}

		// check both source_id and device_id, just in case
		if (!ds.isOpenParticipation() && deviceId.isEmpty()) {
			return notFound("Source device not found");
		}

		// get all devices
		ds.getProject().refresh();
		Optional<Device> deviceOpt = ds.getProject().getDevices().stream().filter(d -> d.getRefId().equals(deviceId))
		        .findFirst();
		if (deviceOpt.isEmpty()) {
			return notFound("Source device not registered");
		}

		return ok(views.html.tools.datalogger.importLog.render(id, token, deviceId, csrfToken(request)));
	}

	/**
	 * import a CSV file into the dataset given by id and token, associated to the device given by deviceId
	 * 
	 * @param request
	 * @param id
	 * @param token
	 * @param deviceId
	 * @param selectedColumns
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> importCSV(Request request, Long id, String token, String deviceId,
	        String selectedColumns) {
		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {
			// check body first
			MultipartFormData<TemporaryFile> mpfd = request.body().asMultipartFormData();
			if (mpfd == null || mpfd.isEmpty() || mpfd.getFiles().isEmpty()) {
				return badRequest("No data (object) given.");
			}

			// check id
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound("Dataset not found");
			} else if (!ds.canAppend()) {
				return forbidden("Dataset not accessible");
			}

			// check API token (both internal and configuration)
			String checkToken = token;
			String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
			if (checkToken == null || (!ds.getApiToken().equals(checkToken) && !checkToken.equals(internalToken))) {
				return forbidden("Api token is not correct");
			}

			// check both source_id and device_id, just in case
			if (!ds.isOpenParticipation() && deviceId.isEmpty()) {
				return notFound("Source device not found");
			}

			// get all devices
			ds.getProject().refresh();
			Optional<Device> deviceOpt = ds.getProject().getDevices().stream()
			        .filter(d -> d.getRefId().equals(deviceId)).findFirst();

			// quick abort if there is no device and dataset not open
			if (deviceOpt.isEmpty() && !ds.isOpenParticipation()) {
				// notify the diagnostics
				cache.set("DatasetsController_httpPostDiagnostics_" + id,
				        "Device " + deviceId + " not found at " + new Date(), 300);

				// abort
				return notFound("Source device not registered");
			}

			// record the information in the database for this data set
			final TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
			final Device device = deviceOpt.get();

			AtomicInteger lineCount = new AtomicInteger(0);
			AtomicLong currentTimestamp = new AtomicLong(0);
			try {
				// get file from form data
				FilePart<TemporaryFile> tempFile = mpfd.getFiles().get(0);
				File file = tempFile.getRef().path().toFile();

				// check file size before parsing
				if (file.length() > 1024 * 1024 * 10) {
					return badRequest("CSV file is too large.");
				}

				// get some more parameters...?
				String columnsStr = selectedColumns.contains("=") ? selectedColumns.split("=", 2)[1] : selectedColumns;
				final String[] columns = columnsStr.split(",");
				CSVFormat format = CSVFormat.Builder.create(CSVFormat.EXCEL).setHeader().setSkipHeaderRecord(true)
				        .build();
				CSVParser.parse(new FileReader(file), format).forEach((record) -> {
					try {
						// parse date and time
						String timestampStr = record.get("time");

						// truncate to seconds
						long timestamp = (Long.parseLong(timestampStr) / 1_000_000_000) * 1000;
						if (timestamp == currentTimestamp.get()) {
							return;
						}

						currentTimestamp.set(timestamp);
						final Date ts = new Date(timestamp);

						// construct JSON Object of all remaining data
						ObjectNode data = Json.newObject();
						for (String col : columns) {
							data.put(col, record.get(col));
						}

						// import line with the given mapping
						tssc.addRecord(device, ts, "", data.toString());
						lineCount.getAndIncrement();
					} catch (Exception e) {
						logger.error("CSV parsing error in import.", e);
					}
				});
			} catch (IOException e) {
				logger.error("CSV parsing error in import.", e);
			}
			return ok("Imported " + lineCount.get() + " lines.");
		});
	}

	/**
	 * import a JSON file into the dataset given by id and token, associated to the device given by deviceId
	 * 
	 * @param request
	 * @param id
	 * @param token
	 * @param deviceId
	 * @param selectedProperties
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> importJSON(Request request, Long id, String token, String deviceId,
	        String selectedProperties) {
		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {
			// check body first
			MultipartFormData<TemporaryFile> mpfd = request.body().asMultipartFormData();
			if (mpfd == null || mpfd.isEmpty() || mpfd.getFiles().isEmpty()) {
				return badRequest("No data (object) given.");
			}

			// check id
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound("Dataset not found");
			} else if (!ds.canAppend()) {
				return forbidden("Dataset not accessible");
			}

			// check API token (both internal and configuration)
			String checkToken = token;
			String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
			if (checkToken == null || (!ds.getApiToken().equals(checkToken) && !checkToken.equals(internalToken))) {
				return forbidden("Api token is not correct");
			}

			// check both source_id and device_id, just in case
			if (!ds.isOpenParticipation() && deviceId.isEmpty()) {
				return notFound("Source device not found");
			}

			// get all devices
			ds.getProject().refresh();
			Optional<Device> deviceOpt = ds.getProject().getDevices().stream()
			        .filter(d -> d.getRefId().equals(deviceId)).findFirst();

			// quick abort if there is no device and dataset not open
			if (deviceOpt.isEmpty() && !ds.isOpenParticipation()) {
				// notify the diagnostics
				cache.set("DatasetsController_httpPostDiagnostics_" + id,
				        "Device " + deviceId + " not found at " + new Date(), 300);

				// abort
				return notFound("Source device not registered");
			}

			// record the information in the database for this data set
			final TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
			final Device device = deviceOpt.get();

			// get some more parameters...?
			String columnsStr = selectedProperties.contains("=") ? selectedProperties.split("=", 2)[1]
			        : selectedProperties;
			final String[] columns = columnsStr.split(",");
			AtomicInteger lineCount = new AtomicInteger(0);
			try {
				// get file from form data
				FilePart<TemporaryFile> tempFile = mpfd.getFiles().get(0);
				File file = tempFile.getRef().path().toFile();

				// check file size before parsing
				if (file.length() > 1024 * 1024 * 10) {
					return badRequest("JSON file is too large.");
				}

				// parse file and group object nodes by second-truncated timestamp
				JsonNode jn = Json.parse(new FileInputStream(file));
				Map<Long, List<ObjectNode>> map = StreamSupport.stream(((ArrayNode) jn).spliterator(), true)
				        .map(node -> (ObjectNode) node).filter(jo -> jo.has("time"))
				        .collect(Collectors.groupingBy(jo -> {
					        // parse date and time
					        String timestampStr = jo.get("time").asText("");

					        // truncate to seconds
					        long timestamp = (Long.parseLong(timestampStr) / 1_000_000_000) * 1000;
					        return timestamp;
				        }));
				// iterate through all object nodes per second, merge, extract and record
				map.entrySet().stream().forEach(e -> {
					Date ts = new Date(e.getKey());
					ObjectNode object = e.getValue().stream().reduce(Json.newObject(), (a, b) -> a.setAll(b));

					ObjectNode data = Json.newObject();
					for (String col : columns) {
						if (object.has(col)) {
							data.put(col, object.get(col).asText(""));
						}
					}

					// only add line if there is actual data added from the parsed object
					if (!data.isEmpty()) {
						// import line with the given mapping
						tssc.addRecord(device, ts, "", data.toString());
						lineCount.getAndIncrement();
					}
				});
			} catch (Exception e) {
				logger.error("JSON parsing error in import.", e);
			}
			return ok("Imported " + lineCount.get() + " lines.");
		});

	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * JSON-based submission of data logging request for a single event
	 * 
	 * @param request
	 * @param id
	 * @param deviceId
	 * @return
	 */
	public CompletionStage<Result> log(Request request, final Long id, final String token, final String deviceId) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// check body first
			final JsonNode jn = request.body().asJson();
			if (jn == null || !jn.isObject()) {
				return badRequest("No data (object) given.");
			}

			// check id
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound("Dataset not found");
			} else if (!ds.canAppend()) {
				return forbidden("Dataset not accessible");
			}

			// check API token (both internal and configuration)
			String checkToken = token;
			String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
			if (checkToken == null || (!ds.getApiToken().equals(checkToken) && !checkToken.equals(internalToken))) {
				return forbidden("Api token is not correct");
			}

			// check both source_id and device_id, just in case
			if (!ds.isOpenParticipation() && deviceId.isEmpty()) {
				return notFound("Source device not found");
			}

			// get all devices
			ds.getProject().refresh();
			Optional<Device> deviceOpt = ds.getProject().getDevices().stream()
			        .filter(d -> d.getRefId().equals(deviceId)).findFirst();

			// quick abort if there is no device and dataset not open
			if (deviceOpt.isEmpty() && !ds.isOpenParticipation()) {
				// notify the diagnostics
				cache.set("DatasetsController_httpPostDiagnostics_" + id,
				        "Device " + deviceId + " not found at " + new Date(), 300);

				// abort
				return notFound("Source device not registered");
			}

			// record the information in the database for this data set
			final TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);

			// retrieve object node
			ObjectNode on = (ObjectNode) jn;

			// access the "payload" array
			JsonNode payloadArray = on.get("payload");
			if (payloadArray == null || !payloadArray.isArray() || payloadArray.size() > 500) {
				// notify the diagnostics
				cache.set("DatasetsController_httpPostDiagnostics_" + id,
				        "Device " + deviceId + " upload is broken or too large at " + new Date(), 300);

				// abort
				return badRequest("Payload broken or too large");
			}

			final long submissionTime = System.currentTimeMillis();
			Map<Long, ObjectNode> itemMaps = new HashMap<>();
			// Iterate through each item in the "payload" array
			for (JsonNode payloadItem : payloadArray) {
				// extract timestamp or assign current time
				final long timestamp;
				JsonNode jsonNode = payloadItem.get("time");
				if (jsonNode != null && jsonNode.isNumber()) {
					long nanoTimestamp = jsonNode.asLong();
					// convert nanoseconds to milliseconds, truncating any sub-second parts
					timestamp = (long) Math.floor(nanoTimestamp / 1_000_000_000) * 1_000L;
				} else {
					timestamp = submissionTime;
				}

				// extract actual values
				JsonNode values = payloadItem.get("values");
				// only process, if correct format and non-empty
				if (values == null || values.isEmpty() || !values.isObject()) {
					continue;
				}

				ObjectNode items = itemMaps.getOrDefault(timestamp, Json.newObject());
				items.setAll((ObjectNode) values);

				values.fields().forEachRemaining(field -> {
					items.set(field.getKey(), field.getValue());
				});

				if (!items.isEmpty()) {
					itemMaps.put(timestamp, items);
				}
			}

			// submit
			for (Map.Entry<Long, ObjectNode> e : itemMaps.entrySet()) {
				if (deviceOpt.isPresent()) {
					tssc.addRecord(deviceOpt.get(), new Date(e.getKey()), "", e.getValue());
				} else {
					tssc.addRecord(deviceId, new Date(e.getKey()), "", e.getValue());
				}
			}

			// notify the diagnostics
			cache.set("DatasetsController_httpPostDiagnostics_" + id, "Device " + deviceId + " uploaded "
			        + StringUtils.pluralize("record", itemMaps.size()) + " at " + new Date(), 300);

			return ok();
		}).exceptionally((e) -> {
			return badRequest();
		});
	}
}

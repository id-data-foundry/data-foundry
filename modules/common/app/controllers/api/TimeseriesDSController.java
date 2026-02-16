package controllers.api;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Project;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import models.sr.Device;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.DateUtils;
import utils.StringUtils;
import utils.components.OnboardingMessage;
import utils.components.OnboardingSupport;

public class TimeseriesDSController extends AbstractDSController {

	private static final Logger.ALogger logger = Logger.of(TimeseriesDSController.class);

	@Inject
	public TimeseriesDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
	        OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		// check permissions
		if (!ds.visibleFor(username)) {
			return redirect(PROJECT(ds.getProject().getId())).addingToSession(request, "error",
			        "Project is not accessible.");
		}

		// if no participant/wearable/cluster are in this project, then popup dialog
		Optional<OnboardingMessage> msg = onboardingSupport.setFlashMsg(ds.getProject(), username, "iot_ds");
		return ok(views.html.datasets.ts.view.render(ds, username, msg, request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		// display the add form page
		return ok(views.html.datasets.ts.add.render(csrfToken(request), p));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data");
		}

		Date[] dates = DateUtils.getDates(df.get("start-date"), df.get("end-date"));

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.IOT, p, df.get("description"),
		        df.get("target_object"), df.get("isPublic"), df.get("license"));
		ds.setStart(dates[0]);
		ds.setEnd(dates[1]);
		ds.save();

		// auto-generate the API token for sending data to the dataset
		ds.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds.getId()));
		ds.update();

		onboardingSupport.updateAfterDone(username, "new_dataset");

		// display the add form page
		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		if (!ds.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		// display the add form page
		return ok(views.html.datasets.ts.edit.render(csrfToken(request), ds));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		if (!ds.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
			        "Expecting some data");
		}

		ds.setName(htmlTagEscape(nss(df.get("dataset_name"), 64)));
		ds.setDescription(htmlTagEscape(nss(df.get("description"))));
		ds.setTargetObject(nss(df.get("target_object")));
		ds.setOpenParticipation(df.get("isPublic") == null ? false : true);
		ds.setLicense(df.get("license"));

		// dates
		storeDates(ds, df);

		// metadata
		storeMetadata(ds, df);
		ds.update();

		// display the add form page
		return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "message",
		        "Changes saved.");
	}

	@Authenticated(UserAuth.class)
	public Result recordForm(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
			        "Dataset is closed (adjust start and end dates to open).");
		}

		Project project = ds.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You need to be either project owner or collaborator to perform this action.");
		}

		List<Device> deviceSelection = new LinkedList<Device>();
		for (Device device : project.getDevices()) {
			if (deviceSelection.size() < 10) {
				deviceSelection.add(device);
			}
		}

		return ok(views.html.datasets.ts.record.render(ds, deviceSelection));
	}

	public CompletionStage<Result> recordApi(Request request, final Long id, final String dsApiToken) {
		return record(request, id, dsApiToken);
	}

	/**
	 * form-based logging of data to an IoT / Timeseries dataset; the data is submitted as
	 * "application/x-www-form-urlencoded" or "multipart/form-data" or JSON, and then parsed as a dynamic form
	 * 
	 * @param request
	 * @param id
	 * @param dsApiToken
	 * @return
	 */
	public CompletionStage<Result> record(Request request, final Long id, final String dsApiToken) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// check id
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
			} else if (!ds.canAppend()) {
				return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
				        "error", "Dataset is closed (adjust start and end dates to open).");
			}

			final String sourceId, deviceId, activity, data, checkBodyToken;
			final boolean testing;

			// try to retrieve values from JSON body
			JsonNode jn = request.body().asJson();
			if (jn != null) {
				sourceId = nss(jn.get("source_id"));
				deviceId = nss(jn.get("device_id"));
				activity = nss(jn.get("activity"));
				if (jn.hasNonNull("data")) {
					JsonNode jsonNode = jn.get("data");
					data = jsonNode.isObject() ? jsonNode.toPrettyString() : jsonNode.asText("");
				} else {
					data = "";
				}
				checkBodyToken = nss(jn.get("api_token"));
				testing = false;
			}
			// try to retrieve values from urlencoded or multipart body
			else {
				DynamicForm df = formFactory.form().bindFromRequest(request);
				if (df == null) {
					return badRequest("Expecting some data");
				}
				sourceId = nss(df.get("source_id"));
				deviceId = nss(df.get("device_id"));
				activity = nss(df.get("activity"));
				data = nss(df.get("data"));
				checkBodyToken = nss(df.get("api_token"));
				testing = nss(df.get("test_form")).equals("true");
			}

			// check API token (both internal and configuration)
			final String checkToken = dsApiToken;
			final String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
			if (checkToken == null || (!ds.getApiToken().equals(checkToken) && !checkToken.equals(internalToken))) {
				if (!ds.getApiToken().equals(checkBodyToken) && !checkBodyToken.equals(internalToken)) {
					return testing ? redirect(controllers.routes.DatasetsController.view(id))
					        : forbidden("Api token is not correct");
				}
			}

			// check both source_id and device_id, just in case
			if (!ds.isOpenParticipation() && sourceId.isEmpty() && deviceId.isEmpty()) {
				return testing ? redirect(controllers.routes.DatasetsController.view(id))
				        : notFound("Source device not found");
			}

			// record the information in the database for this data set
			final TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);

			// if sourceId is empty, use device_Id
			final String refId = sourceId.isEmpty() ? deviceId : sourceId;

			// check if there is any data at all
			if (refId.isEmpty() && activity.isEmpty() && data.isEmpty()) {
				// notify the diagnostics
				cache.set("DatasetsController_httpPostDiagnostics_" + id, "❌ Empty data submitted at " + new Date(),
				        300);
				return badRequest("");
			}

			final Optional<Device> opt = refId.isEmpty() ? Optional.empty()
			        : Device.find.query().setMaxRows(1).where().eq("refId", refId).findOneOrEmpty();
			if (!opt.isPresent()) {
				if (!ds.isOpenParticipation()) {
					// open device id not permitted
					return testing ? redirect(controllers.routes.DatasetsController.view(id))
					        : notFound("Source device not registered");
				} else {
					// submission with textual device id
					tssc.addRecord(refId, new Date(), activity, data);

					// notify the diagnostics
					cache.set("DatasetsController_httpPostDiagnostics_" + id,
					        "Device " + refId + " uploaded 1 record at " + new Date(), 300);

					// response
					return testing ? redirect(controllers.routes.DatasetsController.view(id)) : ok("");
				}
			}

			// device id is given and in the project?
			Device device = opt.get();
			if (!ds.isOpenParticipation() && !ds.getProject().getId().equals(device.getProject().getId())) {
				return testing ? redirect(controllers.routes.DatasetsController.view(id))
				        : forbidden("Source device not permitted");
			}

			// submit
			tssc.addRecord(device, new Date(), activity, data);

			// notify the diagnostics
			cache.set("DatasetsController_httpPostDiagnostics_" + id,
			        "Device " + refId + " uploaded 1 record at " + new Date(), 300);

			// response
			return testing ? redirect(controllers.routes.DatasetsController.view(id)) : ok("");
		});
	}

	/**
	 * header-based submission of log request for a single line / event (API version)
	 * 
	 * @param request
	 * @param id
	 * @param activity
	 * @return
	 */
	public CompletionStage<Result> logApi(Request request, final Long id, final String activity) {
		return log(request, id, activity);
	}

	/**
	 * header-based submission of log request for a single line / event as JSON
	 * 
	 * @param request
	 * @param id
	 * @param activity
	 * @return
	 */
	public CompletionStage<Result> log(Request request, final Long id, final String activity) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// check id
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
			} else if (!ds.canAppend()) {
				return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
				        "error", "Dataset is closed (adjust start and end dates to open).");
			}

			// check all header input
			String dsApiToken = request.header(Dataset.API_TOKEN).orElse("");
			String sourceId = request.header("source_id").orElse("");
			String deviceId = request.header("device_id").orElse("");
			// check API token (both internal and configuration)
			String checkToken = dsApiToken;
			String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
			if (checkToken == null || (!ds.getApiToken().equals(checkToken) && !checkToken.equals(internalToken))) {
				return forbidden("Api token is not correct");
			}

			// check both source_id and device_id, just in case
			if (!ds.isOpenParticipation() && sourceId.isEmpty() && deviceId.isEmpty()) {
				// notify the diagnostics
				cache.set("DatasetsController_httpPostDiagnostics_" + id, "❌ Missing device in update at " + new Date(),
				        300);

				return notFound("Source device not found");
			}

			final JsonNode jn = request.body().asJson();
			if (jn == null || !jn.isObject()) {
				// notify the diagnostics
				cache.set("DatasetsController_httpPostDiagnostics_" + id, "❌ Empty data submitted at " + new Date(),
				        300);

				return badRequest("No data (object) given.");
			}

			// retrieve object node
			ObjectNode on = (ObjectNode) jn;

			// record the information in the database for this data set
			final TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);

			// if sourceId is empty, use device_Id
			final String refId = sourceId.isEmpty() ? deviceId : sourceId;
			final Optional<Device> opt = Device.find.query().setMaxRows(1).where().eq("refId", refId).findOneOrEmpty();
			// device could not be found
			if (!opt.isPresent()) {
				if (!ds.isOpenParticipation()) {
					// notify the diagnostics
					cache.set("DatasetsController_httpPostDiagnostics_" + id,
					        "❌ Missing device in update at " + new Date(), 300);

					// open device id not permitted
					return notFound("Source device not registered");
				} else {
					// submission with textual device id
					tssc.addRecord(refId, new Date(), activity, on);

					// notify the diagnostics
					cache.set("DatasetsController_httpPostDiagnostics_" + id,
					        "Device " + refId + " uploaded 1 record at " + new Date(), 300);

					return ok();
				}
			}

			// device id is given and in the project?
			Device device = opt.get();
			if (!ds.isOpenParticipation() && !ds.getProject().getId().equals(device.getProject().getId())) {
				// notify the diagnostics
				cache.set("DatasetsController_httpPostDiagnostics_" + id,
				        "❌ Device not allowed in update at " + new Date(), 300);

				return forbidden("Source device not permitted");
			}

			// submit
			tssc.addRecord(device, new Date(), activity, on);

			// notify the diagnostics
			cache.set("DatasetsController_httpPostDiagnostics_" + id,
			        "Device " + refId + " uploaded 1 record at " + new Date(), 300);

			return ok();
		});
	}

	/**
	 * header-based submission of log request for an entire CSV file which is included in the POST request body
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public CompletionStage<Result> logFile(Request request, final Long id) {

		logger.info("logFile initiated with id " + id);

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// check id
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				logger.warn(" - dataset " + id + " not found");
				return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
			} else if (!ds.canAppend()) {
				logger.warn(" - dataset " + id + " not appendable");
				return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
				        "error", "Dataset is closed (adjust start and end dates to open).");
			}

			// check all header input
			String dsApiToken = request.header("api_token").orElse("");
			String sourceId = request.header("source_id").orElse("");
			String deviceId = request.header("device_id").orElse("");
			// check API token (both internal and configuration)
			String checkToken = dsApiToken;
			String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
			if (checkToken == null || (!ds.getApiToken().equals(checkToken) && !checkToken.equals(internalToken))) {
				logger.warn(" - api token " + checkToken + " not correct");
				return forbidden("Api token is not correct");
			}

			// check both source_id and device_id, just in case
			if (!ds.isOpenParticipation() && sourceId.isEmpty() && deviceId.isEmpty()) {
				logger.warn(" - source device not found");
				return notFound("Source device not found");
			}

			// record the information in the database for this data set
			final TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);

			// determine the device
			final Device device;
			// if sourceId is empty, use device_Id
			final String refId = sourceId.isEmpty() ? deviceId : sourceId;
			final Optional<Device> opt = Device.find.query().setMaxRows(1).where().eq("refId", refId).findOneOrEmpty();
			// device could not be found
			if (!opt.isPresent()) {
				if (!ds.isOpenParticipation()) {
					logger.warn(" - dataset not open and no source device");
					// open device id not permitted
					return notFound("Source device not registered");

				} else {
					// empty device
					device = new Device();
					device.setId(-1l);
					device.setRefId(refId);
					device.setPublicParameter1(refId);
				}
			} else {
				// device id is given and in the project?
				device = opt.get();
				if (!ds.isOpenParticipation() && !ds.getProject().getId().equals(device.getProject().getId())) {
					logger.warn(" - source device " + refId + " not permitted");
					return forbidden("Source device not permitted");
				}
			}

			// notify the diagnostics
			cache.set("DatasetsController_httpPostDiagnostics_" + id,
			        "Device " + refId + " is uploading data at " + new Date(), 300);

			AtomicInteger recordInsertedCount = new AtomicInteger();
			try {
				// parse body as CSV and add individual lines as records
				final String strBody = request.body().asText();
				logger.info(" - payload " + strBody.length() + " bytes");
				CSVParser csvp = CSVParser.parse(strBody,
				        CSVFormat.Builder.create(CSVFormat.RFC4180).setHeader().setSkipHeaderRecord(true).build());

				// check CSV header first
				if (csvp.getHeaderNames().isEmpty()) {
					logger.warn(" - CSV header is not provided");
					return badRequest("No CSV header provided.");
				}

				// insert line by line
				csvp.forEach(r -> {
					if (r.isConsistent()) {
						String activity = r.isMapped("activity") ? r.get("activity") : "";

						// retrieve object node
						ObjectNode on = Json.newObject();
						csvp.getHeaderNames().stream().forEach(s -> {
							if (r.isMapped(s) && r.isSet(s) && !s.equals("activity")) {
								on.put(s, r.get(s));
							}
						});

						// submit
						tssc.addRecord(device, new Date(), activity, on);
						recordInsertedCount.getAndIncrement();
					} else {
						logger.debug("Record is not consistent: " + r.toString());
					}
				});

				// notify the diagnostics
				cache.set(
				        "DatasetsController_httpPostDiagnostics_" + id, "Device " + refId + " uploaded "
				                + StringUtils.pluralize("record", recordInsertedCount.get()) + " at " + new Date(),
				        300);

				logger.info(" - " + recordInsertedCount.get() + " lines inserted");
				return ok(recordInsertedCount.get() + " lines inserted.");
			} catch (Exception e) {
				logger.error(" - no valid data (CSV) given", e);
				return badRequest("No valid data (CSV) given.");
			}
		});
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////// .

	public Result datavis(Request request, long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		if (!ds.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		return ok(views.html.datasets.ts.datavis.render(ds, ds.getMetaDataProjection()));
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////// .

	public CompletionStage<Result> downloadExternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get device ids (if cluster is given)
		final List<Long> deviceIds = cluster.getDeviceList();
		return CompletableFuture.supplyAsync(() -> internalExport(ds, deviceIds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks)
		                .withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
		                .as("text/csv"));
	}

	public CompletionStage<Result> downloadInternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get device ids (if cluster is given)
		final List<Long> deviceIds = cluster.getDeviceList();
		return CompletableFuture.supplyAsync(() -> internalExport(ds, deviceIds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadRaw(Request request, Long id, Long cluster_id, long limit, long start,
	        long end) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// get device ids (if cluster is given)
		final List<Long> deviceIds;
		if (cluster_id > -1) {
			Cluster c = Cluster.find.byId(cluster_id);
			if (c == null) {
				return redirectCS(HOME);
			}

			deviceIds = c.getDeviceList();
		} else {
			deviceIds = Collections.<Long>emptyList();
		}

		Project project = ds.getProject();
		if (!project.visibleFor(username)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "Project is not accessible."));
		}

		if (acceptLicenseFirst(request, username, project)) {
			// redirect to accept license first
			return redirectCS(controllers.routes.ProjectsController.license(project.getId(),
			        routes.TimeseriesDSController.downloadRaw(id, cluster_id, limit, start, end).relativeTo("/")));
		}

		return CompletableFuture.supplyAsync(() -> internalExportRaw(ds, deviceIds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks)
		                .withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
		                .as("text/csv"));
	}

	public CompletionStage<Result> downloadRawPublic(Request request, String token) {

		// retrieve id from token
		Long id = tokenResolverUtil.getDatasetIdFromToken(token);

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check if the token is current
		String pat = ds.getConfiguration().get(Dataset.PUBLIC_ACCESS_TOKEN);
		if (!token.equals(pat)) {
			return redirectCS(HOME);
		}

		return CompletableFuture.supplyAsync(() -> internalExportRaw(ds, Collections.<Long>emptyList(), -1, -1, -1))
		        .thenApplyAsync(chunks -> ok().chunked(chunks)
		                .withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
		                .as("text/csv"));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * internal export projected data (if projection is available)
	 * 
	 * @param ds
	 * @return
	 */
	private Source<ByteString, ?> internalExport(Dataset ds, List<Long> deviceIds, long limit, long start, long end) {
		TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(queue -> {
			CompletableFuture.runAsync(() -> tssc.exportProjected(queue, deviceIds, limit, start, end));
			return queue;
		});
	}

	/**
	 * internal export just the raw data
	 * 
	 * @param ds
	 * @return
	 */
	private Source<ByteString, ?> internalExportRaw(Dataset ds, List<Long> deviceIds, long limit, long start,
	        long end) {
		TimeseriesDS tssc = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(queue -> {
			CompletableFuture.runAsync(() -> tssc.export(queue, deviceIds, limit, start, end));
			return queue;
		});
	}

	@Authenticated(UserAuth.class)
	public Result tableTree(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		}

		if (!ds.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not accessible.");
		}

		return ok(views.html.datasets.ts.tableTree.render(ds, request));
	}

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> getItemsNested(Request request, Long id) {
		// check user
		String username = getAuthenticatedUserName(request).orElse(null);
		if (username == null) {
			return CompletableFuture.completedFuture(forbidden());
		}

		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(errorJSONResponseObject("Dataset not found."));
			}

			// check access
			if (!ds.visibleFor(username)) {
				return forbidden(errorJSONResponseObject("Project is not accessible."));
			}

			// get items
			final TimeseriesDS tds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
			return ok(tds.getItemsNested()).as("application/json");
		}).exceptionally(e -> {
			logger.error("IoT dataset index problem", e);
			return badRequest();
		});
	}

	private ObjectNode errorJSONResponseObject(String message) {
		ObjectNode on = Json.newObject();
		on.put("result", "error");
		on.put("message", message);
		return on;
	}
}

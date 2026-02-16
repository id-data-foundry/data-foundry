package services.inlets;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.ds.GoogleFitDS;
import models.sr.Wearable;
import play.Logger;
import play.api.db.evolutions.ApplicationEvolutions;
import play.libs.ws.WSClient;
import play.libs.ws.WSResponse;
import services.slack.Slack;
import utils.DateUtils;
import utils.conf.ConfigurationUtils;

@Singleton
public class GoogleFitService implements ScheduledService {

	private static final Logger.ALogger logger = Logger.of(GoogleFitService.class);

	// domain url for authentication
	private static final String OAUTH_TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token";

	// domain url for requesting data
	private static final String API_URL = "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate";

	// map for data urls
	public static final Map<String, String> DATA_URL = new HashMap<>();

	private final WSClient ws;
	private final DatasetConnector datasetConnector;

	public final String APP_CLIENT_ID;
	private final String APP_CLIENT_SECRET;

	@Inject
	public GoogleFitService(WSClient ws, Config config, DatasetConnector datasetConnector,
	        ApplicationEvolutions evolutions) {
		this.ws = ws;
		this.datasetConnector = datasetConnector;

		// configuration
		if (!config.hasPath(ConfigurationUtils.DF_VENDOR_GOOGLEFIT_ID)
		        || !config.hasPath(ConfigurationUtils.DF_VENDOR_GOOGLEFIT_SECRET)) {
			APP_CLIENT_ID = "";
			APP_CLIENT_SECRET = "";
			logger.info("GoogleFit service not configured");
			return;
		}

		APP_CLIENT_ID = config.getString(ConfigurationUtils.DF_VENDOR_GOOGLEFIT_ID);
		APP_CLIENT_SECRET = config.getString(ConfigurationUtils.DF_VENDOR_GOOGLEFIT_SECRET);
		setDataSourceIds();

		// starting service
		if (evolutions.upToDate()) {
			logger.info("GoogleFit service starting");
			// updateWearables();
			updateDatasets();
		}
	}

	private void updateDatasets() {
		List<Dataset> dsList = Dataset.find.query().where().eq("dsType", DatasetType.GOOGLEFIT).findList();

		for (Dataset ds : dsList) {
			if (ds.getTargetObject().contains("::")) {
				ds.setTargetObject(ds.getTargetObject().split("::")[0]);
				ds.update();
				logger.info(" - Updated GoogleFit dataset (" + ds.getRefId() + ")");
			}
		}
	}

	@Override
	public void stop() {
	}

	/**
	 * check whether service is configured
	 * 
	 * @return
	 */
	public boolean isActive() {
		return APP_CLIENT_ID.length() > 0 && APP_CLIENT_SECRET.length() > 0;
	}

	@Override
	public void refresh() {
		logger.info("Starting to sync GoogleFit.");

		try {
			// find applicable wearables for refresh
			final List<Wearable> wearableList = new LinkedList<Wearable>();

			// get active projects (not archived)
			final List<Project> activeProjects = Project.find.query().where().eq("archivedProject", false).findList();
			for (Project ap : activeProjects) {
				if (!ap.getWearables().isEmpty()) {
					for (Wearable w : ap.getWearables()) {
						if (w.isConnected() && w.isGoogleFit()) {
							wearableList.add(w);
						}
					}
				}
			}

			// refresh wearables in list
			if (!wearableList.isEmpty()) {
				logger.info(" - Refreshing " + wearableList.size() + " GoogleFit wearable(s)");
				for (Wearable w : wearableList) {
					refreshAndFetchWearable(w);
				}
			}

		} catch (Exception e) {
			logger.error("Error GoogleFit refresh.", e);
			Slack.call("Exception of GoogleFit daily routine. ", e.getLocalizedMessage());
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get authorization and tokens from GoogleFit server
	 * 
	 * @return
	 */
	public boolean authorizationRequest(Wearable wearable, String authorizationCode, String redirectUrl) {

		// compose authorization token request
		try {
			WSResponse response = ws.url(OAUTH_TOKEN_URL).setContentType("application/x-www-form-urlencoded")
			        .setRequestTimeout(Duration.ofSeconds(5))
			        .post("client_id=" + APP_CLIENT_ID + "&code=" + authorizationCode + "&client_secret="
			                + APP_CLIENT_SECRET + "&grant_type=authorization_code" + "&redirect_uri=" + redirectUrl
			                + "&scope=")
			        .toCompletableFuture().get();

			// retrieve and save the request parameters
			JsonNode jn = response.asJson();

			// access_token
			wearable.setApiToken(jn.get("access_token").asText());

			// refresh_token
			wearable.setApiKey(jn.get("refresh_token").asText());

			// user_id
			wearable.setUserId(getUserId(jn.get("id_token").asText()));

			// check all and save
			wearable.update();

			LabNotesEntry.log(Wearable.class, LabNotesEntryType.CONFIGURE, "Wearable connected: " + wearable.getName(),
			        wearable.getProject(), Dataset.find.byId(wearable.getDatasetId()));
			showWearableInfo(wearable, "googlefit wearable setup");

			return true;

		} catch (Exception e) {
			logger.error("Exception in authorizationTokenReuqest", e);
			Slack.call("Exception: Authorization fail by wearable: " + wearable.getRefId() + ". ",
			        e.getLocalizedMessage());
		} finally {
			// refreshAndFetchWearable(wearable);
			// doFetch(wearable, 1757714400000l);
		}

		return false;
	}

	/**
	 * refresh authorization and tokens from GoogleFit server
	 * 
	 * @return
	 */
	public boolean refreshTokenRequest(Wearable wearable) {
		try {
			// compose refresh token request
			WSResponse response = ws.url(OAUTH_TOKEN_URL).setContentType("application/x-www-form-urlencoded")
			        .setRequestTimeout(Duration.ofSeconds(5))
			        .post("client_id=" + APP_CLIENT_ID + "&grant_type=refresh_token" + "&client_secret="
			                + APP_CLIENT_SECRET + "&refresh_token=" + wearable.getApiKey())
			        .toCompletableFuture().get();

			// retrieve and save the request parameters
			JsonNode jn = response.asJson();

			// access_token
			if (jn.has("access_token")) {
				wearable.setApiToken(jn.get("access_token").asText());

				// user_id
				if (jn.get("id_token") != null) {
					wearable.setUserId(getUserId(jn.get("id_token").asText()));
				}

				// everytime refreshes access token with the same refresh token
				// wearable.apiKey = jn.get("refresh_token").asText();

				// check all and save
				wearable.update();
				return true;
			} else {

				// FIXME: what should we do in this case?

				return false;
			}
		} catch (Exception e) {
			logger.error("Exception in refreshTokenRequest", e);
			// Slack.call("Exception", e.getLocalizedMessage());
			return false;
		}
	}

	/**
	 * generate the authorization value
	 * 
	 * @return
	 */
	private static final String getAuthorizationValue(String access_token) {
		return "Bearer " + access_token;
	}

	/**
	 * refresh tokens, and check whether wearable should be fetched data, if yes, then get the data of yesterday from
	 * GoogleFit server; if no, then wait or update the expiry.
	 * 
	 * @param wearableList
	 * @param ds_id
	 * @param date
	 */
	private void refreshAndFetchWearable(Wearable wearable) {

		final long today = DateUtils.dayBefore(DateUtils.startOfDay(new Date())).getTime();

		try {
			// refresh first
			boolean success = refreshTokenRequest(wearable);

			// failed to refresh tokens
			if (!success) {
				logger.info("  -- Token of wearable " + wearable.getRefId() + " failed to refresh. Aborting.");
				// TODO: send notification to researcher
				return;
			} else {
				logger.info("  -- Token of wearable " + wearable.getRefId() + " refreshed, checking for fetch now.");
			}

			// check linked dataset setting
			if (wearable.getDatasetId() < 0) {
				logger.info("  -- Fail to fetch, dataset of this wearable is not valid: " + wearable.getRefId());
				return;
			}

			// check synchronization of wearable data
			final boolean isSynced = checkSynchronization(wearable, LocalDate.now().minusDays(1l).toString(),
			        LocalDate.now().toString());
			final Dataset ds = Dataset.find.byId(wearable.getDatasetId());

			if (!isSynced) {
				// no fetching, as wearable is not synced
				logger.info("  -- " + wearable.getRefId() + " is not synced by end-user.");
			} else if (wearable.getExpiry() == ds.getStart().getTime() && wearable.getExpiry() > today) {
				// no fetching, as wearable is waiting for next dataset or not synced
				logger.info("  -- " + wearable.getRefId() + " is in waiting for opening dataset.");
			} else if (wearable.getExpiry() > ds.getEnd().getTime()) {
				List<Dataset> nextDSList = wearable.getProject().getNextDSList(ds);
				if (nextDSList.size() <= 1) {
					// no fetching, as there is no next dataset for the wearable, so only update expiry and refresh
					// tokens
					wearable.setExpiry(wearable.getExpiry() + 86400000l);
					wearable.update();
					logger.info("  -- " + wearable.getRefId() + " is updated and idle now.");
				} else {
					// move to next wearable dataset
					Dataset newDS = nextDSList.get(1);
					wearable.setExpiry(newDS.getStart().getTime());
					wearable.setScopes(newDS.getId().toString());
					wearable.update();
					logger.info("  -- " + wearable.getRefId() + " is assigned for new dataset now.");

					// if new dataset is active, fetch data, otherwise pass
					if (newDS.isActive()) {
						doFetch(wearable, today);
						logger.info("  -- " + wearable.getRefId() + " is synced.");
					}
				}
			} else {
				// if wearable.expiry is active for the dataset, fetch data
				if (wearable.getExpiry() >= 0 && wearable.getExpiry() <= today) {
					doFetch(wearable, today);
					logger.info("  -- " + wearable.getRefId() + " is synced.");
				}
				// expiry > today
				else {
					logger.info("  -- " + wearable.getRefId() + " is synced in the earlier fetching.");
				}
			}
		} catch (Exception e) {
			logger.error("Exception in GoogleFitService: refreshAndFetchWearable", e);
		}
	}

	/**
	 * try to fetch data of a wearable and update both expiry and scopes if necesarry
	 * 
	 * @param wearable
	 * @param timeToFetch
	 */
	private void doFetch(Wearable wearable, long timeToFetch) {
		List<Dataset> dsList = wearable.getProject().getNextDSList(wearable.getDatasetId());

		for (int i = 0; i < dsList.size(); i++) {
			Dataset ds = Dataset.find.byId(dsList.get(i).getId());

			if (ds.isActive(wearable.getExpiry()) && wearable.getExpiry() <= timeToFetch
			        && timeToFetch >= DateUtils.startOfDay(ds.getStart()).getTime()) {
				// && timeToFetch >= DatasetUtils.getMillis(ds.startDate(), "00:00:00")) {
				// final long targetDate = timeToFetch > DatasetUtils.getMillis(ds.endDate(), "00:00:00")
				// ? DatasetUtils.getMillis(ds.endDate(), "00:00:00")
				// : timeToFetch;
				final long targetDate = timeToFetch > DateUtils.startOfDay(ds.getEnd()).getTime()
				        ? DateUtils.startOfDay(ds.getEnd()).getTime()
				        : timeToFetch;

				logger.info("  -- Starting to fetch for GoogleFit wearable " + wearable.getRefId() + " for dataset "
				        + ds.getId());

				final GoogleFitDS gfds = (GoogleFitDS) datasetConnector.getDatasetDS(ds.getId());
				try {
					for (long missingDate = wearable.getExpiry(); missingDate <= targetDate; missingDate += 86400000l) {
						if (!gfds.hasRecord(wearable, missingDate)) {
							String[] scopes = ds.getTargetObject().split(" ");

							// fetch sleep data only
							if (scopes.length == 1 && scopes[0].equals("sleep")) {
								dataFetchRequestForSleep(wearable, new String[] { "sleep" },
								        DateUtils.getDateFromMillis(missingDate), ds.getId(), true);
							} else {
								// fetch data of scopes except sleep
								dataFetchRequest(wearable, scopes, DateUtils.getDateFromMillis(missingDate),
								        ds.getId(), false, true);

								// fetch sleep data if necessary
								if (Arrays.stream(scopes).anyMatch("sleep"::equals)) {
									dataFetchRequestForSleep(wearable, new String[] { "sleep" },
									        DateUtils.getDateFromMillis(missingDate), ds.getId(), false);
								}
							}
						}
					}

					long returnDate = targetDate;
					// if (returnDate < DatasetUtils.getMillis(ds.endDate(), "00:00:00")) {
					if (returnDate < DateUtils.startOfDay(ds.getEnd()).getTime()) {
						// still in this ds
						wearable.setExpiry(returnDate + 86400000l);
					} else {
						// just finish this ds
						if (dsList.size() > i + 1) {
							// has next dataset
							// wearable.expiry = DatasetUtils.getMillis(dsList.get(i + 1).startDate(), "00:00:00");
							wearable.setExpiry(DateUtils.startOfDay(dsList.get(i + 1).getStart()).getTime());
							wearable.setScopes(Long.toString(dsList.get(i + 1).getId()));
						} else {
							// no next dataset
							wearable.setExpiry(returnDate + 86400000l);
						}
					}
					wearable.update();
				} catch (Exception e) {
					logger.error("Exception in doFetch. ", e);
				}
			}
		}

		logger.info("  -- Fetch of GoogleFit wearable " + wearable.getRefId() + " finished.");
	}

	/**
	 * get data from GoogleFit server
	 * 
	 * @param wearable
	 * @param scopes
	 * @param data_date
	 * @return
	 */
	public boolean dataFetchRequest(Wearable wearable, String[] scopes, String data_date, long dsId, boolean isSleep,
	        boolean isFirstScope) {
		if (!checkScopes(scopes)) {
			return false;
		}

		try {
			// compose and send data fetch URL
			WSResponse response = ws.url(API_URL)
			        .addHeader("Authorization", getAuthorizationValue(wearable.getApiToken())).setFollowRedirects(true)
			        .setContentType("application/json").setRequestTimeout(Duration.ofSeconds(10))
			        .post(getDataRequestContent(scopes, data_date, 1, true, isSleep)).toCompletableFuture().get();

			// list jsonnodes by time
			List<JsonNode> dsList = response.asJson().findParents("dataset");

			// get target dataset
			final GoogleFitDS gfds = (GoogleFitDS) datasetConnector.getDatasetDS(dsId);

			storeData(gfds, wearable, scopes, dsList);

			return true;
		} catch (Exception e) {
			logger.error("Exception in dataFetchRequest", e);
		}

		return false;
	}

	/**
	 * store data by startTimeMillis(data_date for database)
	 * 
	 * @param gfds
	 * @param wearable
	 * @param scopes
	 * @param jn
	 * @param date
	 */
	private void storeData(GoogleFitDS gfds, Wearable wearable, String[] scopes, List<JsonNode> jn) {
		for (int i = 0; i < jn.size(); i++) {
			long dataDate = jn.get(i).get("startTimeMillis").asLong();
			List<JsonNode> jnn = jn.get(i).findParents("point");
			gfds.addRecord(wearable, setScopeList(scopes, jnn), dataDate);
		}
	}

	/**
	 * get data from GoogleFit server
	 * 
	 * @param wearable
	 * @param scopes
	 * @param data_date
	 * @return
	 */
	public boolean dataFetchRequestForSleep(Wearable wearable, String[] scopes, String data_date, long dsId,
	        boolean isFirstScope) {
		if (!checkScopes(scopes)) {
			return false;
		}

		try {
			// compose and send data fetch URL
			WSResponse response = ws.url(API_URL)
			        .addHeader("Authorization", getAuthorizationValue(wearable.getApiToken())).setFollowRedirects(true)
			        .setContentType("application/json").setRequestTimeout(Duration.ofSeconds(10))
			        .post(getDataRequestContent(scopes, data_date, 1, true, true)).toCompletableFuture().get();

			// list jsonnodes by time
			List<JsonNode> dsList = response.asJson().findParents("startTimeNanos");

			// get target dataset
			final GoogleFitDS gfds = (GoogleFitDS) datasetConnector.getDatasetDS(dsId);

			storeSleepRecord(gfds, wearable, scopes, dsList, isFirstScope);

			return true;
		} catch (Exception e) {
			logger.error("Exception in dataFetchRequestForSleep", e);
		}

		return false;
	}

	/**
	 * store sleep data by millis
	 * 
	 * @param gfds
	 * @param wearable
	 * @param scopes
	 * @param jn
	 * @param date
	 */
	private void storeSleepRecord(GoogleFitDS gfds, Wearable wearable, String[] scopes, List<JsonNode> jn,
	        boolean isFirstScope) {

		if (isFirstScope) {
			String[][] scopeList = { { "activity", "0" }, { "calories", "0" }, { "speed", "0" }, { "heart_rate", "0" },
			        { "step_count", "0" }, { "distance", "0" }, { "weight", "0" }, { "sleep", "0" } };

			for (int i = 0; i < jn.size(); i++) {

				// time period of origin data is by nanos, transform it to millis here
				long startTime = setByMinute(jn.get(i).get("startTimeNanos").asLong() / 1000000l),
				        endTime = setByMinute(jn.get(i).get("endTimeNanos").asLong() / 1000000l);

				// the format of sleep data is by time period in nano seconds(we can't control), set the data in this
				// period as the
				// same value
				scopeList[7][1] = jn.get(i).findValues("intVal").get(0) == null ? "-1"
				        : jn.get(i).findValues("intVal").get(0).toString();

				while (startTime <= endTime) {
					gfds.addRecord(wearable, scopeList, startTime);
					startTime += 60000l;
				}
			}
		} else {
			// same way to deal with the data, but updating data instead of adding for the final step
			for (int i = 0; i < jn.size(); i++) {
				long startTime = setByMinute(jn.get(i).get("startTimeNanos").asLong() / 1000000l),
				        endTime = setByMinute(jn.get(i).get("endTimeNanos").asLong() / 1000000l);

				// JsonNode jnn = jn.get(i).findParents("point");
				String sleepData = jn.get(i).findValue("intVal") == null ? "-1"
				        : jn.get(i).findValue("intVal").toString();

				while (startTime <= endTime) {
					gfds.updateSleepRecord(wearable, startTime, sleepData);
					startTime += 60000l;
				}
			}
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * return the tuple of data to store, fill -1/-1.0 for missing data, and fill 0 for empty data
	 * 
	 * @param scopeList
	 * @param scopes
	 * @param jnn
	 * @return
	 */
	private String[][] setScopeList(String[] scopes, List<JsonNode> jnn) {

		String[][] scopeList = { { "activity", "0" }, { "calories", "0" }, { "speed", "0" }, { "heart_rate", "0" },
		        { "step_count", "0" }, { "distance", "0" }, { "weight", "0" }, { "sleep", "0" } };

		// index of scopeList, ignore the sleep scope
		int scopeNum = 0,
		        scopeLength = Arrays.stream(scopes).anyMatch("sleep"::equals) ? scopes.length - 1 : scopes.length;

		for (int listIndex = 0; listIndex < scopeList.length; listIndex++) {
			if (scopeNum < scopeLength && scopeList[listIndex][0].equals(scopes[scopeNum])) {
				if (jnn.get(scopeNum).findValues("value").isEmpty()) {
					scopeList[listIndex][1] = "-1";
				} else if (scopes[scopeNum].equals("activity") || scopes[scopeNum].equals("step_count")) {
					scopeList[listIndex][1] = jnn.get(scopeNum).findValues("intVal").get(0) == null ? "-1"
					        : jnn.get(scopeNum).findValues("intVal").get(0).toString();
				} else {
					scopeList[listIndex][1] = jnn.get(scopeNum).findValues("fpVal").get(0) == null ? "-1.0"
					        : jnn.get(scopeNum).findValues("fpVal").get(0).toString();
				}
				scopeNum++;
			} else {
				scopeList[listIndex][1] = "0";
			}
		}

		// set sleep data as -1 if sleep scope is required
		scopeList[7][1] = Arrays.stream(scopes).anyMatch("sleep"::equals) ? "-1" : "0";

		return scopeList;
	}

	// /**
	// * as adding a new wearable and belongs to an active dataset, try to fetch data
	// *
	// * @param w
	// */
	// private void firstFetch(Wearable w) {
	// logger.info("first fetch");
	// if (nnne(w.scopes) && Long.parseLong(w.scopes) >= 0
	// && Dataset.find.byId(Long.parseLong(w.scopes)).isActive()) {
	// Dataset ds = Dataset.find.byId(Long.parseLong(w.scopes));
	// CompletableFuture.runAsync(() -> {
	// dataFetchRequest(w, ds.targetObject.split(" "), LocalDate.now().minusDays(1l).toString(), ds.id);
	// });
	// }
	// }

	/**
	 * return unique user id of Google account
	 * 
	 * @param id_token
	 * @return
	 */
	private String getUserId(String id_token) {
		String userId = "";

		try {
			// compose data fetch URL
			WSResponse response = ws.url("https://www.googleapis.com/oauth2/v3/tokeninfo?id_token=" + id_token).get()
			        .toCompletableFuture().get();

			// retrieve and save the request parameters
			JsonNode jn = response.asJson();

			// CompletableFuture.runAsync(() -> {
			if (!jn.isNull() && jn.has("sub")) {
				userId = jn.get("sub").asText();
			} else {
				userId = "error";
			}
		} catch (Exception e) {
			logger.error("Exception in getUserId", e);
		}

		return userId;
	}

	/**
	 * return request statement for checking synchronization or fetching data
	 * 
	 * @param scopes
	 * @param targetDate
	 * @param durationMins : 1440 (mins / 1 day, only one record) for synchronization check, 1 (min, 1440 records) for
	 *                     formal data fetching
	 * @param checkedSync  : false for not checking yet
	 * @param isSleep      : true for sleep data only
	 * @return
	 */
	private String getDataRequestContent(String[] scopes, String targetDate, int durationMins, boolean checkedSync,
	        boolean isSleep) {
		return setDataRequestScopes(scopes, checkedSync, durationMins, isSleep) + "\"startTimeMillis\": "
		        + DateUtils.getMillis(targetDate, "00:00:00") + "," // start time
		        + "\"endTimeMillis\": " + DateUtils.getMillis(targetDate, "23:59:59") // + "," // End Time
				// grouping records by milliseconds, 1440 mins for checking records, 1 min for real data fetching
				// + "\"bucketByTime\": { \"durationMillis\": " + (durationMins * 60 * 1000) + " }"
		        + "}";
	}

	/**
	 * return required scopes for request
	 * 
	 * @param scopes
	 * @param checkedSync  : false for not checking yet
	 * @param durationMins : 1440 (mins / 1 day, only one record) for synchronization check, 1 (min, 1440 records) for
	 *                     formal data fetching
	 * @param isSleep      : true for sleep data only
	 * @return
	 */
	private String setDataRequestScopes(String[] scopes, boolean checkedSync, int durationMins, boolean isSleep) {
		String requestContent = "{\"aggregateBy\": [";

		if (!checkedSync) {
			requestContent += "{\"dataTypeName\": \"" + DATA_URL.get("step_count") + "\"}],";
		} else {
			if (isSleep) {
				// sleep data doesn't need the grouping data statement: bucketByTime..., otherwise no data returned
				requestContent += "{\"dataTypeName\": \"com.google.sleep.segment\"}],";
			} else {
				requestContent += "{\"dataTypeName\": \"" + DATA_URL.get(scopes[0]) + "\"}";

				for (int scope = 1; scope < scopes.length; scope++) {
					if (!scopes[scope].equals("sleep")) {
						requestContent += ",{\"dataTypeName\": \"" + DATA_URL.get(scopes[scope]) + "\"}";
					}
				}

				requestContent += "],";

				// grouping records during the request time by milliseconds
				requestContent += "\"bucketByTime\": { \"durationMillis\": " + (durationMins * 60 * 1000) + " },";
			}
		}

		return requestContent;
	}

	/**
	 * check synchronization of wearable by the data of steps, get false for all zeros policy: if the step_count data
	 * for the target day and the day sending this request both exist, which means the data is synced at the moment, and
	 * return true, otherwise return false
	 * 
	 * @param wearable
	 * @param data_date  : target date for the data request
	 * @param fetch_date : the date sending this request
	 * @return
	 */
	private boolean checkSynchronization(Wearable wearable, String data_date, String fetch_date) {
		String[] scopeList = null;
		if (wearable.getDatasetId() >= 0) {
			scopeList = Dataset.find.byId(wearable.getDatasetId()).getTargetObject().split(" ");
		} else {
			logger.info("Not valid dataset setting for wearable: " + wearable.getRefId());
			return false;
		}

		// check whether there is data for the expected date and the date requested, if both are true, then do fetching
		return getStepData(wearable, scopeList, data_date) && getStepData(wearable, scopeList, fetch_date);
	}

	/**
	 * return true if the step_count data exists, no data writing
	 * 
	 * @param wearable
	 * @param scopeList
	 * @param data_date
	 * @return
	 */
	private boolean getStepData(Wearable wearable, String[] scopeList, String data_date) {

		try {
			// compose and send data fetch URL
			WSResponse response = ws.url(API_URL)
			        .addHeader("Authorization", getAuthorizationValue(wearable.getApiToken())).setFollowRedirects(true)
			        .setContentType("application/json").setRequestTimeout(Duration.ofSeconds(10))
			        .post(getDataRequestContent(scopeList, data_date, 1440, false, false)).toCompletableFuture().get();

			// retrieve and transform response
			JsonNode jn = response.asJson();

			// list jsonnodes by time
			List<JsonNode> dsList = jn.get("bucket").findValues("intVal");

			return dsList.size() > 0 ? true : false;
		} catch (Exception e) {
			logger.error("Exception in checkSynchronization", e);
		}

		return false;
	}

	/**
	 * return false if the scopes are not in DATA_URL
	 * 
	 * @param scopes
	 * @return
	 */
	private boolean checkScopes(String[] scopes) {
		for (String scope : scopes) {
			if (!DATA_URL.containsKey(scope)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * set up the map for corresponding scopes and dataset names for GoogleFit
	 */
	private void setDataSourceIds() {
		// scope activity
		DATA_URL.put("activity", "com.google.activity.segment");
		DATA_URL.put("calories", "com.google.calories.expended");
		DATA_URL.put("step_count", "com.google.step_count.delta");
		// scope location
		DATA_URL.put("speed", "com.google.speed");
		DATA_URL.put("distance", "com.google.distance.delta");
		// scope heart_rate
		DATA_URL.put("heart_rate", "com.google.heart_rate.bpm");
		// scope body
		DATA_URL.put("weight", "com.google.weight");
		// scope sleep
		DATA_URL.put("sleep", "com.google.sleep.segment");
	}

	/**
	 * print out wearable information, only for debugging purposes
	 * 
	 * @param w
	 * @param info
	 */
	private void showWearableInfo(Wearable w, String info) {
		logger.info(info);
		logger.info(w.getBrand() + " access token:" + w.getApiToken());
		logger.info(w.getBrand() + " refresh token:" + w.getApiKey());
		logger.info(w.getBrand() + " expiry:" + w.getExpiry());
		logger.info(w.getBrand() + " dataset id:" + w.getScopeFromDataset());
		logger.info(w.getBrand() + " scopes:" + w.getScopeFromDataset());
		logger.info(w.getBrand() + " user ID:" + w.getUserId());
	}
}

package services.inlets;

import java.time.Duration;
import java.util.Base64;
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
import models.ds.FitbitDS;
import models.sr.Wearable;
import play.Logger;
import play.api.db.evolutions.ApplicationEvolutions;
import play.libs.ws.WSClient;
import play.libs.ws.WSResponse;
import services.slack.Slack;
import utils.DataUtils;
import utils.DateUtils;
import utils.conf.ConfigurationUtils;

@Singleton
public class FitBitService implements ScheduledService {

	private static final Logger.ALogger logger = Logger.of(FitBitService.class);

	// domain url for authentication
	private static final String OAUTH_TOKEN_URL = "https://api.fitbit.com/oauth2/token";

	// domain url for requesting data
	private static final String API_URL = "https://api.fitbit.com";

	// map for data urls
	public final Map<String, String> DATA_URL = new HashMap<>();

	private final WSClient ws;
	private final DatasetConnector datasetConnector;

	public final String APP_CLIENT_ID;
	private final String APP_CLIENT_SECRET;

	@Inject
	public FitBitService(WSClient ws, Config config, DatasetConnector datasetConnector,
			ApplicationEvolutions evolutions) {
		this.ws = ws;
		this.datasetConnector = datasetConnector;

		// configuration
		if (!config.hasPath(ConfigurationUtils.DF_VENDOR_FITBIT_ID)
				|| !config.hasPath(ConfigurationUtils.DF_VENDOR_FITBIT_SECRET)) {
			APP_CLIENT_ID = "";
			APP_CLIENT_SECRET = "";
			logger.info("FitBit service not configured");
			return;
		}

		APP_CLIENT_ID = config.getString(ConfigurationUtils.DF_VENDOR_FITBIT_ID);
		APP_CLIENT_SECRET = config.getString(ConfigurationUtils.DF_VENDOR_FITBIT_SECRET);
		setURLs();

		// starting service
		if (evolutions.upToDate()) {
			logger.info("FitBit service starting");
			updateWearables();
			updateDatasets();
		}
	}

	private void updateWearables() {
		List<Wearable> oldWearables = Wearable.find.all();

		for (Wearable w : oldWearables) {
			boolean isModified = false;
			String tempScopes = "";

			// update scopes if scopes is null, empty, contains :: or -1
			if (!nnne(w.getScopeFromDataset()) || w.getScopeFromDataset().contains("::")
					|| w.getScopeFromDataset().equals("-1")) {
				if (w.isFitbit()) {
					if (w.getProject().getFitbitDataset().getId() >= 0) {
						tempScopes = w.getProject().getFitbitDataset().getId().toString();
					} else if (!w.getProject().getFitbitDatasets().isEmpty()) {
						tempScopes = w.getProject().getFitbitDatasets().get(0).getId().toString();
					}

					if (w.getBrand() == null) {
						w.setBrand(Wearable.FITBIT);
					}
				} else {
					if (w.getProject().getGoogleFitDataset().getId() >= 0) {
						tempScopes = w.getProject().getGoogleFitDataset().getId().toString();
					} else if (!w.getProject().getGoogleFitDatasets().isEmpty()) {
						tempScopes = w.getProject().getGoogleFitDatasets().get(0).getId().toString();
					}
				}
				isModified = true;
			}

			// update wearable expiry if scopes is changed
			if (isModified && nnne(tempScopes)) {
				try {
					w.setExpiry(DateUtils.getMillisFromDS(DataUtils.parseLong(tempScopes))[0]);
					w.setScopes(tempScopes);
					w.update();
					logger.info(" - Updated " + w.getBrand() + " wearable (" + w.getRefId() + ")");
				} catch (Exception e) {
					logger.info(" - Failed to update " + w.getBrand() + " wearable (" + w.getRefId() + ")");
				}
			}
		}
	}

	private void updateDatasets() {
		List<Dataset> dsList = Dataset.find.query().where().eq("dsType", DatasetType.FITBIT).findList();

		for (Dataset ds : dsList) {
			if (ds.getTargetObject().contains("::")) {
				ds.setTargetObject(ds.getTargetObject().split("::")[0]);
				ds.update();
				logger.info(" - Updated FitBit dataset (" + ds.getRefId() + ")");
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

	/*
	 * 1. List all the FITBIT wearables 2. Fetch data by the scopes of dataset which the wearables belong to 3. Pass the
	 * data to FitbitDS.record() and store it
	 */
	@Override
	public void refresh() {
		logger.info("Starting to sync FitBit.");

		try {
			// find applicable wearables for refresh
			final List<Wearable> wearableList = new LinkedList<Wearable>();

			// get active projects (not archived)
			final List<Project> activeProjects = Project.find.query().where().eq("archivedProject", false).findList();
			for (Project ap : activeProjects) {
				if (!ap.getWearables().isEmpty()) {
					for (Wearable w : ap.getWearables()) {
						if (w.isConnected() && w.isFitbit()) {
							wearableList.add(w);
						}
					}
				}
			}

			// refresh wearables in list
			if (!wearableList.isEmpty()) {
				logger.info(" - Refreshing " + wearableList.size() + " Fitbit wearable(s)");
				for (Wearable w : wearableList) {
					refreshAndFetchWearable(w);
				}
			}

		} catch (Exception e) {
			logger.error("Error Fitbit refresh.", e);
			Slack.call("Exception of Fitbit daily routine. ", e.getLocalizedMessage());
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get authorization and tokens from FITBIT server
	 * 
	 * @return
	 */
	public boolean authorizationRequest(Wearable wearable, String authorizationCode, String redirectUrl) {

		// compose authorization token request
		try {
			WSResponse response = ws.url(OAUTH_TOKEN_URL).addHeader("Authorization", getAuthorizationValue())
					.setRequestTimeout(Duration.ofSeconds(10)).setContentType("application/x-www-form-urlencoded")
					.post("grant_type=authorization_code&redirect_uri=" + redirectUrl + "&code=" + authorizationCode)
					.toCompletableFuture().get();

			// retrieve and save the request parameters
			JsonNode jn = response.asJson();

			// check whether all required fields are provided
			if (!jn.has("access_token") || !jn.has("refresh_token") || !jn.has("user_id")) {
				logger.error("Fitbit authorization failed: fields missing. " + jn.toPrettyString());
				return false;
			}

			// access_token
			wearable.setApiToken(jn.get("access_token").asText());

			// refresh_token
			wearable.setApiKey(jn.get("refresh_token").asText());

			// user_id
			wearable.setUserId(jn.get("user_id").asText());

			// check all and save
			wearable.update();

			LabNotesEntry.log(Wearable.class, LabNotesEntryType.CONFIGURE, "Wearable connected: " + wearable.getName(),
					wearable.getProject(), Dataset.find.byId(wearable.getDatasetId()));
			showFitbitInfo(wearable, "fitbit wearable setup");

			return true;

		} catch (Exception e) {
			logger.error("Exception in authorizationTokenRequest", e);
			Slack.call("Exception: Authorization fail by wearable: " + wearable.getRefId() + ". ",
					e.getLocalizedMessage());
		} finally {
			// refreshAndFetchWearable(wearable);
			// refresh();
		}

		return false;
	}

	/**
	 * refresh authorization and tokens from FITBIT server
	 * 
	 * @return
	 */
	private boolean refreshTokenRequest(Wearable wearable) {
		try {
			// compose refresh token request
			WSResponse response = ws.url(OAUTH_TOKEN_URL).addHeader("Authorization", getAuthorizationValue())
					.setRequestTimeout(Duration.ofSeconds(10)).setContentType("application/x-www-form-urlencoded")
					.post("refresh_token=" + wearable.getApiKey() + "&grant_type=refresh_token").toCompletableFuture()
					.get();

			// retrieve and save the request parameters
			JsonNode jn = response.asJson();

			if (!jn.has("access_token") || !jn.has("refresh_token")) {
				return false;
			}

			// access_token
			wearable.setApiToken(jn.get("access_token").asText());

			// refresh_token
			wearable.setApiKey(jn.get("refresh_token").asText());

			// check all and save
			wearable.update();

			return true;
		} catch (Exception e) {
			logger.error("Exception in refreshTokenRequest", e);
			// Slack.call("Exception: Refreshing tokens of Fitbit wearable: " + wearable.id + ".");
			return false;
		}
	}

	/**
	 * generate the authorization value
	 * 
	 * @return
	 */
	private final String getAuthorizationValue() {
		return "Basic " + Base64.getEncoder().encodeToString((APP_CLIENT_ID + ":" + APP_CLIENT_SECRET).getBytes());
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
	 * For a single wearable, do the following: refresh tokens, and check whether wearable should be fetched data, if
	 * yes, then get the data of yesterday from Fitbit server; if no, then wait or update the expiry.
	 * 
	 * @param wearable
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

			// check settings for the linked dataset and the last sync time for the wearable
			if (wearable.getDatasetId() < 0) {
				// dataset is not set
				logger.info("  -- " + wearable.getRefId() + " is not assigned for dataset.");
				return;
			}

			final Dataset ds = Dataset.find.byId(wearable.getDatasetId());
			if (ds == null) {
				logger.info("  -- " + wearable.getRefId() + ": dataset not found.");
				return;
			}

			final long syncTime = getSyncTime(wearable);
			if (syncTime <= -1l) {
				// empty Fitbit account?
				logger.info(
						"  -- Fail to get sync time, maybe " + wearable.getRefId() + " is not connected to end-user.");
				// return;
			} else if (wearable.getExpiry() == ds.start().getTime() && wearable.getExpiry() > today) {
				// no fetching, as wearable is waiting for next dataset starts
				logger.info("  -- " + wearable.getRefId() + " is ready for the dataset starting.");
			} else if (wearable.getExpiry() > ds.end().getTime()) {
				// current dataset is finished, look for following dataset
				List<Dataset> nextDSList = wearable.getProject().getNextDSList(ds);
				if (nextDSList.size() <= 1) {
					// no fetching, the dataset is finished, and there is no next dataset for the wearable, so only
					// update expiry and refresh tokens
					wearable.setExpiry(wearable.getExpiry() + 86400000l);
					wearable.update();
					logger.info("  -- " + wearable.getRefId() + " is updated and idle now. No following dataset.");
				} else {
					// move to next wearable dataset
					Dataset nextDS = nextDSList.get(1);
					wearable.setExpiry(nextDS.getStart().getTime());
					wearable.setScopes(nextDS.getId().toString());
					wearable.update();
					logger.info("  -- " + wearable.getRefId() + " is assigned for new dataset now.");

					// if new dataset is active, fetch data, otherwise pass
					if (nextDS.isActive()) {
						doFetch(wearable, (syncTime - 86400000l) >= today ? today : (syncTime - 86400000l));
						logger.info("  -- " + wearable.getRefId() + " is synced.");
					}
				}
			} else if (wearable.getExpiry() > syncTime) {
				// no fetching, as wearable is not synced
				logger.info("  -- " + wearable.getRefId() + " is not synced by end-user.");
			} else {
				if (wearable.getExpiry() >= 0) {
					doFetch(wearable, (syncTime - 86400000l) >= today ? today : (syncTime - 86400000l));
				}
				logger.info("  -- " + wearable.getRefId() + " is synced.");
			}
		} catch (Exception e) {
			logger.error("Exception in FitBitService: refreshAndFetchWearable", e);
		}
	}

	/**
	 * Let's suppose people won't miss sync over 13 days. (at most 11 scopes/requests per day) try to fetch data of a
	 * wearable and update both expiry and scopes if necessary
	 * 
	 * @param wearable
	 * @param timeToFetch
	 */
	private void doFetch(Wearable wearable, long timeToFetch) {
		List<Dataset> dsList = wearable.getProject().getNextDSList(wearable.getDatasetId());
		int calls = 0;

		for (int i = 0; i < dsList.size(); i++) {
			Dataset ds = Dataset.find.byId(dsList.get(i).getId());

			if (ds.isActive(wearable.getExpiry()) && wearable.getExpiry() <= timeToFetch
					&& timeToFetch >= DateUtils.getMillis(ds.startDate(), "00:00:00")) {
				// final long targetDate = timeToFetch > DatasetUtils.getMillis(ds.endDate(), "00:00:00")
				// ? DatasetUtils.getMillis(ds.endDate(), "00:00:00")
				// : timeToFetch;
				final long targetDate = timeToFetch > DateUtils.startOfDay(ds.getEnd()).getTime()
						? DateUtils.startOfDay(ds.getEnd()).getTime()
						: timeToFetch;

				logger.info("  -- Starting to fetch for Fitbit wearable " + wearable.getRefId() + " for dataset "
						+ ds.getId());

				final FitbitDS fbds = (FitbitDS) datasetConnector.getDatasetDS(ds.getId());
				final int tmpCalls = calls;

				try {
					Long[] rs = null;
					{
						long interCalls = tmpCalls;
						for (long missingDate = wearable
								.getExpiry(); missingDate <= targetDate; missingDate += 86400000l) {
							if (!fbds.hasRecord(wearable, missingDate)) {
								final String requestDate = DateUtils.getDateFromMillis(missingDate);
								logger.info("  --- " + requestDate);
								fetchByWearable(wearable, requestDate, ds, fbds);
								interCalls++;
							}

							if (interCalls >= 13) {
								// restriction of amount of calls per hour for Fitbit API server is 150
								rs = new Long[] { missingDate, interCalls };
								break;
							}
						}
						// calls = interCalls;
						if (rs == null) {
							rs = new Long[] { targetDate, interCalls };
						}
					}

					long returnDate = rs[0];
					long webCalls = rs[1];
					// if (webCalls >= 13 || returnDate < DatasetUtils.getMillis(ds.endDate(), "00:00:00")) {
					if (webCalls >= 13 || returnDate < DateUtils.startOfDay(ds.getEnd()).getTime()) {
						// still in this ds
						wearable.setExpiry(returnDate + 86400000l);
						calls += webCalls;
					} else {
						// check for next dataset
						if (dsList.size() > i + 1) {
							// if exists then apply its start date for wearable expiry
							// wearable.expiry = DatasetUtils.getMillis(dsList.get(i + 1).startDate(), "00:00:00");
							wearable.setExpiry(DateUtils.startOfDay(dsList.get(i + 1).getStart()).getTime());
							wearable.setScopes(Long.toString(dsList.get(i + 1).getId()));
						} else {
							// set the next day
							wearable.setExpiry(returnDate + 86400000l);
						}
					}
					wearable.update();
				} catch (Exception e) {
					logger.error("Exception as fetching Fitbit data. ", e);
				}
			}

			if (calls >= 13) {
				// if continue, it will break the limitation of request number form Fitbit per hour
				logger.warn("Too many calls to Fitbit API for Fitbit wearable " + wearable.getRefId() + ".");
				break;
			}
		}

	}

	/**
	 * fetch data of the date by required scopes
	 * 
	 * @param w
	 * @param date
	 * @param ds
	 * @param fbds
	 */
	private void fetchByWearable(Wearable w, String date, Dataset ds, FitbitDS fbds) {
		// check weight scopes
		List<String> otherScopes = new LinkedList<String>();
		List<String> weightScopes = new LinkedList<String>();

		String[] scopes = ds.getTargetObject().split(" ");
		for (String scope : scopes) {
			switch (scope) {
			case "weight":
			case "fat":
			case "bmi":
				weightScopes.add(scope);
				break;
			default:
				otherScopes.add(scope);
			}
		}

		// fetch other scopes data first if available
		for (int i = 0; i < otherScopes.size(); i++) {
			dataFetchRequest(w, otherScopes.get(i), date, i == 0, fbds, false, null);
		}

		// fetch weight scopes data if available, only one request is required
		if (weightScopes.size() > 0) {
			dataFetchRequest(w, weightScopes.get(0), date, otherScopes.size() <= 0, fbds, true, weightScopes);
		}
	}

	/**
	 * get data from FITBIT server, and deal with weight scopes in one request
	 * 
	 * @param wearable
	 * @param scope
	 * @param date     : ex. "2019-10-11"
	 * @return
	 */
	private boolean dataFetchRequest(Wearable wearable, String scope, String date, boolean firstScope, FitbitDS fbds,
			boolean isWeightScope, List<String> weightScopes) {
		// check scope first
		if (!DATA_URL.containsKey(scope)) {
			logger.info("Fail to fetch, no such scope: " + scope + ".");
			return false;
		}

		logger.info("  -- " + wearable.getRefId() + " Fetching " + scope);

		String getDataUrl = API_URL + DATA_URL.get(scope);
		getDataUrl = getDataUrl.replace("[user-id]", wearable.getUserId());
		getDataUrl = getDataUrl.replace("[date]", date);

		try {
			// retrieve data
			WSResponse response = ws.url(getDataUrl).addHeader("accept", "application/json")
					.addHeader("Authorization", getAuthorizationValue(wearable.getApiToken())).setFollowRedirects(true)
					.setRequestTimeout(Duration.ofSeconds(10)).get().toCompletableFuture().get();

			// parse data as JsonNode
			List<JsonNode> dataList = getDataList(scope, response.asJson());

			// check data
			if (dataList != null && !dataList.isEmpty()) {
				// pass jsonNode object of data and store
				storeData(fbds, wearable, scope, dataList, date, firstScope);

				// handle last weight scopes
				if (isWeightScope && weightScopes != null && weightScopes.size() > 1) {
					firstScope = false;
					for (int i = 1; i < weightScopes.size(); i++) {
						storeData(fbds, wearable, weightScopes.get(i), dataList, date, firstScope);
					}
				}
				return true;
			} else {
				// store empty data
				storeEmptyData(fbds, wearable, scope, date, firstScope);

				// store empty data of last weight scopes
				if (isWeightScope && weightScopes != null && weightScopes.size() > 1) {
					firstScope = false;
					for (int i = 1; i < weightScopes.size(); i++) {
						storeEmptyData(fbds, wearable, weightScopes.get(i), date, firstScope);
					}
				}
				return false;
			}
		} catch (Exception e) {
			logger.error("Exception in dataFetchRequest", e);
			// Slack.call("Exception: Fetching data from Fitbit wearable: " + wearable.id + ". ",
			// e.getLocalizedMessage());
		}

		return false;
	}

	/**
	 * send data to database, if the scope is the first one of the wearable, then use addRecord(), if not, then use
	 * updateRecord()
	 *
	 * @param fbds
	 * @param wearable
	 * @param scope
	 * @param jn
	 * @param date
	 * @param firstScope
	 */
	private void storeData(FitbitDS fbds, Wearable wearable, String scope, List<JsonNode> jn, String date,
			boolean firstScope) {

		logger.info("  -- " + wearable.getRefId() + " storing data for " + date);

		// if (!fbds.hasRecord(wearable, DatasetUtils.getMillis(date, "00:00:00"))) {
		long dataDate = 0l;
		switch (scope) {
		case "sleep":
			long dataDateNext = 0l;
			dataDate = DateUtils.getMillis(jn.get(0).get("dateTime").asText().split(".000")[0].split("T")[0],
					jn.get(0).get("dateTime").asText().split(".000")[0].split("T")[1]);

			// make sure the time is start from the minute with 0 sec
			dataDate = setByMinute(dataDate);

			// write data into database except the last one node
			if (firstScope) {
				for (int i = 0; i < (jn.size() - 1); i++) {
					dataDateNext = DateUtils.getMillis(
							jn.get(i + 1).get("dateTime").asText().split(".000")[0].split("T")[0],
							jn.get(i + 1).get("dateTime").asText().split(".000")[0].split("T")[1]);

					dataDateNext = setByMinute(dataDateNext);

					while (dataDate < dataDateNext) {
						fbds.addRecord(wearable, scope, jn.get(i), dataDate);
						dataDate += 60000l;
					}
					dataDate = dataDateNext;
				}

				// deal with the last one node
				dataDateNext = setByMinute(dataDate + jn.get(jn.size() - 1).get("seconds").asLong() * 1000);
				dataDateNext = dataDateNext % 60000l != 0 ? dataDateNext + (60000l - dataDateNext % 60000l)
						: dataDateNext;

				while (dataDate <= dataDateNext) {
					fbds.addRecord(wearable, scope, jn.get(jn.size() - 1), dataDate);
					dataDate += 60000l;
				}
			} else {
				for (int i = 0; i < (jn.size() - 1); i++) {
					dataDateNext = DateUtils.getMillis(
							jn.get(i + 1).get("dateTime").asText().split(".000")[0].split("T")[0],
							jn.get(i + 1).get("dateTime").asText().split(".000")[0].split("T")[1]);

					dataDateNext = setByMinute(dataDateNext);

					while (dataDate < dataDateNext) {
						fbds.updateRecord(wearable, scope, jn.get(i), dataDate);
						dataDate += 60000l;
					}
					dataDate = dataDateNext;
				}

				// deal with the last one node
				dataDateNext = setByMinute(dataDate + jn.get(jn.size() - 1).get("seconds").asLong() * 1000);
				dataDateNext = dataDateNext % 60000l != 0 ? dataDateNext + (60000l - dataDateNext % 60000l)
						: dataDateNext;

				while (dataDate <= dataDateNext) {
					fbds.updateRecord(wearable, scope, jn.get(jn.size() - 1), dataDate);
					dataDate += 60000l;
				}
			}

			break;
		case "heartrate":
			if (firstScope) {
				for (int i = 0; i < jn.size(); i++) {
					dataDate = DateUtils.getMillis(date, jn.get(i).get("time").asText());
					fbds.addRecord(wearable, scope, jn.get(i), dataDate);
					dataDate += 60000l;
				}
			} else {
				for (int i = 0; i < jn.size(); i++) {
					dataDate = DateUtils.getMillis(date, jn.get(i).get("time").asText());
					fbds.updateRecord(wearable, scope, jn.get(i), dataDate);
					dataDate += 60000l;
				}
			}
			break;
		case "calories":
		case "distance":
		case "elevation":
		case "floors":
		case "steps":
			// always starts from 00:00:00, and fixed difference by +60000l from next record
			dataDate = DateUtils.getMillis(date, "00:00:00");
			if (firstScope) {
				for (int i = 0; i < jn.size(); i++) {
					fbds.addRecord(wearable, scope, jn.get(i), dataDate);
					dataDate += 60000l;
				}
			} else {
				for (int i = 0; i < jn.size(); i++) {
					fbds.updateRecord(wearable, scope, jn.get(i), dataDate);
					dataDate += 60000l;
				}
			}
			break;
		case "weight":
		case "bmi":
		case "fat":
			dataDate = setByMinute(DateUtils.getMillis(date, jn.get(0).get("time").asText()));
			if (firstScope) {
				fbds.addRecord(wearable, scope, jn.get(0), setByMinute(dataDate));
			} else {
				fbds.updateRecord(wearable, scope, jn.get(0), setByMinute(dataDate));
			}
			break;
		case "activity":
			dataDate = DateUtils.getMillis(date, "00:00:00");
			if (firstScope) {
				fbds.addRecord(wearable, scope, jn.get(0), dataDate);
			} else {
				fbds.updateRecord(wearable, scope, jn.get(0), dataDate);
			}
		}
	}

	private void storeEmptyData(FitbitDS fbds, Wearable wearable, String scope, String date, boolean firstScope) {
		// long dataDate = DatasetUtils.getMillis(date, "00:00:00");
		long dataDate = DateUtils.parseDate(date).getTime();

		if (firstScope) {
			fbds.addEmptyRecord(wearable, scope, "-1", setByMinute(dataDate));
		} else {
			fbds.updateEmptyRecord(wearable, scope, "-1", setByMinute(dataDate));
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check the synchronization status of wearable, then return the time of the last updated for the wearable in
	 * milliseconds
	 * 
	 * @param wearable
	 * @param scope
	 * @param date
	 * @return
	 */
	private long getSyncTime(Wearable wearable) {

		logger.info("  -- " + wearable.getRefId() + " - getting sync time");

		String getDataUrl = API_URL + DATA_URL.get("device");
		getDataUrl = getDataUrl.replace("[user-id]", wearable.getUserId());

		try {
			// compose data fetch URL
			WSResponse response = ws.url(getDataUrl)
					.addHeader("Authorization", getAuthorizationValue(wearable.getApiToken())).setFollowRedirects(true)
					.setRequestTimeout(Duration.ofSeconds(10)).get().toCompletableFuture().get();

			// TODO Eden: what should happen here, if there is not lastSyncTime? Just return -1L or do something else?
			// -> basically, there should be always some value in "lastSyncTime", I think it would make sense that the
			// the first sync would be automatically executed as conneting the mobile app to a new Fitbit wearable.
			// And, check again is also good.
			String lastSyncTime = response.asJson().findValues("lastSyncTime").size() > 0
					? response.asJson().get(0).get("lastSyncTime").asText()
					: "";

			long syncedTime = lastSyncTime.trim().length() != 0 ? DateUtils.getMillis(lastSyncTime) : -1l;
			return syncedTime;

		} catch (Exception e) {
			logger.error("Exception in getSyncTime", e);
			// Slack.call("Exception: Fetching data from Fitbit wearable: " + wearable.id + ". ",
			// e.getLocalizedMessage());
			return -1l;
		}

	}

	/**
	 * return Json list of data
	 *
	 * @param scope
	 * @param jn
	 * @return
	 */
	private List<JsonNode> getDataList(String scope, JsonNode jn) {
		if (!DATA_URL.containsKey(scope)) {
			return null;
		}

		try {
			List<JsonNode> dataList = null;

			switch (scope) {
			case "sleep":
				if (!isNullJsonNode(jn.get("sleep")) && jn.get("sleep").findValues("dateTime").size() > 0) {
					dataList = jn.get("sleep").findValue("data").findParents("dateTime");
				}
				break;
			case "weight":
			case "bmi":
			case "fat":
				if (!isNullJsonNode(jn.get("weight")) && jn.get("weight").findValues("weight").size() > 0) {
					dataList = jn.get("weight").findParents("weight");
				}
				break;
			case "activity":
				// not null
				if (!isNullJsonNode(jn.get("activity")) && jn.findValues("summary").size() > 0) {
					dataList = jn.findValues("summary");
				}
				break;
			default:
				// not null
				if (!isNullJsonNode(jn.get(getFieldName(scope)))
						&& jn.get(getFieldName(scope)).findValues("value").size() > 0) {
					dataList = jn.get(getFieldName(scope)).get("dataset").findParents("value");
				}
			}

			return dataList;
		} catch (Exception e) {
			logger.info("exception for getting data list of " + scope + " -- " + e);
			return null;
		}
	}

	/**
	 * return field name of intraday data
	 *
	 * @param scope
	 * @return
	 */
	private String getFieldName(String scope) {
		if (scope.equals("heartrate")) {
			return "activities-heart-intraday";
		} else {
			return "activities-" + scope + "-intraday";
		}
	}

	// /**
	// * try fetch data as register a new wearable
	// *
	// * @param w
	// */
	// private void firstFetch(Wearable w) {
	// if (nnne(w.scopes) && Long.parseLong(w.scopes) >= 0
	// && Dataset.find.byId(Long.parseLong(w.scopes)).isActive()) {
	// CompletableFuture.runAsync(() -> {
	// fetchByWearable(w, LocalDate.now().minusDays(1l).toString(),
	// Dataset.find.byId(Long.parseLong(w.scopes)));
	// });
	// }
	// }

	/**
	 * set corresponding URLs for scopes
	 * 
	 */
	private void setURLs() {
		// not intraday scopes, partial data(not complete data for whole day, 1440 mins)
		////// scope: activity, contains the summary of the activities in one day
		DATA_URL.put("activity", "/1/user/[user-id]/activities/date/[date].json");
		////// scope: activity, with intraday data of complete 1440 mins per day
		DATA_URL.put("calories", "/1/user/[user-id]/activities/calories/date/[date]/1d/1min.json");
		DATA_URL.put("distance", "/1/user/[user-id]/activities/distance/date/[date]/1d/1min.json");
		DATA_URL.put("elevation", "/1/user/[user-id]/activities/elevation/date/[date]/1d/1min.json");
		DATA_URL.put("floors", "/1/user/[user-id]/activities/floors/date/[date]/1d/1min.json");
		DATA_URL.put("steps", "/1/user/[user-id]/activities/steps/date/[date]/1d/1min.json");

		////// scope: sleep, can be stored as intraday format, for the sleep time
		DATA_URL.put("sleep", "/1.2/user/[user-id]/sleep/date/[date].json");

		////// scope: heartrate, wearable would not collect heart rate data while the wearable is taken off
		DATA_URL.put("heartrate", "/1/user/[user-id]/activities/heart/date/[date]/1d/1min.json");

		////// scope: weight
		// weight: the data of the next three scopes is in the same response from Fitbit
		// data for these three scopes can be synced from the app "Health Mate" of Withings with its smart scale
		// how to link Withings account to Fitbit: https://www.fitbit.com/weight/withings (tested)
		// other choices: https://help.fitbit.com/articles/en_US/Help_article/1742
		DATA_URL.put("weight", "/1/user/[user-id]/body/log/weight/date/[date].json");
		DATA_URL.put("fat", "/1/user/[user-id]/body/log/fat/date/[date].json");
		// DATA_URL.put("bmi", "/1/user/[user-id]/body/log/weight/date/[date].json"); // not available anymore

		////// scope: settings, fetching the time of the last synchronization by wearable user, required
		DATA_URL.put("device", "/1/user/[user-id]/devices.json");
	}

	/**
	 * log wearable info in terminal, mainly for debugging purposes
	 * 
	 * @param fitbit
	 * @param info
	 */
	private void showFitbitInfo(Wearable fitbit, String info) {
		logger.info(info);
		fitbit.showInfo();
	}

	/**
	 * check whether the JsonNode object is null or an null node (a JsonNode with null inside)
	 *
	 * @param jn
	 * @return
	 */
	private boolean isNullJsonNode(JsonNode jn) {
		return jn == null || jn.isNull();
	}
}

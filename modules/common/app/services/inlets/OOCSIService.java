package services.inlets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.ds.EntityDS;
import models.ds.TimeseriesDS;
import models.sr.Device;
import nl.tue.id.oocsi.OOCSICommunicator;
import nl.tue.id.oocsi.OOCSIData;
import nl.tue.id.oocsi.OOCSIEvent;
import nl.tue.id.oocsi.client.protocol.EventHandler;
import nl.tue.id.oocsi.client.protocol.Handler;
import nl.tue.id.oocsi.client.protocol.OOCSIMessage;
import nl.tue.id.oocsi.client.services.Responder;
import play.Logger;
import play.libs.Json;
import services.maintenance.RealTimeNotificationService;
import services.slack.Slack;
import utils.flow.EventRateThrottle;
import utils.flow.Throttle;
import utils.oocsi.OOCSIClientUtil;
import utils.validators.Validators;

@Singleton
public class OOCSIService implements ScheduledService {

	// configuration keys for OOCSI functionality
	public static final String OOCSI_CHANNEL = "OOCSI_channel";
	public static final String OOCSI_SERVICE = "OOCSI_service";
	public static final String OOCSI_BACKUP = "OOCSI_backup";

	private static final Logger.ALogger logger = Logger.of(OOCSIService.class);

	private final DatasetConnector datasetConnector;
	private final OOCSICommunicator oocsi;

	// channel inlets
	// note: every dataset can subscribe to 0..1 channels and every channel can be subscribed to by 0..n datasets
	private Map<Long, OOCSIServiceEventHandler> datasetSubscriptionList = new HashMap<Long, OOCSIServiceEventHandler>();

	// service inlets
	private Map<Long, String> datasetServiceList = new HashMap<Long, String>();

	private long lastOOCSIMetricsPing = System.currentTimeMillis();
	private boolean offlineNotification = false;
	private final RealTimeNotificationService realtimeNotifications;

	@Inject
	public OOCSIService(DatasetConnector datasetConnector, OOCSIClientUtil oocsiClientUtil,
	        RealTimeNotificationService realtimeNotifications) {
		this.datasetConnector = datasetConnector;
		this.realtimeNotifications = realtimeNotifications;

		logger.info("OOCSI input service starting");
		oocsi = oocsiClientUtil.createOOCSIClient("DF/OOCSI/inlet");

		// subscribe to OOCSI_metrics channel to receive a ping every second (this works with all OOCSI server variants)
		oocsi.subscribe("OOCSI_metrics", new Handler() {
			@Override
			public void receive(String sender, Map<String, Object> data, long timestamp, String channel,
			        String recipient) {

				// store the timestamp of the last ping
				lastOOCSIMetricsPing = timestamp;
			}
		});
	}

	@Override
	public void refresh() {
		refreshDatasetSubscriptions();
	}

	/**
	 * periodically refresh the OOCSI subscriptions
	 * 
	 */
	private synchronized void refreshDatasetSubscriptions() {

		long start = System.currentTimeMillis();

		// check connection and the last timestamp of the metrics ping, if the latter is longer than 1 min ago, send
		// offline notification once
		if (!oocsi.isConnected() || lastOOCSIMetricsPing < System.currentTimeMillis() - (1000 * 60)) {
			if (!offlineNotification) {
				logger.info("OOCSI inlet connection broken");
				Slack.call("OOCSI service", "OOCSI service inlet connection is offline.");
			}
			offlineNotification = true;
		}
		// if everything is back online, send online notification once
		else {
			if (offlineNotification) {
				logger.info("OOCSI inlet connection back online");
				Slack.call("OOCSI service", "OOCSI service inlet connection is back online.");
			}
			offlineNotification = false;
		}

		try {
			// refresh channels from IoT and Entity datasets
			List<Dataset> oocsiIoTDatasets = Dataset.find.query().where().eq("dsType", DatasetType.IOT).findList()
			        .stream().filter(ds -> ds.isActive()).collect(Collectors.toList());
			refreshChannels(oocsiIoTDatasets);
			List<Dataset> oocsiEntityDatasets = Dataset.find.query().where().eq("dsType", DatasetType.ENTITY).findList()
			        .stream().filter(ds -> ds.isActive()).collect(Collectors.toList());
			refreshChannels(oocsiEntityDatasets);

			// report if this takes too long
			if (System.currentTimeMillis() - start > 1000) {
				logger.info("Refresh tasks [" + (System.currentTimeMillis() - start) + "ms]");
			}
		} catch (Exception e) {
			logger.error("Refresh task exception [" + (System.currentTimeMillis() - start) + "ms]", e);
		}
	}

	/**
	 * periodically refresh selected datasets with OOCSI subscriptions or registered services
	 * 
	 * @param oocsiDatasets
	 */
	private void refreshChannels(List<Dataset> oocsiDatasets) {
		for (Dataset ds : oocsiDatasets) {
			final String channelName;
			if (ds.getDsType() == DatasetType.IOT) {
				// channel subscription
				channelName = ds.configuration(OOCSI_CHANNEL, "");
				if (!channelName.isEmpty() || datasetSubscriptionList.containsKey(ds.getId())) {
					refreshDatasetSubscription(ds, channelName);
				}
			} else if (ds.getDsType() == DatasetType.ENTITY) {
				// channel subscription
				channelName = ds.configuration(OOCSI_BACKUP, "");
				if (!channelName.isEmpty()) {
					// filter out "busy" channel names
					if (Validators.get(OOCSI_BACKUP).validate(channelName)) {
						refreshDatasetSubscription(ds, channelName);
					}
				}

				// service registration
				String serviceName = ds.configuration(OOCSI_SERVICE, "");
				if (!serviceName.isEmpty()) {
					refreshDatasetService(ds, serviceName);
				}
			}
		}
	}

	/**
	 * add an OOCSI subscription for the given dataset and channelName
	 * 
	 * @param ds
	 * @param channelName
	 */
	private void refreshDatasetSubscription(Dataset ds, String channelName) {
		channelName = channelName.trim();

		// existing subscription
		if (datasetSubscriptionList.containsKey(ds.getId())) {
			OOCSIServiceEventHandler subscriptionHandler = datasetSubscriptionList.get(ds.getId());

			// only update on change
			if (!subscriptionHandler.channelName.equals(channelName)) {
				// unsubscribe first
				oocsi.unsubscribe(subscriptionHandler.channelName, subscriptionHandler);
				logger.info("Unsubscribing dataset " + ds.getId() + " from " + subscriptionHandler.channelName);
				datasetSubscriptionList.remove(ds.getId());

				// subscribe then, if channel given
				if (!channelName.isEmpty()) {
					subscribe(channelName, ds);
				}
			}
		}
		// no existing subscription
		else {
			subscribe(channelName, ds);
		}
	}

	/**
	 * subscribe this dataset to an OOCSI channel; will overwrite any prior subscriptions if existing (should not!)
	 * 
	 * @param channelName
	 * @param ds
	 */
	private void subscribe(String channelName, Dataset ds) {
		OOCSIServiceEventHandler oocsiServiceEventHandler = new OOCSIServiceEventHandler(ds, channelName);
		datasetSubscriptionList.put(ds.getId(), oocsiServiceEventHandler);
		logger.info("Subscribing dataset " + ds.getId() + " to " + channelName + " for " + ds.getDsType().name());
		oocsi.subscribe(channelName, oocsiServiceEventHandler);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * add an OOCSI service registration for the given dataset and channelName
	 * 
	 * @param ds
	 * @param channelName
	 */
	private void refreshDatasetService(Dataset ds, String channelName) {
		channelName = channelName.trim();

		// existing subscription
		if (datasetServiceList.containsKey(ds.getId())) {
			String subscription = datasetServiceList.get(ds.getId());
			// only update on change
			if (!subscription.equals(channelName)) {
				// unregister first
				unregister(subscription, ds);

				// register then
				register(channelName, ds);
			}
		}
		// no existing service registration
		else {
			register(channelName, ds);
		}
	}

	private void register(String channelName, Dataset ds) {
		datasetServiceList.put(ds.getId(), channelName);
		logger.info("OOCSI registering service for " + channelName + ", Entity dataset " + ds.getId());
		oocsi.register(channelName, new OOCSIServiceResponder(ds));
	}

	private void unregister(String channelName, Dataset ds) {
		datasetServiceList.remove(ds.getId());

		if (datasetServiceList.values().contains(channelName)) {
			// do not unsubscribe from channel, because other dataset are still subscribed
			return;
		}

		// only unsubscribe if no other dataset depends on this channel
		oocsi.unsubscribe(channelName);
		logger.info("OOCSI unregistering service " + channelName);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void stop() {
		for (OOCSIServiceEventHandler subscriptionHandler : datasetSubscriptionList.values()) {
			oocsi.unsubscribe(subscriptionHandler.channelName, subscriptionHandler);
		}
		for (String channel : datasetServiceList.values()) {
			oocsi.unsubscribe(channel);
		}
		oocsi.disconnect();
		datasetSubscriptionList.clear();
		datasetServiceList.clear();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public OOCSIDiagnostics getDiagnostics(long datasetId) {
		if (datasetSubscriptionList.containsKey(datasetId)) {
			OOCSIServiceEventHandler subscriptionHandler = datasetSubscriptionList.get(datasetId);
			OOCSIDiagnostics idi = subscriptionHandler.diagnostics;
			return idi;
		}
		return new OOCSIDiagnostics();
	}

	static public class OOCSIDiagnostics {
		public String lastSuccessfulEvent = "";

		public String eventInletThrottleMessage = "";
		public String eventInletLoopDetectedMessage = "";
		public String eventInletDeviceIdMessage = "";
		public String eventInletErrorMessage = "";
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * event handler for incoming messages from OOCSI network for an IoT dataset
	 * 
	 */
	class OOCSIServiceEventHandler extends EventHandler {

		private final List<String> EXCLUSION = new ArrayList<String>(Arrays.asList("resource_id", "token", "device_id",
		        "activity", "api-token", "_MESSAGE_ID", "_MESSAGE_HANDLE", "query", "timestamp"));

		private final Long datasetId;
		private final DatasetType dsType;
		private final String channelName;

		private OOCSIDiagnostics diagnostics = new OOCSIDiagnostics();
		private EventRateThrottle inletThrottle = new EventRateThrottle(500, 10 * 1000);

		public OOCSIServiceEventHandler(Dataset ds, String channelName) {
			this.datasetId = ds.getId();
			this.dsType = ds.getDsType();
			this.channelName = channelName;
		}

		@Override
		public void receive(OOCSIEvent event) {

			// debounce / throttle by 500 ms
			if (inletThrottle.check(event.getTime())) {
				return;
			}

			realtimeNotifications.notifyIncomingData(channelName);

			// check event rate
			float eventRate = inletThrottle.getEventRate();
			if (eventRate > 2) {
				diagnostics.eventInletThrottleMessage = "Events are arriving too fast (" + eventRate
				        + "/s) from channel '" + channelName + "', we can only store up to 2 per second.";
			} else {
				diagnostics.eventInletThrottleMessage = "";
			}

			// check whether this is coming straight from a DF outlet, if so we abort because we cannot create feedback
			// loops within DF
			if (event.getSender().startsWith("DF/OOCSI/outlet_")) {
				diagnostics.eventInletLoopDetectedMessage = "A feedback loop was detected from a DF OOCSI outlet to this dataset.";
				return;
			} else {
				diagnostics.eventInletLoopDetectedMessage = "";
			}

			if (dsType == DatasetType.IOT) {
				handleTimeseriesEvent(event);
			} else if (dsType == DatasetType.ENTITY) {
				handleEntityEvent(event);
			}
		}

		private void handleTimeseriesEvent(OOCSIEvent event) {
			final Dataset ds = Dataset.find.byId(datasetId);

			// check if dataset is active, only continue if yes
			if (!ds.isActive()) {
				return;
			}

			Device device = null;
			String device_id = event.getString("device_id", "").trim();
			if (device_id.length() == 0) {
				if (!ds.isOpenParticipation()) {
					diagnostics.eventInletDeviceIdMessage = "No device_id found in data and dataset is not 'open participation', cannot store OOCSI event.";
					return;
				} else {
					// just continue and store the textual OOCSI sender name as device name
				}
			} else {
				device = Device.find.query().where().eq("refId", device_id).findOne();
			}

			if (device != null || ds.isOpenParticipation()) {
				// only if device or api key are ok, let's query the dataset
				if (event.has("query")) {
					// final OOCSIMessage msg = oocsi.channel(event.getSender());
					//
					// // retrieve value
					// // check selectors
					// String selectorStr = event.getString("query");
					// if (selectorStr != null && selectorStr.trim().length() > 0) {
					// String[] selectors = selectorStr.split(",");
					// for (String key : selectors) {
					// msg.data(key, jo.get(key).getAsString());
					// }
					// }
					//
					// msg.send();
				} else {
					try {
						// form payload
						final ObjectNode jo = OOCSIClientUtil.event2json(event, EXCLUSION);

						// add data either relating to a device entity...
						final TimeseriesDS ts = datasetConnector.getTypedDatasetDS(ds);
						if (device != null) {
							ts.addRecord(device, event.getTimestamp(), event.getString("activity", ""), jo);
						}
						// ... or to a device name string (OOCSI sender name)
						else {
							ts.addRecord(event.getSender(), event.getTimestamp(), event.getString("activity", ""), jo);
						}
						diagnostics.lastSuccessfulEvent = event.getTimestamp() + ": " + jo.toString();
						diagnostics.eventInletDeviceIdMessage = "";
						diagnostics.eventInletErrorMessage = "";
					} catch (Exception e) {
						diagnostics.eventInletErrorMessage = "Unspecified message handling error.";
						logger.error("Error in handling timeseries event.", e);
						Slack.call("Exception", e.getLocalizedMessage());
					}
				}
			} else {
				diagnostics.eventInletDeviceIdMessage = "No matching device found in project and dataset is not 'open participation', cannot store OOCSI event.";
			}
		}

		private void handleEntityEvent(OOCSIEvent event) {
			final Dataset ds = Dataset.find.byId(datasetId);

			if (!ds.isActive() || !ds.isOpenParticipation()) {
				diagnostics.eventInletDeviceIdMessage = "Dataset is not 'open participation', cannot handle OOCSI event.";
				return;
			}

			// only if device or api key are ok, let's query the data set
			final EntityDS es = datasetConnector.getTypedDatasetDS(ds);
			if (event.keys().length == 0) {
				final OOCSIMessage msg = oocsi.channel(channelName);
				ObjectNode on = es.getItem(channelName, Optional.of(channelName)).orElse(Json.newObject());
				OOCSIClientUtil.json2event(on, msg);

				// and go
				msg.send();

				// log to diagnostics
				diagnostics.lastSuccessfulEvent = event.getTimestamp() + ": " + on.toString();
			} else {
				ObjectNode on = OOCSIClientUtil.event2json(event, EXCLUSION);
				// timestamp this change
				on.put("lastUpdated", System.currentTimeMillis());
				es.updateItem(channelName, Optional.of(channelName), on);

				// log to diagnostics
				diagnostics.lastSuccessfulEvent = event.getTimestamp() + ": " + on.toString();
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * event handler for incoming requests from OOCSI network for an ENTITY dataset
	 *
	 */
	class OOCSIServiceResponder extends Responder {

		private final List<String> EXCLUSION = new ArrayList<String>(Arrays.asList("resource_id", "token", "api-token",
		        "_MESSAGE_ID", "_MESSAGE_HANDLE", "query", "timestamp"));

		private final Long datasetId;
		private Throttle inletThrottle = new Throttle(500);

		public OOCSIServiceResponder(Dataset ds) {
			this.datasetId = ds.getId();
		}

		@Override
		public void respond(OOCSIEvent event, OOCSIData response) {

			// debounce / throttle
			if (inletThrottle.check(event.getTime())) {
				return;
			}

			// final JsonObject jo = new JsonObject();
			EntityDS es = (EntityDS) datasetConnector.getDatasetDS(datasetId);

			// check if dataset is active, only continue if yes
			if (!es.isActive()) {
				return;
			}

			// only if device or api key are ok, let's query the data set
			if (event.has("query")) {

				String resource_id = event.getString("resource_id");
				String token = event.getString("token");
				String query = event.getString("query");

				logger.info("Entity " + query + " for " + resource_id);

				// prepare data
				ObjectNode jo = OOCSIClientUtil.event2json(event, EXCLUSION);

				// check query
				ObjectNode result = null;
				final Optional<String> finalToken = Optional.of(token);
				if (query.equals("add")) {
					result = es.addItem(resource_id, finalToken, jo).orElse(Json.newObject());
				} else if (query.equals("update")) {
					result = es.updateItem(resource_id, finalToken, jo).orElse(Json.newObject());
				} else if (query.equals("get")) {
					result = es.getItem(resource_id, finalToken).orElse(Json.newObject());
				} else if (query.equals("delete")) {
					result = es.deleteItem(resource_id, finalToken).orElse(Json.newObject());
				}

				OOCSIClientUtil.json2event(result, response);
			}
		}
	}
}

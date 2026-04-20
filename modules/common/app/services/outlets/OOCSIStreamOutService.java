package services.outlets;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import models.Dataset;
import nl.tue.id.oocsi.OOCSICommunicator;
import nl.tue.id.oocsi.client.protocol.OOCSIMessage;
import play.Logger;
import services.inlets.ScheduledService;
import utils.oocsi.OOCSIClientUtil;

@Singleton
public class OOCSIStreamOutService implements ScheduledService {

	private static final Logger.ALogger logger = Logger.of(OOCSIStreamOutService.class);

	// configuration key for output channel
	public static final String OOCSI_OUTPUT = "OOCSI_output";

	private final OOCSICommunicator oocsi;

	private final Map<Long, Long> throttleMap = new HashMap<>();

	@Inject
	public OOCSIStreamOutService(OOCSIClientUtil oocsiClientUtil) {
		oocsi = oocsiClientUtil.createOOCSIClient("DF/OOCSI/outlet");
		logger.info("OOCSI output service starting");
	}

	@Override
	public void refresh() {
		// not necessary, will be triggered from dataset and does not subscribe to anything
	}

	public synchronized void datasetUpdate(Dataset ds, Map<String, Object> data) {
		datasetUpdate(ds, data, null);
	}

	@Override
	public void stop() {
		oocsi.disconnect();
	}

	/**
	 * ensure that all data is a Java type, not Json something
	 * 
	 * @return
	 */
	public static final Builder<String, Object> map() {
		return new ImmutableMap.Builder<String, Object>() {
			@Override
			public Builder<String, Object> put(String key, Object value) {
				if (value instanceof ObjectNode) {
					return super.put(key, OOCSIClientUtil.json2map((ObjectNode) value));
				} else {
					return super.put(key, value);
				}
			}
		};
	}

	public synchronized void datasetUpdate(Dataset ds, Map<String, Object> data, String rawData) {

		// debounce / throttle by 750 ms
		if (throttleMap.containsKey(ds.getId()) && throttleMap.get(ds.getId()) + 750 > System.currentTimeMillis()) {
			return;
		} else {
			throttleMap.put(ds.getId(), System.currentTimeMillis());
		}

		String channelName = ds.getConfiguration().get(OOCSI_OUTPUT);
		if (channelName == null || channelName.trim().length() == 0) {
			return;
		}

		OOCSIMessage message = oocsi.channel(channelName).data(data);

		// integrate raw data into event
		OOCSIClientUtil.json2event(rawData, message);

		message.send();
	}

}

package services.outlets;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

	private final Map<Long, Long> throttleMap = new ConcurrentHashMap<>();

	private final ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(1024), r -> {
				Thread t = new Thread(r, "df-oocsi-outlet");
				t.setDaemon(true);
				return t;
			}, new ThreadPoolExecutor.DiscardPolicy());

	@Inject
	public OOCSIStreamOutService(OOCSIClientUtil oocsiClientUtil) {
		oocsi = oocsiClientUtil.createOOCSIClient("DF/OOCSI/outlet");
		logger.info("OOCSI output service starting");
	}

	@Override
	public void refresh() {
		// prune stale throttle entries older than 1 hour to prevent indefinite retention
		long now = System.currentTimeMillis();
		throttleMap.entrySet().removeIf(entry -> now - entry.getValue() > 3600_000L);
	}

	public void datasetUpdate(Dataset ds, Map<String, Object> data) {
		datasetUpdate(ds, data, null);
	}

	@Override
	public void stop() {
		try {
			executor.shutdownNow();
		} catch (Exception e) {
			// ignore on shutdown
		}
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

	public void datasetUpdate(Dataset ds, Map<String, Object> data, String rawData) {
		if (ds == null || !oocsi.isConnected()) {
			return;
		}

		String channelName = ds.getConfiguration().get(OOCSI_OUTPUT);
		if (channelName == null || channelName.trim().isEmpty()) {
			return;
		}

		// debounce / throttle by 750 ms per dataset
		long now = System.currentTimeMillis();
		Long last = throttleMap.get(ds.getId());
		if (last != null && now - last < 750) {
			return;
		}
		throttleMap.put(ds.getId(), now);

		try {
			OOCSIMessage message = oocsi.channel(channelName).data(data);

			// integrate raw data into event
			OOCSIClientUtil.json2event(rawData, message);

			executor.execute(() -> {
				try {
					message.send();
				} catch (Exception e) {
					logger.warn("Problem sending OOCSI message on channel " + channelName, e);
				}
			});
		} catch (Exception e) {
			logger.warn("Problem preparing OOCSI message", e);
		}
	}

}

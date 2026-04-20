package utils.oocsi;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pekko.actor.ActorSystem;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import nl.tue.id.oocsi.OOCSICommunicator;
import nl.tue.id.oocsi.OOCSIData;
import nl.tue.id.oocsi.OOCSIEvent;
import play.Environment;
import play.Logger;
import play.core.NamedThreadFactory;
import play.libs.Json;
import utils.conf.ConfigurationUtils;

@Singleton
public class OOCSIClientUtil {

	private static final Logger.ALogger logger = Logger.of(OOCSIClientUtil.class);

	private String oocsiServerAddress = null;

	private final ExecutorService executor;

	private final List<OOCSICommunicator> clientRegistry = new LinkedList<>();

	@Inject
	public OOCSIClientUtil(Config config, Environment environment, ActorSystem actorSystem) {

		// create a single thread pool for all OOCSI clients
		this.executor = Executors.newCachedThreadPool(NamedThreadFactory.apply("OOCSI Clients"));

		// check address in configuration
		if (config.hasPath(ConfigurationUtils.DF_OOCSI_SERVER)
				&& config.getString(ConfigurationUtils.DF_OOCSI_SERVER).length() > 0) {
			oocsiServerAddress = config.getString(ConfigurationUtils.DF_OOCSI_SERVER);
		} else {
			oocsiServerAddress = "";
			logger.error("'" + ConfigurationUtils.DF_OOCSI_SERVER + "' is not defined in configuration.");
		}
	}

	public boolean isOfflineOperation() {
		return oocsiServerAddress.isEmpty();
	}

	public OOCSICommunicator createOOCSIClient(String clientHandle) {

		// create dummy clients if there is no connection to server
		if (isOfflineOperation()) {
			return createOOCSIDummyClient(clientHandle);
		}

		// establish OOCSI communications
		OOCSICommunicator oocsi = createOOCSICommunicator(clientHandle + "__####");
		oocsi.setReconnect(true);
		logger.info("  Connecting to '" + oocsiServerAddress + "'...");
		oocsi.connect(oocsiServerAddress, 4444);

		// add to client registry
		clientRegistry.add(oocsi);

		return oocsi;
	}

	private OOCSICommunicator createOOCSICommunicator(String clientHandle) {
		return new OOCSICommunicator(this, clientHandle, executor) {
			@Override
			public void log(String message) {
				logger.info("[%s] %s (%s)".formatted(clientHandle, message, this.getName()));
			}
		};
	}

	private OOCSICommunicator createOOCSIDummyClient(String clientHandle) {
		return new OOCSICommunicator(this, clientHandle) {
			@Override
			public void log(String message) {
				logger.warn("[%s] %s (%s) -- not connected, not delivered".formatted(clientHandle, message,
						this.getName()));
			}

			@Override
			public boolean connect(String hostname, int port) {
				return true;
			}

			@Override
			public void send(String channelName, Map<String, Object> data) {
			}

			@Override
			public void send(String channelName, String message) {
			}

			@Override
			public void disconnect() {
			}

			@Override
			public boolean isConnected() {
				return true;
			}
		};
	}

	/**
	 * stop all OOCSI clients that were created in this util, and clear the registry
	 * 
	 */
	public void stopAll() {
		clientRegistry.stream().forEach(c -> c.disconnect());
		clientRegistry.clear();
	}

	public static final ObjectNode event2json(OOCSIEvent event, List<String> EXCLUSION) {
		final ObjectNode jo = Json.newObject();

		Arrays.stream(event.keys()).filter(s -> !EXCLUSION.contains(s)).forEach(key -> {
			final Object value = event.getObject(key);
			if (value != null) {
				if (value instanceof Number) {
					Number number = (Number) value;
					jo.put(key, number.floatValue());
				} else if (value instanceof String) {
					String string = (String) value;
					jo.put(key, string);
				} else if (value instanceof Boolean) {
					Boolean booleanVal = (Boolean) value;
					jo.put(key, booleanVal);
				} else {
					jo.put(key, value.toString());
				}
			}
		});
		return jo;
	}

	public static final void json2event(ObjectNode on, OOCSIData response) {
		on.fields().forEachRemaining(entry -> {
			String key = entry.getKey();
			JsonNode value = entry.getValue();
			if (value != null) {
				if (value.isNumber()) {
					response.data(key, value.asDouble());
				} else if (value.isTextual()) {
					response.data(key, value.asText());
				} else if (value.isBoolean()) {
					response.data(key, value.asBoolean());
				} else if (value.isArray()) {
					response.data(key, StreamSupport.stream(((ArrayNode) value).spliterator(), false)
							.collect(Collectors.toList()));
				} else if (value.isObject()) {
					final Map<String, Object> map = new HashMap<>();
					((ObjectNode) value).fields().forEachRemaining(f -> {
						map.put(f.getKey(), f.getValue());
					});
					response.data(key, map);
				} else {
					response.data(key, value.toString());
				}
			}
		});
	}

	public static final void json2event(String json, OOCSIData response) {

		if (json == null) {
			return;
		}

		try {
			JsonNode jn = Json.parse(json);
			if (jn.isObject()) {
				OOCSIClientUtil.json2event(((ObjectNode) jn), response);
			}
		} catch (Exception e) {
			response.data("result", json);
		}
	}

	public static final Map<String, Object> json2map(ObjectNode on) {
		final Map<String, Object> result = new HashMap<>();
		on.fields().forEachRemaining(entry -> {
			String key = entry.getKey();
			JsonNode value = entry.getValue();
			if (value != null) {
				if (value.isNumber()) {
					result.put(key, value.asDouble());
				} else if (value.isTextual()) {
					result.put(key, value.asText());
				} else if (value.isBoolean()) {
					result.put(key, value.asBoolean());
				} else if (value.isArray()) {
					result.put(key, StreamSupport.stream(((ArrayNode) value).spliterator(), false)
							.collect(Collectors.toList()));
				} else if (value.isObject()) {
					final Map<String, Object> map = new HashMap<>();
					((ObjectNode) value).fields().forEachRemaining(f -> {
						map.put(f.getKey(), f.getValue());
					});
					result.put(key, map);
				} else {
					result.put(key, value.toString());
				}
			}
		});

		return result;
	}

}

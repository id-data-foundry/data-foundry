package services.jsexecutor;

import java.util.Arrays;
import java.util.Date;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import datasets.DatasetConnector;
import models.Dataset;
import models.Project;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import models.sr.Device;
import play.libs.Json;
import services.api.js.JSDBApiRequest;
import services.api.js.JSDBApiService;

/**
 * proxy for a referenced IoT dataset in project, allows for get and log operations
 *
 */
public class JSDatasetProxy {

	private final JSDBApiService jsdbApiService;
	private final String datasetId;
	private final long projectId;

	// transient values
	private String filterId = "";
	private long limit = 20;
	private long start = -1;
	private long end = -1;

	private final long ttl;

	public JSDatasetProxy(long dsProjectId, String datasetId, JSDBApiService jsdbApiService, long ttl) {
		this.projectId = dsProjectId;
		this.datasetId = datasetId;
		this.jsdbApiService = jsdbApiService;
		this.ttl = ttl;
	}

	/**
	 * get data in JSON format from the dataset, filterId can refer to participant, device, wearable, or cluster
	 * depending on dataset type, limit is in the range of 1 - 8000 (default 20), start and end as usual
	 * 
	 * @param filterId
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	public String getEventData(String filterId, Long limit, Long start, Long end) {

		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return "[]";
		}

		// normalize limit, start and end values
		final long flimit = Math.min(Optional.ofNullable(limit).orElse(20L), 8000);
		final long fstart = Optional.ofNullable(start).orElse(-1L);
		final long fend = Optional.ofNullable(end).orElse(-1L);

		return dispatchJSDBApiRequest(new JSDBApiRequest(projectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Optional<Dataset> datasetOpt = getProjectDataset(project, datasetId);
				if (datasetOpt.isEmpty()) {
					return "[]";
				}
				return internalGetEventData(project, datasetOpt.get(), datasetConnector, filterId, flimit, fstart, fend)
						.toString();
			}
		});
	}

	/**
	 * get data in JSON format from the dataset, filterId can refer to participant, device, wearable, or cluster
	 * depending on dataset type, limit is in the range of 1 - 8000 (default 20), start and end as usual
	 * 
	 * @param filterId
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	private ArrayNode internalGetEventData(Project project, Dataset dataset, DatasetConnector datasetConnector,
			String filterId, long limit, long start, long end) {

		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return Json.newArray();
		}

		// create filtering cluster
		Cluster cluster = new Cluster("Cluster");
		cluster.setId(-1l);

		switch (dataset.getDsType()) {
		// devices
		case IOT:
		case TIMESERIES:
			project.getDevices().stream()
					.filter(d -> d.getRefId().equals(filterId) || d.getId().toString().equals(filterId))
					.forEach(d -> cluster.getDevices().add(d));
			if (cluster.getDevices().isEmpty() && !(filterId == null || filterId.isEmpty())) {
				// return empty array
				return Json.newArray();
			}
			break;
		// participants
		case ANNOTATION:
		case DIARY:
		case ES:
		case MOVEMENT:
		case MEDIA:
			project.getParticipants().stream()
					.filter(p -> p.getRefId().equals(filterId) || p.getId().toString().equals(filterId))
					.forEach(p -> cluster.getParticipants().add(p));
			if (cluster.getParticipants().isEmpty() && !(filterId == null || filterId.isEmpty())) {
				// return empty array
				return Json.newArray();
			}
			break;
		// wearables
		case FITBIT:
		case GOOGLEFIT:
			project.getWearables().stream()
					.filter(w -> w.getRefId().equals(filterId) || w.getId().toString().equals(filterId))
					.forEach(w -> cluster.getWearables().add(w));
			if (cluster.getWearables().isEmpty() && !(filterId == null || filterId.isEmpty())) {
				// return empty array
				return Json.newArray();
			}
			break;
		default:
			// no filtering for all other dataset types
			break;
		}

		return datasetConnector.getDatasetDS(dataset).retrieveProjected(cluster, limit, start, end);
	}

	/**
	 * log an event data item to an IoT dataset in the project
	 * 
	 * @param deviceId
	 * @param activity
	 * @param data
	 */
	public void logEventData(String deviceId, String activity, String data) {

		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return;
		}

		dispatchJSDBApiRequest(new JSDBApiRequest(projectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Dataset ds = null;
				if (datasetId == null || datasetId.isEmpty()) {
					ds = project.getIoTDataset();
				} else {
					Optional<Dataset> datasetOpt = getProjectDataset(project, datasetId);
					if (datasetOpt.isPresent()) {
						ds = datasetOpt.get();
					}
				}

				if (ds != null) {
					TimeseriesDS tds = datasetConnector.getTypedDatasetDS(ds);
					Optional<Device> device = project.getDevices().stream()
							.filter(d -> d.getRefId().equals(deviceId) || d.getId().toString().equals(deviceId))
							.findFirst();
					if (device.isPresent()) {
						tds.addRecord(device.get(), new Date(), activity, data);
					} else if (ds.isOpenParticipation()) {
						tds.addRecord(deviceId, new Date(), activity, data);
					}
				}

				return null;
			}
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * store the filter values
	 * 
	 * @param filterId
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	public JSDatasetProxy filter(String filterId, Long limit, Long start, Long end) {
		this.filterId = filterId;
		this.limit = Math.min(Optional.ofNullable(limit).orElse(20L), 8000);
		this.start = Optional.ofNullable(start).orElse(-1L);
		this.end = Optional.ofNullable(end).orElse(-1L);

		return this;
	}

	/**
	 * compute stats over the dataset items grouped by given dataset keys
	 * 
	 * @param keys either single string or comma-separated list of strings
	 * @return
	 */
	public String stats(String keys) {

		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return "{}";
		}

		// fail fast if no keys are given or the dataset is null
		if (keys.equals("undefined") /* || dataset == null */) {
			return "{}";
		}

		return internalStats(keys, this.filterId, this.limit, this.start, this.end);
	}

	/**
	 * SENSITIVE function, will submit an API request for protection
	 * 
	 * @param keys
	 * @param filterId
	 * @param limit
	 * @param start
	 * @param end
	 * @return
	 */
	private String internalStats(String keys, String filterId, long limit, long start, long end) {

		// normalize limit, start and end values
		final long flimit = Math.min(Optional.ofNullable(limit).orElse(20L), 8000);
		final long fstart = Optional.ofNullable(start).orElse(-1L);
		final long fend = Optional.ofNullable(end).orElse(-1L);

		return dispatchJSDBApiRequest(new JSDBApiRequest(projectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {

				Optional<Dataset> datasetOpt = getProjectDataset(project, datasetId);
				if (datasetOpt.isEmpty()) {
					return "{}";
				}

				// acquire data
				ArrayNode ar = internalGetEventData(project, datasetOpt.get(), datasetConnector, filterId, flimit,
						fstart, fend);

				// useful for load testing, default is commented out!
				// for (int o = 0; o < 20000; o++) {
				// ObjectNode objectNode = ar.addObject();
				// for (int i = 0; i < 100; i++) {
				// objectNode.put("item" + i, Math.random());
				// }
				// }
				// keys = "item" + (int) (Math.random() * 100) + ",item" + (int) (Math.random() * 100) + ",item"
				// + (int) (Math.random() * 100) + ",item" + (int) (Math.random() * 100);

				// configure data structure
				String[] keyComps = keys.split(",");
				Map<String, ExtDoubleSummaryStatistics> modss = new HashMap<>();
				Arrays.asList(keyComps).stream().forEach(s -> {
					modss.put(s, new ExtDoubleSummaryStatistics());
				});

				// process data
				ar.iterator().forEachRemaining(c -> {
					for (String key : keyComps) {
						JsonNode doubleValue = c.get(key);
						if (doubleValue != null && doubleValue.isNumber()) {
							modss.get(key).accept(doubleValue.asDouble());
						}
					}
				});

				// retrieve results and format response
				ObjectNode result = Json.newObject();
				modss.entrySet().stream().forEach(e -> {
					ExtDoubleSummaryStatistics dss = e.getValue();
					result.set(e.getKey(),
							Json.newObject().put("min", dss.getMin()).put("max", dss.getMax())
									.put("mean", dss.getAverage()).put("sum", dss.getSum()).put("count", dss.getCount())
									.put("var", dss.getSampleVariance()).put("stdev", dss.getSampleStdDev()));
				});
				return result.toString();
			}
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private String dispatchJSDBApiRequest(JSDBApiRequest request) {
		try {
			jsdbApiService.submitApiRequest(request).get(500, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			// do nothing but abort and return default result
		}
		return request.getResult();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	class ExtDoubleSummaryStatistics extends DoubleSummaryStatistics {
		// our online mean estimate
		private double mean = 0.0;
		private double m2 = 0.0;

		@Override
		public void accept(double value) {
			super.accept(value);
			double delta = value - mean;
			mean += delta / this.getCount();
			m2 += delta * (value - mean);
		}

		/**
		 * Returns the online version of the mean which may be less accurate, but won't overflow like the version kept
		 * by {@link #getAverage()}.
		 * 
		 * @return
		 */
		public double getMeanEstimate() {
			return mean;
		}

		public double getSampleVariance() {
			long count = this.getCount();
			return count < 2 ? 0.0 : m2 / (count - 1);
		}

		public double getSampleStdDev() {
			return Math.sqrt(getSampleVariance());
		}
	}
}

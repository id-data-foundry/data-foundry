package services.processing;

import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.inject.Inject;
import javax.inject.Singleton;

import io.ebean.DB;
import io.ebean.SqlRow;
import models.Analytics;
import services.inlets.ScheduledService;
import utils.DateUtils;

@Singleton
public class AnalyticsService implements ScheduledService {

	private final Queue<MetricEntry> metricsQueue;

	public enum MetricsType {
		PROJECT_VIEWS("project_views"), //
		PROJECT_UPDATES("project_updates"), //
		DATASET_DOWNLOADS("project_dataset_downloads"), //
		DATASET_UPDATES("project_dataset_updates"), //
		PARTICIPANT_INTERACTIONS("project_participant_interactions"), //
		SCRIPT_INVOCATIONS("project_script_invocations"), //
		WEBSITE_VIEWS("project_website_views");

		private final String name;

		private MetricsType(String name) {
			this.name = name;
		}

		public boolean equalsName(String n) {
			return name.equals(n);
		}

		public String toString() {
			return this.name;
		}
	}

//	private static final Logger.ALogger logger = Logger.of(AnalyticsService.class);

	@Inject
	public AnalyticsService() {
		// create a queue that will contain the metric elements before offloading them to database
		metricsQueue = new LinkedBlockingQueue<AnalyticsService.MetricEntry>();
	}

	public int getQueueSize() {
		return metricsQueue.size();
	}

	public void incMetric(long projectId, MetricsType metricType) {
		incMetric(projectId, metricType, 1);
	}

	public void incMetric(long projectId, MetricsType metricType, int amount) {
		metricsQueue.offer(new MetricEntry(projectId, metricType, amount));
	}

	public long getMetric(long id, MetricsType metricType) {

		SqlRow row = DB
		        .sqlQuery("SELECT project_id, SUM(" + metricType.toString()
		                + ") as ival FROM analytics WHERE project_id = ? GROUP BY project_id;")
		        .setParameter(1, id).findOne();
		if (row != null) {
			Long count = row.getLong("ival");
			return count != null ? count : 0L;
		}
		return 0L;
	}

	/**
	 * flush all queue elements to database
	 */
	@Override
	public void refresh() {
		int itemCount = metricsQueue.size();

		if (itemCount > 0) {
			// determine the timestamp for just _now_
			Date timestamp = DateUtils.endOfDay(new Date());

			// Aggregate metrics in memory by project id and metric type
			Map<Long, Map<MetricsType, Integer>> aggregated = new HashMap<>();
			for (int i = 0; i < itemCount && !metricsQueue.isEmpty(); i++) {
				MetricEntry me = metricsQueue.poll();
				if (me != null) {
					Map<MetricsType, Integer> projectMetrics = aggregated.computeIfAbsent(me.id,
					        k -> new EnumMap<>(MetricsType.class));
					projectMetrics.merge(me.metric, me.increment, Integer::sum);
				}
			}

			// Apply aggregated totals to database in a single round-trip per unique project
			for (Map.Entry<Long, Map<MetricsType, Integer>> entry : aggregated.entrySet()) {
				Long projectId = entry.getKey();
				Map<MetricsType, Integer> metrics = entry.getValue();

				Optional<Analytics> aOpt = Analytics.find.query().where().eq("project_id", projectId)
				        .eq("timestamp", timestamp).findOneOrEmpty();

				// either use version in DB or create new one
				final Analytics a = aOpt.isPresent() ? aOpt.get() : new Analytics(projectId, timestamp);
				for (Map.Entry<MetricsType, Integer> me : metrics.entrySet()) {
					int increment = me.getValue();
					switch (me.getKey()) {
					case PROJECT_VIEWS:
						a.setProjectViews(a.getProjectViews() + increment);
						break;
					case PROJECT_UPDATES:
						a.setProjectUpdates(a.getProjectUpdates() + increment);
						break;
					case DATASET_DOWNLOADS:
						a.setProjectDatasetDownloads(a.getProjectDatasetDownloads() + increment);
						break;
					case DATASET_UPDATES:
						a.setProjectDatasetUpdates(a.getProjectDatasetUpdates() + increment);
						break;
					case PARTICIPANT_INTERACTIONS:
						a.setProjectParticipantInteractions(a.getProjectParticipantInteractions() + increment);
						break;
					case SCRIPT_INVOCATIONS:
						a.setProjectScriptInvocations(a.getProjectScriptInvocations() + increment);
						break;
					case WEBSITE_VIEWS:
						a.setProjectWebsiteViews(a.getProjectWebsiteViews() + increment);
						break;
					default:
						break;
					}
				}
				a.update();
			}
		}

	}

	@Override
	public void stop() {
		// just flush out all retained events, then quit
		refresh();
	}

	class MetricEntry {
		long id;
		MetricsType metric;
		int increment = 1;

		public MetricEntry(long projectId, MetricsType metricType, int amount) {
			this.id = projectId;
			this.metric = metricType;
			this.increment = amount;
		}
	}

}

package services.processing;

import java.util.Date;
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
		long count = row != null ? (long) row.getOrDefault("ival", 0) : 0;

//		Query<Integer> query = Ebean.createQuery(Integer.class);
//		String sql = "SELECT SUM(" + metricType.toString() + ") FROM analytics WHERE project_id = ?";
//		query.setRawSql(RawSqlBuilder.parse(sql).create());
//		query.setParameter(1, id);
//		Integer count = query.findOne();
//		Optional<Analytics> aOpt = Analytics.find.query().select("sum(" + metricType.toString() + ") as project_views")
//		        .where().eq("project_id", id).findOneOrEmpty();
//		if (aOpt.isPresent()) {
//			return aOpt.get().projectViews;
//		} else {
//			return 0;
//		}

		return count;
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

			// find entry in DB or create new one
			for (int i = 0; i < itemCount && !metricsQueue.isEmpty(); i++) {
				MetricEntry me = metricsQueue.poll();
				Optional<Analytics> aOpt = Analytics.find.query().where().eq("project_id", me.id)
				        .eq("timestamp", timestamp).findOneOrEmpty();

				// either use version in DB or create new one
				final Analytics a = aOpt.isPresent() ? aOpt.get() : new Analytics(me.id, timestamp);
				switch (me.metric) {
				case PROJECT_VIEWS:
					a.projectViews += me.increment;
					break;
				case PROJECT_UPDATES:
					a.projectUpdates += me.increment;
					break;
				case DATASET_DOWNLOADS:
					a.projectDatasetDownloads += me.increment;
					break;
				case DATASET_UPDATES:
					a.projectDatasetUpdates += me.increment;
					break;
				case PARTICIPANT_INTERACTIONS:
					a.projectParticipantInteractions += me.increment;
					break;
				case SCRIPT_INVOCATIONS:
					a.projectScriptInvocations += me.increment;
					break;
				case WEBSITE_VIEWS:
					a.projectWebsiteViews += me.increment;
					break;
				default:
					break;
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

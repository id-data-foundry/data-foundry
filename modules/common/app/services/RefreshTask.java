package services;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Cancellable;

import play.Application;
import play.Logger;
import play.api.db.evolutions.ApplicationEvolutions;
import play.cache.SyncCacheApi;
import play.inject.ApplicationLifecycle;
import play.libs.Time.CronExpression;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import services.api.ai.UnmanagedAIApiService;
import services.inlets.FitBitService;
import services.inlets.GoogleFitService;
import services.inlets.OOCSIService;
import services.inlets.ScheduledService;
import services.jsexecutor.JSExecutorService;
import services.maintenance.DatabaseBackupService;
import services.maintenance.ProjectLifecycleService;
import services.outlets.OOCSIStreamOutService;
import services.processing.AnalyticsService;
import services.search.SearchService;
import services.slack.Slack;
import services.telegrambot.TelegramBotService;
import utils.conf.Configurator;

/**
 * 
 * documentation: https://www.playframework.com/documentation/2.7.x/ScheduledTasks
 * 
 */
@Singleton
public class RefreshTask {

	private final ActorSystem actorSystem;
	private final ExecutionContext executionContext;
	private final List<Cancellable> tasks = new ArrayList<Cancellable>();
	private final List<ScheduledService> services = new ArrayList<ScheduledService>();

	private static final Logger.ALogger logger = Logger.of(RefreshTask.class);

	@Inject
	public RefreshTask(Application application, ActorSystem actorSystem, ExecutionContext executionContext,
	        SyncCacheApi cache, ApplicationLifecycle lc, ApplicationEvolutions evolutions, Configurator configurator,
	        OOCSIService oocsiService, OOCSIStreamOutService oocsiOutletService, FitBitService fbService,
	        GoogleFitService gfService, DatabaseBackupService dbBackup, TelegramBotService telegramBotService,
	        JSExecutorService jsExecutorService, ProjectLifecycleService projectLifecycleService,
	        AnalyticsService analytics, SearchService searchService, UnmanagedAIApiService unmanagedAIService) {

		this.actorSystem = actorSystem;
		this.executionContext = executionContext;

		// test cache
		long curTime = System.currentTimeMillis();
		cache.set("cache_test_" + curTime, curTime, 1000);
		if (!cache.get("cache_test_" + curTime).isPresent()
		        || !cache.get("cache_test_" + curTime).get().equals(curTime)) {
			logger.error("Cache seems to be not working.");
		}

		// log application start for production
		if (application.isProd()) {
			Slack.call("System", "Application started");
		}

		lc.addStopHook(new Callable<CompletionStage<?>>() {
			@Override
			public CompletionStage<?> call() throws Exception {
				// log application shutdown for production
				if (application.isProd()) {
					Slack.call("System", "Application shutdown");
				}

//				// debug applications might need a cache shutdown
//				if (application.isDev()) {
//					logger.info("CacheInstance stopped");
//					CacheManager.getInstance().shutdown();
//				}

				return CompletableFuture.runAsync(() -> stop());
			}
		});

		services.add(oocsiService);
		services.add(oocsiOutletService);
		services.add(jsExecutorService);
		services.add(analytics);

		// start Telegram bot service if it's configured properly
		if (configurator.isTelegramAvailable()) {
			services.add(telegramBotService);
		}

		// don't go further if evolutions are not ready yet
		if (!evolutions.upToDate()) {
			return;
		}

		// schedule all refresh tasks
		tasks.add(this.actorSystem.scheduler().scheduleWithFixedDelay(Duration.create(10, TimeUnit.SECONDS), // initialDelay
		        Duration.create(10, TimeUnit.SECONDS), // interval
		        () -> refresh(), this.executionContext));

		logger.info("Refresh task scheduled with " + 10 + " secs period");

		final FiniteDuration ONE_DAY = Duration.create(24, TimeUnit.HOURS);

		// schedule project lifecycle service
		try {
			CronExpression ce = new CronExpression("1 22 22 * * ? *");
			Date nextTime = ce.getNextValidTimeAfter(new Date());
			long intervalMS = nextTime.getTime() - new Date().getTime();
			tasks.add(
			        this.actorSystem.scheduler().scheduleAtFixedRate(Duration.create(intervalMS, TimeUnit.MILLISECONDS),
			                ONE_DAY, () -> projectLifecycleService.refresh(), this.executionContext));
			logger.info("Next project lifecycle check scheduled for " + nextTime);
		} catch (ParseException e) {
			logger.error("Error in initializing the Cron tasks.", e);
		}

		// schedule the Fitbit service if it's configured properly (in the server locale evening)
		if (configurator.isFitbitAvailable()) {
			try {
				CronExpression ce = new CronExpression("1 32 22 * * ? *");
				Date nextTime = ce.getNextValidTimeAfter(new Date());
				long intervalMS = nextTime.getTime() - new Date().getTime();
				tasks.add(this.actorSystem.scheduler().scheduleAtFixedRate(
				        Duration.create(intervalMS, TimeUnit.MILLISECONDS), ONE_DAY, () -> fbService.refresh(),
				        this.executionContext));
				logger.info("Next FitBit sync scheduled for " + nextTime);
			} catch (ParseException e) {
				logger.error("Error in initializing the Cron tasks.", e);
			}
		}

		// schedule the Google fit service (in the server locale evening)
		if (configurator.isGoogleFitAvailable()) {
			try {
				CronExpression ce = new CronExpression("1 42 22 * * ? *");
				Date nextTime = ce.getNextValidTimeAfter(new Date());
				long intervalMS = nextTime.getTime() - new Date().getTime();
				tasks.add(this.actorSystem.scheduler().scheduleAtFixedRate(
				        Duration.create(intervalMS, TimeUnit.MILLISECONDS), ONE_DAY, () -> gfService.refresh(),
				        this.executionContext));
				logger.info("Next GoogleFit sync scheduled for " + nextTime);
			} catch (ParseException e) {
				logger.error("Error in initializing the Cron tasks.", e);
			}
		}

		// schedule database backups (at 01:01:01 in the server locale night)
		try {
			CronExpression ce = new CronExpression("1 1 1 * * ? *");
			Date nextTime = ce.getNextValidTimeAfter(new Date());
			FiniteDuration timeTillNextRun = Duration.create(nextTime.getTime() - System.currentTimeMillis(),
			        TimeUnit.MILLISECONDS);
			tasks.add(this.actorSystem.scheduler().scheduleAtFixedRate(timeTillNextRun, ONE_DAY, () -> {
				dbBackup.refresh();
			}, this.executionContext));
			logger.info("Next database backup scheduled for " + nextTime);
		} catch (ParseException e) {
			logger.error("Error in initializing the Cron tasks.", e);
		}

		// schedule model retrieval from AI backend, first run after 15 seconds, then every 30 minutes
		final FiniteDuration THIRTY_MINUTES = Duration.create(30, TimeUnit.MINUTES);
		tasks.add(this.actorSystem.scheduler().scheduleAtFixedRate(Duration.create(15, TimeUnit.SECONDS),
		        THIRTY_MINUTES, () -> unmanagedAIService.refresh(), this.executionContext));

		// schedule search indexing, first run after 5 seconds, then every 5 minutes
		final FiniteDuration TEN_MINUTES = Duration.create(5, TimeUnit.MINUTES);
		tasks.add(this.actorSystem.scheduler().scheduleAtFixedRate(Duration.create(5, TimeUnit.SECONDS), TEN_MINUTES,
		        () -> searchService.refresh(), this.executionContext));
	}

	private void refresh() {
		synchronized (services) {
			for (ScheduledService service : services) {
				if (service != null) {
					try {
						service.refresh();
					} catch (Exception e) {
						logger.error("Refresh task exception", e);
					}
				}
			}
		}
	}

	private void stop() {
		synchronized (tasks) {
			for (Cancellable task : tasks) {
				if (task != null) {
					task.cancel();
				}
			}
			tasks.clear();
		}

		synchronized (services) {
			for (ScheduledService service : services) {
				if (service != null) {
					service.stop();
				}
			}
			services.clear();
		}

		logger.info("Refresh tasks cancelled, services stopped.");
	}
}

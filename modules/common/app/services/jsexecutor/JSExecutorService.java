package services.jsexecutor;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.ResourceLimits;

import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import play.Logger;
import play.libs.Time.CronExpression;
import services.api.ai.UnmanagedAIApiService;
import services.api.js.JSDBApiService;
import services.api.processing.AudioProcessingApiService;
import services.inlets.ScheduledService;
import services.maintenance.RealTimeNotificationService;
import services.telegrambot.TelegramBotService;
import utils.oocsi.OOCSIClientUtil;

@Singleton
public class JSExecutorService implements ScheduledService {

	private static final Logger.ALogger logger = Logger.of(JSExecutorService.class);

	private final DatasetConnector datasetConnector;
	private final TelegramBotService botService;
	private final JSSandboxFactory sandboxFactory;
	private final OOCSIClientUtil oocsiClientUtil;
	private final UnmanagedAIApiService aiApiService;
	private final AudioProcessingApiService audioProcessing;
	private final JSDBApiService jsdbApiService;
	private final RealTimeNotificationService realtimeNotifications;

	// actors are identified by dataset id
	private Map<Long, JSActor> actors = new HashMap<Long, JSActor>();
	private Map<Long, JSActor> trialActors = new HashMap<Long, JSActor>();
	private Map<Long, String> subscriptions = new HashMap<Long, String>();
	private Map<Long, Long> timerNextRuns = new HashMap<Long, Long>();

	private final ExecutorService EXECUTOR = Executors.newWorkStealingPool();

	@Inject
	public JSExecutorService(DatasetConnector datasetConnector, OOCSIClientUtil oocsiClientFactory,
			TelegramBotService botService, UnmanagedAIApiService aiApiService,
			AudioProcessingApiService audioProcessing, JSDBApiService jsdbApiService,
			RealTimeNotificationService realtimeNotifications) {

		this.datasetConnector = datasetConnector;
		this.oocsiClientUtil = oocsiClientFactory;
		this.botService = botService;
		this.aiApiService = aiApiService;
		this.audioProcessing = audioProcessing;
		this.jsdbApiService = jsdbApiService;
		this.realtimeNotifications = realtimeNotifications;

		// test available engines for JS execution
		boolean tempActivation = false;
		try (Context context = Context.newBuilder("js")
				.resourceLimits(ResourceLimits.newBuilder().statementLimit(10, null).build()).build();) {
			context.eval("js", "1+1");
			// Graal JS is available, continue with Graal JS Engine
			tempActivation = true;
		} catch (Exception e) {
			// Graal JS is not available, continue with Nashorn engine
			tempActivation = false;
		}
		sandboxFactory = new JSSandboxFactory(tempActivation);

		// create an OOCSI client for all outgoing communication
		logger.info("DF scripting service starting with " + (tempActivation ? "Graal sandbox." : "Nashorn sandbox."));

		// initialize Telegram bot service
		this.botService.setJSActorService(this);
	}

	/**
	 * create a new actor and add it into the internal data structure; can be retrieved with <code>getActor()</code>
	 * 
	 * @param ds
	 * @return
	 */
	public JSActor addActor(Dataset ds) {
		JSActor actor = new JSActor(ds, datasetConnector, sandboxFactory, EXECUTOR, oocsiClientUtil, botService,
				aiApiService, audioProcessing, jsdbApiService, realtimeNotifications);
		actors.put(ds.getId(), actor);

		return actor;
	}

	/**
	 * create a new actor and add it into the internal data structure; can be retrieved with <code>getActor()</code>
	 * 
	 * @param ds
	 * @return
	 */
	public JSActor addTrialActor(Dataset ds) {
		JSActor actor = new JSActor(ds, datasetConnector, sandboxFactory, EXECUTOR, oocsiClientUtil, botService,
				aiApiService, audioProcessing, jsdbApiService, realtimeNotifications);
		trialActors.put(ds.getId(), actor);

		return actor;
	}

	private boolean initialized = false;

	@Override
	public void refresh() {
		if (!initialized) {
			initializeSubscriptions();
		}
		checkCronTimers();
	}

	@Override
	public void stop() {
		oocsiClientUtil.stopAll();
		actors.clear();
		subscriptions.clear();
		timerNextRuns.clear();
	}

	/**
	 * initialize active actor datasets on startup
	 */
	public synchronized void initializeSubscriptions() {
		long start = System.currentTimeMillis();
		try {
			List<Dataset> actorDatasets = Dataset.find.query().where().eq("dsType", DatasetType.COMPLETE)
					.eq("collectorType", Dataset.ACTOR).findList().stream()
					.filter(ds -> ds.isActive() && ds.getProject().isActive()).collect(Collectors.toList());

			refreshChannels(actorDatasets);
			initialized = true;
			if (System.currentTimeMillis() - start > 1000) {
				logger.info("Initial JS Actors loaded [" + (System.currentTimeMillis() - start) + "ms]");
			}
		} catch (Exception e) {
			logger.error("Initial JS Actor loading exception", e);
		}
	}

	/**
	 * check and trigger in-memory cron timers without querying the database
	 */
	private void checkCronTimers() {
		long now = System.currentTimeMillis();
		java.util.List<Long> actorIds = new java.util.ArrayList<>(timerNextRuns.keySet());
		for (Long id : actorIds) {
			Long nextRunTime = timerNextRuns.get(id);
			if (nextRunTime != null && now >= nextRunTime) {
				JSActor actor = actors.get(id);
				if (actor != null && subscriptions.containsKey(id)) {
					String channelName = subscriptions.get(id);
					if (channelName != null && channelName.toLowerCase().startsWith("cron:")) {
						String cronExpression = channelName.substring(5).trim();
						try {
							CronExpression ce = new CronExpression(cronExpression);
							JsonObject jo = new JsonObject();
							jo.addProperty("event", "timer");
							jo.addProperty("timestamp", now);
							actor.update(jo);

							Date nextRun = ce.getNextValidTimeAfter(new Date(now));
							if (nextRun != null) {
								timerNextRuns.put(id, nextRun.getTime());
								logger.trace("Next run scheduled for script " + actor.getName() + " at " + nextRun);
							} else {
								timerNextRuns.remove(id);
							}
						} catch (Exception e) {
							logger.error("Error in cron expression for script " + actor.getName(), e);
						}
					}
				}
			}
		}
	}

	/**
	 * update actor datasets based on changed datasets
	 * 
	 * @param changedDatasets
	 * @param changedIds
	 */
	public synchronized void updateDatasets(List<Dataset> changedDatasets, java.util.Set<Long> changedIds) {
		if (!initialized) {
			initializeSubscriptions();
			return;
		}

		java.util.Set<Long> processedIds = new java.util.HashSet<>();

		for (Dataset ds : changedDatasets) {
			processedIds.add(ds.getId());
			if (ds.getDsType() == DatasetType.COMPLETE && Dataset.ACTOR.equals(ds.getCollectorType())) {
				if (ds.isActive() && ds.getProject().isActive()) {
					refreshChannels(java.util.Collections.singletonList(ds));
				} else {
					removeActor(ds.getId());
				}
			} else {
				removeActor(ds.getId());
			}
		}

		for (Long id : changedIds) {
			if (!processedIds.contains(id)) {
				removeActor(id);
			}
		}
	}

	private void removeActor(Long id) {
		JSActor actor = actors.remove(id);
		if (actor != null) {
			logger.info("Removing actor of inactive/removed dataset " + actor.getName());
			unsubscribe(id, actor);
			timerNextRuns.remove(id);
			actor.stop();
		}
	}

	/**
	 * 
	 * @param actorDatasets
	 */
	private void refreshChannels(List<Dataset> actorDatasets) {
		for (Dataset ds : actorDatasets) {

			final String code = ds.getConfiguration().get(Dataset.ACTOR_CODE);

			// first ensure that the actor exists and is initialized with the right code
			if (!actors.containsKey(ds.getId())) {
				// initialize and install a new actor
				JSActor actor = new JSActor(ds, datasetConnector, sandboxFactory, EXECUTOR, oocsiClientUtil, botService,
						aiApiService, audioProcessing, jsdbApiService, realtimeNotifications);
				actors.put(ds.getId(), actor);
				actor.setCode(code, null);
			}

			// get actor
			JSActor actor = actors.get(ds.getId());

			String channelName = ds.configuration(Dataset.ACTOR_CHANNEL, "").trim();
			if (channelName.length() > 0) {

				// check if channel changed
				if (subscriptions.containsKey(ds.getId()) && !subscriptions.get(ds.getId()).equals(channelName)) {
					timerNextRuns.remove(ds.getId());
				}

				// check for cron timer
				if (channelName.toLowerCase().startsWith("cron:")) {
					String cronExpression = channelName.substring(5).trim();
					try {
						CronExpression ce = new CronExpression(cronExpression);
						long now = System.currentTimeMillis();
						if (!timerNextRuns.containsKey(ds.getId())) {
							Date nextRun = ce.getNextValidTimeAfter(new Date(now));
							if (nextRun != null) {
								timerNextRuns.put(ds.getId(), nextRun.getTime());
							}
						} else {
							long nextRunTime = timerNextRuns.get(ds.getId());
							if (now >= nextRunTime) {
								// trigger actor
								JsonObject jo = new JsonObject();
								jo.addProperty("event", "timer");
								jo.addProperty("timestamp", now);
								actor.update(jo);

								// schedule next run
								Date nextRun = ce.getNextValidTimeAfter(new Date(now));
								if (nextRun != null) {
									timerNextRuns.put(ds.getId(), nextRun.getTime());
									logger.trace("Next run scheduled for script " + actor.getName() + " at " + nextRun);
								} else {
									timerNextRuns.remove(ds.getId());
								}
							}
						}
					} catch (Exception e) {
						logger.error("Error in cron expression for script " + actor.getName(), e);
					}
				}

				// existing subscription
				if (subscriptions.containsKey(ds.getId())) {
					if (!subscriptions.get(ds.getId()).equals(channelName)) {
						// unsubscribe first
						unsubscribe(ds.getId(), actor);

						// subscribe then
						subscribe(ds.getId(), actor, channelName);
					} else {
						// leave subscription be
					}
				}
				// no existing subscription
				else {
					subscribe(ds.getId(), actor, channelName);
				}
			} else if (subscriptions.containsKey(ds.getId())) {
				unsubscribe(ds.getId(), actor);
				timerNextRuns.remove(ds.getId());
			}
		}
	}

	private void subscribe(Long id, JSActor actor, String channelName) {
		subscriptions.put(id, channelName);
		actor.subscribe(channelName);
	}

	private void unsubscribe(Long id, JSActor actor) {
		String channelName = subscriptions.remove(id);
		if (channelName == null) {
			logger.error("Tried to remove non-existent subscription " + actor.getName());
			return;
		}

		actor.unsubscribe(channelName);
	}

	public JSActor getActor(Long key) {
		return actors.get(key);
	}

	public JSActor getTrialActor(Long key) {
		return trialActors.get(key);
	}

	public List<JSActor> getSubscribedActors(final String channelName) {
		return subscriptions.entrySet().stream().filter(e -> e.getValue().equals(channelName))
				.map(e -> getActor(e.getKey())).filter(a -> a != null).collect(Collectors.toList());
	}

}

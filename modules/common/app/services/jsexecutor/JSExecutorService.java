package services.jsexecutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.ResourceLimits;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import play.Logger;
import services.api.ai.ManagedAIApiService;
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
	private final ManagedAIApiService managedAiApiService;
	private final AudioProcessingApiService audioProcessing;
	private final JSDBApiService jsdbApiService;
	private final RealTimeNotificationService realtimeNotifications;

	// actors are identified by dataset id
	private Map<Long, JSActor> actors = new HashMap<Long, JSActor>();
	private Map<Long, JSActor> trialActors = new HashMap<Long, JSActor>();
	private Map<Long, String> subscriptions = new HashMap<Long, String>();

	private final ExecutorService EXECUTOR = Executors.newWorkStealingPool();

	@Inject
	public JSExecutorService(DatasetConnector datasetConnector, OOCSIClientUtil oocsiClientFactory,
	        TelegramBotService botService, ManagedAIApiService managedAIApiService,
	        AudioProcessingApiService audioProcessing, JSDBApiService jsdbApiService,
	        RealTimeNotificationService realtimeNotifications) {

		this.datasetConnector = datasetConnector;
		this.oocsiClientUtil = oocsiClientFactory;
		this.botService = botService;
		this.managedAiApiService = managedAIApiService;
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
		        managedAiApiService, audioProcessing, jsdbApiService, realtimeNotifications);
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
		        managedAiApiService, audioProcessing, jsdbApiService, realtimeNotifications);
		trialActors.put(ds.getId(), actor);

		return actor;
	}

	@Override
	public void refresh() {
		refreshDatasetSubscriptions();
	}

	@Override
	public void stop() {
		oocsiClientUtil.stopAll();
		actors.clear();
	}

	private synchronized void refreshDatasetSubscriptions() {

		// refresh channels from IoT datasets
		List<Dataset> actorDatasets = Dataset.find.query().where().eq("dsType", DatasetType.COMPLETE)
		        .eq("collectorType", Dataset.ACTOR).findList().stream()
		        .filter(ds -> ds.isActive() && ds.getProject().isActive()).collect(Collectors.toList());

		List<Long> actorDatasetIds = actorDatasets.stream().map(ds -> ds.getId()).collect(Collectors.toList());

		// disconnect actors that are not active anymore
		List<Long> inactiveActors = actors.keySet().stream().filter(id -> !actorDatasetIds.contains(id))
		        .collect(Collectors.toList());
		inactiveActors.forEach(id -> {
			JSActor actor = actors.get(id);
			if (actor != null) {
				logger.info("Removing actor of inactive dataset " + actor.getName());
				unsubscribe(id, actor);
				actors.remove(id);
				actor.stop();
			}
		});

		// refresh all active actors
		refreshChannels(actorDatasets);
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
				        managedAiApiService, audioProcessing, jsdbApiService, realtimeNotifications);
				actors.put(ds.getId(), actor);
				actor.setCode(code, null);
			}

			// get actor
			JSActor actor = actors.get(ds.getId());

			String channelName = ds.configuration(Dataset.ACTOR_CHANNEL, "").trim();
			if (channelName.length() > 0) {

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

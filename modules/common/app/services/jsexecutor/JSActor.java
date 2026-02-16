package services.jsexecutor;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.script.ScriptException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;

import datasets.DatasetConnector;
import delight.nashornsandbox.NashornSandbox;
import delight.nashornsandbox.exceptions.ScriptCPUAbuseException;
import delight.nashornsandbox.exceptions.ScriptMemoryAbuseException;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.ds.EntityDS;
import models.sr.Participant;
import nl.tue.id.oocsi.OOCSICommunicator;
import nl.tue.id.oocsi.OOCSIEvent;
import nl.tue.id.oocsi.client.protocol.EventHandler;
import play.Logger;
import play.libs.Json;
import services.api.ApiServiceConstants;
import services.api.ai.ManagedAIApiService;
import services.api.js.JSDBApiRequest;
import services.api.js.JSDBApiService;
import services.api.processing.AudioProcessingApiService;
import services.api.remoting.RemoteApiRequest;
import services.maintenance.RealTimeNotificationService;
import services.slack.Slack;
import services.telegrambot.TelegramBotService;
import utils.GenericJSONMapDeserializer;
import utils.oocsi.OOCSIClientUtil;

public class JSActor {

	private static final Logger.ALogger logger = Logger.of(JSActor.class);

	// default timeout for script runs
	private static final int DEFAULT_TIMEOUT = 1000;
	// penalty timeout for scripts that exceed the default timeout
	private static final long DEFAULT_TIMEOUT_SE = DEFAULT_TIMEOUT * 3;
	// execution constraints
	private static final long MAX_CPU = 200;
	private static final long MAX_MEM = 5 * 1024 * 1024;
	private static final int MAX_STATEMENTS = 100;

	private final List<String> EXCLUSION = new ArrayList<String>(
	        Arrays.asList("resource_id", "token", "api-token", "_MESSAGE_ID", "_MESSAGE_HANDLE", "query", "timestamp"));

	private final SimpleDateFormat fileTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private final long dsId;
	private final String dsName;
	private final long dsProjectId;

	private final DatasetConnector datasetConnector;
	private final ExecutorService EXECUTOR;
	private final TelegramBotService botService;
	private final ManagedAIApiService managedAIApiService;
	private final AudioProcessingApiService audioProcessing;
	private final JSDBApiService jsdbApiService;
	private final OOCSIClientUtil oocsiClientUtil;
	private final RealTimeNotificationService realtimeNotifications;
	private final Semaphore compiledExecutionPermission = new Semaphore(1);
	private final Semaphore trialExecutionPermission = new Semaphore(1);
	private final String owner;

	// OOCSI client
	private OOCSICommunicator oocsi = null;
	// OOCSI channel to receive triggers
	private String channelName;

	// code to run
	private String code;

	// time of last run since restart of engine
	private Date lastUpdate = new Date();
	// indicate execution side effects for prolonging the default script timeout by factor 3
	private boolean sideEffects = false;
	// timeout until the next opening of the OOCSI stream for this script / js actor
	private long nextPossibleRun = DEFAULT_TIMEOUT;

	// number of runs since last change of code or restart of engine
	private int runs = 0;

	// queue of message from the installed script execution
	public final Deque<String> messages = new LinkedList<String>();

	// sandbox
	private final JSSandboxFactory sandboxFactory;
	private NashornSandbox sandbox;
	private boolean compiledProgram;

	private NashornSandbox trialSandbox;

	public JSActor(Dataset ds, DatasetConnector datasetConnector, JSSandboxFactory sandboxFactory,
	        ExecutorService executorService, OOCSIClientUtil oocsiClientUtil, TelegramBotService botService,
	        ManagedAIApiService managedAIApiService, AudioProcessingApiService audioProcessing,
	        JSDBApiService jsdbApiService, RealTimeNotificationService realtimeNotifications) {
		this.dsId = ds.getId();
		this.dsName = ds.getName();
		this.dsProjectId = ds.getProject().getId();

		// check info
		if (this.dsId <= 0 || this.dsName == null || this.dsName.isEmpty() || this.dsProjectId <= 0) {
			logger.error("JSActor instantiation problem: dataset or project info inconsistent.");
		}

		this.datasetConnector = datasetConnector;
		this.sandboxFactory = sandboxFactory;
		this.EXECUTOR = executorService;
		this.oocsiClientUtil = oocsiClientUtil;
		this.managedAIApiService = managedAIApiService;
		this.audioProcessing = audioProcessing;
		this.jsdbApiService = jsdbApiService;
		this.realtimeNotifications = realtimeNotifications;
		this.owner = ds.getProject().getOwner().getName();
		this.botService = botService;

		// create sandbox
		this.sandbox = sandboxFactory.createSandbox(executorService, MAX_CPU, MAX_MEM, MAX_STATEMENTS);
	}

	public boolean canRun() {
		return code != null && !code.isEmpty() && getChannelName() != null && !getChannelName().isEmpty();
	}

	public boolean isAlive() {
		return !(this.sandbox.getExecutor().isShutdown() || this.sandbox.getExecutor().isTerminated());
	}

	public String getName() {
		return getDatasetName() + " (" + getDatasetId() + ")";
	}

	private void setChannel(String channelName) {
		this.channelName = channelName;
		runs = 0;
	}

	public synchronized boolean setCode(String code, Person person) {

		// don't do anything if the code is null or unchanged
		if ((this.code == null && code == null) || code.equals(this.code)) {
			return true;
		}

		// acquire all permits to prohibit any concurrent executions
		this.compiledExecutionPermission.drainPermits();

		try {
			// try compiling new code
			final String integratedCode = views.js.tools.actor.code_embedding.render(code).toString();
			this.sandbox.eval(integratedCode);

			// store code, set success flag
			this.code = code;
			this.compiledProgram = true;

			// initialize code and runtime metrics
			this.runs = 0;

			// manage permits
			synchronized (this.compiledExecutionPermission) {
				// drain again to ensure that there is only one permit at most
				this.compiledExecutionPermission.drainPermits();
				// (force) enable script after save, with one permit released
				this.compiledExecutionPermission.release();
			}

			if (person != null) {
				saveCodeSnapshot(code, person);
				logger.info("New script compiled for " + person.getName() + " (" + getDatasetId() + ")");
			} else {
				logger.info("New script compiled for " + owner + " (owner, " + getDatasetId() + ")");
			}
			this.messages.add("--- Script code updated ---");
			return true;
		} catch (ScriptCPUAbuseException | ScriptMemoryAbuseException e) {
			this.code = null;
			this.compiledProgram = false;
			this.messages.offerFirst(e.getLocalizedMessage());
			// expected stuff
			logger.error("Problem in script resources use for " + owner + " (" + getDatasetId() + ")");
			return false;
		} catch (ScriptException e) {
			this.code = null;
			this.compiledProgram = false;
			this.messages.offerFirst(e.getLocalizedMessage());
			// expected stuff
			logger.error("Problem in script for " + owner + " (" + getDatasetId() + ")");
			return false;
		} catch (Exception e) {
			this.code = null;
			compiledProgram = false;
			// really unexpected stuff is sent to Slack
			Slack.call("Script problem (setCode)", e.getLocalizedMessage());
			logger.error("Unexpected problem (setCode) in script for " + owner + " (" + getDatasetId() + "): "
			        + e.getLocalizedMessage());
			return false;
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private synchronized void saveCodeSnapshot(String code, Person person) {

		Date now = new Date();
		String filename = fileTimestamp.format(now) + "_" + "snapshot.txt";

		// write the output messages to the log file
		Dataset ds = Dataset.find.byId(getDatasetId());
		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		Optional<File> codeFileOpt = cpds.getFileTemp(filename);

		// write to log file, try again for good measure
		try (FileWriter fw = new FileWriter(codeFileOpt.get())) {
			fw.write("// code snapshot at " + fileTimestamp.format(now) + " saved by " + person.getName() + "\n\n");
			fw.write(code);
		} catch (Exception e) {
			// do nothing
		}

		// create log file
		cpds.addRecord(filename, "code snapshot saved by " + person.getName(), now);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * update the actor with an event from OOCSI
	 * 
	 * @param event
	 */
	public synchronized void update(OOCSIEvent event) {
		// make an exception for events coming from the "http-web-request" client
		if (event.getSender().equals("http-web-request")) {
			// exception: don't check throttling, just run the update below
		} else {
			// check timeout
			if (System.currentTimeMillis() < nextPossibleRun || !canRun()) {
				return;
			}
		}

		internalUpdate(event2json(event, EXCLUSION));
	}

	/**
	 * update the actor with an event from Telegram
	 * 
	 * @param data
	 */
	public synchronized void update(JsonObject data) {
		// check timeout
		if (System.currentTimeMillis() < nextPossibleRun || !canRun()) {
			logger.info("Timeout has not passed for " + owner + " (" + getDatasetId() + ")");
			return;
		}

		internalUpdate(data);
	}

	/**
	 * update the actor with JSON data
	 * 
	 * @param data
	 */
	private synchronized void internalUpdate(JsonObject data) {
		Date now = new Date();
		if (this.compiledProgram) {
			String outputMessages = runCompiled(data);
			if (!outputMessages.isEmpty()) {
				messages.offerFirst(outputMessages);
			}

			// write the output messages to the log file
			Dataset ds = Dataset.find.byId(getDatasetId());
			final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
			Optional<File> logFileOpt = cpds.getFileTemp("log.txt");

			// create log file in records if the file does not exist
			if (!logFileOpt.get().exists()) {
				cpds.addRecord("log.txt", "Log file for the script output", now);
			}

			// write to log file
			try (FileWriter fw = new FileWriter(logFileOpt.get(), true)) {
				fw.write(outputMessages + "\n");
			} catch (Exception e) {
				// do nothing
			}
		} else {
			// log nothing, this might simply occur a lot
		}

		// keep track of this run
		lastUpdate = now;
		runs++;

		// reduce the messages queue
		while (messages.size() > 10) {
			messages.pollLast();
		}
	}

	/**
	 * run the actor's compiled code with the supplied data from the OOCSI or Telegram inlet
	 * 
	 * @param data
	 * @return
	 */
	private String runCompiled(JsonObject data) {

		// acquire the lock
		if (!compiledExecutionPermission.tryAcquire()) {
			logger.info(
			        "Compiled script not executed due to missing permits for " + owner + " (" + getDatasetId() + ")");
			return "";
		}

		long start = System.currentTimeMillis();

		JSActorProxy actor = null;
		try {

			// log side effects, first reset them
			sideEffects = false;

			// create actor proxy
			actor = createActorProxy(data);

			try {
				sandbox.inject("actor", actor);
				sandbox.eval("program(actor)");

				long finished = System.currentTimeMillis();
				// only log if execution takes longer than 1s
				final long runtimeDuration = finished - start;
				if (runtimeDuration > DEFAULT_TIMEOUT) {
					// set longer timeout as penalty
					nextPossibleRun = finished + runtimeDuration * 2;
					logger.info("Compiled script executed in " + runtimeDuration + "ms for " + owner + " ("
					        + getDatasetId() + ")");
				} else {
					nextPossibleRun = finished + DEFAULT_TIMEOUT;
				}
			} catch (ScriptCPUAbuseException | ScriptMemoryAbuseException e) {
				// expected problems
				logger.error("Problem in script resources use for " + owner + " (" + getDatasetId() + ")");
			} catch (ScriptException e) {
				// log to actor
				actor.error(e.getLocalizedMessage());
			} catch (IllegalStateException e) {
				// do nothing
			} catch (IllegalArgumentException e) {
				actor.error("Problem in script: " + e.getLocalizedMessage());
				// expected stuff: syntax problem
				logger.error("Problem in script for " + owner + " (" + getDatasetId() + ")");
			} catch (Exception e) {
				// really unexpected problems are sent to Slack
				Slack.call("Script problem (runCompiled) for ds " + getDatasetId() + " for " + owner,
				        e.getLocalizedMessage());
				logger.error("Unexpected problem (runCompiled) in script " + getDatasetId() + " for " + owner + " ("
				        + getDatasetId() + "): " + e.getLocalizedMessage());
			}

		} catch (Exception e) {
			// do nothing
		}

		// release the lock
		compiledExecutionPermission.release();

		return actor != null ? actor.getMessages() : "";
	}

	/**
	 * run script ac-hoc from the web UI, usually for testing and debugging; no penalty in terms of timeout
	 * 
	 * @param script
	 * @return
	 */
	public String runTrial(String script) {

		// acquire the lock or early abort if no lock available
		if (!trialExecutionPermission.tryAcquire()) {
			return "";
		}

		long start = System.currentTimeMillis();

		// create actor proxy
		final JSActorProxy actor = createActorProxy(new JsonObject());

		try {
			// create new sandbox for testing if not existing
			if (trialSandbox == null) {
				trialSandbox = sandboxFactory.createSandbox(EXECUTOR, MAX_CPU, MAX_MEM, MAX_STATEMENTS);
			}

			// reset the duration to measure just code execution
			start = System.currentTimeMillis();

			final String integratedCode = views.js.tools.actor.code_embedding.render(script).toString();
			trialSandbox.eval(integratedCode);
			trialSandbox.inject("actor", actor);
			trialSandbox.eval("program(actor)");
		} catch (ScriptCPUAbuseException e) {
			// expected stuff: OOCSI, TG, or database access too slow
			actor.error("Script took too long to run.");
			logger.error("Problem in script CPU use for " + owner + " (" + getDatasetId() + ")");
		} catch (ScriptMemoryAbuseException e) {
			// expected stuff: too many loop iterations or similar
			actor.error("Script too heavy for memory.");
			logger.error("Problem in script memory use for " + owner + " (" + getDatasetId() + ")");
		} catch (IllegalStateException e) {
			// do nothing
		} catch (IllegalArgumentException e) {
			// expected stuff: syntax problem
			actor.error("Problem compiling script: " + e.getLocalizedMessage());
		} catch (ScriptException e) {
			// expected stuff: script execution problem
			actor.error("Problem running script: " + e.getLocalizedMessage());
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				// expected stuff
				logger.error("Problem in script for " + owner + " (" + getDatasetId() + ")");

				// releasing the trial sandbox in 2 minutes
				EXECUTOR.submit(() -> {

					logger.error("... reset trial sandbox in 2 mins");

					try {
						Thread.sleep(2 * 60 * DEFAULT_TIMEOUT);
					} catch (InterruptedException e1) {
					}

					// release the sandbox
					trialExecutionPermission.tryAcquire();
					trialSandbox = null;
					// release the lock
					trialExecutionPermission.release();
					logger.error("Trial sandbox reset for " + owner + " (" + getDatasetId() + ")");
				});
			} else {
				// really unexpected stuff
				actor.error(e.getLocalizedMessage());
				logger.error("Unexpected problem in script for " + owner + " (" + getDatasetId() + "): "
				        + e.getLocalizedMessage(), e);
			}
		}

		// only log if execution takes longer than 800ms
		long runtimeDuration = System.currentTimeMillis() - start;
		if (runtimeDuration >= 800) {
			logger.info("Script executed in " + runtimeDuration + "ms for " + owner + " (" + getDatasetId() + ")");
		} else {
			try {
				Thread.sleep(800 - runtimeDuration);
			} catch (Exception e) {
				// do nothing
			}
		}

		// release the lock
		trialExecutionPermission.release();

		// output
		return actor.getMessages();
	}

	/**
	 * stop this actor from getting executed again
	 * 
	 */
	public void stop() {
		setChannel("");
		compiledExecutionPermission.drainPermits();
		compiledProgram = false;
		logger.info("Script inactive and stopped for " + owner + " (" + getDatasetId() + ")");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public void subscribe(String channelName) {
		setChannel(channelName);

		// check for Telegram subscription
		if (channelName.toLowerCase().startsWith("telegram_")) {
			// nothing else necessary
			logger.info("Installed Telegram responder script " + getName());
		} else {
			// create OOCSI client if not existing
			if (oocsi == null) {
				oocsi = oocsiClientUtil.createOOCSIClient("DF/OOCSI/script_" + getDatasetId());
			}

			// subscribe with handler
			oocsi.subscribe(channelName, new EventHandler() {
				@Override
				public void receive(OOCSIEvent event) {
					try {
						realtimeNotifications.notifyIncomingData(channelName);
						update(event);
					} catch (Exception e) {
						logger.error("JS execution problem", e);
					}
				}
			});
			logger.info("OOCSI subscribing to " + channelName + " for script " + getName());
		}
	}

	/**
	 * unsubscribe from the current channel
	 * 
	 * @param channel
	 */
	public void unsubscribe(String channelName) {

		setChannel("");

		// check for Telegram subscription
		if (channelName.toLowerCase().startsWith("telegram_")) {
			logger.info("Removed Telegram responder script " + getName());
		} else {
			if (oocsi != null) {
				oocsi.unsubscribe(channelName);
				oocsi.disconnect();
				oocsi = null;
			}
			logger.info("OOCSI unsubscribed from " + channelName + " for script " + getName());
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * prepare the actor proxy
	 * 
	 * @param data
	 * @param actor
	 */
	private JSActorProxy createActorProxy(JsonObject data) {

		final JSActorProxy actor = new JSActorProxy(this);
		actor.data = data;
		actor.participants = Json.newArray();
		actor.devices = Json.newArray();
		actor.wearables = Json.newArray();
		actor.clusters = Json.newArray();
		actor.datasets = Json.newArray();

		// get the project details
		Project project = getProject();
		project.getParticipants().stream().forEach(p -> {
			// collect devices for each participant
			ArrayNode participantDevices = Json.newArray();
			project.getClusters().stream().filter(c -> c.hasParticipant(p))
			        .forEach(c -> c.getDevices().forEach(d -> participantDevices.add(d.getId())));

			// collect wearables for each participant
			ArrayNode participantWearables = Json.newArray();
			project.getClusters().stream().filter(c -> c.hasParticipant(p))
			        .forEach(c -> c.getDevices().forEach(d -> participantWearables.add(d.getId())));

			// create participant object with list of devices and wearables and add it to list
			ObjectNode participantObject = Json.newObject() //
			        .put("id", p.getId()) //
			        .put("participant_id", p.getRefId()) //
			        .put("name", p.getName()) //
			        .put("pp1", p.getPublicParameter1()) //
			        .put("pp2", p.getPublicParameter2()) //
			        .put("pp3", p.getPublicParameter3());
			participantObject.set("devices", participantDevices);
			participantObject.set("wearables", participantWearables);
			actor.participants.add(participantObject);
		});
		project.getDevices().stream().forEach(d -> {
			// collect participants for each device
			ArrayNode deviceParticipants = Json.newArray();
			project.getClusters().stream().filter(c -> c.hasDevice(d))
			        .forEach(c -> c.getParticipants().forEach(p -> deviceParticipants.add(p.getId())));

			// create device object with list of participants
			actor.devices.add(Json.newObject() //
			        .put("id", d.getId()) //
			        .put("device_id", d.getRefId()) //
			        .put("name", d.getName()) //
			        .put("pp1", d.getPublicParameter1()) //
			        .put("pp2", d.getPublicParameter2()) //
			        .put("pp3", d.getPublicParameter3()) //
			        .set("participants", deviceParticipants));
		});
		project.getWearables().stream().forEach(d -> {
			// collect participants for each wearable
			ArrayNode wearableParticipants = Json.newArray();
			project.getClusters().stream().filter(c -> c.hasWearable(d))
			        .forEach(c -> c.getParticipants().forEach(p -> wearableParticipants.add(p.getId())));

			// create wearable object with list of participants
			actor.wearables.add(Json.newObject() //
			        .put("id", d.getId()) //
			        .put("wearable_id", d.getRefId()) //
			        .put("name", d.getName()) //
			        .put("brand", d.getBrand()) //
			        .put("pp1", d.getPublicParameter1()) //
			        .put("pp2", d.getPublicParameter2()) //
			        .put("pp3", d.getPublicParameter3()) //
			        .set("participants", wearableParticipants));
		});
		project.getClusters().stream().forEach(c -> {
			// collect participants, devices, and wearables
			ArrayNode participants = Json.newArray();
			c.getParticipants().forEach(d -> participants.add(d.getId()));
			ArrayNode devices = Json.newArray();
			c.getDevices().forEach(d -> devices.add(d.getId()));
			ArrayNode wearables = Json.newArray();
			c.getDevices().forEach(d -> wearables.add(d.getId()));

			// create cluster object
			ObjectNode cluster = Json.newObject().put("name", c.getName());
			cluster.set("participants", participants);
			cluster.set("devices", devices);
			cluster.set("wearables", wearables);
			actor.clusters.add(cluster);
		});
		project.getDatasets().stream().forEach(d -> actor.datasets.add(Json.newObject() //
		        .put("id", d.getId()) //
		        .put("dataset_id", d.getRefId()) //
		        .put("name", d.getName()) //
		        .put("type", d.getDsType().name())));

		return actor;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public JSDatasetProxy getDatasetProxy(String datasetId, long ttl) {
		sideEffects = true;
		return new JSDatasetProxy(dsProjectId, datasetId, jsdbApiService, ttl);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public String getEntityResources() {
		sideEffects = true;
		return submitJSDBApiRequest(new JSDBApiRequest(dsProjectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Dataset ds = project.getEntityDataset();
				EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
				return eds.getItems().toString();
			}
		});
	}

	public String getEntityResourcesMatching(String resourceId) {
		sideEffects = true;
		return submitJSDBApiRequest(new JSDBApiRequest(dsProjectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Dataset ds = project.getEntityDataset();
				EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
				return eds.getItemsMatching(resourceId, Optional.empty()).toString();
			}
		});
	}

	public String getEntityResource(String resourceId) {
		sideEffects = true;
		return submitJSDBApiRequest(new JSDBApiRequest(dsProjectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Dataset ds = project.getEntityDataset();
				EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
				return eds.getItem(resourceId, Optional.empty()).orElse(Json.newObject()).toString();
			}
		});
	}

	public void updateEntityResource(String resourceId, String data) {
		sideEffects = true;
		submitJSDBApiRequest(new JSDBApiRequest(dsProjectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Dataset ds = project.getEntityDataset();
				EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
				eds.updateItem(resourceId, Optional.empty(), data);
				return null;
			}
		});
	}

	public void addEntityResource(String resourceId, String data) {
		sideEffects = true;
		submitJSDBApiRequest(new JSDBApiRequest(dsProjectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Dataset ds = project.getEntityDataset();
				EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
				eds.addItem(resourceId, Optional.empty(), data);
				return null;
			}
		});
	}

	public void deleteEntityResource(String resourceId) {
		sideEffects = true;
		submitJSDBApiRequest(new JSDBApiRequest(dsProjectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Dataset ds = project.getEntityDataset();
				EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
				eds.deleteItem(resourceId, Optional.empty());
				return null;
			}
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * submit the request to the service which will handle interruptions transparently; returns either a result or an
	 * empty string; time limit is 0.5s
	 * 
	 * @param request
	 * @return
	 */
	private String submitJSDBApiRequest(JSDBApiRequest request) {
		try {
			jsdbApiService.submitApiRequest(request).get(500, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			// do nothing but abort and return default result
		}
		return request.getResult();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public void sendOOCSIMessage(String channelName, String message) {
		if (message.equals("undefined")) {
			return;
		}

		sideEffects = true;

		// run this in a different thread to avoid side-effects on actor cancellation
		CompletableFuture.runAsync(() -> {
			// ensure there is an OOCSI client available
			if (oocsi == null) {
				oocsi = oocsiClientUtil.createOOCSIClient("DF/OOCSI/script_" + getDatasetId());
			}

			try {
				JsonNode jn = Json.parse(message);
				oocsi.channel(channelName).data(GenericJSONMapDeserializer.deserialize(jn)).send();
				logger.trace("OOCSI event from script " + this.getDatasetId() + " to channel '" + channelName + "': "
				        + message);
			} catch (Exception e) {
				logger.error("Error parsing OOCSI event payload from script " + this.getDatasetId() + " to channel '"
				        + channelName + "': " + message);
			}
		}, EXECUTOR);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public void replyTelegramMessage(String chatId, String message) {
		if (botService == null) {
			return;
		}

		sideEffects = true;
		botService.replyMessage(chatId, getProjectId(), message, EXECUTOR);
		logger.info("Telegram reply from script " + this.getDatasetId() + " to chat " + chatId + ": " + message);
	}

	public void sendTelegramMessageToResearchers(String message) {
		if (botService == null) {
			return;
		}

		sideEffects = true;
		botService.sendMessageToResearchers(getDatasetName(), getProjectId(), message, EXECUTOR);
		logger.info("Telegram message from script " + this.getDatasetId() + " to researchers: " + message);
	}

	public void sendTelegramMessageToParticipant(String participantId, String message) {
		if (botService == null) {
			return;
		}

		sideEffects = true;

		// isolate the DB request for project and participants
		String email = submitJSDBApiRequest(new JSDBApiRequest(dsProjectId) {
			@Override
			public String run(Project project, DatasetConnector datasetConnector) {
				Optional<Participant> pa = project.getParticipants().stream()
				        .filter(p -> p.getRefId().equals(participantId)).findFirst();
				return pa.isPresent() ? pa.get().getEmail() : null;
			}
		});

		// proceed with normal operation
		if (email != null && !email.isEmpty()) {
			botService.sendMessageToParticipant(getProjectId(), email, message, EXECUTOR);
			logger.info("Telegram message from script " + getDatasetId() + " to participant '" + participantId + "': "
			        + message);
		} else {
			logger.info("Telegram message not sent, participant '" + participantId + "' not found.");
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * send the api request to right service (api). since these are long-running operations, we post the request and
	 * then check whether it is completed. the check runs repeatedly and with a max number of checks to abort if
	 * something failed.
	 * 
	 * @param api
	 * @param data
	 * @return
	 */
	public String apiDispatch(String api, String data) {
		sideEffects = true;
		if (api.equalsIgnoreCase("openai-gpt") || api.equalsIgnoreCase("openai") || api.equalsIgnoreCase("localai-gpt")
		        || api.equalsIgnoreCase("localai")) {
			// create and post request
			RemoteApiRequest ar = new RemoteApiRequest("", 10000, this.owner, "", getProjectId(), data);
			return managedAIApiService.submitApiRequest(ar);
		} else if (api.equalsIgnoreCase("t2s")) {
			try {
				// create and post request
				RemoteApiRequest ar = new RemoteApiRequest("", ApiServiceConstants.API_REQUEST_JS_TIMEOUT_MS,
				        this.owner, "", getProjectId(), data);
				audioProcessing.submitApiRequest(ar).get(ApiServiceConstants.API_REQUEST_JS_TIMEOUT_MS,
				        TimeUnit.MILLISECONDS);
				return ar.getResult();
			} catch (InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
				return Json.newObject().put("error", "No results, API timed out.").toString();
			}
		}

		return Json.newObject().put("error", "API not found.").toString();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private long getProjectId() {
		return this.dsProjectId;
	}

	private Project getProject() {
		return Project.find.byId(getProjectId());
	}

	private long getDatasetId() {
		return dsId;
	}

	private String getDatasetName() {
		return dsName;
	}

	public String getChannelName() {
		return channelName == null ? "" : channelName;
	}

	@Override
	public String toString() {
		return canRun()
		        ? "Subscribed to '" + this.channelName + "' with " + runs + " runs so far (last:  " + lastUpdate + ")"
		        : "Not active.";
	}

	private static final JsonObject event2json(OOCSIEvent event, List<String> EXCLUSION) {
		final JsonObject jo = new JsonObject();

		Arrays.stream(event.keys()).filter(s -> !EXCLUSION.contains(s)).forEach(key -> {
			final Object value = event.getObject(key);
			if (value != null) {
				if (value instanceof Number) {
					Number number = (Number) value;
					jo.addProperty(key, number);
				} else if (value instanceof String) {
					String string = (String) value;
					jo.addProperty(key, string);
				} else if (value instanceof Boolean) {
					Boolean booleanVal = (Boolean) value;
					jo.addProperty(key, booleanVal);
				} else {
					jo.addProperty(key, value.toString());
				}
			}
		});
		return jo;
	}
}

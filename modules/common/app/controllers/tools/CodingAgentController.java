package controllers.tools;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.FileUtils;
import org.apache.pekko.NotUsed;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.javadsl.BroadcastHub;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.MergeHub;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import controllers.AbstractAsyncController;
import controllers.api.CompleteDSController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.extensions.model.openai.OpenAIChatModel;
import io.agentscope.core.state.AgentState;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig.TruncateArgsConfig;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import models.vm.TimedMedia;
import play.Logger;
import play.cache.SyncCacheApi;
import play.filters.csrf.AddCSRFToken;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import play.mvc.WebSocket;
import services.api.ApiServiceConstants;
import services.api.GenericApiService.ProjectAPIInfo;
import services.api.ai.LocalModelMetadata;
import services.api.ai.UnmanagedAIApiService;
import utils.conf.ConfigurationUtils;
import utils.validators.FileTypeUtils;

@Singleton
public class CodingAgentController extends AbstractAsyncController {

	private static final Logger.ALogger logger = Logger.of(CodingAgentController.class);

	private final DatasetConnector datasetConnector;
	private final UnmanagedAIApiService aiAPIService;
	private final Materializer materializer;
	private final SyncCacheApi cache;
	private final LocalModelMetadata localModelMetadata;
	private final Config config;

	// Shared WebSocket flows per dataset
	private final Map<Long, DatasetContext> datasetContexts = new HashMap<>();

	@Inject
	public CodingAgentController(DatasetConnector datasetConnector, UnmanagedAIApiService aiAPIService,
			SyncCacheApi cache, ActorSystem actorSystem, Materializer materializer,
			LocalModelMetadata localModelMetadata, Config config) {
		this.datasetConnector = datasetConnector;
		this.aiAPIService = aiAPIService;
		this.cache = cache;
		this.materializer = materializer;
		this.localModelMetadata = localModelMetadata;
		this.config = config;
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result index(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		List<Project> openProjects = new LinkedList<>();
		openProjects.addAll(user.projects());
		openProjects.addAll(user.collaborations());
		openProjects = openProjects.stream().filter(p -> !p.isArchivedProject())
				.sorted((a, b) -> -Long.compare(a.getLastUpdated(), b.getLastUpdated())).collect(Collectors.toList());

		Map<Project, List<Dataset>> filteredProjects = new LinkedHashMap<>();
		for (Project p : openProjects) {
			List<Dataset> datasets = p.getCompleteDatasets().stream().filter(ds -> ds.isWebsite()
					&& !ds.isNarrativeSurvey() && !ds.getName().equals(Dataset.PROJECT_DATA_EXPORT))
					.collect(Collectors.toList());
			if (!datasets.isEmpty()) {
				filteredProjects.put(p, datasets);
			}
		}

		return ok(views.html.tools.codingagent.index.render(user, filteredProjects, request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result view(Request request, Long id, Long fileId) {
		String username = request.attrs().get(play.mvc.Security.USERNAME);
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.visibleFor(username)) {
			return forbidden("Dataset not accessible");
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		final List<TimedMedia> fileList = cpds.getFiles().stream()
				.filter(tl -> FileTypeUtils.looksLikeEditableFile(tl.link)).collect(Collectors.toList());

		String fileName = "";
		String fileType = "";
		String fileContent = "";
		if (fileId != -1L) {
			Optional<File> requestedFileOpt = cpds.getFile(fileId);
			if (requestedFileOpt.isPresent()) {
				File f = requestedFileOpt.get();
				fileName = f.getName();
				String comps[] = fileName.split("\\.");
				fileType = comps[comps.length - 1];
				try {
					fileContent = FileUtils.readFileToString(f, Charset.defaultCharset());
				} catch (Exception e) {
					logger.error("Error reading file content", e);
				}
			}
		}

		return ok(views.html.tools.codingagent.view.render(ds, username, fileId, fileName, fileType, fileContent,
				fileList, csrfToken(request), request));
	}

	@Authenticated(UserAuth.class)
	public Result getFileList(Request request, Long id) {
		String username = request.attrs().get(play.mvc.Security.USERNAME);
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.visibleFor(username)) {
			return forbidden("Dataset not accessible");
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		final List<TimedMedia> fileList = cpds.getFiles().stream()
				.filter(tl -> FileTypeUtils.looksLikeEditableFile(tl.link)).collect(Collectors.toList());

		ArrayNode array = Json.newArray();
		for (TimedMedia tm : fileList) {
			ObjectNode item = Json.newObject();
			item.put("id", tm.getId());
			item.put("filename", tm.link);
			array.add(item);
		}
		return ok(array);
	}

	public WebSocket ws(Long id) {
		return WebSocket.Json.accept(request -> {
			Optional<Person> userOpt = getAuthenticatedUser(request);
			if (userOpt.isEmpty()) {
				return Flow.fromSinkAndSource(Sink.ignore(),
						Source.single(Json.newObject().put("type", "error").put("message", "Unauthorized")));
			}

			Person user = userOpt.get();
			Dataset ds = Dataset.find.byId(id);
			if (ds == null || !ds.visibleFor(user)) {
				return Flow.fromSinkAndSource(Sink.ignore(),
						Source.single(Json.newObject().put("type", "error").put("message", "Forbidden")));
			}

			String username = user.getFirstname() + " " + user.getLastname().toUpperCase().charAt(0);
			String userEmail = user.getEmail();
			return getDatasetFlow(request, id, username, userEmail);
		});
	}

	private synchronized Flow<JsonNode, JsonNode, ?> getDatasetFlow(play.mvc.Http.RequestHeader request, Long datasetId,
			String username, String userEmail) {
		final String sessionId = datasetId + "-session";
		DatasetContext context = datasetContexts.computeIfAbsent(datasetId, id -> {
			// Hub for broadcasting messages to all users in this dataset
			Pair<Sink<JsonNode, NotUsed>, Source<JsonNode, NotUsed>> hub = MergeHub.of(JsonNode.class, 16)
					.toMat(BroadcastHub.of(JsonNode.class, 256), Keep.both()).run(materializer);

			Sink<JsonNode, ?> sink = hub.first();
			Source<JsonNode, ?> source = hub.second();

			Dataset ds = Dataset.find.byId(datasetId);
			CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

			// Seed knowledge files into workspace
			File agentscopeDir = new File(cpds.getFolder(), ".agentscope");
			File knowledgeDir = new File(agentscopeDir, "knowledge");
			if (!knowledgeDir.exists()) {
				knowledgeDir.mkdirs();
			}

			try {
				FileUtils.writeStringToFile(new File(knowledgeDir, "KNOWLEDGE.md"),
						views.html.tools.codingagent.knowledge.index.render().body(), Charset.defaultCharset());
				FileUtils.writeStringToFile(new File(knowledgeDir, "OOCSI.md"),
						views.html.tools.codingagent.knowledge.oocsi.render().body(), Charset.defaultCharset());
				FileUtils.writeStringToFile(new File(knowledgeDir, "DF-iot-dataset.md"),
						views.html.tools.codingagent.knowledge.iot.render().body(), Charset.defaultCharset());
				FileUtils.writeStringToFile(new File(knowledgeDir, "DF-entity-dataset.md"),
						views.html.tools.codingagent.knowledge.entity.render().body(), Charset.defaultCharset());
				FileUtils.writeStringToFile(new File(knowledgeDir, "DF-media-dataset.md"),
						views.html.tools.codingagent.knowledge.media.render().body(), Charset.defaultCharset());
				FileUtils.writeStringToFile(new File(knowledgeDir, "Local-AI.md"),
						views.html.tools.codingagent.knowledge.local_ai.render().body(), Charset.defaultCharset());
			} catch (Exception e) {
				logger.error("Could not seed knowledge files", e);
			}

			UncompactedHistory history = new UncompactedHistory(cpds.getFolder(), sessionId);

			DatasetContext ctx = new DatasetContext(sink, source, materializer, null, null, new Toolkit(), cpds, history);
			ctx.setLocalProxyHost(request.host());
			ctx.setLocalProxySecure(request.secure());
			checkAndReloadAgent(ctx, datasetId, sessionId, userEmail);
			return ctx;
		});

		context.setLocalProxyHost(request.host());
		context.setLocalProxySecure(request.secure());

		// Fetch and replay history for this session
		List<JsonNode> historyNodes = context.history().load();
		if (historyNodes.isEmpty()) {
			// Fallback to AgentState and bootstrap history file
			AgentState state = context.agent().getDelegate().getAgentState("global", sessionId);
			List<Msg> history = state.getContext();
			historyNodes = history.stream().map(msg -> {
				ObjectNode node = Json.newObject().put("type", "chat")
						.put("user", msg.getRole() == MsgRole.USER ? username : "Agent")
						.put("message", msg.getTextContent()).put("timestamp", new Date().getTime())
						.put("formattedTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMM d, HH:mm")));
				context.history().append(node);
				return (JsonNode) node;
			}).collect(Collectors.toList());
		}

		if (context.isThinking()) {
			ObjectNode typingStart = Json.newObject().put("type", "typing").put("active", true);
			historyNodes.add(typingStart);
		}

		Source<JsonNode, ?> historySource = Source.from(historyNodes);

		return Flow.fromSinkAndSource(Sink.foreach(json -> {
			if (json.has("type") && "chat".equals(json.get("type").asText())) {
				String message = json.get("message").asText();

				if (message.trim().equalsIgnoreCase("/reset") || message.trim().equalsIgnoreCase("/clear")) {
					// 1. Export session
					List<JsonNode> exportHistory = context.history().load();
					if (!exportHistory.isEmpty()) {
						String filename = "session_export_"
								+ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmm")) + ".json";
						ArrayNode arr = Json.newArray();
						exportHistory.forEach(arr::add);
						String content = Json.stringify(arr);

						try {
							File tempFile = File.createTempFile("codingagent-export-", ".tmp");
							FileUtils.writeStringToFile(tempFile, content, Charset.defaultCharset());

							Optional<String> storedFileOpt = context.cpds().storeFile(tempFile, filename);
							if (storedFileOpt.isPresent()) {
								String finalFileName = storedFileOpt.get();
								context.cpds().addRecord(finalFileName, "Session Export", new Date());
								cache.remove(CompleteDSController.CACHE_FILES + datasetId);

								Optional<Long> fileIdOpt = context.cpds().getLatestFileVersionId(finalFileName);
								if (fileIdOpt.isPresent()) {
									ObjectNode syncMsg = Json.newObject().put("type", "file-sync")
											.put("fileId", fileIdOpt.get()).put("filename", finalFileName);
									Source.single((JsonNode) syncMsg).runWith(context.sink(), materializer);
								}
							}
							tempFile.delete();
						} catch (IOException e) {
							logger.error("Error exporting session", e);
						}
					}

					// 2. Clear history
					context.history().clear();

					// 3. Clear Agent state
					try {
						AgentState state = context.agent().getDelegate().getAgentState("global", sessionId);
						if (state != null && state.contextMutable() != null) {
							state.contextMutable().clear();
						}
						io.agentscope.core.state.AgentStateStore store = context.agent().getStateStore();
						if (store != null) {
							store.delete("global", sessionId);
						}
					} catch (Exception e) {
						logger.error("Error clearing agent state", e);
					}

					try {
						if (context.subAgent() != null) {
							AgentState subState = context.subAgent().getDelegate().getAgentState("global", sessionId + "-subagent");
							if (subState != null && subState.contextMutable() != null) {
								subState.contextMutable().clear();
							}
							io.agentscope.core.state.AgentStateStore subStore = context.subAgent().getStateStore();
							if (subStore != null) {
								subStore.delete("global", sessionId + "-subagent");
							}
						}
					} catch (Exception e) {
						logger.error("Error clearing sub agent state", e);
					}

					// Broadcast reset message
					ObjectNode sysMsg = Json.newObject().put("type", "chat").put("user", "System")
							.put("message", "Session has been reset and exported.")
							.put("timestamp", new Date().getTime()).put("formattedTime",
									LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMM d, HH:mm")));
					context.history().append(sysMsg);
					Source.single((JsonNode) sysMsg).runWith(context.sink(), materializer);

					return;
				}

				// Broadcast user message to everyone
				ObjectNode userMsg = Json.newObject().put("type", "chat").put("user", username).put("message", message)
						.put("timestamp", new Date().getTime())
						.put("formattedTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMM d, HH:mm")));

				context.history().append(userMsg);
				Source.single((JsonNode) userMsg).runWith(context.sink(), materializer);

				// Trigger AgentScope processing
				CompletableFuture.runAsync(() -> {
					context.setThinking(true);
					try {
						// Dynamically sync and reload workspace rules if they have changed
						checkAndReloadAgent(context, datasetId, sessionId, userEmail);

						// Send typing: true
						ObjectNode typingStart = Json.newObject().put("type", "typing").put("active", true);
						Source.single((JsonNode) typingStart).runWith(context.sink(), materializer);

						Msg input = Msg.builder().role(MsgRole.USER).textContent(message).build();
						RuntimeContext runtimeCtx = RuntimeContext.builder().userId("global").sessionId(sessionId)
								.build();
						Msg response = context.agent().call(input, runtimeCtx).block();

						if (response != null) {
							String messageContent = cleanThinkingTags(response.getTextContent());
							ObjectNode agentMsg = Json.newObject().put("type", "chat").put("user", "Agent")
									.put("message", messageContent).put("timestamp", new Date().getTime())
									.put("formattedTime",
											LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMM d, HH:mm")));

							// Extract token usage
							ChatUsage usage = response.getChatUsage();
							if (usage != null) {
								agentMsg.put("inputTokens", usage.getInputTokens());
								agentMsg.put("outputTokens", usage.getOutputTokens());
								agentMsg.put("totalTokens", usage.getTotalTokens());
								agentMsg.put("generationTime", usage.getTime());
							}

							context.history().append(agentMsg);
							Source.single((JsonNode) agentMsg).runWith(context.sink(), materializer);
						}
					} catch (Exception e) {
						logger.error("Error in agent call", e);
					} finally {
						context.setThinking(false);
						// Send typing: false
						ObjectNode typingEnd = Json.newObject().put("type", "typing").put("active", false);
						Source.single((JsonNode) typingEnd).runWith(context.sink(), materializer);
					}
				});
			}
		}), historySource.concat(context.source()));
	}

	private synchronized void checkAndReloadAgent(DatasetContext context, Long datasetId, String sessionId, String userEmail) {
		Optional<File> sourceAgentsMdOpt = context.cpds().getFile("AGENTS.md");
		if (sourceAgentsMdOpt.isEmpty()) {
			sourceAgentsMdOpt = context.cpds().getFile(".agents/AGENTS.md");
		}

		long currentLastModified = sourceAgentsMdOpt.map(File::lastModified).orElse(0L);

		if (context.agent() == null || currentLastModified != context.getAgentsMdLastModified()) {
			context.setAgentsMdLastModified(currentLastModified);

			File agentscopeDir = new File(context.cpds().getFolder(), ".agentscope");
			File targetAgentsMd = new File(agentscopeDir, "AGENTS.md");

			if (sourceAgentsMdOpt.isPresent()) {
				try {
					FileUtils.copyFile(sourceAgentsMdOpt.get(), targetAgentsMd);
					logger.info("Synced updated AGENTS.md to agent workspace.");
				} catch (Exception e) {
					logger.error("Could not sync AGENTS.md to agent workspace", e);
				}
			} else {
				if (targetAgentsMd.exists()) {
					targetAgentsMd.delete();
					logger.info("Deleted AGENTS.md from agent workspace as it was removed from dataset.");
				}
			}

			try {
				Dataset ds = Dataset.find.byId(datasetId);
				String defaultCodingModel = "qwen/qwen3.6-27b";
				if (config.hasPath(ConfigurationUtils.DF_AI_MODEL_CODING)
						&& !config.getString(ConfigurationUtils.DF_AI_MODEL_CODING).isEmpty()) {
					defaultCodingModel = config.getString(ConfigurationUtils.DF_AI_MODEL_CODING);
				} else if (config.hasPath(ConfigurationUtils.DF_AI_MODEL_DEFAULT)
						&& !config.getString(ConfigurationUtils.DF_AI_MODEL_DEFAULT).isEmpty()) {
					defaultCodingModel = config.getString(ConfigurationUtils.DF_AI_MODEL_DEFAULT);
				}
				String modelName = localModelMetadata
						.mapModelId(ds.configuration(Dataset.CHATBOT_MODEL, defaultCodingModel));
				GenerateOptions defaultOptions = GenerateOptions.builder()
						.additionalBodyParam("preserve_thinking", true)
						.additionalHeader(ApiServiceConstants.X_API_MODEL, modelName)
						.additionalHeader(ApiServiceConstants.X_API_USER, userEmail != null ? userEmail : "").build();

				String chatCompletionsPath = controllers.api2.routes.UnmanagedAIApiController.chatCompletion().url();
				String basePath = chatCompletionsPath.replace("/chat/completions", "");

				String scheme = context.isLocalProxySecure() ? "https://" : "http://";
				String host = context.getLocalProxyHost();
				if (host == null || host.isEmpty()) {
					if (config.hasPath(ConfigurationUtils.DF_BASEURL)
							&& !config.getString(ConfigurationUtils.DF_BASEURL).isEmpty()) {
						String baseUrl = config.getString(ConfigurationUtils.DF_BASEURL);
						scheme = baseUrl.startsWith("https://") ? "https://" : "http://";
						host = baseUrl.replace("http://", "").replace("https://", "");
					} else {
						host = "localhost:9000";
						scheme = "http://";
					}
				}
				String localProxyUrl = scheme + host + basePath;

				OpenAIChatModel model = OpenAIChatModel.builder().modelName(modelName)
						.apiKey(aiAPIService.getInternalDocumentationAPIKey()).baseUrl(localProxyUrl)
						.generateOptions(defaultOptions).build();

				// Build shared read-only tool and sub-agent mutation tool
				ReadOnlyFileTool readOnlyTool = new ReadOnlyFileTool(context.cpds(), datasetId, aiAPIService);
				FileMutationTool mutationTool = new FileMutationTool(context.cpds(), context.sink(), context.materializer(), cache, datasetId);

				// Build Coding Sub-Agent Toolkit & Agent
				Toolkit subAgentToolkit = new Toolkit();
				subAgentToolkit.registerTool(readOnlyTool);
				subAgentToolkit.registerTool(mutationTool);

				String subAgentSysPrompt = views.html.tools.codingagent.subagent_system_prompt.render().body().trim();
				HarnessAgent subAgent = HarnessAgent.builder() //
						.name("CodingSubAgent").model(model) //
						.toolkit(subAgentToolkit).disableShellTool().disableFilesystemTools() //
						.sysPrompt(subAgentSysPrompt) //
						.compaction(CompactionConfig.builder().triggerTokens(50_000) // fire at 50k tokens
								.triggerMessages(10) // fire at 10 messages
								.keepMessages(5) // keep last 5 verbatim
								.truncateArgs(TruncateArgsConfig.builder().triggerTokens(20_000) // fire at 20k tokens
										.triggerMessages(6) // fire at 6 messages
										.maxArgLength(2000) //
										.build()) //
								.build())
						.workspace(Paths.get(context.cpds().getFolder().getAbsolutePath(), ".agentscope")).build();

				context.setSubAgent(subAgent);

				// Build Main Agent Toolkit & Agent
				SubAgentDelegationTool delegationTool = new SubAgentDelegationTool(context, sessionId);
				Toolkit mainToolkit = new Toolkit();
				mainToolkit.registerTool(readOnlyTool);
				mainToolkit.registerTool(delegationTool);

				String mainSysPrompt = ds.configuration(Dataset.CHATBOT_SYSTEM_PROMPT,
						views.html.tools.codingagent.system_prompt.render().body().trim());

				HarnessAgent mainAgent = HarnessAgent.builder() //
						.name("Agent").model(model) //
						.toolkit(mainToolkit).disableShellTool().disableFilesystemTools() //
						.sysPrompt(mainSysPrompt) //
						.compaction(CompactionConfig.builder().triggerTokens(50_000) // fire at 50k tokens
								.triggerMessages(10) // fire at 10 messages
								.keepMessages(5) // keep last 5 verbatim
								.truncateArgs(TruncateArgsConfig.builder().triggerTokens(20_000) // fire at 20k tokens
										.triggerMessages(6) // fire at 6 messages
										.maxArgLength(2000) //
										.build()) //
								.build())
						.workspace(Paths.get(context.cpds().getFolder().getAbsolutePath(), ".agentscope")).build();

				context.setAgent(mainAgent);
				logger.info("Recreated Main HarnessAgent and CodingSubAgent instances.");
			} catch (Exception e) {
				logger.error("Error recreating agent after AGENTS.md change", e);
			}
		}
	}

	private static String cleanThinkingTags(String text) {
		if (text == null) {
			return "";
		}
		// Remove anything between <think> and </think> tags (including the tags themselves)
		return text.replaceAll("(?s)<think>.*?</think>", "").trim();
	}

	public static class DatasetContext {
		private final Sink<JsonNode, ?> sink;
		private final Source<JsonNode, ?> source;
		private final Materializer materializer;
		private HarnessAgent agent;
		private HarnessAgent subAgent;
		private final Toolkit toolkit;
		private final CompleteDS cpds;
		private final UncompactedHistory history;
		private long agentsMdLastModified = -1L;
		private volatile boolean isThinking = false;
		private String localProxyHost;
		private boolean localProxySecure;

		public DatasetContext(Sink<JsonNode, ?> sink, Source<JsonNode, ?> source, Materializer materializer, HarnessAgent agent,
				HarnessAgent subAgent, Toolkit toolkit, CompleteDS cpds, UncompactedHistory history) {
			this.sink = sink;
			this.source = source;
			this.materializer = materializer;
			this.agent = agent;
			this.subAgent = subAgent;
			this.toolkit = toolkit;
			this.cpds = cpds;
			this.history = history;
		}

		public Sink<JsonNode, ?> sink() {
			return sink;
		}

		public Source<JsonNode, ?> source() {
			return source;
		}

		public Materializer materializer() {
			return materializer;
		}

		public HarnessAgent agent() {
			return agent;
		}

		public void setAgent(HarnessAgent agent) {
			this.agent = agent;
		}

		public HarnessAgent subAgent() {
			return subAgent;
		}

		public void setSubAgent(HarnessAgent subAgent) {
			this.subAgent = subAgent;
		}

		public Toolkit toolkit() {
			return toolkit;
		}

		public CompleteDS cpds() {
			return cpds;
		}

		public UncompactedHistory history() {
			return history;
		}

		public long getAgentsMdLastModified() {
			return agentsMdLastModified;
		}

		public void setAgentsMdLastModified(long agentsMdLastModified) {
			this.agentsMdLastModified = agentsMdLastModified;
		}

		public boolean isThinking() {
			return isThinking;
		}

		public void setThinking(boolean thinking) {
			this.isThinking = thinking;
		}

		public String getLocalProxyHost() {
			return localProxyHost;
		}

		public void setLocalProxyHost(String localProxyHost) {
			this.localProxyHost = localProxyHost;
		}

		public boolean isLocalProxySecure() {
			return localProxySecure;
		}

		public void setLocalProxySecure(boolean localProxySecure) {
			this.localProxySecure = localProxySecure;
		}
	}

	public static class UncompactedHistory {
		private final File historyFile;

		public UncompactedHistory(File datasetFolder, String sessionId) {
			File agentscopeDir = new File(datasetFolder, ".agentscope");
			if (!agentscopeDir.exists()) {
				agentscopeDir.mkdirs();
			}
			this.historyFile = new File(agentscopeDir, sessionId + "-history.json");
		}

		public synchronized void append(ObjectNode message) {
			try {
				ArrayNode history;
				if (historyFile.exists()) {
					history = (ArrayNode) Json.parse(FileUtils.readFileToString(historyFile, Charset.defaultCharset()));
				} else {
					history = Json.newObject().putArray("history");
				}
				history.add(message);
				FileUtils.writeStringToFile(historyFile, Json.stringify(history), Charset.defaultCharset());
			} catch (IOException e) {
				logger.error("Error appending to uncompacted history", e);
			}
		}

		public synchronized List<JsonNode> load() {
			try {
				if (historyFile.exists()) {
					JsonNode node = Json.parse(FileUtils.readFileToString(historyFile, Charset.defaultCharset()));
					if (node.isArray()) {
						List<JsonNode> result = new LinkedList<>();
						node.forEach(result::add);
						return result;
					}
				}
			} catch (IOException e) {
				logger.error("Error loading uncompacted history", e);
			}
			return new LinkedList<>();
		}

		public synchronized void clear() {
			try {
				if (historyFile.exists()) {
					historyFile.delete();
				}
			} catch (Exception e) {
				logger.error("Error clearing uncompacted history", e);
			}
		}
	}

	public static class ReadOnlyFileTool {
		private final CompleteDS cpds;
		private final Long datasetId;
		private final UnmanagedAIApiService aiAPIService;

		public ReadOnlyFileTool(CompleteDS cpds, Long datasetId, UnmanagedAIApiService aiAPIService) {
			this.cpds = cpds;
			this.datasetId = datasetId;
			this.aiAPIService = aiAPIService;
		}

		@Tool(description = "Read the content of a file in the dataset")
		public String read_file(
				@ToolParam(name = "filename", description = "The name of the file to read") String filename)
				throws IOException {
			Optional<File> fOpt = cpds.getFile(filename);
			if (fOpt.isPresent()) {
				return FileUtils.readFileToString(fOpt.get(), Charset.defaultCharset());
			}

			// Fallback: check in agent's workspace knowledge directory strictly
			if (filename.startsWith("knowledge/")) {
				File workspaceFile = new File(new File(cpds.getFolder(), ".agentscope"), filename);
				try {
					File knowledgeFolder = new File(new File(cpds.getFolder(), ".agentscope"), "knowledge");
					if (workspaceFile.exists()
							&& workspaceFile.getCanonicalPath().startsWith(knowledgeFolder.getCanonicalPath())) {
						return FileUtils.readFileToString(workspaceFile, Charset.defaultCharset());
					}
				} catch (IOException e) {
					// Ignore and fall through
				}
			}

			return "Error: File not found";
		}

		@Tool(description = "Retrieve project metadata including datasets, their API keys, and resources like devices, wearables, and participants.")
		public String get_project_metadata() {
			Dataset ds = Dataset.find.byId(datasetId);
			if (ds == null || ds.getProject() == null) {
				return "Error: Project not found";
			}
			Project project = ds.getProject();

			ObjectNode projectMetadata = Json.newObject();
			projectMetadata.put("id", project.getId());
			projectMetadata.put("name", project.getName());
			projectMetadata.put("description", project.getDescription());

			String apiKey = "";
			if (project.getOwner() != null) {
				ProjectAPIInfo apiKeyInfo = aiAPIService.getProjectAPIAccess(project.getOwner(), project);
				if (apiKeyInfo != null && apiKeyInfo.apiKey != null) {
					apiKey = apiKeyInfo.apiKey;
				}
			}
			projectMetadata.put("apiKey", apiKey);

			ArrayNode datasetsNode = projectMetadata.putArray("datasets");
			for (Dataset dataset : project.getDatasets()) {
				ObjectNode dsNode = Json.newObject();
				dsNode.put("id", dataset.getId());
				dsNode.put("name", dataset.getName());
				dsNode.put("type", dataset.getDsType().name());
				dsNode.put("description", dataset.getDescription());
				dsNode.put("apiToken", dataset.configuration(Dataset.API_TOKEN, ""));
				datasetsNode.add(dsNode);
			}

			ArrayNode devicesNode = projectMetadata.putArray("devices");
			for (Device device : project.getDevices()) {
				ObjectNode deviceNode = Json.newObject();
				deviceNode.put("id", device.getId());
				deviceNode.put("refId", device.getRefId());
				devicesNode.add(deviceNode);
			}

			ArrayNode wearablesNode = projectMetadata.putArray("wearables");
			for (Wearable wearable : project.getWearables()) {
				ObjectNode wearableNode = Json.newObject();
				wearableNode.put("id", wearable.getId());
				wearableNode.put("refId", wearable.getRefId());
				wearablesNode.add(wearableNode);
			}

			ArrayNode participantsNode = projectMetadata.putArray("participants");
			for (Participant participant : project.getParticipants()) {
				ObjectNode participantNode = Json.newObject();
				participantNode.put("id", participant.getId());
				participantNode.put("refId", participant.getRefId());
				participantsNode.add(participantNode);
			}

			return projectMetadata.toString();
		}

		@Tool(description = "List all files in the dataset")
		public String list_files() {
			return cpds.getFiles().stream().map(TimedMedia::getLink).collect(Collectors.joining(", "));
		}
	}

	public static class FileMutationTool {
		private final CompleteDS cpds;
		private final Sink<JsonNode, ?> broadcastSink;
		private final Materializer materializer;
		private final SyncCacheApi cache;
		private final Long datasetId;

		public FileMutationTool(CompleteDS cpds, Sink<JsonNode, ?> broadcastSink, Materializer materializer, SyncCacheApi cache,
				Long datasetId) {
			this.cpds = cpds;
			this.broadcastSink = broadcastSink;
			this.materializer = materializer;
			this.cache = cache;
			this.datasetId = datasetId;
		}

		@Tool(description = "Write or overwrite a file in the dataset. Note: The dataset only supports a flat file structure; do not use subdirectories.")
		public String write_file(
				@ToolParam(name = "filename", description = "The name of the file to write (no subdirectories)") String filename,
				@ToolParam(name = "content", description = "The content to write to the file") String content)
				throws IOException {

			// Enforce flat structure: reject any filename containing path separators
			if (filename.contains("/") || filename.contains("\\")) {
				return "Error: Subdirectories are not supported. Please use a flat filename.";
			}

			// Create temporary file
			File tempFile = File.createTempFile("codingagent-", ".tmp");
			try {
				FileUtils.writeStringToFile(tempFile, content, Charset.defaultCharset());

				// Official store method
				Optional<String> storedFileOpt = cpds.storeFile(tempFile, filename);
				if (storedFileOpt.isEmpty()) {
					return "Error: Failed to store file in dataset.";
				}

				String finalFileName = storedFileOpt.get();

				// Add record to dataset if it's a new file
				Optional<Long> latestFileVersionId = cpds.getLatestFileVersionId(finalFileName);
				if (latestFileVersionId.isEmpty() || latestFileVersionId.get() == 0) {
					cpds.addRecord(finalFileName, "Created by Coding Agent Subagent", new Date());
					latestFileVersionId = cpds.getLatestFileVersionId(finalFileName);
				}

				// Invalidate cache
				cache.remove(CompleteDSController.CACHE_FILES + datasetId);

				// Broadcast file-sync event
				Optional<Long> fileIdOpt = latestFileVersionId;
				if (fileIdOpt.isPresent()) {
					ObjectNode syncMsg = Json.newObject().put("type", "file-sync").put("fileId", fileIdOpt.get())
							.put("filename", finalFileName);
					Source.single((JsonNode) syncMsg).runWith(broadcastSink, materializer);
				}

				return "Success: File '" + finalFileName + "' written";
			} finally {
				tempFile.delete();
			}
		}

		@Tool(description = "Surgically edit a file by replacing a specific string. Note: Only flat filenames are supported.")
		public String edit_file(
				@ToolParam(name = "filename", description = "The name of the file to edit (no subdirectories)") String filename,
				@ToolParam(name = "old_string", description = "The exact string to be replaced") String oldString,
				@ToolParam(name = "new_string", description = "The new string to insert") String newString)
				throws IOException {

			// Enforce flat structure
			if (filename.contains("/") || filename.contains("\\")) {
				return "Error: Subdirectories are not supported.";
			}

			Optional<File> fOpt = cpds.getFile(filename);
			if (fOpt.isEmpty())
				return "Error: File not found";
			File f = fOpt.get();

			String content = FileUtils.readFileToString(f, Charset.defaultCharset());
			if (!content.contains(oldString)) {
				return "Error: 'old_string' not found in file";
			}

			String newContent = content.replace(oldString, newString);
			FileUtils.writeStringToFile(f, newContent, Charset.defaultCharset());

			// Invalidate cache
			cache.remove(CompleteDSController.CACHE_FILES + datasetId);

			// Broadcast file-sync event
			Optional<Long> fileIdOpt = cpds.getLatestFileVersionId(f.getName());
			if (fileIdOpt.isPresent()) {
				ObjectNode syncMsg = Json.newObject().put("type", "file-sync").put("fileId", fileIdOpt.get())
						.put("filename", f.getName());
				Source.single((JsonNode) syncMsg).runWith(broadcastSink, materializer);
			}

			return "Success: File '" + f.getName() + "' edited";
		}
	}

	public static class SubAgentDelegationTool {
		private final DatasetContext context;
		private final String sessionId;

		public SubAgentDelegationTool(DatasetContext context, String sessionId) {
			this.context = context;
			this.sessionId = sessionId;
		}

		@Tool(description = "Delegate a concrete coding, code generation, or file modification task to the Coding Sub-Agent. Use this tool whenever code needs to be generated or files need to be created/edited.")
		public String execute_coding_task(
				@ToolParam(name = "task_description", description = "Detailed description of the coding or file implementation task to be performed by the coding sub-agent") String taskDescription) {

			if (context.subAgent() == null) {
				return "Error: Coding sub-agent is not initialized.";
			}

			// Broadcast playful, friendly system start notification
			ObjectNode startMsg = Json.newObject().put("type", "chat").put("user", "System")
					.put("message", "🤖 Coding helper is rolling up their sleeves to build your changes... 🚀")
					.put("timestamp", new Date().getTime())
					.put("formattedTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMM d, HH:mm")));
			Source.single((JsonNode) startMsg).runWith(context.sink(), context.materializer());

			try {
				Msg subInput = Msg.builder().role(MsgRole.USER).textContent(taskDescription).build();
				RuntimeContext subRuntimeCtx = RuntimeContext.builder().userId("global")
						.sessionId(sessionId + "-subagent").build();

				Msg subResponse = context.subAgent().call(subInput, subRuntimeCtx).block();

				// Broadcast playful completion notification
				ObjectNode endMsg = Json.newObject().put("type", "chat").put("user", "System")
						.put("message", "✨ Coding helper finished updating your files!")
						.put("timestamp", new Date().getTime())
						.put("formattedTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMM d, HH:mm")));
				Source.single((JsonNode) endMsg).runWith(context.sink(), context.materializer());

				if (subResponse != null && subResponse.getTextContent() != null) {
					return cleanThinkingTags(subResponse.getTextContent());
				}
				return "Task completed by sub-agent.";
			} catch (Exception e) {
				logger.error("Error executing sub-agent coding task", e);
				return "Error executing sub-agent coding task: " + e.getMessage();
			}
		}
	}
}

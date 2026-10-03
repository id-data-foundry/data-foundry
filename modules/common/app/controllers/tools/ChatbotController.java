package controllers.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.typesafe.config.Config;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.tool.Tool;
import io.agentscope.core.tool.ToolParam;
import io.agentscope.core.tool.Toolkit;
import io.agentscope.extensions.model.openai.OpenAIChatModel;
import io.agentscope.harness.agent.HarnessAgent;
import utils.conf.ConfigurationUtils;
import utils.tools.ChatbotMemoryUtils;
import utils.tools.ChatbotMemoryUtils.UserSessionSummary;
import utils.tools.CodingAgentUtils;

import controllers.AbstractAsyncController;
import controllers.api.CompleteDSController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.vm.TimedMedia;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.api.ApiServiceConstants;
import services.api.GenericApiService.ProjectAPIInfo;
import services.api.ai.LocalModelMetadata;
import services.api.ai.UnmanagedAIApiService;
import services.api.remoting.RemoteApiRequest;
import services.processing.MediaProcessingService;
import utils.DataUtils;
import utils.auth.TokenResolverUtil;
import utils.concurrent.DatabaseExecutionContext;
import utils.rendering.MarkdownRenderer;
import utils.validators.FileTypeUtils;

public class ChatbotController extends AbstractAsyncController {

	private static final String CHAT_CONTROLLER_CACHE_PREFIX = "ChatController_chat_";

	private String getChatCacheKey(long dsId, String conversationId) {
		return CHAT_CONTROLLER_CACHE_PREFIX + dsId + "_" + conversationId;
	}

	private final FormFactory formFactory;
	private final DatasetConnector datasetConnector;
	private final CompleteDSController completeDSController;
	private final UnmanagedAIApiService aiAPIService;
	private final MediaProcessingService mediaProcessingService;
	private final SyncCacheApi cache;
	private final LocalModelMetadata localModelMetadata;
	private final TokenResolverUtil tokenResolver;
	private final DatabaseExecutionContext databaseExecutionContext;

	private static final Logger.ALogger logger = Logger.of(ChatbotController.class);

	private final Config config;
	private final ExecutorService agentExecutor = Executors.newWorkStealingPool();
	private final Map<Long, ChatbotAgentContext> agentContexts = new ConcurrentHashMap<>();
	private static final ThreadLocal<RequestScopeTracker> CURRENT_TRACKER = new ThreadLocal<>();

	@Inject
	public ChatbotController(FormFactory formFactory, DatasetConnector datasetConnector,
			CompleteDSController completeDSController, UnmanagedAIApiService aiAPIService,
			MediaProcessingService mediaProcessingService, SyncCacheApi cache, LocalModelMetadata lmmd,
			TokenResolverUtil tokenResolver, Config config, DatabaseExecutionContext databaseExecutionContext) {
		this.formFactory = formFactory;
		this.datasetConnector = datasetConnector;
		this.completeDSController = completeDSController;
		this.aiAPIService = aiAPIService;
		this.mediaProcessingService = mediaProcessingService;
		this.cache = cache;
		this.localModelMetadata = lmmd;
		this.tokenResolver = tokenResolver;
		this.config = config;
		this.databaseExecutionContext = databaseExecutionContext;
	}

	public static class RequestScopeTracker {
		private final List<SearchResult> citations = new CopyOnWriteArrayList<>();
		private final List<String> memoryActivities = new CopyOnWriteArrayList<>();
		private final List<ExecutionTraceStep> traceSteps = new CopyOnWriteArrayList<>();

		public void recordCitations(Collection<SearchResult> hits) {
			citations.addAll(hits);
		}

		public void recordMemoryActivity(String activity) {
			memoryActivities.add(activity);
		}

		public void recordTraceStep(ExecutionTraceStep step) {
			traceSteps.add(step);
		}

		public List<SearchResult> getCitations() {
			return citations;
		}

		public List<String> getMemoryActivities() {
			return memoryActivities;
		}

		public List<ExecutionTraceStep> getTraceSteps() {
			return traceSteps;
		}
	}

	public record ExecutionTraceStep(String type, String name, String details, String result) {
		public static ExecutionTraceStep toolCall(String toolName, String args, String result) {
			return new ExecutionTraceStep("tool", toolName, args, result);
		}
		public static ExecutionTraceStep thinking(String content) {
			return new ExecutionTraceStep("think", "Reasoning", content, "");
		}
	}

	public class KnowledgeSearchTool {
		private final CompleteDS cpds;
		private final Dataset dataset;

		public KnowledgeSearchTool(CompleteDS cpds, Dataset dataset) {
			this.cpds = cpds;
			this.dataset = dataset;
		}

		@Tool(description = "Search the dataset's uploaded knowledge base files (PDFs, docs, notes) using semantic vector search. Call this ONLY when the user's question requires information from the dataset knowledge files.")
		public String search_knowledge_base(
				@ToolParam(name = "query", description = "Targeted search query to locate relevant knowledge base sections") String query) {
			int maxHits = DataUtils.parseInt(dataset.configuration(Dataset.CHATBOT_RAG_MAX_HITS, "10"), 10);
			float threshold = DataUtils.parseFloat(dataset.configuration(Dataset.CHATBOT_RAG_SCORE_THRESHOLD, "0.6"),
					0.6f);

			Collection<SearchResult> hits = searchIndex(cpds, query, maxHits, threshold);
			RequestScopeTracker tracker = CURRENT_TRACKER.get();
			if (tracker != null) {
				tracker.recordCitations(hits);
				tracker.recordTraceStep(ExecutionTraceStep.toolCall("search_knowledge_base",
						"query=\"" + query + "\"", hits.size() + " chunks retrieved"));
			}

			if (hits.isEmpty()) {
				return "No matching documents found in knowledge base for query: " + query;
			}

			StringBuilder sb = new StringBuilder();
			int idx = 1;
			for (SearchResult hit : hits) {
				sb.append(String.format("[%d] (Source: %s, Relevance: %.0f%%)\n%s\n\n", idx++, hit.file(),
						hit.score() * 100, hit.content()));
			}
			return sb.toString();
		}
	}

	public class UserMemoryTool {
		private final File datasetFolder;

		public UserMemoryTool(File datasetFolder) {
			this.datasetFolder = datasetFolder;
		}

		@Tool(description = "Save or update facts, preferences, background, or learning progress about the current user for long-term retention.")
		public String update_user_memory(RuntimeContext context,
				@ToolParam(name = "topic", description = "The topic or category (e.g. 'academic_profile', 'thesis_project', 'preferences')") String topic,
				@ToolParam(name = "note", description = "The specific fact or detail to retain about this user") String note) {
			boolean ok = ChatbotMemoryUtils.saveUserProfileEntry(datasetFolder, context.getUserId(), topic, note);
			RequestScopeTracker tracker = CURRENT_TRACKER.get();
			if (tracker != null) {
				tracker.recordMemoryActivity("Saved **" + topic + "** to your profile");
				tracker.recordTraceStep(ExecutionTraceStep.toolCall("update_user_memory",
						"topic=\"" + topic + "\", note=\"" + note + "\"", ok ? "Success" : "Failed"));
			}
			return ok ? "Successfully saved to user profile memory: " + topic : "Error saving memory";
		}

		@Tool(description = "Write or edit a markdown file in the current user's private notes space (e.g. 'notes.md').")
		public String write_user_file(RuntimeContext context,
				@ToolParam(name = "filename", description = "Target filename (only flat .md files allowed, e.g. 'notes.md')") String filename,
				@ToolParam(name = "content", description = "Markdown content to write") String content) {
			String res = ChatbotMemoryUtils.writeUserFile(datasetFolder, context.getUserId(), filename, content);
			RequestScopeTracker tracker = CURRENT_TRACKER.get();
			if (tracker != null) {
				if (res.startsWith("Success:")) {
					tracker.recordMemoryActivity("Updated **" + filename + "**");
				}
				tracker.recordTraceStep(ExecutionTraceStep.toolCall("write_user_file",
						"filename=\"" + filename + "\"", res));
			}
			return res;
		}

		@Tool(description = "Read a file from the current user's private notes space (e.g. 'notes.md' or 'profile.md').")
		public String read_user_file(RuntimeContext context,
				@ToolParam(name = "filename", description = "Name of the user file to read") String filename) {
			String res = ChatbotMemoryUtils.readUserFile(datasetFolder, context.getUserId(), filename);
			RequestScopeTracker tracker = CURRENT_TRACKER.get();
			if (tracker != null) {
				tracker.recordTraceStep(ExecutionTraceStep.toolCall("read_user_file",
						"filename=\"" + filename + "\"", res.startsWith("Error:") ? res : res.length() + " chars"));
			}
			return res;
		}

		@Tool(description = "Delete a markdown file from the current user's private notes space (e.g. 'notes.md').")
		public String delete_user_file(RuntimeContext context,
				@ToolParam(name = "filename", description = "Target filename to delete (e.g. 'notes.md')") String filename) {
			String res = ChatbotMemoryUtils.deleteUserFile(datasetFolder, context.getUserId(), filename);
			RequestScopeTracker tracker = CURRENT_TRACKER.get();
			if (tracker != null) {
				if (res.startsWith("Success:")) {
					tracker.recordMemoryActivity("Deleted **" + filename + "**");
				}
				tracker.recordTraceStep(ExecutionTraceStep.toolCall("delete_user_file",
						"filename=\"" + filename + "\"", res));
			}
			return res;
		}

		@Tool(description = "Remove a specific topic or attribute from the current user's remembered profile.")
		public String delete_user_memory(RuntimeContext context,
				@ToolParam(name = "topic", description = "The topic or category to delete from profile memory") String topic) {
			boolean ok = ChatbotMemoryUtils.deleteUserProfileEntry(datasetFolder, context.getUserId(), topic);
			RequestScopeTracker tracker = CURRENT_TRACKER.get();
			if (tracker != null) {
				if (ok) {
					tracker.recordMemoryActivity("Removed **" + topic + "** from profile");
				}
				tracker.recordTraceStep(ExecutionTraceStep.toolCall("delete_user_memory",
						"topic=\"" + topic + "\"", ok ? "Success" : "Failed"));
			}
			return ok ? "Successfully removed topic '" + topic + "' from user memory." : "Error removing topic from memory.";
		}

		@Tool(description = "Reset and clear all stored profile facts and notes files for the current user.")
		public String reset_user_memory(RuntimeContext context) {
			boolean ok = ChatbotMemoryUtils.resetUserMemory(datasetFolder, context.getUserId());
			RequestScopeTracker tracker = CURRENT_TRACKER.get();
			if (tracker != null) {
				if (ok) {
					tracker.recordMemoryActivity("Reset all user memory and notes files");
				}
				tracker.recordTraceStep(ExecutionTraceStep.toolCall("reset_user_memory", "", ok ? "Success" : "Failed"));
			}
			return ok ? "Successfully reset all stored user memory and files." : "Error resetting user memory.";
		}
	}

	public static class ChatbotAgentContext {
		private HarnessAgent agent;
		private final Toolkit toolkit;
		private final CompleteDS cpds;
		private final Dataset dataset;
		private long agentsMdLastModified = -1L;
		private String configuredModel = "";

		public ChatbotAgentContext(HarnessAgent agent, Toolkit toolkit, CompleteDS cpds, Dataset dataset) {
			this.agent = agent;
			this.toolkit = toolkit;
			this.cpds = cpds;
			this.dataset = dataset;
		}

		public HarnessAgent agent() {
			return agent;
		}

		public void setAgent(HarnessAgent agent) {
			this.agent = agent;
		}

		public Toolkit toolkit() {
			return toolkit;
		}

		public CompleteDS cpds() {
			return cpds;
		}

		public Dataset dataset() {
			return dataset;
		}

		public long getAgentsMdLastModified() {
			return agentsMdLastModified;
		}

		public void setAgentsMdLastModified(long agentsMdLastModified) {
			this.agentsMdLastModified = agentsMdLastModified;
		}

		public String getConfiguredModel() {
			return configuredModel;
		}

		public void setConfiguredModel(String configuredModel) {
			this.configuredModel = configuredModel;
		}
	}

	private String resolveDefaultCodingModel() {
		if (config != null && config.hasPath(ConfigurationUtils.DF_AI_MODEL_CODING)
				&& !config.getString(ConfigurationUtils.DF_AI_MODEL_CODING).isEmpty()) {
			return config.getString(ConfigurationUtils.DF_AI_MODEL_CODING);
		} else if (config != null && config.hasPath(ConfigurationUtils.DF_AI_MODEL_DEFAULT)
				&& !config.getString(ConfigurationUtils.DF_AI_MODEL_DEFAULT).isEmpty()) {
			return config.getString(ConfigurationUtils.DF_AI_MODEL_DEFAULT);
		}
		return "qwen/qwen3.6-27b";
	}

	private void checkAndReloadAgent(ChatbotAgentContext context, Dataset ds, CompleteDS cpds) {
		synchronized (context) {
			Optional<File> sourceAgentsMdOpt = cpds.getFile("AGENTS.md");
			if (sourceAgentsMdOpt.isEmpty()) {
				sourceAgentsMdOpt = cpds.getFile(".agents/AGENTS.md");
			}

			long currentLastModified = sourceAgentsMdOpt.map(File::lastModified).orElse(0L);
			boolean isAgentic = "true".equals(ds.configuration(Dataset.CHATBOT_ENABLE_AGENTIC, "false"));
			String rawModel = isAgentic ? resolveDefaultCodingModel() : ds.configuration(Dataset.CHATBOT_MODEL, "");
			if (rawModel.isEmpty()) {
				rawModel = resolveDefaultCodingModel();
			}

			if (context.agent() == null || currentLastModified != context.getAgentsMdLastModified()
					|| !rawModel.equals(context.getConfiguredModel())) {
				context.setAgentsMdLastModified(currentLastModified);
				context.setConfiguredModel(rawModel);

				File agentscopeDir = new File(cpds.getFolder(), ".agentscope");
				if (!agentscopeDir.exists()) {
					agentscopeDir.mkdirs();
				}
				File targetAgentsMd = new File(agentscopeDir, "AGENTS.md");

				if (sourceAgentsMdOpt.isPresent()) {
					try {
						FileUtils.copyFile(sourceAgentsMdOpt.get(), targetAgentsMd);
						logger.info("Synced updated AGENTS.md to chatbot agent workspace.");
					} catch (Exception e) {
						logger.error("Could not sync AGENTS.md to chatbot agent workspace", e);
					}
				} else {
					if (targetAgentsMd.exists()) {
						targetAgentsMd.delete();
					}
				}

				try {
					String mainModelName = localModelMetadata.mapModelId(rawModel);
					int agentMaxTokens = 4096;
					if (config != null && config.hasPath(ConfigurationUtils.DF_AI_AGENT_MAX_TOKENS)) {
						agentMaxTokens = config.getInt(ConfigurationUtils.DF_AI_AGENT_MAX_TOKENS);
					}

					GenerateOptions mainOptions = GenerateOptions.builder()
							.maxTokens(agentMaxTokens)
							.additionalHeader(ApiServiceConstants.X_API_MODEL, mainModelName)
							.build();

					String localProxyUrl = CodingAgentUtils.resolveLocalProxyUrl(config);

					OpenAIChatModel mainModel = OpenAIChatModel.builder()
							.modelName(mainModelName)
							.apiKey(aiAPIService.getInternalDocumentationAPIKey())
							.baseUrl(localProxyUrl)
							.generateOptions(mainOptions)
							.build();

					Toolkit toolkit = new Toolkit();
					if (cpds.getFiles() != null && !cpds.getFiles().isEmpty()) {
						toolkit.registerTool(new KnowledgeSearchTool(cpds, ds));
					}
					if ("true".equals(ds.configuration(Dataset.CHATBOT_ENABLE_USER_MEMORY, "true"))) {
						toolkit.registerTool(new UserMemoryTool(cpds.getFolder()));
					}

					String mainSysPrompt = ds.configuration(Dataset.CHATBOT_SYSTEM_PROMPT,
							"""
							You are an intelligent, helpful assistant.
							Current date: $DATE
							""");
					mainSysPrompt = mainSysPrompt.replace("$DATE",
							SimpleDateFormat.getDateInstance(SimpleDateFormat.SHORT).format(new Date()));

					StringBuilder promptBuilder = new StringBuilder(mainSysPrompt);
					if ("true".equals(ds.configuration(Dataset.CHATBOT_ENABLE_USER_MEMORY, "true"))) {
						promptBuilder.append("""


								## Memory & Long-Term Knowledge Directives:
								You have access to persistent user memory tools:
								- `update_user_memory(topic, note)`: Proactively record any personal facts, background, academic discipline, research topics, preferences, or project details the user mentions.
								- `delete_user_memory(topic)`: Remove a specific remembered topic when requested by the user.
								- `write_user_file(filename, content)`: Write or update user notes files (e.g., 'notes.md') when asked to take notes, save summaries, or track plans.
								- `read_user_file(filename)`: Read existing notes files.
								- `delete_user_file(filename)`: Delete a specific notes file when requested by the user.
								- `reset_user_memory()`: Reset all user memory and notes files when explicitly instructed by the user.
								""");
					}
					if (cpds.getFiles() != null && !cpds.getFiles().isEmpty()) {
						promptBuilder.append("""


								## Knowledge Base Retrieval Directives:
								- `search_knowledge_base(query)`: Call this tool whenever the user asks questions that relate to documents or reference materials uploaded to this dataset's knowledge base.
								""");
					}
					mainSysPrompt = promptBuilder.toString();

					HarnessAgent agent = HarnessAgent.builder()
							.name("Agent")
							.model(mainModel)
							.toolkit(toolkit)
							.disableShellTool()
							.disableFilesystemTools()
							.sysPrompt(mainSysPrompt)
							.workspace(Paths.get(cpds.getFolder().getAbsolutePath(), ".agentscope"))
							.build();

					context.setAgent(agent);
					logger.info("Recreated Chatbot HarnessAgent (model: {}) for dataset {}", mainModelName, ds.getId());
				} catch (Exception e) {
					logger.error("Error creating Chatbot HarnessAgent", e);
				}
			}
		}
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result index(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		if (user.projects().size() + user.collaborations().size() == 0) {
			return redirect(controllers.routes.ProjectsController.index()).addingToSession(request, "message",
					"No project chatbots to show.");
		}

		return ok(views.html.tools.actor.index.render(user, csrfToken(request)));
	}

	// Helper method for the view to determine if an .idx file exists for a given TimedMedia
	private boolean hasIndexFile(CompleteDS cpds, TimedMedia file) {
		// Construct the expected .idx file path based on the file's link (filename)
		File indexFile = new File(cpds.getFolder().getAbsolutePath() + File.separator + file.link + ".idx");
		return indexFile.exists();
	}

	/**
	 * add a chatbot
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result add(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data.");
		}

		// scripts can only be added by the project owner
		Project p = Project.find.byId(id);
		if (p == null || !p.belongsTo(user)) {
			return noContent(); // redirect(routes.ChatbotController.index());
		}

		// create new dataset for the actor
		Dataset ds = datasetConnector.create(nss(df.get("name")), DatasetType.COMPLETE, p, "Chatbot dataset",
				"Data Foundry chatbots", null, df.get("license"));
		ds.setCollectorType(Dataset.CHATBOT);
		ds.save();

		// add default configuration
		ds.getConfiguration().put(Dataset.CHATBOT_TEMPERATURE, "0.7");
		ds.getConfiguration().put(Dataset.CHATBOT_RAG_MAX_HITS, "10");
		ds.getConfiguration().put(Dataset.CHATBOT_RAG_SCORE_THRESHOLD, "0.6");
		String defaultCodingModel = resolveDefaultCodingModel();
		ds.getConfiguration().put(Dataset.CHATBOT_MODEL, defaultCodingModel);
		ds.getConfiguration().put(Dataset.CHATBOT_ENABLE_AGENTIC, "true");
		ds.getConfiguration().put(Dataset.CHATBOT_ENABLE_MULTISESSION, "true");
		ds.getConfiguration().put(Dataset.CHATBOT_ENABLE_USER_MEMORY, "true");
		ds.getConfiguration().put(Dataset.CHATBOT_SYSTEM_PROMPT,
				"""
						You are DataFoundryGPT, an intelligent assistant powered by AgentScope.
						Current date: $DATE""");
		ds.update();

		// create initial starter AGENTS.md in dataset folder if absent
		final CompleteDS cpdsNew = (CompleteDS) datasetConnector.getDatasetDS(ds);
		if (cpdsNew != null) {
			File agentscopeDir = new File(cpdsNew.getFolder(), ".agentscope");
			if (!agentscopeDir.exists()) {
				agentscopeDir.mkdirs();
			}
			File targetAgentsMd = new File(agentscopeDir, "AGENTS.md");
			if (!targetAgentsMd.exists()) {
				try {
					String starterAgentsMd = """
# Agent Guidelines & Memory Configuration

You are an intelligent assistant running on the AgentScope harness.

## Behavioral Directives
- Respond directly, helpfully, and concisely unless in-depth reasoning is requested.
- Maintain a professional and friendly tone.

## Memory Guidelines
- When the user shares facts about themselves (preferences, role, goals, background), call `update_user_memory(topic, note)`.
- Use `write_user_file(filename, content)` to create or update personal notes or logs for the user.
- Read private user files using `read_user_file(filename)` when needed.

## Knowledge Base Guidelines
- Only call `search_knowledge_base(query)` when the user's question requires information from uploaded documents.
- Do NOT perform vector searches for general conversational queries.
""";
					Files.writeString(targetAgentsMd.toPath(), starterAgentsMd, StandardCharsets.UTF_8);
					File rootAgentsMd = new File(cpdsNew.getFolder(), "AGENTS.md");
					Files.writeString(rootAgentsMd.toPath(), starterAgentsMd, StandardCharsets.UTF_8);
				} catch (Exception e) {
					logger.error("Error creating initial AGENTS.md for dataset {}", ds.getId(), e);
				}
			}
		}

		// ensure that the project has an activated API key for local AI
		ProjectAPIInfo pai = aiAPIService.getProjectAPIAccess(user, p);
		if (pai.apiKey.isEmpty()) {
			aiAPIService.activateProjectAPIAccess(user, p);
		} else if (pai.tokensMax < pai.tokensUsed + 1000) {
			// TODO here we need to increase the token allowance
		}

		return redirect(routes.ChatbotController.view(ds.getId())).addingToSession(request, "message",
				"Chatbot " + ds.getName() + " created in project " + p.getName());
	}

	/**
	 * generate the chatbot edit page
	 * 
	 * @param request
	 * @param dsId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result view(Request request, long dsId) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(dsId);
		if (ds == null || !ds.editableBy(user.getEmail())) {
			return redirect(routes.ChatbotController.index());
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		List<TimedMedia> files = cpds.getFiles();

		List<FileWithIndexStatus> filesWithStatus = files.stream()
				.map(file -> new FileWithIndexStatus(file, hasIndexFile(cpds, file))).collect(Collectors.toList());

		String agentsMdContent = "";
		if (cpds != null) {
			File agentscopeDir = new File(cpds.getFolder(), ".agentscope");
			File agentsMdFile = new File(agentscopeDir, "AGENTS.md");
			if (agentsMdFile.exists()) {
				try {
					agentsMdContent = Files.readString(agentsMdFile.toPath(), StandardCharsets.UTF_8);
				} catch (Exception e) {
					logger.error("Error reading .agentscope/AGENTS.md", e);
				}
			} else {
				Optional<File> rootAgentsOpt = cpds.getFile("AGENTS.md");
				if (rootAgentsOpt.isPresent()) {
					try {
						agentsMdContent = Files.readString(rootAgentsOpt.get().toPath(), StandardCharsets.UTF_8);
					} catch (Exception e) {
						logger.error("Error reading root AGENTS.md", e);
					}
				}
			}
		}

		// show chatbot configuration interface
		return ok(views.html.tools.chatbots.view.render(user, ds, filesWithStatus, localModelMetadata,
				agentsMdContent, csrfToken(request)));
	}

	/**
	 * save the chatbot settings
	 * 
	 * @param request
	 * @param id
	 * @param route
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result save(Request request, long id, String route) {
		String user = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.editableBy(user)) {
			return redirect(routes.ChatbotController.index());
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		boolean updateDS = Arrays.stream(new String[] { Dataset.CHATBOT_INTRODUCTION, Dataset.CHATBOT_SYSTEM_PROMPT,
				Dataset.CHATBOT_ASSISTANT_PROMPT, Dataset.CHATBOT_USER_PROMPT, Dataset.CHATBOT_MODEL,
				Dataset.CHATBOT_RAG_MAX_HITS, Dataset.CHATBOT_RAG_SCORE_THRESHOLD, Dataset.CHATBOT_TEMPERATURE })
				.map(key -> {
					Optional<Object> result = df.value(key);
					if (result.isPresent()) {
						ds.getConfiguration().put(key, result.get().toString());
					}
					return result;
				}).filter(r -> r.isPresent()).collect(Collectors.counting()) > 0;

		{
			Optional<Object> result = df.value(Dataset.CHATBOT_PUBLIC);
			if (result.isPresent() && !ds.getConfiguration().containsKey(Dataset.CHATBOT_PUBLIC)) {
				ds.getConfiguration().put(Dataset.CHATBOT_PUBLIC, "true");
				updateDS = true;
			} else if (!result.isPresent() && ds.getConfiguration().containsKey(Dataset.CHATBOT_PUBLIC)) {
				ds.getConfiguration().remove(Dataset.CHATBOT_PUBLIC);
				updateDS = true;
			}
		}

		{
			Optional<Object> result = df.value(Dataset.CHATBOT_STORE_CHATS);
			if (result.isPresent() && !ds.getConfiguration().containsKey(Dataset.CHATBOT_STORE_CHATS)) {
				ds.getConfiguration().put(Dataset.CHATBOT_STORE_CHATS, "true");
				updateDS = true;
			} else if (!result.isPresent() && ds.getConfiguration().containsKey(Dataset.CHATBOT_STORE_CHATS)) {
				ds.getConfiguration().remove(Dataset.CHATBOT_STORE_CHATS);
				updateDS = true;
			}
		}

		{
			Optional<Object> result = df.value(Dataset.CHATBOT_SHOW_SOURCES);
			if (result.isPresent() && !ds.getConfiguration().containsKey(Dataset.CHATBOT_SHOW_SOURCES)) {
				ds.getConfiguration().put(Dataset.CHATBOT_SHOW_SOURCES, "true");
				updateDS = true;
			} else if (!result.isPresent() && ds.getConfiguration().containsKey(Dataset.CHATBOT_SHOW_SOURCES)) {
				ds.getConfiguration().remove(Dataset.CHATBOT_SHOW_SOURCES);
				updateDS = true;
			}
		}

		{
			Optional<Object> result = df.value(Dataset.CHATBOT_ENABLE_AGENTIC);
			if (result.isPresent()) {
				if (!"true".equals(ds.configuration(Dataset.CHATBOT_ENABLE_AGENTIC, "false"))) {
					ds.getConfiguration().put(Dataset.CHATBOT_ENABLE_AGENTIC, "true");
					updateDS = true;
				}
				String codingModel = resolveDefaultCodingModel();
				if (!codingModel.equals(ds.configuration(Dataset.CHATBOT_MODEL, ""))) {
					ds.getConfiguration().put(Dataset.CHATBOT_MODEL, codingModel);
					updateDS = true;
				}
			} else if (ds.getConfiguration().containsKey(Dataset.CHATBOT_ENABLE_AGENTIC)) {
				ds.getConfiguration().remove(Dataset.CHATBOT_ENABLE_AGENTIC);
				updateDS = true;
			}
		}

		{
			Optional<Object> result = df.value(Dataset.CHATBOT_ENABLE_MULTISESSION);
			if (result.isPresent() && !ds.getConfiguration().containsKey(Dataset.CHATBOT_ENABLE_MULTISESSION)) {
				ds.getConfiguration().put(Dataset.CHATBOT_ENABLE_MULTISESSION, "true");
				updateDS = true;
			} else if (!result.isPresent() && ds.getConfiguration().containsKey(Dataset.CHATBOT_ENABLE_MULTISESSION)) {
				ds.getConfiguration().remove(Dataset.CHATBOT_ENABLE_MULTISESSION);
				updateDS = true;
			}
		}

		{
			Optional<Object> result = df.value(Dataset.CHATBOT_ENABLE_USER_MEMORY);
			if (result.isPresent() && !ds.getConfiguration().containsKey(Dataset.CHATBOT_ENABLE_USER_MEMORY)) {
				ds.getConfiguration().put(Dataset.CHATBOT_ENABLE_USER_MEMORY, "true");
				updateDS = true;
			} else if (!result.isPresent() && ds.getConfiguration().containsKey(Dataset.CHATBOT_ENABLE_USER_MEMORY)) {
				ds.getConfiguration().remove(Dataset.CHATBOT_ENABLE_USER_MEMORY);
				updateDS = true;
			}
		}

		// update chatbot name?
		Optional<Object> result = df.value(Dataset.CHATBOT_NAME);
		if (result.isPresent() && !result.get().toString().trim().equals(ds.getName())) {
			ds.setName(result.get().toString().trim());
			updateDS = true;
		}

		// update dataset
		if (updateDS) {
			ds.update();
			agentContexts.remove(ds.getId());
		}

		try {
			Thread.sleep(500);
		} catch (Exception e) {
		}

		return noContent();
	}

	@Authenticated(UserAuth.class)
	public Result saveAgentsMd(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.editableBy(user.getEmail())) {
			return forbidden("Dataset not accessible");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		String content = df != null ? df.get("agents_md") : null;
		if (content == null) {
			content = "";
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		if (cpds != null) {
			File agentscopeDir = new File(cpds.getFolder(), ".agentscope");
			if (!agentscopeDir.exists()) {
				agentscopeDir.mkdirs();
			}
			File targetAgentsMd = new File(agentscopeDir, "AGENTS.md");
			try {
				Files.writeString(targetAgentsMd.toPath(), content, StandardCharsets.UTF_8);
				File rootAgentsMd = new File(cpds.getFolder(), "AGENTS.md");
				Files.writeString(rootAgentsMd.toPath(), content, StandardCharsets.UTF_8);
				agentContexts.remove(ds.getId());
				return ok("<span style=\"color: #16a34a; font-weight: 600;\">✓ AGENTS.md saved and agent reloaded!</span>");
			} catch (Exception e) {
				logger.error("Error saving AGENTS.md", e);
				return internalServerError("Error saving AGENTS.md: " + e.getMessage());
			}
		}
		return badRequest("Dataset not found");
	}

	private String renderUserMemoryHtml(CompleteDS cpds, String userEmail, long dsId, String statusBanner) {
		ChatbotMemoryUtils.UserMemorySnapshot snapshot = ChatbotMemoryUtils.getUserMemorySnapshot(cpds.getFolder(),
				userEmail);

		StringBuilder html = new StringBuilder();
		html.append("<div class=\"user-memory-container\" style=\"font-size: 0.9rem;\">");

		if (statusBanner != null && !statusBanner.trim().isEmpty()) {
			html.append("<div style=\"background: #f0fdf4; border: 1px solid #bbf7d0; color: #166534; padding: 6px 12px; border-radius: 4px; margin-bottom: 12px; font-weight: 500;\">")
					.append(escapeHtml(statusBanner)).append("</div>");
		}

		html.append("<p style=\"color: #64748b; margin-bottom: 12px;\">User: <strong>")
				.append(escapeHtml(userEmail)).append("</strong></p>");

		html.append("<h6 style=\"margin-top: 12px; margin-bottom: 6px;\">Profile Attributes</h6>");
		if (snapshot.profileAttributes().isEmpty()) {
			html.append("<p style=\"color: #94a3b8; font-style: italic;\">No profile facts stored yet. The bot will automatically remember key facts you share.</p>");
		} else {
			html.append("<table style=\"width: 100%; border-collapse: collapse; margin-bottom: 16px;\">");
			html.append("<thead><tr style=\"border-bottom: 1px solid #cbd5e1; text-align: left;\">")
					.append("<th style=\"padding: 4px 8px;\">Topic</th>")
					.append("<th style=\"padding: 4px 8px;\">Detail</th>")
					.append("<th style=\"padding: 4px 8px; width: 40px; text-align: center;\">Action</th>")
					.append("</tr></thead><tbody>");
			for (Map.Entry<String, String> entry : snapshot.profileAttributes().entrySet()) {
				html.append("<tr style=\"border-bottom: 1px solid #f1f5f9;\">");
				html.append("<td style=\"padding: 4px 8px; font-weight: 600;\">")
						.append(escapeHtml(entry.getKey())).append("</td>");
				html.append("<td style=\"padding: 4px 8px;\">")
						.append(escapeHtml(entry.getValue())).append("</td>");
				html.append("<td style=\"padding: 4px 8px; text-align: center;\">")
						.append("<button type=\"button\" class=\"outline danger\" style=\"padding: 2px 6px; font-size: 0.75rem; border: none; background: transparent; cursor: pointer;\" title=\"Delete this topic\" ")
						.append("hx-post=\"").append(controllers.tools.routes.ChatbotController.deleteUserProfileEntry(dsId, entry.getKey())).append("\" ")
						.append("hx-target=\"closest .user-memory-container\" ")
						.append("hx-confirm=\"Forget memory for topic '").append(escapeHtml(entry.getKey())).append("'?\">❌</button>")
						.append("</td>");
				html.append("</tr>");
			}
			html.append("</tbody></table>");
		}

		html.append("<h6 style=\"margin-top: 12px; margin-bottom: 6px;\">Saved Notes & Files</h6>");
		if (snapshot.userFiles().isEmpty()) {
			html.append("<p style=\"color: #94a3b8; font-style: italic;\">No private notes files created yet.</p>");
		} else {
			for (Map.Entry<String, String> entry : snapshot.userFiles().entrySet()) {
				html.append("<details style=\"margin-bottom: 8px; border: 1px solid #e2e8f0; border-radius: 4px; padding: 6px;\">");
				html.append("<summary style=\"cursor: pointer; font-weight: 600; display: flex; justify-content: space-between; align-items: center;\">");
				html.append("<span>📄 ").append(escapeHtml(entry.getKey())).append("</span>");
				html.append("<button type=\"button\" class=\"outline danger\" style=\"padding: 2px 8px; font-size: 0.75rem; border-radius: 4px; cursor: pointer;\" ")
						.append("hx-post=\"").append(controllers.tools.routes.ChatbotController.deleteUserFile(dsId, entry.getKey())).append("\" ")
						.append("hx-target=\"closest .user-memory-container\" ")
						.append("hx-confirm=\"Are you sure you want to delete file '").append(escapeHtml(entry.getKey())).append("'?\" ")
						.append("onclick=\"event.stopPropagation();\">🗑️ Delete</button>");
				html.append("</summary>");
				html.append("<pre style=\"background: #f8fafc; padding: 8px; margin-top: 6px; font-size: 0.8rem; max-height: 200px; overflow-y: auto;\">")
						.append(escapeHtml(entry.getValue())).append("</pre>");
				html.append("</details>");
			}
		}

		html.append("<div style=\"margin-top: 20px; border-top: 1px solid #e2e8f0; padding-top: 12px; text-align: right;\">");
		html.append("<button type=\"button\" class=\"outline secondary\" style=\"padding: 4px 12px; font-size: 0.8rem;\" ");
		html.append("hx-post=\"").append(controllers.tools.routes.ChatbotController.resetUserMemory(dsId)).append("\" ");
		html.append("hx-target=\"closest .user-memory-container\" ");
		html.append("hx-confirm=\"Are you sure you want to reset all your stored memory and notes for this bot?\">");
		html.append("🗑️ Reset All Memory & Files</button>");
		html.append("</div>");
		html.append("</div>");

		return html.toString();
	}

	@Authenticated(UserAuth.class)
	public Result getUserMemory(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound();
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		return ok(renderUserMemoryHtml(cpds, user.getEmail(), ds.getId(), null)).as("text/html");
	}

	@Authenticated(UserAuth.class)
	public Result resetUserMemory(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound();
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		ChatbotMemoryUtils.resetUserMemory(cpds.getFolder(), user.getEmail());
		return ok(renderUserMemoryHtml(cpds, user.getEmail(), ds.getId(), "✓ All memory facts and files have been cleared."))
				.as("text/html");
	}

	@Authenticated(UserAuth.class)
	public Result deleteUserFile(Request request, long id, String filename) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound();
		}

		String targetFile = filename;
		if (targetFile == null || targetFile.trim().isEmpty()) {
			DynamicForm df = formFactory.form().bindFromRequest(request);
			targetFile = df.get("filename");
		}
		if (targetFile == null || targetFile.trim().isEmpty()) {
			return badRequest("Filename is required");
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		String res = ChatbotMemoryUtils.deleteUserFile(cpds.getFolder(), user.getEmail(), targetFile);
		String banner = res.startsWith("Success:") ? "✓ File '" + targetFile + "' deleted." : res;
		return ok(renderUserMemoryHtml(cpds, user.getEmail(), ds.getId(), banner)).as("text/html");
	}

	@Authenticated(UserAuth.class)
	public Result deleteUserProfileEntry(Request request, long id, String topic) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound();
		}

		String targetTopic = topic;
		if (targetTopic == null || targetTopic.trim().isEmpty()) {
			DynamicForm df = formFactory.form().bindFromRequest(request);
			targetTopic = df.get("topic");
		}
		if (targetTopic == null || targetTopic.trim().isEmpty()) {
			return badRequest("Topic is required");
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		boolean ok = ChatbotMemoryUtils.deleteUserProfileEntry(cpds.getFolder(), user.getEmail(), targetTopic);
		String banner = ok ? "✓ Memory for topic '" + targetTopic + "' deleted." : "Error deleting topic";
		return ok(renderUserMemoryHtml(cpds, user.getEmail(), ds.getId(), banner)).as("text/html");
	}

	private static String escapeHtml(String text) {
		if (text == null) {
			return "";
		}
		return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")
				.replace("'", "&#39;");
	}

	private static String stripHtml(String html) {
		if (html == null) {
			return "";
		}
		return html.replaceAll("<[^>]*>", "").replaceAll("\\s+", " ").trim();
	}

	private void storeChatSession(CompleteDS cpds, String conversationId, ConversationHistory ch) {
		File tempFile = null;
		try {
			String chatFileName = "chat_" + conversationId + ".json";
			String json = Json.toJson(ch).toPrettyString();
			tempFile = File.createTempFile("chat", ".json");
			Files.writeString(tempFile.toPath(), json);

			Optional<String> storedFile = cpds.storeFile(tempFile, chatFileName);
			if (storedFile.isPresent()) {
				cpds.addRecord(chatFileName, "Chat session " + conversationId, new Date());
			}
		} catch (Exception e) {
			logger.error("Error storing chat session", e);
		} finally {
			if (tempFile != null) {
				tempFile.delete();
			}
		}
	}

	/**
	 * shows the test view
	 * 
	 * @param request
	 * @param dsId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result test(Request request, long dsId, String conversationId) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		// if there is no conversation, immediately redirect to a new one
		if (conversationId.isEmpty()) {
			return redirect(controllers.tools.routes.ChatbotController.test(dsId, UUID.randomUUID().toString()));
		}

		// check dataset and allow access if
		Dataset ds = Dataset.find.byId(dsId);
		if (ds == null || !(ds.editableBy(user)
				|| !ds.getConfiguration().getOrDefault(Dataset.WEB_ACCESS_TOKEN, "").trim().isEmpty())) {
			return redirect(routes.ChatbotController.index());
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		List<TimedMedia> rawFiles = cpds != null ? cpds.getFiles() : Collections.emptyList();
		List<FileWithIndexStatus> filesWithStatus = rawFiles.stream()
				.map(file -> new FileWithIndexStatus(file, hasIndexFile(cpds, file))).collect(Collectors.toList());

		// show the chat interface for this chatbot
		return ok(views.html.tools.chatbots.test.render(user, ds, conversationId, csrfToken(request), filesWithStatus));
	}

	/**
	 * generate one chat response based on system prompt and submitted user prompt
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> testProcess(Request request, long id, String conversationId) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		return CompletableFuture.supplyAsync(() -> {

			Dataset ds = Dataset.find.byId(id);
			if (ds == null || !ds.editableBy(user)) {
				return redirect(routes.ChatbotController.index());
			}

			DynamicForm df = formFactory.form().bindFromRequest(request);
			String originalUserPrompt = (String) df.value(Dataset.CHATBOT_USER_PROMPT)
					.orElseGet(() -> ds.configuration(Dataset.CHATBOT_USER_PROMPT, ""));

			// process the chat history and context to obtain the next chat item
			ConversationFragment resultFragment = internalChatProcess(conversationId, user, ds, originalUserPrompt);

			if (resultFragment.response() == null) {
				return ok("""
						<div class="msg-left">
						<p class="role">system</p>
						<article>%s</article>
						</div>""".formatted("We have encountered a problem. Perhaps try again later."));
			}

			RequestScopeTracker tracker = resultFragment.tracker();
			StringBuilder traceHtml = new StringBuilder();
			if (tracker != null && (!tracker.getTraceSteps().isEmpty() || !tracker.getMemoryActivities().isEmpty())) {
				traceHtml.append("<hr><div role=\"trace\" style=\"background: #f8fafc; border: 1px solid #cbd5e1; border-radius: 6px; padding: 12px; margin-bottom: 12px;\">");
				traceHtml.append("<div style=\"font-weight: 700; color: #0f172a; margin-bottom: 8px;\">🤖 AgentScope Execution Trace</div>");

				if (!tracker.getMemoryActivities().isEmpty()) {
					traceHtml.append("<div style=\"margin-bottom: 8px;\">");
					for (String act : tracker.getMemoryActivities()) {
						traceHtml.append("<span style=\"display: inline-block; background: #e0f2fe; color: #0369a1; border-radius: 12px; padding: 2px 8px; font-size: 0.75rem; margin-right: 6px;\">🧠 ")
								.append(escapeHtml(act)).append("</span>");
					}
					traceHtml.append("</div>");
				}

				if (!tracker.getTraceSteps().isEmpty()) {
					traceHtml.append("<details open><summary style=\"cursor: pointer; font-weight: 600;\">Steps & Tool Calls (").append(tracker.getTraceSteps().size()).append(")</summary>");
					traceHtml.append("<ul style=\"margin: 6px 0 0 0; padding-left: 20px; font-family: monospace; font-size: 0.8rem;\">");
					for (ExecutionTraceStep step : tracker.getTraceSteps()) {
						traceHtml.append("<li style=\"margin-bottom: 4px;\">");
						traceHtml.append("<strong style=\"color: #2563eb;\">[").append(escapeHtml(step.name())).append("]</strong> ");
						traceHtml.append("<span style=\"color: #475569;\">").append(escapeHtml(step.details())).append("</span>");
						if (step.result() != null && !step.result().isEmpty()) {
							traceHtml.append(" &rarr; <span style=\"color: #059669;\">").append(escapeHtml(step.result())).append("</span>");
						}
						traceHtml.append("</li>");
					}
					traceHtml.append("</ul></details>");
				}
				traceHtml.append("</div>");
			}

			// generate richer output for the testing
			ConversationItem prompt = resultFragment.prompt();
			final String input;
			if (prompt != null) {

				String context = (prompt.context() == null || prompt.context().isEmpty()) ? "-"
						: prompt.context().stream()
								.filter(cc -> cc != null && cc.content() != null && !cc.content().trim().isEmpty())
								.map(cc -> """
										<details>
											<summary>%s</summary>
											<pre>%s</pre>
										</details>
										""".formatted(cc.toString(), cc.content())).collect(Collectors.joining());
				if (context.isEmpty()) {
					context = "-";
				}

				input = """
						<hr>
						<div role="prompt">
						<div class="user">
						<span class="role">user prompt</span>
						<article>%s</article>
						</div>
						<div>
						<span class="internal">processed prompt</span>
						<article>%s</article>
						</div>
						<div>
						<span class="internal">prompt context</span>
						<article>%s</article>
						</div>
						</div>
						""".formatted(prompt.renderedContent(), prompt.content(), context);
			} else {
				input = "";
			}

			ConversationItem response = resultFragment.response();
			String context = (response.context() == null || response.context().isEmpty()) ? "-"
					: response.context().stream()
							.filter(cc -> cc != null && cc.content() != null && !cc.content().trim().isEmpty())
							.map(cc -> """
									<details>
										<summary>messages to LLM</summary>
										<pre>%s</pre>
									</details>
									""".formatted(cc.content())).collect(Collectors.joining());
			if (context.isEmpty()) {
				context = "-";
			}

			String turnContent = traceHtml.toString() + input + """
					<hr>
					<div role="response">
					<div>
					<span class="internal">internal messages</span>
					<article>%s</article>
					</div>
					<div class="assistant">
					<span class="role">assistant</span>
					<article>%s</article>
					</div>
					</div>
					""".formatted(context, response.renderedContent());

			String promptSnippet = stripHtml(originalUserPrompt);
			if (promptSnippet.length() > 45) {
				promptSnippet = promptSnippet.substring(0, 42) + "...";
			}
			String assistantSnippet = stripHtml(response.content());
			if (assistantSnippet.length() > 65) {
				assistantSnippet = assistantSnippet.substring(0, 62) + "...";
			}

			String wrappedTurn = """
					<details class="chat-turn" open style="margin-bottom: 16px; border: 1px solid #cbd5e1; border-radius: 6px; padding: 10px 14px; background: #ffffff;">
						<summary style="cursor: pointer; font-weight: 600; color: #1e293b; outline: none; padding: 4px 0;">
							💬 <strong>User:</strong> %s &nbsp;&bull;&nbsp; 🤖 <strong>Assistant:</strong> %s
						</summary>
						<div class="chat-turn-content" style="margin-top: 10px;">
							%s
						</div>
					</details>
					""".formatted(escapeHtml(promptSnippet), escapeHtml(assistantSnippet), turnContent);

			return ok(wrappedTurn);
		}, databaseExecutionContext);
	}

	/**
	 * shows the chat view
	 * 
	 * @param request
	 * @param dsId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result chat(Request request, long dsId, String conversationId) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		// if there is no conversation, immediately redirect to a new one
		if (conversationId.isEmpty()) {
			return redirect(controllers.tools.routes.ChatbotController.chat(dsId, UUID.randomUUID().toString()));
		}

		// check dataset and allow access if
		Dataset ds = Dataset.find.byId(dsId);
		if (ds == null || !(ds.editableBy(user)
				|| !ds.getConfiguration().getOrDefault(Dataset.CHATBOT_PUBLIC, "").trim().isEmpty())) {
			return redirect(routes.ChatbotController.index());
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

		// retrieve conversation history from cache or create new
		ConversationHistory ch;
		Optional<ConversationHistory> chOpt = cache.get(getChatCacheKey(dsId, conversationId));
		if (!chOpt.isPresent()) {
			ch = new ConversationHistory(conversationId, new CopyOnWriteArrayList<ConversationItem>());
			// add start prompt
			String assistantStartPrompt = ds.getConfiguration().getOrDefault(Dataset.CHATBOT_ASSISTANT_PROMPT, "")
					.trim();
			if (!assistantStartPrompt.isEmpty()) {
				ch.items().add(new ConversationItem(ConversationItem.ASSISTANT, assistantStartPrompt, "",
						assistantStartPrompt));
			}
		} else {
			ch = chOpt.get();
		}

		cache.set(getChatCacheKey(dsId, conversationId), ch, 3600);

		List<ChatbotMemoryUtils.UserSessionSummary> userSessions = Collections.emptyList();
		if ("true".equals(ds.configuration(Dataset.CHATBOT_ENABLE_MULTISESSION, "false")) && cpds != null) {
			userSessions = ChatbotMemoryUtils.loadUserSessions(cpds.getFolder(), user.getEmail());
		}

		// show the chat interface for this chatbot
		return ok(views.html.tools.chatbots.chat.render(user, ds, conversationId, ch, userSessions, csrfToken(request)));
	}

	/**
	 * generate one chat response based on system prompt and submitted user prompt
	 * 
	 * @param request
	 * @param id
	 * @param conversationId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> chatProcess(Request request, long id, String conversationId) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		return CompletableFuture.supplyAsync(() -> {
			// access dataset, check permissions
			Dataset ds = Dataset.find.byId(id);
			if (ds == null || !(ds.editableBy(user)
					|| !ds.getConfiguration().getOrDefault(Dataset.CHATBOT_PUBLIC, "").trim().isEmpty())) {
				return noContent();
			}

			// retrieve user prompt
			DynamicForm df = formFactory.form().bindFromRequest(request);
			String originalUserPrompt = (String) df.value(Dataset.CHATBOT_USER_PROMPT)
					.orElseGet(() -> ds.configuration(Dataset.CHATBOT_USER_PROMPT, ""));

			// process the chat history and context to obtain the next chat item
			ConversationFragment resultFragment = internalChatProcess(conversationId, user, ds, originalUserPrompt);

			String responseHtml = resultFragment.response() != null ? resultFragment.response().renderedContent()
					: "We have encountered a problem. Perhaps try again later.";

			RequestScopeTracker tracker = resultFragment.tracker();
			if (tracker != null && !tracker.getMemoryActivities().isEmpty()) {
				StringBuilder badges = new StringBuilder("<div class=\"memory-activity-chips\" style=\"margin-bottom: 8px;\">");
				for (String act : tracker.getMemoryActivities()) {
					badges.append("<span class=\"memory-activity-chip\">🧠 ").append(escapeHtml(act)).append("</span> ");
				}
				badges.append("</div>");
				responseHtml = badges.toString() + responseHtml;
			}

			if ("true".equals(ds.configuration(Dataset.CHATBOT_SHOW_SOURCES, "false"))
					&& resultFragment.prompt() != null) {
				List<ConversationContext> contexts = resultFragment.prompt().context();
				if (!contexts.isEmpty() && contexts.get(0).ranking() > 0) { // ranking > 0 to avoid empty/dummy contexts
					StringBuilder refs = new StringBuilder(
							"<div class=\"sources-refs\" style=\"margin-top: 10px; border-top: 1px solid #ccc; padding-top: 5px; font-size: 0.8rem;\">Sources: ");
					for (int i = 0; i < contexts.size(); i++) {
						ConversationContext ctx = contexts.get(i);
						if (ctx.document() == null || ctx.document().isEmpty())
							continue;

						// Render markdown for the modal with HTML escaping enabled (DF-23)
						String renderedSource = new MarkdownRenderer(true).render(ctx.content());
						String b64Content = java.util.Base64.getEncoder().encodeToString(
								renderedSource.getBytes(java.nio.charset.StandardCharsets.UTF_8));
						String b64Title = java.util.Base64.getEncoder().encodeToString(
								ctx.document().getBytes(java.nio.charset.StandardCharsets.UTF_8));

						refs.append(String.format(
								"<a href=\"#\" data-content=\"%s\" data-title=\"%s\" onclick=\"showSourceFromEl(this); return false;\">[%d]</a> ",
								b64Content, b64Title, i + 1));
					}
					refs.append("</div>");
					responseHtml += refs.toString();
				}
			}

			return ok("""
					<div class="msg-left">
					<p class="role">assistant</p>
					<article>%s</article>
					</div>""".formatted(responseHtml));
		}, databaseExecutionContext);
	}

	/**
	 * OpenAI-compatible chatbot API
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public CompletionStage<Result> chatApi(Request request, long id) {
		return CompletableFuture.supplyAsync(() -> {
			// 1. Extract Bearer token
			String authHeader = request.header("Authorization").orElse("");
			if (authHeader.isEmpty() || !authHeader.startsWith("Bearer ")) {
				return unauthorized(Json.newObject().put("error", "Missing or invalid Authorization header"));
			}
			String token = authHeader.substring(7);

			// 2. Validate token and get project ID
			Long tokenProjectId = tokenResolver.getProjectIdFromParticipationToken(token.replace("df-", ""));
			if (tokenProjectId == -1L) {
				Long dsId = tokenResolver.getDatasetIdFromToken(token);
				if (dsId != -1L && dsId == id) {
					Dataset d = Dataset.find.byId(dsId);
					if (d != null) {
						tokenProjectId = d.getProject().getId();
					}
				}
			}
			if (tokenProjectId == -1L) {
				return unauthorized(Json.newObject().put("error", "Invalid API key"));
			}

			// 3. Find dataset and check permissions
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(Json.newObject().put("error", "Chatbot not found"));
			}
			if (ds.getDsType() != DatasetType.COMPLETE || ds.configuration(Dataset.CHATBOT_MODEL, "").isEmpty()) {
				return badRequest(Json.newObject().put("error", "Dataset is not a configured chatbot"));
			}
			if (ds.getProject().getId() != tokenProjectId.longValue()) {
				return forbidden(Json.newObject().put("error", "API key does not have access to this project"));
			}

			// 4. Parse JSON body
			JsonNode json = request.body().asJson();
			if (json == null || !json.has("message")) {
				return badRequest(Json.newObject().put("error", "Expecting JSON with 'message' field"));
			}
			String message = json.get("message").asText();
			String conversationId = json.has("conversationId") ? json.get("conversationId").asText()
					: UUID.randomUUID().toString();

			// 5. Process chat
			// Use project owner as the "user" for processing
			Person owner = ds.getProject().getOwner();
			ConversationFragment resultFragment = internalChatProcess(conversationId, owner, ds, message);

			if (resultFragment.response() == null) {
				return internalServerError(Json.newObject().put("error", "Failed to generate response"));
			}

			// 6. Format response (OpenAI-compatible)
			ObjectNode response = Json.newObject();
			response.put("id", "chatcmpl-" + UUID.randomUUID().toString().substring(0, 8));
			response.put("object", "chat.completion");
			response.put("created", System.currentTimeMillis() / 1000);
			response.put("model", ds.configuration(Dataset.CHATBOT_MODEL, "unknown"));
			response.put("conversationId", conversationId);

			ArrayNode choices = response.putArray("choices");
			ObjectNode choice = choices.addObject();
			choice.put("index", 0);
			ObjectNode msg = choice.putObject("message");
			msg.put("role", "assistant");
			msg.put("content", resultFragment.response().content());
			choice.put("finish_reason", "stop");

			ObjectNode usage = response.putObject("usage");
			usage.put("total_tokens", 0); // Placeholder

			return ok(response);
		}, databaseExecutionContext);
	}

	/**
	 * API endpoint to upload a file to a chatbot knowledge base
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public CompletionStage<Result> uploadFileApi(Request request, long id) {
		return CompletableFuture.supplyAsync(() -> {
			// 1. Find dataset and check permissions
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(Json.newObject().put("error", "Chatbot not found"));
			}

			// 2. Validate dataset type (must be COMPLETE)
			if (ds.getDsType() != DatasetType.COMPLETE) {
				return badRequest(Json.newObject().put("error", "Dataset is not a complete dataset"));
			}

			if (!ds.canAppend()) {
				return forbidden(Json.newObject().put("error", "Dataset is not active"));
			}

			// 3. Authenticate: caller must have project edit rights (via session or user API token)
			// or provide the dataset's specific API token. Participation tokens are not allowed.
			boolean authorized = false;

			// Check session user first
			Optional<Person> userOpt = getAuthenticatedUser(request);
			if (userOpt.isPresent() && ds.getProject().editableBy(userOpt.get())) {
				authorized = true;
			}

			// If not authorized by session, check token (Bearer or api_token header)
			if (!authorized) {
				String token = "";
				String authHeader = request.header("Authorization").orElse("");
				if (authHeader.startsWith("Bearer ")) {
					token = authHeader.substring(7).trim();
				} else if (!authHeader.isEmpty()) {
					token = authHeader.trim();
				} else {
					token = request.header(Dataset.API_TOKEN).orElse(request.header("api_token").orElse("")).trim();
				}

				if (!token.isEmpty()) {
					// Check against dataset API token
					String configuredToken = ds.configuration(Dataset.API_TOKEN, "");
					if (!configuredToken.isEmpty() && java.security.MessageDigest.isEqual(
							token.getBytes(java.nio.charset.StandardCharsets.UTF_8),
							configuredToken.getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
						authorized = true;
					} else {
						// Check against user access token
						Long userId = tokenResolver.retrieveUserIdFromUserAccessToken(token);
						Long tokenTimeout = tokenResolver.retrieveTimeoutFromUserAccessToken(token);
						if (userId != -1L && tokenTimeout >= System.currentTimeMillis()) {
							Person user = Person.find.byId(userId);
							if (user != null && ds.getProject().editableBy(user)) {
								authorized = true;
							}
						}
					}
				}
			}

			if (!authorized) {
				return unauthorized(Json.newObject().put("error",
						"Unauthorized: upload requires project edit access or dataset API token"));
			}

			final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

			try {
				Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
				if (body == null) {
					return badRequest(Json.newObject().put("error", "Expecting multipart form data"));
				}

				DynamicForm df = formFactory.form().bindFromRequest(request);
				String description = df != null ? nss(df.get("description")) : "";

				List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
				if (fileParts.isEmpty()) {
					return badRequest(Json.newObject().put("error", "No files provided"));
				}

				int successCount = 0;
				for (Http.MultipartFormData.FilePart<TemporaryFile> filePart : fileParts) {
					if (internalUploadKnowledgeBaseFile(ds, cpds, filePart, description)) {
						successCount++;
					}
				}

				if (successCount > 0) {
					indexAllDocuments(cpds);
					return ok(Json.newObject().put("status", "success").put("files_uploaded", successCount));
				} else {
					return badRequest(Json.newObject().put("error", "Failed to process files"));
				}
			} catch (Exception e) {
				logger.error("Error uploading API file", e);
				return internalServerError(Json.newObject().put("error", "Internal server error"));
			}
		}, databaseExecutionContext);
	}

	private ConversationFragment internalChatProcess(String conversationId, Person user, Dataset ds,
			String originalUserPrompt) {

		// check whether the chatbot owner has enough tokens, if not quick abort
		ProjectAPIInfo pai = aiAPIService.getProjectAPIAccess(ds.getProject().getOwner(), ds.getProject());
		if (pai.apiKey.isEmpty()) {
			return new ConversationFragment(new ConversationItem("user", "", "", ""),
					new ConversationItem("assistant", "", "",
							"You need an API key or more tokens for your <a href=\"%s#api-access\">API key</a>"
									.formatted(controllers.routes.ProjectsController.edit(ds.getProject().getId()))));
		}

		// retrieve conversation history from cache or create new
		ConversationHistory ch;
		Optional<ConversationHistory> chOpt = cache.get(getChatCacheKey(ds.getId(), conversationId));
		if (!chOpt.isPresent()) {
			ch = new ConversationHistory(conversationId, new CopyOnWriteArrayList<ConversationItem>());
		} else {
			ch = chOpt.get();
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		if (cpds == null) {
			logger.error("CompleteDS object is null for dataset ID: {}", ds.getId());
			return new ConversationFragment(new ConversationItem("user", originalUserPrompt, "", ""),
					new ConversationItem("assistant", "", "",
							"An internal error occurred: Could not retrieve dataset information."));
		}

		boolean isAgentic = "true".equals(ds.configuration(Dataset.CHATBOT_ENABLE_AGENTIC, "false"));

		if (isAgentic) {
			RequestScopeTracker tracker = new RequestScopeTracker();
			CURRENT_TRACKER.set(tracker);
			try {
				ChatbotAgentContext context = agentContexts.computeIfAbsent(ds.getId(),
						k -> new ChatbotAgentContext(null, null, cpds, ds));
				checkAndReloadAgent(context, ds, cpds);
				HarnessAgent agent = context.agent();
				if (agent == null) {
					throw new IllegalStateException("HarnessAgent could not be initialized");
				}

				String userPromptWithProfile = originalUserPrompt;
				if ("true".equals(ds.configuration(Dataset.CHATBOT_ENABLE_USER_MEMORY, "true"))) {
					String profile = ChatbotMemoryUtils.loadUserProfile(cpds.getFolder(), user.getEmail());
					if (!profile.isEmpty()) {
						userPromptWithProfile = "[User Profile / Memory Notes]:\n" + profile + "\n\n"
								+ originalUserPrompt;
					}
				}

				Msg input = Msg.builder().role(MsgRole.USER).name("User")
						.textContent(userPromptWithProfile).build();

				RuntimeContext runtimeCtx = RuntimeContext.builder().userId(user.getEmail()).sessionId(conversationId)
						.build();

				Msg responseMsg = agent.call(input, runtimeCtx).block();

				String rawResult = responseMsg != null ? responseMsg.getTextContent() : "";
				String cleanedResult = CodingAgentUtils.cleanThinkingTags(rawResult);

				List<ConversationContext> contexts = tracker.getCitations().stream()
						.map(sr -> new ConversationContext(sr.content(), sr.file(), sr.score()))
						.collect(Collectors.toList());

				ConversationItem promptItem = new ConversationItem(ConversationItem.USER, originalUserPrompt,
						contexts, originalUserPrompt);
				ConversationItem responseItem = new ConversationItem(ConversationItem.ASSISTANT, cleanedResult, "",
						new MarkdownRenderer(true).render(cleanedResult));

				synchronized (ch) {
					ch.items().add(promptItem);
					ch.items().add(responseItem);
					cache.set(getChatCacheKey(ds.getId(), conversationId), ch, 3600);
				}

				if ("true".equals(ds.configuration(Dataset.CHATBOT_STORE_CHATS, "false"))) {
					storeChatSession(cpds, conversationId, ch);
				}

				if ("true".equals(ds.configuration(Dataset.CHATBOT_ENABLE_MULTISESSION, "false"))) {
					ChatbotMemoryUtils.saveUserSession(cpds.getFolder(), user.getEmail(), conversationId,
							originalUserPrompt);
				}

				return new ConversationFragment(promptItem, responseItem, tracker);
			} catch (Exception e) {
				logger.error("Error executing agentic chat process for dataset {}", ds.getId(), e);
				ConversationItem errorPrompt = new ConversationItem(ConversationItem.USER, originalUserPrompt,
						Collections.emptyList(), originalUserPrompt);
				ConversationItem errorResponse = new ConversationItem(ConversationItem.ASSISTANT, "", "",
						"An error occurred while communicating with the agent: " + e.getMessage());
				return new ConversationFragment(errorPrompt, errorResponse, tracker);
			} finally {
				CURRENT_TRACKER.remove();
			}
		}

		// Legacy execution path (non-agentic)
		// if there is a conversation history, run a quick request to reformulate the user prompt in context of the
		// history
		boolean hasUserMessage = ch.items().stream().anyMatch(ConversationItem::isUser);
		String userPrompt = !hasUserMessage ? originalUserPrompt
				: reformulateUserPromptWithHistory(user.getEmail(), ds.getProject().getId(), pai.apiKey,
						ds.configuration(Dataset.CHATBOT_MODEL, ""), originalUserPrompt, ch).orElse(originalUserPrompt);

		Collection<SearchResult> searchResults;
		if (!cpds.getFiles().isEmpty()) {
			int max_hits = DataUtils.parseInt(ds.configuration(Dataset.CHATBOT_RAG_MAX_HITS, "10"), 10);
			float score_threshold = DataUtils.parseFloat(ds.configuration(Dataset.CHATBOT_RAG_SCORE_THRESHOLD, "0.7"),
					0.7f);
			// search knowledge base for interesting hits
			searchResults = searchIndex(cpds, userPrompt, max_hits, score_threshold);
		} else {
			// otherwise empty sources
			searchResults = Collections.emptyList();
		}

		// append search results to prompt
		List<ConversationContext> contexts = searchResults.stream()
				.map(sr -> new ConversationContext(sr.content(), sr.file(), sr.score())).collect(Collectors.toList());

		final ConversationItem promptItem = new ConversationItem(ConversationItem.USER, userPrompt, contexts,
				originalUserPrompt);

		// prepare the request to generate the conversation item
		ObjectNode requestJson = Json.newObject();
		requestJson.put(ApiServiceConstants.REQUEST_TASK, ApiServiceConstants.REQUEST_TASK_CHAT_COMPLETION);
		requestJson.put(ApiServiceConstants.REQUEST_API_TOKEN, pai.apiKey);
		requestJson.put(ApiServiceConstants.REQUEST_MODEL, ds.configuration(Dataset.CHATBOT_MODEL, ""));
		float temperature = DataUtils.parseFloat(ds.configuration(Dataset.CHATBOT_TEMPERATURE, "0.7"), 0.7f);
		requestJson.put(ApiServiceConstants.REQUEST_TEMPERATURE, temperature);
		requestJson.put(ApiServiceConstants.REQUEST_MAX_TOKENS, 1000);
		// prepare messages
		ArrayNode messages = requestJson.putArray(ApiServiceConstants.REQUEST_MESSAGES);
		// add system prompt
		String systemPrompt = ds.configuration(Dataset.CHATBOT_SYSTEM_PROMPT, "");
		systemPrompt = systemPrompt.replace("$DATE",
				SimpleDateFormat.getDateInstance(SimpleDateFormat.SHORT).format(new Date()));
		messages.add(Json.newObject().put("role", ConversationItem.SYSTEM).put("content", systemPrompt));
		// add conversation history
		for (ConversationItem item : ch.items()) {
			messages.add(Json.newObject().put("role", item.actor()).put("content", item.content()));
		}
		// add user prompt
		messages.add(Json.newObject().put("role", ConversationItem.USER).put("content", promptItem.fullPrompt()));

		// AFTER completing messages, add to history and story history in cache
		synchronized (ch) {
			ch.items().add(promptItem);
		}

		// run the LLM
		ConversationItem responseItem = null;
		try {
			final RemoteApiRequest apiRequest = new RemoteApiRequest(ApiServiceConstants.REQUEST_TASK_CHAT_COMPLETION,
					ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, user.getEmail(), pai.apiKey,
					ds.getProject().getId(), requestJson);
			String llmResult = aiAPIService.submitApiRequestSync(apiRequest);
			ObjectNode on = aiAPIService.parseChatCompletionResponse(llmResult);

			if (on.has(ApiServiceConstants.RESPONSE_ERROR)) {
				throw new RuntimeException(on.get(ApiServiceConstants.RESPONSE_ERROR).asText());
			}

			String resultAsText = on.path(ApiServiceConstants.RESPONSE_CONTENT).asText("");

			// generate response item, including the messages (HTML escaping enabled via MarkdownRenderer(true))
			responseItem = new ConversationItem(ConversationItem.ASSISTANT, resultAsText, messages.toPrettyString(),
					new MarkdownRenderer(true).render(resultAsText));

			synchronized (ch) {
				ch.items().add(responseItem);
				cache.set(getChatCacheKey(ds.getId(), conversationId), ch, 3600);
			}

			if ("true".equals(ds.configuration(Dataset.CHATBOT_STORE_CHATS, "false"))) {
				storeChatSession(cpds, conversationId, ch);
			}
		} catch (RuntimeException e) { // Catch other runtime exceptions (e.g., from MarkdownRenderer)
			logger.error("Runtime error during LLM response processing for conversation {}", conversationId, e);
			responseItem = new ConversationItem("assistant", "", "",
					"An unexpected error occurred while processing the AI response.");
		} catch (Exception e) { // General fallback for any other unexpected exception
			logger.error("An unexpected error occurred in internalChatProcess for conversation {}", conversationId, e);
			responseItem = new ConversationItem("assistant", "", "", "An unknown error occurred.");
		}
		return new ConversationFragment(promptItem, responseItem, null);
	}

	private Optional<String> reformulateUserPromptWithHistory(String user, long projectId, String apiKey, String model,
			String userPrompt, ConversationHistory ch) {
		ObjectNode requestJson = Json.newObject();
		requestJson.put(ApiServiceConstants.REQUEST_TASK, ApiServiceConstants.REQUEST_TASK_CHAT_COMPLETION);
		requestJson.put(ApiServiceConstants.REQUEST_API_TOKEN, apiKey);
		requestJson.put(ApiServiceConstants.REQUEST_MODEL, model);
		requestJson.put(ApiServiceConstants.REQUEST_MAX_TOKENS, 1000);

		// prepare messages
		ArrayNode messages = requestJson.putArray(ApiServiceConstants.REQUEST_MESSAGES);
		messages.add(Json.newObject().put("role", ConversationItem.SYSTEM).put("content",
				"You are a helpful assistant. Given the following conversation and a follow up question, rephrase the follow up question to be a standalone question, in its original language. Keep as much details as possible from previous messages. Keep entity names and all. Return ONLY the standalone question, no intro or outro."));

		// contextualize the user prompt given the chat history
		StringBuilder sb = new StringBuilder();
		for (ConversationItem item : ch.items()) {
			sb.append(item.actor());
			sb.append(": ");
			sb.append(item.content());
			sb.append("\n");
		}
		messages.add(Json.newObject().put("role", ConversationItem.USER).put("content", """
				Chat History:
				%s
				Follow Up Input: %s
				Standalone question:""".formatted(sb.toString(), userPrompt)));

		try {
			final RemoteApiRequest apiRequest = new RemoteApiRequest(ApiServiceConstants.REQUEST_TASK_CHAT_COMPLETION,
					ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, user, apiKey, projectId, requestJson);
			String result = aiAPIService.submitApiRequestSync(apiRequest);
			ObjectNode on = aiAPIService.parseChatCompletionResponse(result);

			if (on.has(ApiServiceConstants.RESPONSE_ERROR)) {
				return Optional.empty();
			}

			String resultAsText = on.path(ApiServiceConstants.RESPONSE_CONTENT).asText("");

			// check whether we have thinking tokens in the result, if so just take what's behind
			if (resultAsText.contains("final<|message|>")) {
				resultAsText = resultAsText
						.substring(resultAsText.indexOf("final<|message|>") + "final<|message|>".length());
			}

			return Optional.of(resultAsText.trim());
		} catch (Exception e) {
			return Optional.empty();
		}
	}

	static public record ConversationFragment(ConversationItem prompt, ConversationItem response,
			RequestScopeTracker tracker) {
		public ConversationFragment(ConversationItem prompt, ConversationItem response) {
			this(prompt, response, null);
		}
	}

	static public record ConversationHistory(String conversationId, List<ConversationItem> items) {
		public ConversationHistory {
			if (items == null) {
				items = new CopyOnWriteArrayList<>();
			} else if (!(items instanceof CopyOnWriteArrayList)) {
				items = new CopyOnWriteArrayList<>(items);
			}
		}
	}

	static public record ConversationItem(String actor, String content, List<ConversationContext> context,
			String renderedContent) {

		public static final String SYSTEM = "system";

		public static final String ASSISTANT = "assistant";

		public static final String USER = "user";

		public ConversationItem(String actor, String content, String context, String renderedContent) {
			this(actor, content,
					(context == null || context.trim().isEmpty()) ? Collections.emptyList()
							: Collections.singletonList(new ConversationContext(context, "", 1.0f)),
					renderedContent);
		}

		public boolean isAssistant() {
			return actor().equals(ASSISTANT);
		}

		public boolean isUser() {
			return actor().equals(USER);
		}

		public String contextStr() {
			return context() != null ? context().stream().map(cc -> cc.content()).collect(Collectors.joining()) : "";
		}

		/**
		 * reformulated user prompt + context
		 * 
		 * @return
		 */
		public String fullPrompt() {
			return (content() + (context() != null && !context().isEmpty() ? """

					Respond with the following context:
					""" + contextStr() : "")).trim();
		}
	}

	static public record ConversationContext(String content, String document, float ranking) {
		public String toString() {
			if (content == null || content.isEmpty()) {
				return "(" + (document != null ? document : "") + "): " + Math.round(ranking * 100) + "% match";
			}
			int snippetLen = Math.min(content.length(), 45);
			String snippet = content.substring(0, snippetLen);
			String ellipsis = content.length() > 45 ? "..." : "";
			String docLabel = document != null && !document.isEmpty() ? " (" + document + ")" : "";
			return snippet + ellipsis + docLabel + ": " + Math.round(ranking * 100) + "% match";
		}

	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * upload a PDF file for chatbot context
	 * 
	 * @param request
	 * @param dsId
	 * @return
	 */
	/**
	 * core logic for uploading a file to a chatbot knowledge base
	 * 
	 * @param ds
	 * @param cpds
	 * @param filePart
	 * @param description
	 * @return
	 */
	private boolean internalUploadKnowledgeBaseFile(Dataset ds, CompleteDS cpds,
			Http.MultipartFormData.FilePart<TemporaryFile> filePart, String description) {
		TemporaryFile tempFile = filePart.getRef();
		String tempFileName = nss(filePart.getFilename());

		// check filename length and shorten if needed
		if (tempFileName.length() > 60) {
			int lastDot = tempFileName.lastIndexOf(".");
			if (lastDot >= 0 && lastDot < tempFileName.length()) {
				tempFileName = tempFileName.substring(0, Math.min(50, lastDot)) + tempFileName.substring(lastDot);
			} else {
				tempFileName = tempFileName.substring(0, 60);
			}
		}

		// filename-based quick check
		if (FileTypeUtils.looksLikeExecutableFile(tempFileName)) {
			logger.error("   Document upload rejected due to executable-like filename: " + tempFileName);
			return false;
		}

		// content-based validation
		if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.KNOWLEDGE_BASE)) {
			return false;
		}

		final String fileName = tempFileName;
		// check if file exists; if so, delete it
		cpds.getFile(fileName).ifPresent((file) -> {
			logger.info("   '" + fileName + "' file exists; file and index will be deleted first");
			if (file.exists()) {
				file.delete();
			}
			cpds.deleteRecord(fileName);
			File indexFile = new File(cpds.getFolder().getAbsolutePath() + File.separator + fileName + ".idx");
			if (indexFile.exists()) {
				indexFile.delete();
			}
		});

		// store file, add record
		Optional<String> storeFile = cpds.storeFile(tempFile.path().toFile(), fileName);
		if (storeFile.isPresent()) {
			cpds.addRecord(storeFile.get(), description, new Date());

			// process the file in a document extraction queue
			// retrieve document contents
			try {
				String finalFileName = storeFile.get();
				String detectedMime = FileTypeUtils.detectMime(tempFile.path().toFile());
				String contents = mediaProcessingService.scheduleMediaToTextProcess(tempFile.path().toFile(), "en",
						detectedMime, "SYSTEM", UUID.randomUUID().toString()).toCompletableFuture().get();

				ArrayNode documentIndex = Json.newArray();

				// chunk the document in paragraphs and reproduce them as a list with related headers
				List<String> chunks = produceChunks(contents);
				logger.info("   " + chunks.size() + " chunks for '" + finalFileName + "' in "
						+ cpds.getFolder().getAbsolutePath());

				// process chunks to embeddings and store them in file
				List<List<Double>> embeddings = aiAPIService.dispatchEmbeddingRequest("SYSTEM", chunks);
				int counter = 0;
				for (String str : chunks) {
					ArrayNode ar = Json.newArray();
					embeddings.get(counter++).forEach(d -> {
						ar.add(d.floatValue());
					});
					documentIndex
							.add(Json.newObject().put("file", finalFileName).put("content", str).set("embedding", ar));
				}

				// write the index to disk
				File indexFile = new File(cpds.getFolder().getAbsolutePath() + File.separator + finalFileName + ".idx");
				if (indexFile.exists()) {
					indexFile.delete();
				}
				Files.writeString(indexFile.toPath(), documentIndex.toString());

				logger.info("   Chunks complete for '" + finalFileName + "' in " + cpds.getFolder().getAbsolutePath());
				return true;
			} catch (InterruptedException | ExecutionException | IOException e) {
				logger.error("Error processing knowledge base file", e);
			}
		}
		return false;
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public CompletionStage<Result> uploadFile(Request request, long dsId) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		return CompletableFuture.supplyAsync(() -> {
			// check dataset
			Dataset ds = Dataset.find.byId(dsId);
			if (ds == null || !ds.editableBy(user.getEmail())) {
				return forbidden("Dataset not accessible");
			}

			final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

			// standard upload similar to complete ds
			try {
				Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
				if (body == null) {
					return badRequest("Bad request");
				}

				DynamicForm df = formFactory.form().bindFromRequest(request);
				if (df == null) {
					return badRequest("Expecting some data");
				}

				List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
				if (!fileParts.isEmpty()) {

					for (int i = 0; i < fileParts.size(); i++) {
						FilePart<TemporaryFile> filePart = fileParts.get(i);
						internalUploadKnowledgeBaseFile(ds, cpds, filePart, nss(df.get("description")));
					}

					LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.DATA,
							"Files uploaded to dataset: " + ds.getName(), ds.getProject());
				}
			} catch (NullPointerException e) {
				logger.error("Error uploading file to dataset.", e);
				systemNotifications.send("Exception", e.getLocalizedMessage());
			}

			// re-index all documents
			indexAllDocuments(cpds);

			return view(request, dsId);
		}, databaseExecutionContext);
	}

	/**
	 * delete a previously uploaded file
	 * 
	 * @param request
	 * @param dsId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public CompletionStage<Result> deleteFile(Request request, long dsId, long fileId) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		return CompletableFuture.supplyAsync(() -> {
			// check dataset
			Dataset ds = Dataset.find.byId(dsId);
			if (ds == null || !ds.editableBy(user.getEmail())) {
				return forbidden("Dataset not accessible");
			}

			// delete document

			// retrieve filename before actual deletion
			final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
			Optional<String> filenameOpt = cpds.getFileName(fileId);

			// now delete the file itself
			completeDSController.delete(request, dsId, fileId);

			// delete the index file as well
			if (filenameOpt.isPresent()) {
				String filename = filenameOpt.get();
				File indexFile = new File(cpds.getFolder().getAbsolutePath() + File.separator + filename + ".idx");
				if (indexFile.exists()) {
					indexFile.delete();
				}
			}

			// re-index all documents
			indexAllDocuments(cpds);

			return ok("");
		}, databaseExecutionContext);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * produce chunks for an uploaded document
	 * 
	 * @param content
	 * @return
	 */
	private List<String> produceChunks(String content) {
		List<String> headerTokens = List.of("# ", "## ", "### ", "#### ");
		String[] lines = content.split("\n");
		List<String> headers = new ArrayList<String>();
		StringBuilder currentChunk = new StringBuilder();
		ArrayList<String> result = new ArrayList<String>();
		for (String line : lines) {
			var header = headerTokens.stream().filter(line::startsWith).findFirst();
			if (header.isPresent()) {
				String readyChunk = currentChunk.toString().trim();
				if (!readyChunk.isEmpty()) {
					result.add(String.join("\n", headers) + readyChunk);
				}
				currentChunk.setLength(0);
				var level = headerTokens.indexOf(header.get());
				// Drop headers that are deeper than the current level
				while (level < headers.size()) {
					headers.remove(headers.size() - 1);
				}
				headers.add(line + "\n");
			} else {
				// check if the total length of the chunk exceeds 500 chars
				if (currentChunk.length() > 500) {
					// add chunk to result
					String readyChunk = currentChunk.toString().trim();
					if (!readyChunk.isEmpty()) {
						result.add(String.join("\n", headers) + readyChunk);
					}
					currentChunk.setLength(0);
				} else {
					currentChunk.append(line).append("\n");
				}
			}
		}

		// add chunk to result
		String readyChunk = currentChunk.toString().trim();
		if (!readyChunk.isEmpty()) {
			result.add(String.join("\n", headers) + readyChunk);
		}
		return result;
	}

	/**
	 * run the indexing process for all active documents
	 * 
	 * @param cpds
	 */
	private void indexAllDocuments(CompleteDS cpds) {
		logger.info("Indexing all document chunks...");

		try (FSDirectory indexDirectory = FSDirectory.open(getSearchIndexDir(cpds));
				StandardAnalyzer analyzer = new StandardAnalyzer();) {
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			try (IndexWriter indexWriter = new IndexWriter(indexDirectory, config)) {
				// find all relevant context
				List<TimedMedia> files = cpds.getFiles();
				for (TimedMedia timedMedia : files) {
					File originalFile = new File(cpds.getFolder() + File.separator + timedMedia.link);
					File indexFile = new File(cpds.getFolder() + File.separator + timedMedia.link + ".idx");
					if (originalFile.exists() && indexFile.exists() && indexFile.length() > 1000) {
						try {
							String contents = Files.readString(indexFile.toPath());
							JsonNode jn = Json.parse(contents);
							if (jn.isArray()) {
								// let use the info in the file for real
								ArrayNode ar = (ArrayNode) jn;
								ar.forEach((jo) -> {

									// check contents
									if (!jo.has("content") || !jo.has("file") || !jo.has("embedding")) {
										return;
									}

									// index document with both the original text and its embedding, and the file for
									// referencing
									Document document = new Document();
									document.add(new TextField("content", jo.get("content").asText(), Field.Store.YES));
									document.add(new TextField("file", jo.get("file").asText(), Field.Store.YES));
									ArrayNode embedding = (ArrayNode) jo.get("embedding");

									final int dims = embedding.size();
									float[] floatArray = new float[dims];
									for (int i = 0; i < floatArray.length; i++) {
										floatArray[i] = embedding.get(i).floatValue();
									}

									document.add(new KnnVectorField("contents-vector", floatArray,
											VectorSimilarityFunction.DOT_PRODUCT));
									try {
										indexWriter.addDocument(document);
									} catch (IOException e) {
									}

								});

							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		} catch (IOException e) {
			logger.error("Indexing error", e);
		}

		logger.info("...done.");
	}

	/**
	 * search the embeddings (index) for relevant chunks to use in prompt processing and return max three items from the
	 * search result set sorted by score DESC
	 * 
	 * @param cpds
	 * @param queryStr
	 * @param numResults
	 * @param threshold
	 * @return
	 */
	private Collection<SearchResult> searchIndex(CompleteDS cpds, String queryStr, int numResults, float threshold) {
		Set<SearchResult> results = new HashSet<>();

		// check if we have anything to search
		if (!hasSearchIndex(cpds)) {
			return results;
		}

		try (FSDirectory indexDirectory = FSDirectory.open(getSearchIndexDir(cpds));
				DirectoryReader indexReader = DirectoryReader.open(indexDirectory);) {
			IndexSearcher searcher = new IndexSearcher(indexReader);

			// embed query
			List<List<Double>> embeddings = aiAPIService.dispatchEmbeddingRequest("SYSTEM", Arrays.asList(queryStr));
			int dims = embeddings.get(0).size();
			float[] floatArray = new float[dims];
			for (int i = 0; i < floatArray.length; i++) {
				floatArray[i] = embeddings.get(0).get(i).floatValue();
			}

			// search
			KnnVectorQuery query = new KnnVectorQuery("contents-vector", floatArray, numResults);
			TopDocs topDocs = searcher.search(query, numResults);
			for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
				if (scoreDoc.score > threshold) {
					Document doc = searcher.doc(scoreDoc.doc);
					results.add(new SearchResult(doc.get("content"), doc.get("file"), scoreDoc.score));
				}
			}
		} catch (IOException e) {
			logger.error("Error searching index", e);
		} catch (Exception e) {
			logger.error("An unexpected error occurred during index search", e);
		}

		// return a list of max three items from the sorted set -> stream (sorted by score DESC)
		return results.stream().sorted((a, b) -> -Float.compare(a.score, b.score)).limit(3)
				.collect(Collectors.toList());
	}

	private Path getSearchIndexDir(CompleteDS cpds) {
		File searchIndexDir = new File(cpds.getFolder(), "_search_index");
		if (!searchIndexDir.exists()) {
			searchIndexDir.mkdirs();
		}
		return searchIndexDir.toPath();
	}

	private boolean hasSearchIndex(CompleteDS cpds) {
		File searchIndexDir = new File(cpds.getFolder(), "_search_index");
		if (!searchIndexDir.exists()) {
			searchIndexDir.mkdirs();
		}
		return searchIndexDir.list().length > 0;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	record SearchResult(String content, String file, float score) implements Comparable<SearchResult> {

		@Override
		public int compareTo(SearchResult sr) {
			return this.content().compareTo(sr.content());
		}
	}

	// Define a record to hold file and its index status for the view
	public record FileWithIndexStatus(TimedMedia file, boolean hasIndex) {
	}

}

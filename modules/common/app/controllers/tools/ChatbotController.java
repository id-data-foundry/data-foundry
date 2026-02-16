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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

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
import services.api.ai.ManagedAIApiService;
import services.api.remoting.RemoteApiRequest;
import services.processing.MediaProcessingService;
import services.slack.Slack;
import utils.DataUtils;
import utils.rendering.MarkdownRenderer;
import utils.auth.TokenResolverUtil;
import utils.validators.FileTypeUtils;

public class ChatbotController extends AbstractAsyncController {

	private static final String CHAT_CONTROLLER_CACHE_PREFIX = "ChatController_chat_";

	private final FormFactory formFactory;
	private final DatasetConnector datasetConnector;
	private final CompleteDSController completeDSController;
	private final ManagedAIApiService managedAIAPIService;
	private final MediaProcessingService mediaProcessingService;
	private final SyncCacheApi cache;
	private final LocalModelMetadata localModelMetadata;
	private final TokenResolverUtil tokenResolver;

	private static final Logger.ALogger logger = Logger.of(ChatbotController.class);

	@Inject
	public ChatbotController(FormFactory formFactory, DatasetConnector datasetConnector,
	        CompleteDSController completeDSController, ManagedAIApiService managedAiAPIService,
	        MediaProcessingService mediaProcessingService, SyncCacheApi cache, LocalModelMetadata lmmd,
	        TokenResolverUtil tokenResolver) {
		this.formFactory = formFactory;
		this.datasetConnector = datasetConnector;
		this.completeDSController = completeDSController;
		this.managedAIAPIService = managedAiAPIService;
		this.mediaProcessingService = mediaProcessingService;
		this.cache = cache;
		this.localModelMetadata = lmmd;
		this.tokenResolver = tokenResolver;
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
		ds.getConfiguration().put(Dataset.CHATBOT_MODEL, "hermes-2-pro-llama-3-8b");
		ds.getConfiguration().put(Dataset.CHATBOT_SYSTEM_PROMPT,
		        """
		                You are DataFoundryGPT, a large language model. You are chatting with the user via a chatbot.
		                This means most of the time your lines should be a sentence or two, unless the user's request requires reasoning or long-form outputs.
		                Never use emojis, unless explicitly asked to.
		                Current date: $DATE""");
		ds.update();

		// ensure that the project has an activated API key for local AI
		ProjectAPIInfo pai = managedAIAPIService.getProjectAPIAccess(user, p);
		if (pai.apiKey.isEmpty()) {
			managedAIAPIService.activateProjectAPIAccess(user, p);
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

		// show chatbot configuration interface
		return ok(views.html.tools.chatbots.view.render(user, ds, filesWithStatus, localModelMetadata,
		        csrfToken(request)));
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

		// update chatbot name?
		Optional<Object> result = df.value(Dataset.CHATBOT_NAME);
		if (result.isPresent() && !result.get().toString().trim().equals(ds.getName())) {
			ds.setName(result.get().toString().trim());
			updateDS = true;
		}

		// update dataset
		if (updateDS) {
			ds.update();
		}

		try {
			Thread.sleep(500);
		} catch (Exception e) {
		}

		return noContent();
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

		// show the chat interface for this chatbot
		return ok(views.html.tools.chatbots.test.render(user, ds, conversationId, csrfToken(request)));
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

			// generate richer output for the testing
			ConversationItem prompt = resultFragment.prompt();
			final String input;
			if (prompt != null) {

				String context = prompt.context().isEmpty() ? "-" : prompt.context().stream().map(cc -> """
				        <details>
				        	<summary>%s</summary>
				        	<pre>%s</pre>
				        </details>
				        """.formatted(cc.toString(), cc.content())).collect(Collectors.joining());

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
			String context = response.context().isEmpty() ? "-" : response.context().stream().map(cc -> """
			        <details>
			        	<summary>messages to LLM</summary>
			        	<pre>%s</pre>
			        </details>
			        """.formatted(cc.content())).collect(Collectors.joining());

			return ok(input + """
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
			        """.formatted(context, response.renderedContent()));
		});
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

		// retrieve conversation history from cache or create new
		ConversationHistory ch;
		Optional<ConversationHistory> chOpt = cache.get(CHAT_CONTROLLER_CACHE_PREFIX + conversationId);
		if (!chOpt.isPresent()) {
			ch = new ConversationHistory(conversationId, new LinkedList<ConversationItem>());
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

		cache.set(CHAT_CONTROLLER_CACHE_PREFIX + conversationId, ch, 3600);

		// show the chat interface for this chatbot
		return ok(views.html.tools.chatbots.chat.render(user, ds, conversationId, ch, csrfToken(request)));
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

			return ok("""
			        <div class="msg-left">
			        <p class="role">assistant</p>
			        <article>%s</article>
			        </div>""".formatted(resultFragment.response() != null ? resultFragment.response().renderedContent()
			        : "We have encountered a problem. Perhaps try again later."));
		});
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
				return unauthorized(Json.newObject().put("error", "Invalid API key"));
			}

			// 3. Find dataset and check permissions
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(Json.newObject().put("error", "Chatbot not found"));
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
		});
	}

	private ConversationFragment internalChatProcess(String conversationId, Person user, Dataset ds,
	        String originalUserPrompt) {

		// check whether the chatbot owner has enough tokens, if not quick abort
		ProjectAPIInfo pai = managedAIAPIService.getProjectAPIAccess(ds.getProject().getOwner(), ds.getProject());
		if (pai.apiKey.isEmpty()) {
			return new ConversationFragment(new ConversationItem("user", "", "", ""),
			        new ConversationItem("assistant", "", "",
			                "You need an API key or more tokens for your <a href=\"%s#api-access\">API key</a>"
			                        .formatted(controllers.routes.ProjectsController.edit(ds.getProject().getId()))));
		}

		// retrieve conversation history from cache or create new
		ConversationHistory ch;
		Optional<ConversationHistory> chOpt = cache.get(CHAT_CONTROLLER_CACHE_PREFIX + conversationId);
		if (!chOpt.isPresent()) {
			ch = new ConversationHistory(conversationId, new LinkedList<ConversationItem>());
		} else {
			ch = chOpt.get();
		}

		// if there is a conversation history, run a quick request to reformulate the user prompt in context of the
		// history
		String userPrompt = ch.items
		        .isEmpty()
		                ? originalUserPrompt
		                : reformulateUserPromptWithHistory(user.getEmail(), ds.getProject().getId(), pai.apiKey,
		                        ds.configuration(Dataset.CHATBOT_MODEL, ""), originalUserPrompt, ch)
		                                .orElse(originalUserPrompt);

		// search index for relevant information chunks for the user request
		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		if (cpds == null) {
			logger.error("CompleteDS object is null for dataset ID: {}", ds.getId());
			return new ConversationFragment(new ConversationItem("user", originalUserPrompt, "", ""),
			        new ConversationItem("assistant", "", "",
			                "An internal error occurred: Could not retrieve dataset information."));
		}
		Collection<SearchResult> searchResults;
		if (!cpds.getFiles().isEmpty()) {
			int max_hits = DataUtils.parseInt(ds.configuration(Dataset.CHATBOT_RAG_MAX_HITS, "10"), 10);
			float score_threshold = DataUtils.parseFloat(ds.configuration(Dataset.CHATBOT_RAG_SCORE_THRESHOLD, "0.7"),
			        0.7f);
			// search knowledge base for interesting hits
			searchResults = searchIndex(cpds, userPrompt, max_hits, score_threshold);
		} else {
			// otherwise empty soures
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
		ch.items().add(promptItem);

		// run the LLM
		ConversationItem responseItem = null;
		try {
			final RemoteApiRequest apiRequest = new RemoteApiRequest("",
			        ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, user.getEmail(), pai.apiKey,
			        ds.getProject().getId(), requestJson);
			String llmResult = managedAIAPIService.submitApiRequest(apiRequest);
			ObjectNode on = (ObjectNode) Json.parse(llmResult);
			JsonNode contentNode = on.get("content");
			String resultAsText = (contentNode != null && !contentNode.isNull()) ? contentNode.asText() : "";

			// generate response item, including the messages
			responseItem = new ConversationItem(ConversationItem.ASSISTANT, resultAsText, messages.toPrettyString(),
			        new MarkdownRenderer().render(resultAsText));

			ch.items().add(responseItem);
			cache.set(CHAT_CONTROLLER_CACHE_PREFIX + conversationId, ch, 3600);

			if ("true".equals(ds.configuration(Dataset.CHATBOT_STORE_CHATS, "false"))) {
				try {
					String chatFileName = "chat_" + conversationId + ".json";
					String json = Json.toJson(ch).toPrettyString();
					File tempFile = File.createTempFile("chat", ".json");
					Files.writeString(tempFile.toPath(), json);

					Optional<String> storedFile = cpds.storeFile(tempFile, chatFileName);
					if (storedFile.isPresent()) {
						cpds.addRecord(chatFileName, "Chat session " + conversationId, new Date());
					}
					tempFile.delete();
				} catch (Exception e) {
					logger.error("Error storing chat session", e);
				}
			}
		} catch (RuntimeException e) { // Catch other runtime exceptions (e.g., from MarkdownRenderer)
			logger.error("Runtime error during LLM response processing for conversation {}", conversationId, e);
			responseItem = new ConversationItem("assistant", "", "",
			        "An unexpected error occurred while processing the AI response.");
		} catch (Exception e) { // General fallback for any other unexpected exception
			logger.error("An unexpected error occurred in internalChatProcess for conversation {}", conversationId, e);
			responseItem = new ConversationItem("assistant", "", "", "An unknown error occurred.");
		}
		return new ConversationFragment(promptItem, responseItem);
	}

	private Optional<String> reformulateUserPromptWithHistory(String user, long projectId, String apiKey, String model,
	        String userPrompt, ConversationHistory ch) {
		ObjectNode requestJson = Json.newObject();
		requestJson.put(ApiServiceConstants.REQUEST_TASK, ApiServiceConstants.REQUEST_TASK_COMPLETION);
		requestJson.put(ApiServiceConstants.REQUEST_API_TOKEN, apiKey);
		requestJson.put(ApiServiceConstants.REQUEST_MODEL, model);
		requestJson.put(ApiServiceConstants.REQUEST_MAX_TOKENS, 1000);

		// contextualize the user prompt given the chat history
		StringBuilder sb = new StringBuilder();
		for (ConversationItem item : ch.items()) {
			sb.append(item.actor() + ": " + item.content() + "\n");
		}
		requestJson.put(ApiServiceConstants.REQUEST_PROMPT,
		        """
		                Given the following conversation and a follow up question, rephrase the follow up question to be a standalone question, in its original language. Keep as much details as possible from previous messages. Keep entity names and all.

		                Chat History:
		                %s
		                Follow Up Input: %s
		                Standalone question:
		                      		"""
		                .formatted(sb.toString(), userPrompt));

		try {
			final RemoteApiRequest apiRequest = new RemoteApiRequest("",
			        ApiServiceConstants.API_REQUEST_DEFAULT_TIMEOUT_MS, user, apiKey, projectId, requestJson);
			String result = managedAIAPIService.submitApiRequest(apiRequest);
			ObjectNode on = (ObjectNode) Json.parse(result);
			String resultAsText = on.get("text").asText();

			// check whether we have thinking tokens in the result, if so just take what's behind
			if (resultAsText.contains("final<|message|>")) {
				resultAsText = resultAsText
				        .substring(resultAsText.indexOf("final<|message|>") + "final<|message|>".length());
			}

			return Optional.of(resultAsText);
		} catch (Exception e) {
			return Optional.empty();
		}
	}

	static public record ConversationFragment(ConversationItem prompt, ConversationItem response) {
	}

	static public record ConversationHistory(String conversationId, List<ConversationItem> items) {
	}

	static public record ConversationItem(String actor, String content, List<ConversationContext> context,
	        String renderedContent) {

		public static final String SYSTEM = "system";

		public static final String ASSISTANT = "assistant";

		public static final String USER = "user";

		public ConversationItem(String actor, String content, String context, String renderedContent) {
			this(actor, content, Collections.singletonList(new ConversationContext(context, "", 1.0f)),
			        renderedContent);
		}

		public boolean isAssistant() {
			return actor().equals(ASSISTANT);
		}

		public boolean isUser() {
			return actor().equals(USER);
		}

		public String contextStr() {
			return context().stream().map(cc -> cc.content()).collect(Collectors.joining());
		}

		/**
		 * reformulated user prompt + context
		 * 
		 * @return
		 */
		public String fullPrompt() {
			return (content() + (!context().isEmpty() ? """

			        Respond with the following context:
			        """ + contextStr() : "")).trim();
		}
	}

	static public record ConversationContext(String content, String document, float ranking) {
		public String toString() {
			return content.substring(0, Math.min(content.length() - 1, 45)) + "... (" + document + "): "
			        + Math.round(ranking * 100) + "% match";
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
						TemporaryFile tempFile = filePart.getRef();
						String tempFileName = nss(filePart.getFilename());

						logger.info("Context upload '" + tempFileName + "', processing...");

						// check filename length and shorten if needed
						if (tempFileName.length() > 60) {
							tempFileName = tempFileName.substring(0, 60)
							        + tempFileName.substring(tempFileName.lastIndexOf("."));
							logger.info("   Uploaded file name shortened: " + tempFileName);
						}

						// filename-based quick check
						if (FileTypeUtils.looksLikeExecutableFile(tempFileName)) {
							logger.error(
							        "   Document upload rejected due to executable-like filename: " + tempFileName);
							continue;
						}

						// content-based validation
						if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.PDF)) {
							continue;
						}

						final String fileName = tempFileName;
						// check if file exists; if so, delete it
						cpds.getFile(fileName).ifPresent((file) -> {
							logger.info("   '" + fileName + "' file exists; file and index will be deleted first");
							if (file.exists()) {
								file.delete();
							}
							cpds.deleteRecord(fileName);
							File indexFile = new File(
							        cpds.getFolder().getAbsolutePath() + File.separator + fileName + ".idx");
							if (indexFile.exists()) {
								indexFile.delete();
							}
						});

						// store file, add record
						Optional<String> storeFile = cpds.storeFile(tempFile.path().toFile(), fileName);
						if (storeFile.isPresent()) {
							cpds.addRecord(storeFile.get(), nss(df.get("description")), new Date());
						}

						// process the file in a document extraction queue
						// retrieve document contents
						try {
							String finalFileName = storeFile.get();
							String contents = mediaProcessingService
							        .scheduleMediaToTextProcess(tempFile.path().toFile(), "en", "application/pdf",
							                "SYSTEM", UUID.randomUUID().toString())
							        .toCompletableFuture().get();

							ArrayNode documentIndex = Json.newArray();

							// chunk the document in paragraphs and reproduce them as a list with related headers
							List<String> chunks = produceChunks(contents);
							logger.info("   " + chunks.size() + " chunks for '" + finalFileName + "' in "
							        + cpds.getFolder().getAbsolutePath());

							// process chunks to embeddings and store them in file
							List<List<Double>> embeddings = managedAIAPIService.dispatchEmbeddingRequest("SYSTEM",
							        chunks);
							int counter = 0;
							for (String str : chunks) {
								ArrayNode ar = Json.newArray();
								embeddings.get(counter++).forEach(d -> {
									ar.add(d.floatValue());
								});
								documentIndex.add(Json.newObject().put("file", storeFile.get()).put("content", str)
								        .set("embedding", ar));
							}

							// write the index to disk
							File indexFile = new File(
							        cpds.getFolder().getAbsolutePath() + File.separator + finalFileName + ".idx");
							if (indexFile.exists()) {
								indexFile.delete();
							}
							Files.writeString(indexFile.toPath(), documentIndex.toString());

							logger.info("   Chunks complete for '" + finalFileName + "' in "
							        + cpds.getFolder().getAbsolutePath());
						} catch (InterruptedException | ExecutionException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.DATA,
					        "Files uploaded to dataset: " + ds.getName(), ds.getProject());
				}
			} catch (NullPointerException e) {
				logger.error("Error uploading file to dataset.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}

			// re-index all documents
			indexAllDocuments(cpds);

			return view(request, dsId);
		});
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
			String filename = cpds.getFile(fileId).get().getName();

			// now delete the file itself
			completeDSController.delete(request, dsId, fileId);

			// delete the index file as well
			File indexFile = new File(cpds.getFolder().getAbsolutePath() + File.separator + filename + ".idx");
			if (indexFile.exists()) {
				indexFile.delete();
			}

			// re-index all documents
			indexAllDocuments(cpds);

			return ok("");
		});
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
			List<List<Double>> embeddings = managedAIAPIService.dispatchEmbeddingRequest("SYSTEM",
			        Arrays.asList(queryStr));
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

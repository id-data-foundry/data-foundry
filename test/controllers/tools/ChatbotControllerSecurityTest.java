package controllers.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.BAD_REQUEST;
import static play.mvc.Http.Status.FORBIDDEN;
import static play.mvc.Http.Status.OK;
import static play.mvc.Http.Status.UNAUTHORIZED;
import static play.test.Helpers.GET;
import static play.test.Helpers.POST;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.tools.ChatbotController.ConversationHistory;
import controllers.tools.ChatbotController.ConversationItem;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import utils.tools.ChatbotMemoryUtils;
import play.Application;
import play.cache.SyncCacheApi;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Security;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class ChatbotControllerSecurityTest extends WithApplication {

	private ChatbotController chatbotController;
	private DatasetConnector datasetConnector;
	private TokenResolverUtil tokenResolver;
	private SyncCacheApi cache;
	private SessionStore sessionStore;

	private Person ownerUser;
	private Person participantUser;
	private Project project;
	private Dataset completeDataset;
	private Dataset timeseriesDataset;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.keys.project", "test-project-token-secret-123456")
				.configure("df.keys.registration", Collections.singletonList("reg-key"))
				.build();
	}

	private Person createPerson(String prefix) {
		Person person = new Person();
		person.setUser_id(UUID.randomUUID().toString());
		person.setFirstname(prefix);
		person.setLastname("User");
		person.setEmail(prefix.toLowerCase() + "_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		person.save();
		return person;
	}

	@Before
	public void setUp() throws Exception {
		chatbotController = app.injector().instanceOf(ChatbotController.class);
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
		tokenResolver = app.injector().instanceOf(TokenResolverUtil.class);
		cache = app.injector().instanceOf(SyncCacheApi.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);

		ownerUser = createPerson("owner");
		participantUser = createPerson("participant");

		project = Project.create("Chatbot Security Project " + UUID.randomUUID(), ownerUser, "Chatbot Description", false, false);
		project.save();

		completeDataset = datasetConnector.create("Chatbot KB", DatasetType.COMPLETE, project, "Complete KB", "Target", "true");
		completeDataset.getConfiguration().put(Dataset.API_TOKEN, tokenResolver.getDatasetToken(completeDataset.getId()));
		completeDataset.getConfiguration().put(Dataset.CHATBOT_MODEL, "gpt-4");
		completeDataset.save();

		timeseriesDataset = datasetConnector.create("Sensor Timeseries", DatasetType.TIMESERIES, project, "Timeseries DS", "Target", "true");
		timeseriesDataset.getConfiguration().put(Dataset.API_TOKEN, tokenResolver.getDatasetToken(timeseriesDataset.getId()));
		timeseriesDataset.save();
	}

	private Http.Request createAuthenticatedRequest(String method, String uri, Person user) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method(method).uri(uri);
		if (user != null) {
			requestBuilder.attr(Security.USERNAME, user.getEmail());
			Http.Request request = requestBuilder.build();
			PlayWebContext context = new PlayWebContext(request);
			ProfileManager manager = new ProfileManager(context, sessionStore);
			CommonProfile profile = new CommonProfile();
			profile.setId(user.getEmail());
			profile.addAttribute(Person.USER_NAME, user.getEmail());
			profile.addAttribute(Person.USER_ID, user.getId());
			manager.save(true, profile, false);
			return context.supplementRequest(request);
		}
		return requestBuilder.build();
	}

	@Test
	public void testDF20_ConversationCacheKeyScopedByDatasetId() {
		// DF-20: Ensure conversation history cache is scoped by dataset ID so same conversationId
		// on different datasets does not leak transcripts.
		String conversationId = "shared-conversation-" + UUID.randomUUID();

		Http.Request request1 = createAuthenticatedRequest(GET, "/tools/chatbots/" + completeDataset.getId() + "/chat/" + conversationId, ownerUser);
		Result result1 = chatbotController.chat(request1, completeDataset.getId(), conversationId);
		assertEquals(OK, result1.status());

		// Verify cached under scoped key: ChatController_chat_<dsId>_<convId>
		String scopedKey1 = "ChatController_chat_" + completeDataset.getId() + "_" + conversationId;
		Optional<ConversationHistory> ch1 = cache.get(scopedKey1);
		assertTrue("Cache must contain scoped key for completeDataset", ch1.isPresent());

		// Verify global unscoped key is NOT populated
		String unscopedKey = "ChatController_chat_" + conversationId;
		Optional<ConversationHistory> chUnscoped = cache.get(unscopedKey);
		assertFalse("Global unscoped key must not exist", chUnscoped.isPresent());
	}

	@Test
	public void testDF12_UploadFileApiRejectsParticipationTokens() throws Exception {
		// DF-12: Verify that participation tokens cannot upload files to CompleteDS via uploadFileApi
		String participationToken = tokenResolver.getParticipationToken(project.getId(), participantUser.getId());

		Http.RequestBuilder requestBuilder = new Http.RequestBuilder()
				.method(POST)
				.uri("/api/v1/chatbots/" + completeDataset.getId() + "/file")
				.header("Authorization", "Bearer " + participationToken);

		Result result = chatbotController.uploadFileApi(requestBuilder.build(), completeDataset.getId()).toCompletableFuture().get(5, TimeUnit.SECONDS);
		assertEquals(UNAUTHORIZED, result.status());
	}

	@Test
	public void testDF12_UploadFileApiRejectsNonCompleteDataset() throws Exception {
		// DF-12: Calling uploadFileApi on a non-COMPLETE dataset must return 400 Bad Request
		String datasetToken = timeseriesDataset.configuration(Dataset.API_TOKEN, "");

		Http.RequestBuilder requestBuilder = new Http.RequestBuilder()
				.method(POST)
				.uri("/api/v1/chatbots/" + timeseriesDataset.getId() + "/file")
				.header("Authorization", "Bearer " + datasetToken);

		Result result = chatbotController.uploadFileApi(requestBuilder.build(), timeseriesDataset.getId()).toCompletableFuture().get(5, TimeUnit.SECONDS);
		assertEquals(BAD_REQUEST, result.status());
	}

	@Test
	public void testDF12_ChatApiRejectsNonCompleteOrUnconfiguredDataset() throws Exception {
		// DF-12: Calling chatApi on a non-COMPLETE dataset must return 400 Bad Request
		String participationToken = tokenResolver.getParticipationToken(project.getId(), participantUser.getId());

		ObjectNode body = Json.newObject();
		body.put("message", "Hello");

		Http.RequestBuilder requestBuilder = new Http.RequestBuilder()
				.method(POST)
				.uri("/api/v1/chatbots/" + timeseriesDataset.getId() + "/chat")
				.header("Authorization", "Bearer " + participationToken)
				.bodyJson(body);

		Result result = chatbotController.chatApi(requestBuilder.build(), timeseriesDataset.getId()).toCompletableFuture().get(5, TimeUnit.SECONDS);
		assertEquals(BAD_REQUEST, result.status());
	}

	@Test
	public void testDF34_ConversationHistoryThreadSafety() throws Exception {
		// DF-34: Verify ConversationHistory is thread-safe and doesn't throw ConcurrentModificationException
		// when multiple concurrent threads append items.
		ConversationHistory history = new ConversationHistory("test-conv", new ArrayList<>());
		int numThreads = 10;
		int itemsPerThread = 50;
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		CountDownLatch latch = new CountDownLatch(numThreads);

		for (int i = 0; i < numThreads; i++) {
			final int threadId = i;
			executor.submit(() -> {
				try {
					for (int j = 0; j < itemsPerThread; j++) {
						history.items().add(new ConversationItem(ConversationItem.USER, "Msg from " + threadId + "-" + j, "", ""));
					}
				} finally {
					latch.countDown();
				}
			});
		}

		boolean completed = latch.await(10, TimeUnit.SECONDS);
		executor.shutdown();
		assertTrue("All concurrent modifications must complete without exception", completed);
		assertEquals(numThreads * itemsPerThread, history.items().size());
	}

	@Test
	public void testSaveAgentsMdSecurity() throws Exception {
		// Non-owner / unauthorized user cannot edit AGENTS.md
		Http.Request forbiddenReq = createAuthenticatedRequest(POST,
				"/tools/chatbots/" + completeDataset.getId() + "/agents-md", participantUser);
		Result forbiddenRes = chatbotController.saveAgentsMd(forbiddenReq, completeDataset.getId());
		assertEquals(FORBIDDEN, forbiddenRes.status());

		// Owner can save AGENTS.md
		java.util.Map<String, String> formData = new java.util.HashMap<>();
		formData.put("agents_md", "# Custom Test Agents Directive\n- Rule 1");
		Http.Request ownerReq = new Http.RequestBuilder()
				.method(POST)
				.uri("/tools/chatbots/" + completeDataset.getId() + "/agents-md")
				.bodyForm(formData)
				.attr(Security.USERNAME, ownerUser.getEmail())
				.build();
		PlayWebContext context = new PlayWebContext(ownerReq);
		ProfileManager manager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(ownerUser.getEmail());
		profile.addAttribute(Person.USER_NAME, ownerUser.getEmail());
		profile.addAttribute(Person.USER_ID, ownerUser.getId());
		manager.save(true, profile, false);
		Http.Request supplementedReq = context.supplementRequest(ownerReq);

		Result ownerRes = chatbotController.saveAgentsMd(supplementedReq, completeDataset.getId());
		assertEquals(OK, ownerRes.status());
	}

	@Test
	public void testUserMemoryEndpoints() {
		final models.ds.CompleteDS cpds = (models.ds.CompleteDS) datasetConnector.getDatasetDS(completeDataset);
		ChatbotMemoryUtils.saveUserProfileEntry(cpds.getFolder(), ownerUser.getEmail(), "thesis", "My AI Project");
		ChatbotMemoryUtils.writeUserFile(cpds.getFolder(), ownerUser.getEmail(), "test_notes.md", "# Notes");

		Http.Request getMemReq = createAuthenticatedRequest(GET,
				"/tools/chatbots/" + completeDataset.getId() + "/memory", ownerUser);
		Result getMemRes = chatbotController.getUserMemory(getMemReq, completeDataset.getId());
		assertEquals(OK, getMemRes.status());
		assertTrue(play.test.Helpers.contentAsString(getMemRes).contains("user-memory-container"));
		assertTrue(play.test.Helpers.contentAsString(getMemRes).contains("test_notes.md"));
		assertTrue(play.test.Helpers.contentAsString(getMemRes).contains("thesis"));

		// Delete user file
		Http.Request delFileReq = createAuthenticatedRequest(POST,
				"/tools/chatbots/" + completeDataset.getId() + "/memory/file/delete", ownerUser);
		Result delFileRes = chatbotController.deleteUserFile(delFileReq, completeDataset.getId(), "test_notes.md");
		assertEquals(OK, delFileRes.status());
		String delFileContent = play.test.Helpers.contentAsString(delFileRes);
		assertTrue(delFileContent.contains("test_notes.md") && delFileContent.contains("deleted"));
		assertTrue(delFileContent.contains("No private notes files created yet."));

		// Delete user profile entry
		Http.Request delProfReq = createAuthenticatedRequest(POST,
				"/tools/chatbots/" + completeDataset.getId() + "/memory/profile/delete", ownerUser);
		Result delProfRes = chatbotController.deleteUserProfileEntry(delProfReq, completeDataset.getId(), "thesis");
		assertEquals(OK, delProfRes.status());
		String delProfContent = play.test.Helpers.contentAsString(delProfRes);
		assertTrue(delProfContent.contains("thesis") && delProfContent.contains("deleted"));
		assertTrue(delProfContent.contains("No profile facts stored yet."));

		// Reset memory
		Http.Request resetMemReq = createAuthenticatedRequest(POST,
				"/tools/chatbots/" + completeDataset.getId() + "/memory/reset", ownerUser);
		Result resetMemRes = chatbotController.resetUserMemory(resetMemReq, completeDataset.getId());
		assertEquals(OK, resetMemRes.status());
		assertTrue(play.test.Helpers.contentAsString(resetMemRes).contains("user-memory-container"));
		assertTrue(play.test.Helpers.contentAsString(resetMemRes).contains("cleared"));
	}

	@Test
	public void testConversationContextToString_SafeBoundaries() {
		// Empty content should not throw StringIndexOutOfBoundsException
		ChatbotController.ConversationContext emptyCtx = new ChatbotController.ConversationContext("", "doc.pdf", 0.85f);
		assertEquals("(doc.pdf): 85% match", emptyCtx.toString());

		// Null content should not throw NullPointerException
		ChatbotController.ConversationContext nullCtx = new ChatbotController.ConversationContext(null, "doc.pdf", 0.5f);
		assertEquals("(doc.pdf): 50% match", nullCtx.toString());

		// 1-character string should not drop character or throw exception
		ChatbotController.ConversationContext singleCharCtx = new ChatbotController.ConversationContext("A", "doc.pdf", 0.9f);
		assertEquals("A (doc.pdf): 90% match", singleCharCtx.toString());

		// Short string (< 45 chars) should preserve all characters without trailing ellipsis
		ChatbotController.ConversationContext shortCtx = new ChatbotController.ConversationContext("Hello World", "doc.pdf", 0.75f);
		assertEquals("Hello World (doc.pdf): 75% match", shortCtx.toString());

		// Exactly 45 chars
		String exactly45 = "123456789012345678901234567890123456789012345";
		ChatbotController.ConversationContext exactCtx = new ChatbotController.ConversationContext(exactly45, "doc.pdf", 1.0f);
		assertEquals(exactly45 + " (doc.pdf): 100% match", exactCtx.toString());

		// > 45 chars should truncate to 45 with ellipsis
		String longContent = "12345678901234567890123456789012345678901234567890";
		ChatbotController.ConversationContext longCtx = new ChatbotController.ConversationContext(longContent, "doc.pdf", 0.95f);
		assertEquals(exactly45 + "... (doc.pdf): 95% match", longCtx.toString());
	}

	@Test
	public void testConversationItemEmptyContextHandling() {
		// Instantiating ConversationItem with empty or null context should produce empty context list
		ConversationItem itemEmpty = new ConversationItem(ConversationItem.ASSISTANT, "Hello", "", "<p>Hello</p>");
		assertTrue(itemEmpty.context().isEmpty());
		assertEquals("", itemEmpty.contextStr());

		ConversationItem itemNull = new ConversationItem(ConversationItem.USER, "Hi", (String) null, "<p>Hi</p>");
		assertTrue(itemNull.context().isEmpty());
		assertEquals("", itemNull.contextStr());

		// Non-empty context string wraps as single context
		ConversationItem itemWithCtx = new ConversationItem(ConversationItem.USER, "Query", "Sample text", "<p>Query</p>");
		assertEquals(1, itemWithCtx.context().size());
		assertEquals("Sample text", itemWithCtx.contextStr());
	}

	@Test
	public void testAgenticModePresetsCodingModel() {
		// 1. Initially set a legacy / arbitrary model
		completeDataset.getConfiguration().put(Dataset.CHATBOT_MODEL, "legacy-custom-model-7b");
		completeDataset.getConfiguration().remove(Dataset.CHATBOT_ENABLE_AGENTIC);
		completeDataset.update();

		// 2. Save settings enabling agentic mode
		java.util.Map<String, String> formData = new java.util.HashMap<>();
		formData.put(Dataset.CHATBOT_ENABLE_AGENTIC, "true");
		formData.put(Dataset.CHATBOT_MODEL, "legacy-custom-model-7b");

		Http.Request saveReq = new Http.RequestBuilder()
				.method(POST)
				.uri("/tools/chatbots/" + completeDataset.getId() + "/save")
				.bodyForm(formData)
				.attr(Security.USERNAME, ownerUser.getEmail())
				.build();
		PlayWebContext context = new PlayWebContext(saveReq);
		ProfileManager manager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(ownerUser.getEmail());
		profile.addAttribute(Person.USER_NAME, ownerUser.getEmail());
		profile.addAttribute(Person.USER_ID, ownerUser.getId());
		manager.save(true, profile, false);
		Http.Request supplementedReq = context.supplementRequest(saveReq);

		Result res = chatbotController.save(supplementedReq, completeDataset.getId(), "save");
		assertEquals(204, res.status());

		// 3. Verify dataset configuration has preset to coding model
		Dataset updatedDs = Dataset.find.byId(completeDataset.getId());
		assertNotNull(updatedDs);
		assertEquals("true", updatedDs.configuration(Dataset.CHATBOT_ENABLE_AGENTIC, "false"));
		// Model must be preset to coding model, not the legacy custom model
		assertFalse("legacy-custom-model-7b".equals(updatedDs.configuration(Dataset.CHATBOT_MODEL, "")));
		assertFalse(updatedDs.configuration(Dataset.CHATBOT_MODEL, "").isEmpty());
	}

	@Test
	public void testTestViewCollapsesDiagramAndCountsDocumentFilesOnly() throws Exception {
		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(completeDataset);
		assertNotNull(cpds);

		// Store 1 knowledge base document file
		File docFile = File.createTempFile("testdoc", ".txt");
		Files.writeString(docFile.toPath(), "Sample document content");
		cpds.storeFile(docFile, "sample_guide.txt");
		cpds.addRecord("sample_guide.txt", "Knowledge guide", new Date());
		docFile.delete();

		// Store 2 chat session files (simulating test/chat executions)
		File chat1 = File.createTempFile("chat1", ".json");
		Files.writeString(chat1.toPath(), "{}");
		cpds.storeFile(chat1, "chat_123.json");
		cpds.addRecord("chat_123.json", "Chat session 123", new Date());
		chat1.delete();

		File chat2 = File.createTempFile("chat2", ".json");
		Files.writeString(chat2.toPath(), "{}");
		cpds.storeFile(chat2, "chat_456.json");
		cpds.addRecord("chat_456.json", "Chat session 456", new Date());
		chat2.delete();

		// Create request authenticated as owner
		Http.Request req = new Http.RequestBuilder()
				.method(GET)
				.uri("/tools/chatbots/" + completeDataset.getId() + "/test/test-convo-id")
				.attr(Security.USERNAME, ownerUser.getEmail())
				.build();
		PlayWebContext context = new PlayWebContext(req);
		ProfileManager manager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(ownerUser.getEmail());
		profile.addAttribute(Person.USER_NAME, ownerUser.getEmail());
		profile.addAttribute(Person.USER_ID, ownerUser.getId());
		manager.save(true, profile, false);
		Http.Request supplementedReq = context.supplementRequest(req);

		Result res = chatbotController.test(supplementedReq, completeDataset.getId(), "test-convo-id");
		assertEquals(OK, res.status());

		String html = play.test.Helpers.contentAsString(res);
		// 1. Verify accurate document file count: 1 document, not 3
		assertTrue(html.contains("1 document available for on-demand search"));
		assertFalse(html.contains("3 documents available for on-demand search"));
		assertFalse(html.contains("3 document(s) available for on-demand search"));

		// 2. Verify visual overview diagram card has <details> collapsed by default (no open) and is near the top
		assertTrue(html.contains("💡 How this Assistant Works (Visual Overview)"));
		int diagramIdx = html.indexOf("💡 How this Assistant Works (Visual Overview)");
		int submitIdx = html.indexOf("Submit user prompt");
		int kbIdx = html.indexOf("📚 Dataset Knowledge Base:");
		assertTrue("Visual overview diagram must precede the prompt submit button", diagramIdx < submitIdx);
		assertTrue("Submit button must precede the knowledge base card", submitIdx < kbIdx);

		// Check that the details tag wrapping the visual overview is collapsed (no <details open>)
		int detailsStart = html.lastIndexOf("<details", diagramIdx);
		assertTrue(detailsStart != -1);
		String detailsTag = html.substring(detailsStart, diagramIdx);
		assertFalse("<details> tag for visual overview should not be open", detailsTag.contains("open"));

		// 3. Verify source modal dialog is included for citation inspection
		assertTrue(html.contains("dialog id=\"source-modal\""));
	}

	@Test
	public void testDeleteFileRebuildsCleanSearchIndex() throws Exception {
		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(completeDataset);
		assertNotNull(cpds);

		// Helper to create an embedding JSON string > 1000 bytes
		StringBuilder dummyEmbedding = new StringBuilder("[");
		for (int i = 0; i < 200; i++) {
			dummyEmbedding.append(0.12345f).append(i < 199 ? "," : "");
		}
		dummyEmbedding.append("]");

		// Store fileA
		File docA = File.createTempFile("docA", ".txt");
		Files.writeString(docA.toPath(), "Content of file A");
		cpds.storeFile(docA, "fileA.txt");
		cpds.addRecord("fileA.txt", "File A", new Date());
		docA.delete();

		File idxA = new File(cpds.getFolder(), "fileA.txt.idx");
		String idxContentA = "[{\"file\":\"fileA.txt\",\"content\":\"Content of file A\",\"embedding\":" + dummyEmbedding + "}]";
		Files.writeString(idxA.toPath(), idxContentA);
		assertTrue(idxA.length() > 1000);

		// Store fileB
		File docB = File.createTempFile("docB", ".txt");
		Files.writeString(docB.toPath(), "Content of file B");
		cpds.storeFile(docB, "fileB.txt");
		cpds.addRecord("fileB.txt", "File B", new Date());
		docB.delete();

		File idxB = new File(cpds.getFolder(), "fileB.txt.idx");
		String idxContentB = "[{\"file\":\"fileB.txt\",\"content\":\"Content of file B\",\"embedding\":" + dummyEmbedding + "}]";
		Files.writeString(idxB.toPath(), idxContentB);
		assertTrue(idxB.length() > 1000);

		// Get file ID of fileA
		Long fileAId = null;
		for (models.vm.TimedMedia tm : cpds.getFiles()) {
			if ("fileA.txt".equals(tm.getLink())) {
				fileAId = tm.getId();
				break;
			}
		}
		assertNotNull(fileAId);

		// Authenticated request as owner
		Http.Request deleteReq = new Http.RequestBuilder()
				.method("DELETE")
				.uri("/tools/chatbots/" + completeDataset.getId() + "/files/" + fileAId)
				.attr(Security.USERNAME, ownerUser.getEmail())
				.build();
		PlayWebContext context = new PlayWebContext(deleteReq);
		ProfileManager manager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(ownerUser.getEmail());
		profile.addAttribute(Person.USER_NAME, ownerUser.getEmail());
		profile.addAttribute(Person.USER_ID, ownerUser.getId());
		manager.save(true, profile, false);
		Http.Request supplementedReq = context.supplementRequest(deleteReq);

		Result res = chatbotController.deleteFile(supplementedReq, completeDataset.getId(), fileAId).toCompletableFuture().get(10, TimeUnit.SECONDS);
		assertEquals(OK, res.status());

		// 1. Verify fileA.txt.idx was removed
		assertFalse(idxA.exists());
		// 2. Verify fileB.txt.idx still exists
		assertTrue(idxB.exists());

		// 3. Inspect Lucene index directory and verify fileA chunks are completely gone
		File searchIndexDir = new File(cpds.getFolder(), "_search_index");
		assertTrue(searchIndexDir.exists());
		try (FSDirectory dir = FSDirectory.open(searchIndexDir.toPath());
				DirectoryReader reader = DirectoryReader.open(dir)) {
			IndexSearcher searcher = new IndexSearcher(reader);
			assertEquals(1, reader.numDocs());
			Document doc = searcher.doc(0);
			assertEquals("fileB.txt", doc.get("file"));
			assertEquals("Content of file B", doc.get("content"));
		}
	}

	@Test
	public void testTestProcessRendersConversationArtifactsProperly() throws Exception {
		String conversationId = "test-conv-" + UUID.randomUUID();
		Http.Request request = createAuthenticatedRequest(POST,
				"/tools/chatbots/" + completeDataset.getId() + "/testProcess/" + conversationId, ownerUser);

		Result result = chatbotController.testProcess(request, completeDataset.getId(), conversationId)
				.toCompletableFuture().get(10, TimeUnit.SECONDS);

		assertEquals(OK, result.status());
		String body = play.test.Helpers.contentAsString(result);
		assertNotNull(body);

		// Verify turn structure
		assertTrue("Must contain chat-turn container", body.contains("chat-turn"));
		assertTrue("Must contain chat-turn-content", body.contains("chat-turn-content"));

		// Verify conversation artifact rows
		assertTrue("Must contain conversation-row user", body.contains("conversation-row user"));
		assertTrue("Must contain conversation-row assistant", body.contains("conversation-row assistant"));
		assertTrue("Must contain conversation-row internal processed-prompt",
				body.contains("conversation-row internal processed-prompt"));
		assertTrue("Must contain conversation-row internal prompt-context",
				body.contains("conversation-row internal prompt-context"));
		assertTrue("Must contain conversation-row internal internal-messages",
				body.contains("conversation-row internal internal-messages"));

		// Verify role labels
		assertTrue("Must contain user prompt label", body.contains("user prompt"));
		assertTrue("Must contain processed prompt label", body.contains("processed prompt"));
		assertTrue("Must contain prompt context label", body.contains("prompt context"));
		assertTrue("Must contain internal messages label", body.contains("internal messages"));
		assertTrue("Must contain assistant label", body.contains("assistant"));
		assertTrue("Must contain role-label class", body.contains("role-label"));

		// Verify article content class
		assertTrue("Must contain convo-content class", body.contains("convo-content"));
	}

	@Test
	public void testResolveFallbackForEmptyResult() {
		// 1. When knowledge base search returned 0 hits
		ChatbotController.RequestScopeTracker trackerKBEmpty = new ChatbotController.RequestScopeTracker();
		trackerKBEmpty.recordTraceStep(ChatbotController.ExecutionTraceStep.toolCall("search_knowledge_base", "query=\"quantum\"", "0 chunks"));
		String fb1 = chatbotController.resolveFallbackForEmptyResult("", trackerKBEmpty);
		assertEquals("I searched the knowledge base, but could not find any matching documents for your request.", fb1);

		// 2. When knowledge base search returned citations
		ChatbotController.RequestScopeTracker trackerKBCitations = new ChatbotController.RequestScopeTracker();
		trackerKBCitations.recordCitations(Collections.singletonList(new ChatbotController.SearchResult("Content", "doc.pdf", 0.9f)));
		String fb2 = chatbotController.resolveFallbackForEmptyResult("", trackerKBCitations);
		assertEquals("I found relevant information in the knowledge base.", fb2);

		// 3. When memory activities took place
		ChatbotController.RequestScopeTracker trackerMemory = new ChatbotController.RequestScopeTracker();
		trackerMemory.recordMemoryActivity("Saved topic to profile");
		String fb3 = chatbotController.resolveFallbackForEmptyResult("", trackerMemory);
		assertEquals("I have updated your memory and notes.", fb3);

		// 4. When read_user_file was executed
		ChatbotController.RequestScopeTracker trackerRead = new ChatbotController.RequestScopeTracker();
		trackerRead.recordTraceStep(ChatbotController.ExecutionTraceStep.toolCall("read_user_file", "filename=\"notes.md\"", "100 chars"));
		String fb4 = chatbotController.resolveFallbackForEmptyResult("", trackerRead);
		assertEquals("I checked your notes file, but have no additional information to report.", fb4);

		// 5. When delete tool was executed
		ChatbotController.RequestScopeTracker trackerDelete = new ChatbotController.RequestScopeTracker();
		trackerDelete.recordTraceStep(ChatbotController.ExecutionTraceStep.toolCall("delete_user_file", "filename=\"notes.md\"", "Success"));
		String fb5 = chatbotController.resolveFallbackForEmptyResult("", trackerDelete);
		assertEquals("I have processed your deletion request.", fb5);

		// 6. When raw output had thinking tags that were stripped
		String fb6 = chatbotController.resolveFallbackForEmptyResult("<think>Reasoning tokens here</think>", null);
		assertEquals("I processed your request, but was unable to complete the response in time. Please try asking again or simplifying your query.", fb6);

		// 7. Generic fallback for empty string and no tracker
		String fb7 = chatbotController.resolveFallbackForEmptyResult("", null);
		assertEquals("I'm sorry, I wasn't able to generate a response. Please let me know how I can help.", fb7);
	}

	@Test
	public void testChatProcessNeverReturnsEmptyArticle() throws Exception {
		String conversationId = "test-conv-chatProcess-" + UUID.randomUUID();
		Http.Request request = createAuthenticatedRequest(POST,
				"/tools/chatbots/" + completeDataset.getId() + "/chatProcess/" + conversationId, ownerUser);

		Result result = chatbotController.chatProcess(request, completeDataset.getId(), conversationId)
				.toCompletableFuture().get(10, TimeUnit.SECONDS);

		assertEquals(OK, result.status());
		String body = play.test.Helpers.contentAsString(result);
		assertNotNull(body);
		assertTrue("chatProcess must wrap message in msg-left", body.contains("msg-left"));
		assertTrue("chatProcess must contain role assistant", body.contains("<p class=\"role\">assistant</p>"));
		assertFalse("chatProcess must not return empty article", body.contains("<article></article>"));
		assertFalse("chatProcess must not return empty whitespace article", body.contains("<article> </article>"));
		assertTrue("chatProcess must contain an article tag with content", body.contains("<article>"));
	}
}

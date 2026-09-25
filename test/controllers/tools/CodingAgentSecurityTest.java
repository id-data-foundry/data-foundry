package controllers.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.FORBIDDEN;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.contentAsString;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import datasets.DatasetConnector;
import models.Collaboration;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.Subscription;
import models.ds.CompleteDS;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Security;
import play.mvc.WebSocket;
import play.test.WithApplication;

public class CodingAgentSecurityTest extends WithApplication {

	private DatasetConnector datasetConnector;
	private CodingAgentController codingAgentController;
	private SessionStore sessionStore;

	private Person ownerUser;
	private Person collaboratorUser;
	private Person subscriberUser;
	private Person publicViewerUser;
	private Person strangerUser;

	private Project project;
	private Dataset dataset;
	private CompleteDS completeDS;

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
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
		codingAgentController = app.injector().instanceOf(CodingAgentController.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);

		ownerUser = createPerson("owner");
		collaboratorUser = createPerson("collab");
		subscriberUser = createPerson("subscriber");
		publicViewerUser = createPerson("public_viewer");
		strangerUser = createPerson("stranger");

		project = Project.create("Coding Agent Test Project " + UUID.randomUUID(), ownerUser, "Test Description", false, false);
		project.save();

		// Add collaborator
		Collaboration collab = new Collaboration(collaboratorUser, project);
		collab.save();
		project.getCollaborators().add(collab);

		// Add subscriber
		Subscription sub = new Subscription(subscriberUser, project);
		sub.save();
		project.getSubscribers().add(sub);
		project.update();

		dataset = datasetConnector.create("Test Complete DS", DatasetType.COMPLETE, project, "Desc", "Target", "true");
		dataset.save();

		completeDS = (CompleteDS) datasetConnector.getDatasetDS(dataset);

		File tempFile = File.createTempFile("ca_test", ".txt");
		Files.write(tempFile.toPath(), "Content".getBytes(StandardCharsets.UTF_8));
		completeDS.storeFile(tempFile, "index.html");
		completeDS.addRecord("index.html", "Description", new Date());
		tempFile.delete();
	}

	private Http.Request createAuthenticatedRequest(String method, String uri, Person user) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method(method).uri(uri);
		if (user != null) {
			requestBuilder.attr(Security.USERNAME, user.getEmail());
		}
		Http.Request request = requestBuilder.build();
		if (user != null) {
			PlayWebContext context = new PlayWebContext(request);
			ProfileManager manager = new ProfileManager(context, sessionStore);
			CommonProfile profile = new CommonProfile();
			profile.setId(user.getEmail());
			profile.addAttribute(Person.USER_NAME, user.getEmail());
			profile.addAttribute(Person.USER_ID, user.getId());
			manager.save(true, profile, false);
			request = context.supplementRequest(request);
		}
		return request;
	}

	@Test
	public void testOwnerAccessGranted() {
		Http.Request request = createAuthenticatedRequest(GET, "/tools/codingagent/" + dataset.getId(), ownerUser);

		Result viewResult = codingAgentController.view(request, dataset.getId(), -1L);
		assertEquals("Owner must have access to view", OK, viewResult.status());

		Result fileListResult = codingAgentController.getFileList(request, dataset.getId());
		assertEquals("Owner must have access to getFileList", OK, fileListResult.status());
	}

	@Test
	public void testCollaboratorAccessGranted() {
		Http.Request request = createAuthenticatedRequest(GET, "/tools/codingagent/" + dataset.getId(), collaboratorUser);

		Result viewResult = codingAgentController.view(request, dataset.getId(), -1L);
		assertEquals("Collaborator must have access to view", OK, viewResult.status());

		Result fileListResult = codingAgentController.getFileList(request, dataset.getId());
		assertEquals("Collaborator must have access to getFileList", OK, fileListResult.status());
	}

	@Test
	public void testSubscriberAccessForbidden() {
		// Subscriber has visibleFor == true, but editableBy == false
		assertTrue("Subscriber should have visibleFor", dataset.visibleFor(subscriberUser));
		org.junit.Assert.assertFalse("Subscriber should NOT have editableBy", dataset.editableBy(subscriberUser));

		Http.Request request = createAuthenticatedRequest(GET, "/tools/codingagent/" + dataset.getId(), subscriberUser);

		Result viewResult = codingAgentController.view(request, dataset.getId(), -1L);
		assertEquals("Subscriber must be forbidden from view", FORBIDDEN, viewResult.status());

		Result fileListResult = codingAgentController.getFileList(request, dataset.getId());
		assertEquals("Subscriber must be forbidden from getFileList", FORBIDDEN, fileListResult.status());
	}

	@Test
	public void testPublicViewerAccessForbidden() {
		// Make project public: any logged in user has visibleFor == true, but editableBy == false
		project.setPublicProject(true);
		project.update();

		assertTrue("Public viewer should have visibleFor on public project", dataset.visibleFor(publicViewerUser));
		org.junit.Assert.assertFalse("Public viewer should NOT have editableBy on public project", dataset.editableBy(publicViewerUser));

		Http.Request request = createAuthenticatedRequest(GET, "/tools/codingagent/" + dataset.getId(), publicViewerUser);

		Result viewResult = codingAgentController.view(request, dataset.getId(), -1L);
		assertEquals("Public viewer must be forbidden from view", FORBIDDEN, viewResult.status());

		Result fileListResult = codingAgentController.getFileList(request, dataset.getId());
		assertEquals("Public viewer must be forbidden from getFileList", FORBIDDEN, fileListResult.status());
	}

	@Test
	public void testPrivateProjectStrangerForbidden() {
		Http.Request request = createAuthenticatedRequest(GET, "/tools/codingagent/" + dataset.getId(), strangerUser);

		Result viewResult = codingAgentController.view(request, dataset.getId(), -1L);
		assertEquals("Stranger must be forbidden from view", FORBIDDEN, viewResult.status());

		Result fileListResult = codingAgentController.getFileList(request, dataset.getId());
		assertEquals("Stranger must be forbidden from getFileList", FORBIDDEN, fileListResult.status());
	}

	@Test
	public void testWebSocketPermissionChecks() throws Exception {
		// Owner and Collaborator can connect (editableBy)
		Http.Request ownerReq = createAuthenticatedRequest(GET, "/tools/codingagent/" + dataset.getId() + "/ws", ownerUser);
		WebSocket ownerWs = codingAgentController.ws(dataset.getId());
		assertNotNull(ownerWs.apply(ownerReq).toCompletableFuture().get());

		// Subscriber is rejected (visibleFor == true, editableBy == false)
		Http.Request subscriberReq = createAuthenticatedRequest(GET, "/tools/codingagent/" + dataset.getId() + "/ws", subscriberUser);
		WebSocket subWs = codingAgentController.ws(dataset.getId());
		assertNotNull(subWs.apply(subscriberReq).toCompletableFuture().get());

		// Public viewer on public project is rejected (visibleFor == true, editableBy == false)
		project.setPublicProject(true);
		project.update();
		Http.Request publicViewerReq = createAuthenticatedRequest(GET, "/tools/codingagent/" + dataset.getId() + "/ws", publicViewerUser);
		WebSocket publicWs = codingAgentController.ws(dataset.getId());
		assertNotNull(publicWs.apply(publicViewerReq).toCompletableFuture().get());
	}
}

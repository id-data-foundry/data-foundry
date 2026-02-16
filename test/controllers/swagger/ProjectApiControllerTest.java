package controllers.swagger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.DELETE;
import static play.test.Helpers.GET;
import static play.test.Helpers.POST;
import static play.test.Helpers.contentAsString;
import static play.test.Helpers.route;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import models.Collaboration;
import models.Person;
import models.Project;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class ProjectApiControllerTest extends WithApplication {

	private TokenResolverUtil tokenUtil;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder().configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true).build();
	}

	@Before
	public void setUp() {
		tokenUtil = app.injector().instanceOf(TokenResolverUtil.class);
	}

	// Helper to create a user and return their API token
	private Person createUser(String firstName, String email) {
		Person user = new Person();
		user.setFirstname(firstName);
		user.setLastname("TestUser");
		user.setEmail(email);
		user.setUser_id(UUID.randomUUID().toString());
		user.save();

		String token = tokenUtil.createUserAccessToken(user.getId(), System.currentTimeMillis() + 1000000);
		user.setAccesscode(token);
		user.update();
		return user;
	}

	private Project createProject(Person owner, String name) {
		Project project = Project.create(name, owner, "Test Intro", false, false);
		project.save();
		return project;
	}

	private Http.RequestBuilder authenticatedRequest(String method, String uri, String token) {
		return new Http.RequestBuilder().method(method).uri(uri).header("X-API-Key", "test-api-key")
				.header("X-API-Token", token);
	}

	@Test
	public void testListProjects() {
		Person user = createUser("Alice", "alice@example.com");
		String token = user.getAccesscode();

		Result result = route(app, authenticatedRequest(GET, "/api/v2/projects", token));
		assertEquals(OK, result.status());
		JsonNode json = Json.parse(contentAsString(result));
		assertTrue(json.has("ownProjects"));
		assertEquals(0, json.get("ownProjects").size());
	}

	@Test
	public void testSearchProjects() {
		Person user = createUser("Searcher", "searcher@example.com");
		Project p1 = createProject(user, "Alpha Project");
		p1.setPublicProject(true);
		p1.update();

		Project p2 = createProject(user, "Beta Project");
		p2.setPublicProject(true);
		p2.update();

		// Search all public
		Result result = route(app, authenticatedRequest(GET, "/api/v2/projects/search", user.getAccesscode()));
		assertEquals(OK, result.status());
		JsonNode json = Json.parse(contentAsString(result));
		assertTrue(json.get("data").get("projects").size() >= 2);

		// Search query
		result = route(app, authenticatedRequest(GET, "/api/v2/projects/search/Alpha", user.getAccesscode()));
		assertEquals(OK, result.status());
		json = Json.parse(contentAsString(result));
		assertTrue(json.get("data").get("projects").size() >= 1);
	}

	@Test
	public void testProjectCRUD() {
		Person user = createUser("Creator", "creator@example.com");
		String token = user.getAccesscode();

		// ADD
		ObjectNode projectData = Json.newObject();
		projectData.put("name", "CRUD Project");
		projectData.put("intro", "Intro");
		projectData.put("license", "MIT license");

		Result addResult = route(app, authenticatedRequest(POST, "/api/v2/projects/add", token).bodyJson(projectData));
		assertEquals(OK, addResult.status());
		long projectId = Json.parse(contentAsString(addResult)).get("id").asLong();

		// VIEW
		Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/projects/" + projectId, token));
		assertEquals(OK, viewResult.status());
		assertEquals("CRUD Project", Json.parse(contentAsString(viewResult)).get("name").asText());

		// EDIT
		ObjectNode editData = Json.newObject();
		editData.put("name", "Updated Project");
		Result editResult = route(app,
				authenticatedRequest(POST, "/api/v2/projects/" + projectId + "/edit", token).bodyJson(editData));
		assertEquals(OK, editResult.status());

		// Verify Edit
		viewResult = route(app, authenticatedRequest(GET, "/api/v2/projects/" + projectId, token));
		assertEquals("Updated Project", Json.parse(contentAsString(viewResult)).get("name").asText());

		// ARCHIVE
		Result archiveResult = route(app,
				authenticatedRequest(POST, "/api/v2/projects/" + projectId + "/archive", token));
		assertEquals(OK, archiveResult.status());

		viewResult = route(app, authenticatedRequest(GET, "/api/v2/projects/" + projectId, token));
		assertTrue(Json.parse(contentAsString(viewResult)).get("isArchived").asBoolean());

		// REOPEN
		Result reopenResult = route(app,
				authenticatedRequest(POST, "/api/v2/projects/" + projectId + "/reopen", token));
		assertEquals(OK, reopenResult.status());

		viewResult = route(app, authenticatedRequest(GET, "/api/v2/projects/" + projectId, token));
		assertFalse(Json.parse(contentAsString(viewResult)).get("isArchived").asBoolean());
	}

	@Test
	public void testLabNotes() {
		Person user = createUser("Scientist", "scientist@example.com");
		Project project = createProject(user, "Lab Project");

		Result result = route(app,
				authenticatedRequest(GET, "/api/v2/projects/" + project.getId() + "/labnotes", user.getAccesscode()));
		assertEquals(OK, result.status());
		assertTrue(Json.parse(contentAsString(result)).isArray());
	}

	@Test
	public void testSubscriptions() {
		Person owner = createUser("Owner", "owner@example.com");
		Person subscriber = createUser("Subscriber", "sub@example.com");
		Project project = createProject(owner, "Public Project");
		project.setShareableProject(true); // Allow direct subscription
		project.save();

		// Subscribe
		Result subResult = route(app, authenticatedRequest(POST,
				"/api/v2/projects/" + project.getId() + "/subscriptions", subscriber.getAccesscode()));
		assertEquals(OK, subResult.status());

		// Check subscription
		project.refresh();
		assertEquals(1, project.getSubscribers().size());

		// Unsubscribe
		Result unsubResult = route(app, authenticatedRequest(DELETE,
				"/api/v2/projects/" + project.getId() + "/subscriptions", subscriber.getAccesscode()));
		assertEquals(OK, unsubResult.status());

		project.refresh();
		assertEquals(0, project.getSubscribers().size());
	}

	@Test
	public void testSubscriptionWithToken() {
		Person owner = createUser("Owner2", "owner2@example.com");
		Person subscriber = createUser("Subscriber2", "sub2@example.com");
		Project project = createProject(owner, "Private Project");
		project.setShareableProject(false);
		project.save();

		// 1. Request subscription (sends email, but we simulate token usage)
		// This endpoint sends email if not shareable
		Result reqResult = route(app, authenticatedRequest(POST,
				"/api/v2/projects/" + project.getId() + "/subscriptions", subscriber.getAccesscode()));
		assertEquals(OK, reqResult.status());

		// 2. Accept subscription using token (Owner action)
		String subToken = tokenUtil.getSubscriptionToken(project.getId(), subscriber.getId());
		Result accResult = route(app, authenticatedRequest(POST,
				"/api/v2/projects/" + project.getId() + "/subscriptions/" + subToken, owner.getAccesscode()));
		assertEquals(OK, accResult.status());

		project.refresh();
		assertEquals(1, project.getSubscribers().size());

		// 3. Remove subscription (Owner action) - note: controller expects subscriber
		// ID, not subscription ID
		Result remResult = route(app, authenticatedRequest(DELETE,
				"/api/v2/projects/" + project.getId() + "/subscriptions/" + subscriber.getId(), owner.getAccesscode()));
		assertEquals(OK, remResult.status());

		project.refresh();
		assertEquals(0, project.getSubscribers().size());
	}

	@Test
	public void testCollaborations() {
		Person owner = createUser("CollabOwner", "cowner@example.com");
		Person collaborator = createUser("CollabPartner", "partner@example.com");
		Project project = createProject(owner, "Collab Project");

		// 1. Invite Collaborator
		Result inviteResult = route(app,
				authenticatedRequest(POST,
						"/api/v2/projects/" + project.getId() + "/collaborators/" + collaborator.getEmail(),
						owner.getAccesscode()));
		assertEquals(OK, inviteResult.status());

		// 2. Accept Collaboration (Partner action via token)
		String collabToken = tokenUtil.getCollaborationToken(project.getId(), collaborator.getEmail());
		Result accResult = route(app,
				authenticatedRequest(POST, "/api/v2/projects/" + project.getId() + "/collaborations/" + collabToken,
						collaborator.getAccesscode()));
		assertEquals(OK, accResult.status());

		project.refresh();
		assertEquals(1, project.getCollaborators().size());

		// 3. Cancel Collaboration (Partner action)
		Result cancelResult = route(app, authenticatedRequest(DELETE,
				"/api/v2/projects/" + project.getId() + "/collaborations", collaborator.getAccesscode()));
		assertEquals(OK, cancelResult.status());

		project.refresh();
		assertEquals(0, project.getCollaborators().size());

		// 4. Re-invite and Remove by Owner
		// Manually add for brevity or re-invite
		Collaboration c = new Collaboration(collaborator, project);
		c.save();
		project.getCollaborators().add(c);
		project.update();
		project.refresh();

		Result removeResult = route(app,
				authenticatedRequest(DELETE,
						"/api/v2/projects/" + project.getId() + "/collaborators/" + collaborator.getId(),
						owner.getAccesscode()));
		assertEquals(OK, removeResult.status());

		project.refresh();
		assertEquals(0, project.getCollaborators().size());
	}
}
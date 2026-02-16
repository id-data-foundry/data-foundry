package controllers.swagger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.POST;
import static play.test.Helpers.contentAsString;
import static play.test.Helpers.route;

import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import models.Person;
import models.Project;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class ClusterApiControllerTest extends WithApplication {

	private TokenResolverUtil tokenUtil;
	private static final String TEST_ACCESS_CODE = "test-access-code";
	private static final String TEST_PROJECT_SECRET = "test-project-secret";
	private static final String TEST_API_KEY = "test-api-key";

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder().configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				// Configure the registration key so validation passes
				.configure("df.keys.registration", Collections.singletonList(TEST_ACCESS_CODE))
				// Ensure project secret is present for TokenResolverUtil initialization
				.configure("df.keys.project", TEST_PROJECT_SECRET).configure("df.keys.v2.user.api", TEST_API_KEY)
				.build();
	}

	@Before
	public void setUp() {
		tokenUtil = app.injector().instanceOf(TokenResolverUtil.class);
	}

	private Http.RequestBuilder authenticatedRequest(String method, String uri, String token) {
		return new Http.RequestBuilder().method(method).uri(uri).header("X-API-Key", TEST_API_KEY).header("X-API-Token",
				token);
	}

	private Person createUser(String firstName, String email) {
		Person user = Person.register(UUID.randomUUID().toString(), firstName, "User", email, "", "password123",
				TEST_ACCESS_CODE);
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

	private Cluster createCluster(Project project, String name) {
		Cluster cluster = new Cluster(name);
		cluster.create();
		cluster.setProject(project);
		cluster.save();
		project.getClusters().add(cluster);
		project.update();
		return cluster;
	}

	private Device createDevice(Project project, String name) {
		Device device = new Device();
		device.setName(name);
		device.setProject(project);
		device.save();
		project.getDevices().add(device);
		project.update();
		return device;
	}

	private Participant createParticipant(Project project) {
		Participant participant = new Participant("First name", "Last name");
		participant.setProject(project);
		participant.save();
		project.getParticipants().add(participant);
		project.update();
		return participant;
	}

	private Wearable createWearable(Project project, String name) {
		Wearable wearable = new Wearable();
		wearable.setName(name);
		wearable.setProject(project);
		wearable.save();
		project.getWearables().add(wearable);
		project.update();
		return wearable;
	}

	@Test
	public void testClustersList() {
		Person user = createUser("ClusterUser", "cluster@example.com");
		Project p = createProject(user, "Cluster Project");
		createCluster(p, "C1");
		createCluster(p, "C2");

		Result result = route(app, authenticatedRequest(GET, "/api/v2/clusters", user.getAccesscode()));
		assertEquals(OK, result.status());
		JsonNode json = Json.parse(contentAsString(result));
		assertTrue(json.get("data").get("clusters").isArray());
		assertEquals(2, json.get("data").get("clusters").size());
	}

	@Test
	public void testClusterCRUD() {
		Person user = createUser("ClusterCRUD", "crud_cluster@example.com");
		Project p = createProject(user, "CRUD Project");
		String token = user.getAccesscode();

		// ADD
		ObjectNode clusterData = Json.newObject();
		clusterData.put("name", "New Cluster");
		clusterData.put("project_id", p.getId().toString()); // api expects string ID according to controller
																// implementation? -> Long.parseLong(projectId)

		Result addResult = route(app, authenticatedRequest(POST, "/api/v2/clusters/add", token).bodyJson(clusterData));
		assertEquals(OK, addResult.status());
		long clusterId = Json.parse(contentAsString(addResult)).get("id").asLong();

		// VIEW
		Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/clusters/" + clusterId, token));
		assertEquals(OK, viewResult.status());
		assertEquals("New Cluster", Json.parse(contentAsString(viewResult)).get("name").asText());

		// RENAME
		Result renameResult = route(app,
				authenticatedRequest(GET, "/api/v2/clusters/rename/" + clusterId + "/RenamedCluster", token));
		assertEquals(OK, renameResult.status());

		// Verify Rename
		viewResult = route(app, authenticatedRequest(GET, "/api/v2/clusters/" + clusterId, token));
		assertEquals("RenamedCluster", Json.parse(contentAsString(viewResult)).get("name").asText());

		// DELETE
		Result deleteResult = route(app, authenticatedRequest(GET, "/api/v2/clusters/delete/" + clusterId, token));
		assertEquals(OK, deleteResult.status());

		// Verify Delete
		Result notFoundResult = route(app, authenticatedRequest(GET, "/api/v2/clusters/" + clusterId, token));
		assertEquals(NOT_FOUND, notFoundResult.status());
	}

	@Test
	public void testClusterComponents() {
		Person user = createUser("CompUser", "comp@example.com");
		Project p = createProject(user, "Comp Project");
		Cluster c = createCluster(p, "Comp Cluster");
		String token = user.getAccesscode();

		Device d = createDevice(p, "TestDevice");
		Participant part = createParticipant(p);
		Wearable w = createWearable(p, "TestWearable");

		// Add Device
		Result addDevice = route(app,
				authenticatedRequest(GET, "/api/v2/clusters/addDevice/" + c.getId() + "/" + d.getId(), token));
		assertEquals(OK, addDevice.status());

		// Add Participant
		Result addPart = route(app,
				authenticatedRequest(GET, "/api/v2/clusters/addParticipant/" + c.getId() + "/" + part.getId(), token));
		assertEquals(OK, addPart.status());

		// Add Wearable
		Result addWearable = route(app,
				authenticatedRequest(GET, "/api/v2/clusters/addWearable/" + c.getId() + "/" + w.getId(), token));
		assertEquals(OK, addWearable.status());

		// Verify via View
		Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/clusters/" + c.getId(), token));
		JsonNode json = Json.parse(contentAsString(viewResult));
		assertEquals(1, json.get("devices").size());
		assertEquals(1, json.get("participants").size());
		assertEquals(1, json.get("wearables").size());

		// Remove Device
		Result removeDevice = route(app,
				authenticatedRequest(GET, "/api/v2/clusters/removeDevice/" + c.getId() + "/" + d.getId(), token));
		assertEquals(OK, removeDevice.status());

		// Remove Participant
		Result removePart = route(app, authenticatedRequest(GET,
				"/api/v2/clusters/removeParticipant/" + c.getId() + "/" + part.getId(), token));
		assertEquals(OK, removePart.status());

		// Remove Wearable
		Result removeWearable = route(app,
				authenticatedRequest(GET, "/api/v2/clusters/removeWearable/" + c.getId() + "/" + w.getId(), token));
		assertEquals(OK, removeWearable.status());

		// Verify Empty
		viewResult = route(app, authenticatedRequest(GET, "/api/v2/clusters/" + c.getId(), token));
		json = Json.parse(contentAsString(viewResult));
		assertEquals(0, json.get("devices").size());
		assertEquals(0, json.get("participants").size());
		assertEquals(0, json.get("wearables").size());
	}
}

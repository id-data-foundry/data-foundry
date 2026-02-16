package controllers.swagger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.FORBIDDEN;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.DELETE;
import static play.test.Helpers.GET;
import static play.test.Helpers.POST;
import static play.test.Helpers.PUT;
import static play.test.Helpers.contentAsString;
import static play.test.Helpers.route;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class DatasetApiControllerTest extends WithApplication {

	private TokenResolverUtil tokenUtil;
	private DatasetConnector datasetConnector;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder().configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true).build();
	}

	@Before
	public void setUp() {
		tokenUtil = app.injector().instanceOf(TokenResolverUtil.class);
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
	}

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

	private Dataset createDataset(Project project, String name, DatasetType type) {
		Dataset ds = datasetConnector.create(name, type, project, "Test Description", "Test Target", null);
		ds.save();
		return ds;
	}

	private Http.RequestBuilder authenticatedRequest(String method, String uri, String token) {
		return new Http.RequestBuilder().method(method).uri(uri).header("X-API-Key", "test-api-key")
				.header("X-API-Token", token);
	}

	@Test
	public void testListDatasets() {
		Person user = createUser("DataUser", "data@example.com");
		String token = user.getAccesscode();

		Project p = createProject(user, "Data Project");
		createDataset(p, "DS1", DatasetType.IOT);
		createDataset(p, "DS2", DatasetType.MOVEMENT);

		// use /api/v2/datasets without trailing /
		Result result = route(app, authenticatedRequest(GET, "/api/v2/datasets", token));
		assertEquals(OK, result.status());
		JsonNode json = Json.parse(contentAsString(result));
		// The deprecated endpoint might return a different structure, or simple OK.
		// Based on routes: "list all datasets of the user"
		assertNotNull(json);
	}

	@Test
	public void testViewDataset() {
		Person user = createUser("Viewer", "viewer@example.com");
		Project p = createProject(user, "View Project");
		Dataset ds = createDataset(p, "ViewDS", DatasetType.IOT);

		Result result = route(app, authenticatedRequest(GET, "/api/v2/datasets/" + ds.getId(), user.getAccesscode()));
		assertEquals(OK, result.status());
		JsonNode json = Json.parse(contentAsString(result));
		assertEquals("ViewDS", json.get("name").asText());
	}

	@Test
	public void testSearchDatasets() {
		Person user = createUser("SearcherDS", "searcherds@example.com");
		Project p = createProject(user, "Search Project");
		p.setPublicProject(true);
		p.update();

		Dataset ds1 = createDataset(p, "Alpha DS", DatasetType.IOT);
		ds1.update();

		Dataset ds2 = createDataset(p, "Beta DS", DatasetType.MOVEMENT);
		ds2.update();

		System.out.println("Test 0");

		// Search All
		Result result = route(app, authenticatedRequest(GET, "/api/v2/datasets/search", user.getAccesscode()));
		assertEquals(OK, result.status());
		JsonNode json = Json.parse(contentAsString(result));
		assertTrue(json.get("data").get("datasets").isArray());

		System.out.println("Test 1");

		// Search with Query
		result = route(app,
				authenticatedRequest(GET, "/api/v2/datasets/search/All%20dataset/Alpha", user.getAccesscode()));
		assertEquals(OK, result.status());

		System.out.println("Test 2");

		// Search with Filter
		result = route(app, authenticatedRequest(GET, "/api/v2/datasets/search/Movement", user.getAccesscode()));
		assertEquals(OK, result.status());
	}

	@Test
	public void testDatasetCRUD() {
		Person user = createUser("CRUDUser", "crud@example.com");
		Project p = createProject(user, "CRUD Project");
		String token = user.getAccesscode();

		// ADD
		ObjectNode dsData = Json.newObject();
		dsData.put("dataset_name", "New IOT DS");
		dsData.put("project_id", p.getId());
		dsData.put("dataset_type", "IOT");
		dsData.put("description", "Description");
		dsData.put("start-date", "2023-01-01");
		dsData.put("end-date", "2023-12-31");
		dsData.put("isOpenParticipation", false);

		Result addResult = route(app, authenticatedRequest(POST, "/api/v2/datasets/add", token).bodyJson(dsData));
		assertEquals(OK, addResult.status());
		long dsId = Json.parse(contentAsString(addResult)).get("id").asLong();

		// EDIT
		ObjectNode editData = Json.newObject();
		editData.put("dataset_name", "Updated IOT DS");
		Result editResult = route(app,
				authenticatedRequest(POST, "/api/v2/datasets/edit/" + dsId, token).bodyJson(editData));
		assertEquals(OK, editResult.status());

		// Verify
		Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/datasets/" + dsId, token));
		assertEquals("Updated IOT DS", Json.parse(contentAsString(viewResult)).get("name").asText());
	}

	@Test
	public void testIoTRecord() {
		Person user = createUser("IoTUser", "iot@example.com");
		Project p = createProject(user, "IoT Project");
		Dataset ds = createDataset(p, "IoT DS", DatasetType.IOT);
		ds.setOpenParticipation(true);
		ds.update();

		ObjectNode record = Json.newObject();
		record.put("source_id", "device_1");
		record.put("activity", "test");
		record.put("data", "{\"val\": 123}");

		Result result = route(app,
				authenticatedRequest(POST, "/api/v2/datasets/iot/" + ds.getId(), user.getAccesscode())
						.bodyJson(record));
		assertEquals(OK, result.status());
	}

	@Test
	public void testEntityCRUD() {
		Person user = createUser("EntityUser", "entity@example.com");
		Project p = createProject(user, "Entity Project");
		Dataset ds = createDataset(p, "Entity DS", DatasetType.ENTITY);
		String token = user.getAccesscode();

		String resId = "res1";
		String itemToken = "secret123";

		// ADD Item
		ObjectNode addData = Json.newObject();
		addData.put("resource_id", resId);
		addData.put("token", itemToken);
		addData.put("data", "{\"name\": \"Item1\"}");

		Result addResult = route(app,
				authenticatedRequest(POST, "/api/v2/datasets/entity/" + ds.getId(), token).bodyJson(addData));
		assertEquals(OK, addResult.status());

		// GET Item
		// Query params in uri
		String uri = "/api/v2/datasets/entity/" + ds.getId() + "?resource_id=" + resId + "&token=" + itemToken;
		Result getResult = route(app, authenticatedRequest(GET, uri, token));
		assertEquals(OK, getResult.status());

		// UPDATE Item
		ObjectNode updateData = Json.newObject();
		updateData.put("resource_id", resId);
		updateData.put("token", itemToken);
		updateData.put("data", "{\"name\": \"Item1_Updated\"}");

		Result putResult = route(app,
				authenticatedRequest(PUT, "/api/v2/datasets/entity/" + ds.getId(), token).bodyJson(updateData));
		assertEquals(OK, putResult.status());

		// DELETE Item
		ObjectNode deleteData = Json.newObject();
		deleteData.put("resource_id", resId);
		deleteData.put("token", itemToken);

		// DELETE method body support depends on server/client. Play supports it.
		Result delResult = route(app,
				authenticatedRequest(DELETE, "/api/v2/datasets/entity/" + ds.getId(), token).bodyJson(deleteData));
		assertEquals(OK, delResult.status());
	}

	@Test
	public void testDeleteDataset() {
		Person user = createUser("DeleteUser", "delete@example.com");
		Project p = createProject(user, "Delete Project");
		Dataset ds = createDataset(p, "To Be Deleted", DatasetType.IOT);
		String token = user.getAccesscode();

		// DELETE
		Result deleteResult = route(app, authenticatedRequest(DELETE, "/api/v2/datasets/" + ds.getId(), token));
		assertEquals(OK, deleteResult.status());

		// Verify dataset is gone
		Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/datasets/" + ds.getId(), token));
		assertEquals(NOT_FOUND, viewResult.status());

		// Test deletion by non-owner
		Person otherUser = createUser("OtherUser", "other@example.com");
		Dataset ds2 = createDataset(p, "Other DS", DatasetType.IOT); // Project owned by user
		Result forbiddenDelete = route(app,
				authenticatedRequest(DELETE, "/api/v2/datasets/" + ds2.getId(), otherUser.getAccesscode()));
		assertEquals(FORBIDDEN, forbiddenDelete.status());
	}
}

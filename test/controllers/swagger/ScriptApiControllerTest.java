package controllers.swagger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.POST;
import static play.test.Helpers.PUT;
import static play.test.Helpers.contentAsString;
import static play.test.Helpers.route;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

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

public class ScriptApiControllerTest extends WithApplication {

	private TokenResolverUtil tokenUtil;
	private DatasetConnector datasetConnector;
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
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
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

	private Dataset createScript(Project project, String name) {
		Dataset ds = datasetConnector.create(name, DatasetType.COMPLETE, project, "Test Description", "Test Target",
				null);
		ds.setCollectorType(Dataset.ACTOR);
		ds.save();
		return ds;
	}

	@Test
	public void testScriptCRUD() {
		Person user = createUser("ScriptCRUD", "crud_script@example.com");
		Project p = createProject(user, "Script Project");
		String token = user.getAccesscode();

		// ADD
		ObjectNode scriptData = Json.newObject();
		scriptData.put("script_name", "New Script");
		scriptData.put("description", "Description of script");

		Result addResult = route(app,
				authenticatedRequest(POST, "/api/v2/scripts/add/" + p.getId(), token).bodyJson(scriptData));
		assertEquals(OK, addResult.status());
		long scriptId = Json.parse(contentAsString(addResult)).get("id").asLong();

		// VIEW
		Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/scripts/" + scriptId, token));
		assertEquals(OK, viewResult.status());
		assertEquals("New Script", Json.parse(contentAsString(viewResult)).get("name").asText());

		// EDIT
		ObjectNode editData = Json.newObject();
		editData.put("script_name", "Updated Script");
		editData.put("description", "Updated Description");

		Result editResult = route(app,
				authenticatedRequest(PUT, "/api/v2/scripts/edit/" + scriptId, token).bodyJson(editData));
		assertEquals(OK, editResult.status());

		// Verify Edit
		viewResult = route(app, authenticatedRequest(GET, "/api/v2/scripts/" + scriptId, token));
		assertEquals("Updated Script", Json.parse(contentAsString(viewResult)).get("name").asText());
	}

	@Test
	public void testSaveCode() {
		Person user = createUser("Coder", "coder@example.com");
		Project p = createProject(user, "Code Project");
		Dataset ds = createScript(p, "Code Script");
		String token = user.getAccesscode();

		ObjectNode codeData = Json.newObject();
		codeData.put("code", "DF.print(\"Hello World\");");

		Result result = route(app,
				authenticatedRequest(PUT, "/api/v2/scripts/saveCode/" + ds.getId(), token).bodyJson(codeData));
		assertEquals(OK, result.status());

		// Verify code via view
		Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/scripts/" + ds.getId(), token));
		assertEquals("DF.print(\"Hello World\");", Json.parse(contentAsString(viewResult)).get("code").asText());
	}

	@Test
	public void testInstallUninstall() {
		Person user = createUser("Installer", "install@example.com");
		Project p = createProject(user, "Install Project");
		Dataset ds = createScript(p, "Install Script");
		String token = user.getAccesscode();

		Result installResult = route(app,
				authenticatedRequest(PUT, "/api/v2/scripts/install/" + ds.getId() + "/testChannel", token));
		assertEquals(OK, installResult.status());

		Result uninstallResult = route(app,
				authenticatedRequest(PUT, "/api/v2/scripts/uninstall/" + ds.getId(), token));
		assertEquals(OK, uninstallResult.status());
	}

	@Test
	public void testLog() {
		Person user = createUser("Logger", "logger@example.com");
		Project p = createProject(user, "Log Project");
		Dataset ds = createScript(p, "Log Script");
		String token = user.getAccesscode();

		Result result = route(app, authenticatedRequest(GET, "/api/v2/scripts/log/" + ds.getId(), token));
		assertEquals(OK, result.status());
	}

	@Test
	public void testExecute() throws Exception {
		Person user = createUser("Executer", "execute@example.com");
		Project p = createProject(user, "Execute Project");
		Dataset ds = createScript(p, "Execute Script");
		String token = user.getAccesscode();

		// Random number
		int randomNumber = (int) (Math.random() * 10000);
		String data = "var data = {};";
		String code = "DF.print(" + randomNumber + ");";

		// Construct URL with encoded query params
		String url = "/api/v2/scripts/execute/" + ds.getId() + "?data="
				+ URLEncoder.encode(data, StandardCharsets.UTF_8.toString()) + "&code="
				+ URLEncoder.encode(code, StandardCharsets.UTF_8.toString());

		// EXECUTE
		Result result = route(app, authenticatedRequest(GET, url, token));
		assertEquals(OK, result.status());

		String output = contentAsString(result);
		assertTrue("Output should contain " + randomNumber, output.contains(String.valueOf(randomNumber)));
	}
}

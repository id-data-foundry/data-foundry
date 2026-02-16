package controllers.swagger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.FORBIDDEN;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.POST;
import static play.test.Helpers.contentAsString;
import static play.test.Helpers.route;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import models.Person;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class UserApiControllerTest extends WithApplication {

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
				.configure("df.keys.project", TEST_PROJECT_SECRET)
				// user multi user api key
				.configure("df.keys.v2.multi.api", TEST_API_KEY).build();
	}

	@Before
	public void setUp() {
		tokenUtil = app.injector().instanceOf(TokenResolverUtil.class);
	}

	private Http.RequestBuilder apiRequest(String method, String uri) {
		return new Http.RequestBuilder().method(method).uri(uri).header("X-API-Key", TEST_API_KEY);
	}

	private Http.RequestBuilder authenticatedRequest(String method, String uri, String token) {
		return apiRequest(method, uri).header("X-API-Token", token);
	}

	@Test
	public void testRegisterMe() {
		ObjectNode registerData = Json.newObject();
		registerData.put("first_name", "John");
		registerData.put("last_name", "Doe");
		registerData.put("email", "john.doe@example.com");
		registerData.put("password", "password123");
		registerData.put("re_password", "password123");
		registerData.put("access_code", TEST_ACCESS_CODE);
		registerData.put("website", "https://example.com");
		registerData.put("user_id", "testUser1");

		// Use apiRequest for endpoints that only require API Key (V2MultiUserApiAuth)
		Result result = route(app, apiRequest(POST, "/api/v2/users/register").bodyJson(registerData));
		assertEquals(OK, result.status());
		JsonNode json = Json.parse(contentAsString(result));
		assertNotNull(json.get("id"));

		// Verify user exists in DB
		Optional<Person> userOpt = Person.findByEmail("john.doe@example.com");
		assertTrue(userOpt.isPresent());
		assertEquals("John", userOpt.get().getFirstname());
	}

	@Test
	public void testRegisterMeInvalidAccessCode() {
		ObjectNode registerData = Json.newObject();
		registerData.put("first_name", "Jane");
		registerData.put("last_name", "Doe");
		registerData.put("email", "jane.doe@example.com");
		registerData.put("password", "password123");
		registerData.put("re_password", "password123");
		registerData.put("access_code", "wrong-code");
		registerData.put("user_id", "testUser2");

		Result result = route(app, apiRequest(POST, "/api/v2/users/register").bodyJson(registerData));
		assertEquals(FORBIDDEN, result.status());
	}

	@Test
	public void testLoginUser() {
		// Create user first
		Person user = Person.register(UUID.randomUUID().toString(), "Login", "User", "login@example.com", "",
				"password123", TEST_ACCESS_CODE);
		user.save();
		// Set access token (although login generates one, it's good to have a user
		// fully set up)
		user.setAccesscode(tokenUtil.createUserAccessToken(user.getId(), System.currentTimeMillis() + 1000000));
		user.update();

		ObjectNode loginData = Json.newObject();
		loginData.put("username", "login@example.com");
		loginData.put("password", "password123");

		Result result = route(app, apiRequest(POST, "/api/v2/users/login").bodyJson(loginData));
		assertEquals(OK, result.status());
		JsonNode json = Json.parse(contentAsString(result));
		assertEquals("login@example.com", json.get("email").asText());
		assertNotNull(json.get("api-token"));
	}

	@Test
	public void testEditUser() {
		Person user = Person.register(UUID.randomUUID().toString(), "Edit", "User", "edit@example.com", "",
				"password123", TEST_ACCESS_CODE);
		user.save();
		// Needed for authentication
		String token = tokenUtil.createUserAccessToken(user.getId(), System.currentTimeMillis() + 1000000);
		user.setAccesscode(token);
		user.update();

		ObjectNode editData = Json.newObject();
		editData.put("first_name", "EditedName");

		// Use authenticatedRequest for endpoints that require API Key AND User Token
		// (V2UserApiAuth)
		Result result = route(app, authenticatedRequest(POST, "/api/v2/users/edit", token).bodyJson(editData));
		assertEquals(OK, result.status());

		Person updatedUser = Person.findByEmail("edit@example.com").get();
		assertEquals("EditedName", updatedUser.getFirstname());
	}

	@Test
	public void testLogout() {
		Person user = Person.register(UUID.randomUUID().toString(), "Logout", "User", "logout@example.com", "",
				"password123", TEST_ACCESS_CODE);
		user.save();
		String token = tokenUtil.createUserAccessToken(user.getId(), System.currentTimeMillis() + 1000000);
		user.setAccesscode(token);
		user.update();

		// Logout uses V2MultiUserApiAuth (Key only), but sending token doesn't hurt,
		// and we can test it like a user action
		Result result = route(app, apiRequest(GET, "/api/v2/users/logout"));
		assertEquals(OK, result.status());
	}
}
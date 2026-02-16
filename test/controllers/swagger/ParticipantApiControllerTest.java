package controllers.swagger;

import org.junit.Before;
import org.junit.Test;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import models.Person;
import models.Project;
import models.sr.Participant;
import utils.auth.TokenResolverUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import play.libs.Json;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.*;

public class ParticipantApiControllerTest extends WithApplication {

    private TokenResolverUtil tokenUtil;

    @Override
    protected Application provideApplication() {
        return new GuiceApplicationBuilder()
            .configure("db.default.driver", "org.h2.Driver")
            .configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
            .configure("play.evolutions.db.default.autoApply", true)
            // Mocking mailer configuration if needed, or assuming NotificationService handles nulls/test mode
            .configure("play.mailer.mock", true) 
            .build();
    }

    @Before
    public void setUp() {
        tokenUtil = app.injector().instanceOf(TokenResolverUtil.class);
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

    private Participant createParticipant(Project project, String email, String firstName, String lastName) {
        Participant participant = Participant.createInstance(firstName, lastName, email, project);
        participant.save();
        project.getParticipants().add(participant);
        project.update();
        return participant;
    }

    private Http.RequestBuilder authenticatedRequest(String method, String uri, String token) {
        return new Http.RequestBuilder()
                .method(method)
                .uri(uri)
                .header("X-API-Key", "test-api-key")
                .header("X-API-Token", token);
    }

    @Test
    public void testListParticipants() {
        Person user = createUser("PartUser", "part@example.com");
        String token = user.getAccesscode();
        
        Project p = createProject(user, "Participant Project");
        createParticipant(p, "p1@test.com", "P1", "Test");
        createParticipant(p, "p2@test.com", "P2", "Test");

        Result result = route(app, authenticatedRequest(GET, "/api/v2/participants", token));
        assertEquals(OK, result.status());
        JsonNode json = Json.parse(contentAsString(result));
        assertTrue(json.get("data").get("participants").isArray());
        assertEquals(2, json.get("data").get("participants").size());
    }

    @Test
    public void testViewParticipant() {
        Person user = createUser("ViewerPart", "viewpart@example.com");
        Project p = createProject(user, "View Part Project");
        Participant pt = createParticipant(p, "target@test.com", "Target", "Person");

        Result result = route(app, authenticatedRequest(GET, "/api/v2/participants/" + pt.getId(), user.getAccesscode()));
        assertEquals(OK, result.status());
        JsonNode json = Json.parse(contentAsString(result));
        assertEquals("target@test.com", json.get("email").asText());
    }

    @Test
    public void testAddParticipant() {
        Person user = createUser("AddPartUser", "addpart@example.com");
        Project p = createProject(user, "Add Part Project");
        String token = user.getAccesscode();

        ObjectNode partData = Json.newObject();
        partData.put("project_id", p.getId());
        partData.put("email", "new@test.com");
        partData.put("first_name", "New");
        partData.put("last_name", "User");

        Result result = route(app, authenticatedRequest(POST, "/api/v2/participants/add", token).bodyJson(partData));
        assertEquals(OK, result.status());
        
        JsonNode json = Json.parse(contentAsString(result));
        assertTrue(json.has("id"));
    }

    @Test
    public void testEditParticipant() {
        Person user = createUser("EditPartUser", "editpart@example.com");
        Project p = createProject(user, "Edit Part Project");
        Participant pt = createParticipant(p, "old@test.com", "Old", "Name");
        String token = user.getAccesscode();

        ObjectNode partData = Json.newObject();
        partData.put("first_name", "Updated");
        partData.put("last_name", "Name");
        partData.put("email", "updated@test.com");

        Result result = route(app, authenticatedRequest(POST, "/api/v2/participants/edit/" + pt.getId(), token).bodyJson(partData));
        assertEquals(OK, result.status());

        // Verify
        Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/participants/" + pt.getId(), token));
        JsonNode json = Json.parse(contentAsString(viewResult));
        assertEquals("updated@test.com", json.get("email").asText());
        assertEquals("Updated", json.get("firstname").asText());
    }

    @Test
    public void testDeleteParticipant() {
        Person user = createUser("DelPartUser", "delpart@example.com");
        Project p = createProject(user, "Del Part Project");
        Participant pt = createParticipant(p, "todelete@test.com", "To", "Delete");
        String token = user.getAccesscode();

        Result result = route(app, authenticatedRequest(DELETE, "/api/v2/participants/delete/" + pt.getId(), token));
        assertEquals(OK, result.status());

        // Verify it is gone
        Participant check = Participant.find.byId(pt.getId());
        assertTrue(check == null);
    }
}

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
import models.Dataset;
import models.DatasetType;
import models.sr.Wearable;
import datasets.DatasetConnector;
import utils.auth.TokenResolverUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import play.libs.Json;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.*;

public class WearableApiControllerTest extends WithApplication {

    private TokenResolverUtil tokenUtil;
    private DatasetConnector datasetConnector;

    @Override
    protected Application provideApplication() {
        return new GuiceApplicationBuilder()
            .configure("db.default.driver", "org.h2.Driver")
            .configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
            .configure("play.evolutions.db.default.autoApply", true)
            .build();
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

    private Wearable createWearable(Project project, Dataset ds, String name) {
        Wearable wearable = new Wearable();
        wearable.create();
        wearable.setName(name);
        wearable.setProject(project);
        wearable.setBrand(ds.getDsType().name());
        wearable.setScopes(String.valueOf(ds.getId()));
        wearable.save();
        project.getWearables().add(wearable);
        project.update();
        return wearable;
    }

    private Http.RequestBuilder authenticatedRequest(String method, String uri, String token) {
        return new Http.RequestBuilder()
                .method(method)
                .uri(uri)
                .header("X-API-Key", "test-api-key")
                .header("X-API-Token", token);
    }

    @Test
    public void testListWearables() {
        Person user = createUser("WearUser", "wear@example.com");
        String token = user.getAccesscode();
        
        Project p = createProject(user, "Wear Project");
        Dataset ds = createDataset(p, "FitbitDS", DatasetType.FITBIT);
        
        createWearable(p, ds, "Wear1");
        createWearable(p, ds, "Wear2");

        Result result = route(app, authenticatedRequest(GET, "/api/v2/wearables", token));
        assertEquals(OK, result.status());
        JsonNode json = Json.parse(contentAsString(result));
        assertTrue(json.get("data").get("wearables").isArray());
        assertEquals(2, json.get("data").get("wearables").size());
    }

    @Test
    public void testViewWearable() {
        Person user = createUser("ViewerWear", "viewwear@example.com");
        Project p = createProject(user, "View Wear Project");
        Dataset ds = createDataset(p, "FitbitDS", DatasetType.FITBIT);
        Wearable w = createWearable(p, ds, "ViewWear");

        Result result = route(app, authenticatedRequest(GET, "/api/v2/wearables/" + w.getId(), user.getAccesscode()));
        assertEquals(OK, result.status());
        JsonNode json = Json.parse(contentAsString(result));
        assertEquals("ViewWear", json.get("name").asText());
    }

    @Test
    public void testAddWearable() {
        Person user = createUser("AddWearUser", "addwear@example.com");
        Project p = createProject(user, "Add Wear Project");
        Dataset ds = createDataset(p, "FitbitDS", DatasetType.FITBIT);
        String token = user.getAccesscode();

        ObjectNode wearData = Json.newObject();
        wearData.put("dataset_id", ds.getId());
        wearData.put("brand", "FITBIT");
        wearData.put("name", "New Wearable");

        Result result = route(app, authenticatedRequest(POST, "/api/v2/wearables/add", token).bodyJson(wearData));
        assertEquals(OK, result.status());
        
        JsonNode json = Json.parse(contentAsString(result));
        assertTrue(json.has("id"));
    }

    @Test
    public void testEditWearable() {
        Person user = createUser("EditWearUser", "editwear@example.com");
        Project p = createProject(user, "Edit Wear Project");
        Dataset ds = createDataset(p, "FitbitDS", DatasetType.FITBIT);
        Wearable w = createWearable(p, ds, "Old Wear Name");
        String token = user.getAccesscode();

        ObjectNode wearData = Json.newObject();
        wearData.put("name", "Updated Wear Name");

        Result result = route(app, authenticatedRequest(POST, "/api/v2/wearables/edit/" + w.getId(), token).bodyJson(wearData));
        assertEquals(OK, result.status());

        // Verify
        Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/wearables/" + w.getId(), token));
        JsonNode json = Json.parse(contentAsString(viewResult));
        assertEquals("Updated Wear Name", json.get("name").asText());
    }
    
    @Test
    public void testResetWearable() {
        Person user = createUser("ResetWearUser", "resetwear@example.com");
        Project p = createProject(user, "Reset Wear Project");
        Dataset ds = createDataset(p, "FitbitDS", DatasetType.FITBIT);
        Wearable w = createWearable(p, ds, "Reset Wear");
        String token = user.getAccesscode();

        Result result = route(app, authenticatedRequest(POST, "/api/v2/wearables/reset/" + w.getId(), token));
        assertEquals(OK, result.status());
    }

    @Test
    public void testDeleteWearable() {
        Person user = createUser("DelWearUser", "delwear@example.com");
        Project p = createProject(user, "Del Wear Project");
        Dataset ds = createDataset(p, "FitbitDS", DatasetType.FITBIT);
        Wearable w = createWearable(p, ds, "ToDelete");
        String token = user.getAccesscode();

        Result result = route(app, authenticatedRequest(DELETE, "/api/v2/wearables/delete/" + w.getId(), token));
        assertEquals(OK, result.status());

        // Verify deletion
        Wearable deletedW = Wearable.find.byId(w.getId());
        assertTrue(deletedW == null);
    }
}

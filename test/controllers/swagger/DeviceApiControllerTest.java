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
import models.sr.Device;
import utils.auth.TokenResolverUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import play.libs.Json;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static play.test.Helpers.*;

public class DeviceApiControllerTest extends WithApplication {

    private TokenResolverUtil tokenUtil;

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

    private Device createDevice(Project project, String name) {
        Device device = new Device();
        device.create();
        device.setName(name);
        device.setProject(project);
        device.save();
        project.getDevices().add(device);
        project.update();
        return device;
    }

    private Http.RequestBuilder authenticatedRequest(String method, String uri, String token) {
        return new Http.RequestBuilder()
                .method(method)
                .uri(uri)
                .header("X-API-Key", "test-api-key")
                .header("X-API-Token", token);
    }

    @Test
    public void testListDevices() {
        Person user = createUser("DeviceUser", "device@example.com");
        String token = user.getAccesscode();
        
        Project p = createProject(user, "Device Project");
        createDevice(p, "Device1");
        createDevice(p, "Device2");

        Result result = route(app, authenticatedRequest(GET, "/api/v2/devices", token));
        assertEquals(OK, result.status());
        JsonNode json = Json.parse(contentAsString(result));
        assertTrue(json.get("data").get("devices").isArray());
        assertEquals(2, json.get("data").get("devices").size());
    }

    @Test
    public void testViewDevice() {
        Person user = createUser("Viewer", "viewdevice@example.com");
        Project p = createProject(user, "View Project");
        Device d = createDevice(p, "ViewDevice");

        Result result = route(app, authenticatedRequest(GET, "/api/v2/devices/" + d.getId(), user.getAccesscode()));
        assertEquals(OK, result.status());
        JsonNode json = Json.parse(contentAsString(result));
        assertEquals("ViewDevice", json.get("name").asText());
    }

    @Test
    public void testAddDevice() {
        Person user = createUser("AddDeviceUser", "adddevice@example.com");
        Project p = createProject(user, "Add Device Project");
        String token = user.getAccesscode();

        ObjectNode deviceData = Json.newObject();
        deviceData.put("project_id", p.getId());
        deviceData.put("name", "New Device");
        deviceData.put("category", "Sensor");
        deviceData.put("configuration", "{}");

        Result result = route(app, authenticatedRequest(POST, "/api/v2/devices/add", token).bodyJson(deviceData));
        assertEquals(OK, result.status());
        
        JsonNode json = Json.parse(contentAsString(result));
        assertTrue(json.has("id"));
    }

    @Test
    public void testEditDevice() {
        Person user = createUser("EditDeviceUser", "editdevice@example.com");
        Project p = createProject(user, "Edit Device Project");
        Device d = createDevice(p, "Old Device Name");
        String token = user.getAccesscode();

        ObjectNode deviceData = Json.newObject();
        deviceData.put("name", "Updated Device Name");
        deviceData.put("category", "Updated Category");

        Result result = route(app, authenticatedRequest(POST, "/api/v2/devices/edit/" + d.getId(), token).bodyJson(deviceData));
        assertEquals(OK, result.status());

        // Verify
        Result viewResult = route(app, authenticatedRequest(GET, "/api/v2/devices/" + d.getId(), token));
        JsonNode json = Json.parse(contentAsString(viewResult));
        assertEquals("Updated Device Name", json.get("name").asText());
        assertEquals("Updated Category", json.get("category").asText());
    }

    @Test
    public void testDeleteDevice() {
        Person user = createUser("DelDeviceUser", "deldevice@example.com");
        Project p = createProject(user, "Del Device Project");
        Device d = createDevice(p, "DeviceToDelete");
        String token = user.getAccesscode();

        Result result = route(app, authenticatedRequest(DELETE, "/api/v2/devices/delete/" + d.getId(), token));
        assertEquals(OK, result.status());

        // Verify it is gone
        Device check = Device.find.byId(d.getId());
        assertTrue(check == null);
    }
}

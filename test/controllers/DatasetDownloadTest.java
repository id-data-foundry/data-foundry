package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;
import static play.test.Helpers.contentAsString;
import static play.test.Helpers.route;

import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import org.apache.pekko.stream.Materializer;
import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import com.fasterxml.jackson.databind.JsonNode;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.EntityDS;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import models.sr.Device;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class DatasetDownloadTest extends WithApplication {

	private DatasetConnector datasetConnector;
	private TokenResolverUtil tokenResolver;
	private DatasetsController datasetsController;
	private Materializer mat;
	private SessionStore sessionStore;

	private Person ownerUser;
	private Project project;
	private Device device1;
	private Device device2;
	private Cluster cluster2;
	private Dataset timeseriesDs;
	private Dataset entityDs;
	private String tsPublicToken;
	private String entityPublicToken;

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

	@Before
	public void setUp() throws Exception {
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
		tokenResolver = app.injector().instanceOf(TokenResolverUtil.class);
		datasetsController = app.injector().instanceOf(DatasetsController.class);
		mat = app.injector().instanceOf(Materializer.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);

		ownerUser = new Person();
		ownerUser.setUser_id(UUID.randomUUID().toString());
		ownerUser.setFirstname("Owner");
		ownerUser.setLastname("User");
		ownerUser.setEmail("owner_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		ownerUser.save();

		project = Project.create("TestProject_" + UUID.randomUUID(), ownerUser, "Desc", false, false);
		project.setPublicProject(true);
		project.save();

		device1 = new Device();
		device1.setName("Device1_" + UUID.randomUUID().toString().substring(0, 8));
		device1.setProject(project);
		device1.save();
		project.getDevices().add(device1);

		device2 = new Device();
		device2.setName("Device2_" + UUID.randomUUID().toString().substring(0, 8));
		device2.setProject(project);
		device2.save();
		project.getDevices().add(device2);

		cluster2 = new Cluster("Cluster2");
		cluster2.create();
		cluster2.setProject(project);
		cluster2.getDevices().add(device2);
		cluster2.save();
		project.getClusters().add(cluster2);

		project.update();

		// Create Timeseries Dataset
		timeseriesDs = datasetConnector.create("Timeseries DS", DatasetType.IOT, project, "TS DS", "Target", "true");
		timeseriesDs.save();

		tsPublicToken = tokenResolver.getDatasetToken(timeseriesDs.getId());
		timeseriesDs.getConfiguration().put(Dataset.PUBLIC_ACCESS_TOKEN, tsPublicToken);
		timeseriesDs.update();

		// Add 55 records to device1
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(timeseriesDs);
		for (int i = 0; i < 55; i++) {
			tsds.internalAddRecord(device1, new Date(), "reading", "{\"index\":" + i + "}");
		}
		// Add 10 records to device2
		for (int i = 0; i < 10; i++) {
			tsds.internalAddRecord(device2, new Date(), "reading", "{\"cluster2_index\":" + i + "}");
		}
		TimeseriesDS.flushQueue();

		// Create Entity Dataset
		entityDs = datasetConnector.create("Entity DS", DatasetType.ENTITY, project, "Entity DS", "Target", "true");
		entityDs.save();

		entityPublicToken = tokenResolver.getDatasetToken(entityDs.getId());
		entityDs.getConfiguration().put(Dataset.PUBLIC_ACCESS_TOKEN, entityPublicToken);
		entityDs.update();

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(entityDs);
		eds.addItem("res_1", Optional.empty(), "{\"entity\":\"data1\"}");
		eds.addItem("res_2", Optional.empty(), "{\"entity\":\"data2\"}");
		eds.addItem("res_3", Optional.empty(), "{\"entity\":\"data3\"}");
	}

	private Http.Request createAuthenticatedRequest(String method, String uri, Person user) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method(method).uri(uri);
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
	public void testTimeseriesPublicDownloadJsonReturnsAllRows() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/downloadPublic/json/" + tsPublicToken);

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result, mat);
		JsonNode json = Json.parse(body);
		assertTrue(json.isArray());
		assertEquals("JSON public download should return all 65 records (55 device1 + 10 device2)", 65, json.size());
	}

	@Test
	public void testTimeseriesPublicDownloadCsvReturnsAllRowsMatchingJson() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/downloadPublic/" + tsPublicToken);

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result, mat);
		assertNotNull(body);

		String[] lines = body.trim().split("\n");
		// 1 header row + 65 data rows = 66 lines
		assertEquals("CSV public download should return 66 lines (1 header + 65 data rows)", 66, lines.length);
	}

	@Test
	public void testTimeseriesOwnerDownloadCsvReturnsAllRows() throws Exception {
		Http.Request request = createAuthenticatedRequest(GET, "/datasets/download/" + timeseriesDs.getId(), ownerUser);

		Result result = datasetsController.download(request, timeseriesDs.getId(), -1L, -1L, -1L)
				.toCompletableFuture().get();
		assertEquals(OK, result.status());

		String body = contentAsString(result, mat);
		assertNotNull(body);

		String[] lines = body.trim().split("\n");
		assertEquals("Owner CSV download should return 66 lines (1 header + 65 data rows)", 66, lines.length);
	}

	@Test
	public void testTimeseriesPublicDownloadCsvFilteredByCluster() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/downloadPublic/" + tsPublicToken + "?cluster_id=" + cluster2.getId());

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result, mat);
		assertNotNull(body);

		String[] lines = body.trim().split("\n");
		// 1 header row + 10 data rows for cluster2 = 11 lines
		assertEquals("Cluster-filtered CSV download should return 11 lines (1 header + 10 data rows)", 11, lines.length);
		assertTrue("Response must contain cluster2 data", body.contains("cluster2_index"));
	}

	@Test
	public void testEntityPublicDownloadCsvReturnsAllRows() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/datasets/downloadPublic/" + entityPublicToken);

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result, mat);
		assertNotNull(body);

		String[] lines = body.trim().split("\n");
		// 1 header row + 3 entities = 4 lines
		assertEquals("Entity CSV public download should return 4 lines (1 header + 3 data rows)", 4, lines.length);
		assertTrue(body.contains("res_1"));
		assertTrue(body.contains("res_2"));
		assertTrue(body.contains("res_3"));
	}
}

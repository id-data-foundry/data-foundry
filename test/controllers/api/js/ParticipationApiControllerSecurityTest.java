package controllers.api.js;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.FORBIDDEN;
import static play.mvc.Http.Status.NOT_FOUND;
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

import com.fasterxml.jackson.databind.JsonNode;

import controllers.auth.ParticipantAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.EntityDS;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class ParticipationApiControllerSecurityTest extends WithApplication {

	private DatasetConnector datasetConnector;
	private TokenResolverUtil tokenResolver;
	private Materializer mat;

	private Person ownerUser;
	private Project project;
	private Participant participantA;
	private Participant participantB;
	private Device deviceB;
	private Cluster clusterB;

	private Dataset timeseriesDs;
	private Dataset entityDs;
	private Dataset formDs;
	private Dataset completeDs;
	private Dataset annotationDs;

	private String tokenA;
	private String tokenB;

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
		tokenResolver = app.injector().instanceOf(TokenResolverUtil.class);
		mat = app.injector().instanceOf(Materializer.class);

		ownerUser = createPerson("owner");

		project = Project.create("SecurityHardeningProject_" + UUID.randomUUID(), ownerUser, "Desc", false, false);
		project.setPublicProject(true);
		project.save();

		// Participant A: enrolled but has NO devices
		participantA = Participant.createInstance("Participant", "A", "part_a_" + UUID.randomUUID() + "@example.com", project);
		project.getParticipants().add(participantA);

		// Participant B: enrolled and assigned to Cluster B with Device B
		participantB = Participant.createInstance("Participant", "B", "part_b_" + UUID.randomUUID() + "@example.com", project);
		project.getParticipants().add(participantB);

		deviceB = new Device();
		deviceB.setName("DeviceB_" + UUID.randomUUID().toString().substring(0, 8));
		deviceB.setProject(project);
		deviceB.save();
		project.getDevices().add(deviceB);

		clusterB = new Cluster("ClusterB");
		clusterB.create();
		clusterB.setProject(project);
		clusterB.getParticipants().add(participantB);
		clusterB.getDevices().add(deviceB);
		clusterB.save();
		project.getClusters().add(clusterB);

		project.update();

		// Refresh participants to ensure relations are loaded
		participantA.refresh();
		participantB.refresh();

		tokenA = tokenResolver.getParticipationToken(project.getId(), participantA.getId());
		tokenB = tokenResolver.getParticipationToken(project.getId(), participantB.getId());

		// Datasets
		timeseriesDs = datasetConnector.create("Timeseries DS", DatasetType.IOT, project, "TS DS", "Target", "true");
		timeseriesDs.save();

		entityDs = datasetConnector.create("Entity DS", DatasetType.ENTITY, project, "Entity DS", "Target", "true");
		entityDs.save();

		formDs = datasetConnector.create("Form DS", DatasetType.FORM, project, "Form DS", "Target", "true");
		formDs.save();

		completeDs = datasetConnector.create("Complete DS", DatasetType.COMPLETE, project, "Complete DS", "Target", "true");
		completeDs.save();

		annotationDs = datasetConnector.create("Annotation DS", DatasetType.ANNOTATION, project, "Annotation DS", "Target", "true");
		annotationDs.save();

		// Insert Timeseries data for Device B
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(timeseriesDs);
		tsds.internalAddRecord(deviceB, new Date(), "heartrate", "{\"secret\":\"secret_reading_from_device_b\"}");
		TimeseriesDS.flushQueue();

		// Insert Entity data for Participant A and Participant B
		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(entityDs);
		eds.addItem(participantA.getRefId(), Optional.empty(), "{\"user_data\":\"secret_for_participant_a\"}");
		eds.addItem(participantB.getRefId(), Optional.empty(), "{\"user_data\":\"secret_for_participant_b\"}");
	}

	@Test
	public void testDFSecurity_ParticipantWithoutDevicesGetsZeroRowsFromTimeseriesDatasetCSV() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + timeseriesDs.getId() + ".csv")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result, mat);
		assertNotNull(body);
		// Must NOT leak device B's data to Participant A
		assertFalse("Participant A must not see Device B timeseries data", body.contains("secret_reading_from_device_b"));
		// Verify no records returned (only header row or empty)
		String[] lines = body.trim().split("\n");
		assertTrue("CSV should have at most 1 header row when no data belongs to participant", lines.length <= 1);
	}

	@Test
	public void testDFSecurity_ParticipantWithoutDevicesGetsEmptyArrayFromTimeseriesDatasetJson() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + timeseriesDs.getId() + ".json")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result);
		JsonNode json = Json.parse(body);
		assertTrue(json.isArray());
		assertEquals("Participant A must receive 0 timeseries records", 0, json.size());
	}

	@Test
	public void testDFSecurity_ParticipantWithDeviceGetsOnlyTheirOwnTimeseriesDataCSV() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + timeseriesDs.getId() + ".csv")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenB);

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result, mat);
		assertNotNull(body);
		assertTrue("Participant B should see their own device data", body.contains("secret_reading_from_device_b"));
	}

	@Test
	public void testDFSecurity_ParticipantGetsOnlyTheirOwnEntityDatasetRecordsCSV() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + entityDs.getId() + ".csv")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result, mat);
		assertNotNull(body);
		assertTrue("Participant A must see their own entity data", body.contains("secret_for_participant_a"));
		assertFalse("Participant A must not see Participant B entity data", body.contains("secret_for_participant_b"));
	}

	@Test
	public void testDFSecurity_ParticipantGetsOnlyTheirOwnEntityDatasetRecordsJson() {
		Http.RequestBuilder request = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + entityDs.getId() + ".json")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result result = route(app, request);
		assertEquals(OK, result.status());

		String body = contentAsString(result);
		JsonNode json = Json.parse(body);
		assertTrue(json.isArray());
		assertEquals("Participant A must receive exactly 1 entity record", 1, json.size());
		assertTrue("Participant A must receive their own record", body.contains("secret_for_participant_a"));
		assertFalse("Participant A must not see Participant B data", body.contains("secret_for_participant_b"));
	}

	@Test
	public void testDFSecurity_ParticipantForbiddenFromExportingFormDataset() {
		Http.RequestBuilder requestCsv = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + formDs.getId() + ".csv")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result resultCsv = route(app, requestCsv);
		assertEquals("FORM dataset export CSV must be forbidden for participants", FORBIDDEN, resultCsv.status());

		Http.RequestBuilder requestJson = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + formDs.getId() + ".json")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result resultJson = route(app, requestJson);
		assertEquals("FORM dataset export JSON must be forbidden for participants", FORBIDDEN, resultJson.status());
	}

	@Test
	public void testDFSecurity_ParticipantForbiddenFromExportingCompleteDataset() {
		Http.RequestBuilder requestCsv = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + completeDs.getId() + ".csv")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result resultCsv = route(app, requestCsv);
		assertEquals("COMPLETE dataset export CSV must be forbidden for participants", FORBIDDEN, resultCsv.status());

		Http.RequestBuilder requestJson = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + completeDs.getId() + ".json")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result resultJson = route(app, requestJson);
		assertEquals("COMPLETE dataset export JSON must be forbidden for participants", FORBIDDEN, resultJson.status());
	}

	@Test
	public void testDFSecurity_ParticipantForbiddenFromExportingAnnotationDataset() {
		Http.RequestBuilder requestCsv = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + annotationDs.getId() + ".csv")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result resultCsv = route(app, requestCsv);
		assertEquals("ANNOTATION dataset export CSV must be forbidden for participants", FORBIDDEN, resultCsv.status());

		Http.RequestBuilder requestJson = new Http.RequestBuilder()
				.method(GET)
				.uri("/api/v1/participation/getData/" + annotationDs.getId() + ".json")
				.session(ParticipantAuth.PARTICIPANT_ID, tokenA);

		Result resultJson = route(app, requestJson);
		assertEquals("ANNOTATION dataset export JSON must be forbidden for participants", FORBIDDEN, resultJson.status());
	}
}

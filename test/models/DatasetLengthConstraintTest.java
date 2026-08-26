package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import datasets.DatasetConnector;
import models.ds.CompleteDS;
import models.ds.EntityDS;
import models.ds.FitbitDS;
import models.ds.GoogleFitDS;
import models.sr.Wearable;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.libs.Json;
import play.test.WithApplication;

public class DatasetLengthConstraintTest extends WithApplication {

	private DatasetConnector datasetConnector;
	private Project project;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder().configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true).build();
	}

	@Before
	public void setUp() {
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);

		Person user = new Person();
		user.setFirstname("Length");
		user.setLastname("Tester");
		user.setEmail("length_tester_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		user.setUser_id(UUID.randomUUID().toString());
		user.save();

		project = Project.create("Length Test Project", user, "Intro", false, false);
		project.save();
	}

	@Test
	public void testEntityDSLengthConstraints() {
		Dataset ds = datasetConnector.create("Entity Length DS", DatasetType.ENTITY, project, "Desc", "Target", "true");
		ds.save();

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// resource_id of 70 chars (> 63 chars schema limit)
		String longResourceId = "resource_id_1234567890_1234567890_1234567890_1234567890_1234567890_extra_chars";
		// token of 70 chars (> 63 chars schema limit)
		String longToken = "token_1234567890_1234567890_1234567890_1234567890_1234567890_extra_chars";

		ObjectNode data = Json.newObject().put("name", "Test Entity").put("val", 42);

		// Add item with long resource_id and token
		Optional<ObjectNode> added = eds.addItem(longResourceId, Optional.of(longToken), data);
		assertTrue("Item should be successfully added without SQL exception", added.isPresent());

		// Verify retrieval using the truncated resource_id / token
		Optional<ObjectNode> retrieved = eds.getItem(longResourceId, Optional.of(longToken));
		assertTrue("Item should be retrievable", retrieved.isPresent());
		assertEquals("Test Entity", retrieved.get().get("name").asText());

		// Test update with long token and updated data
		ObjectNode updateData = Json.newObject().put("name", "Updated Entity");
		Optional<ObjectNode> updated = eds.updateItem(longResourceId, Optional.of(longToken), updateData);
		assertTrue("Item should be successfully updated", updated.isPresent());
	}

	@Test
	public void testCompleteDSLengthConstraints() {
		Dataset ds = datasetConnector.create("Complete Length DS", DatasetType.COMPLETE, project, "Desc", "Target", "true");
		ds.save();

		CompleteDS cds = (CompleteDS) datasetConnector.getDatasetDS(ds);

		String longFileName = "a".repeat(300);
		String longDescription = "b".repeat(300);

		// Should not throw exception even with >255 char strings
		cds.addRecord(longFileName, longDescription, new Date());
		cds.updateRecord(1L, "c".repeat(300));
	}

	@Test
	public void testFitbitDSLengthConstraints() {
		Dataset ds = datasetConnector.create("Fitbit Length DS", DatasetType.FITBIT, project, "Desc", "Target", "true");
		ds.save();

		FitbitDS fbds = (FitbitDS) datasetConnector.getDatasetDS(ds);

		Wearable wearable = new Wearable();
		wearable.setName("Fitbit Test");
		wearable.setBrand(Wearable.FITBIT);
		wearable.setUserId("user_id_".repeat(10)); // > 50 chars
		wearable.setProject(project);
		wearable.setRefId("w" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		wearable.save();

		JsonNode sleepNode = Json.newObject().put("level", "very_long_sleep_level_name_12345"); // > 10 chars
		JsonNode activityNode = Json.newObject().put("details", "x".repeat(20000)); // > 16383 chars

		// Should not throw SQL exception
		fbds.addRecord(wearable, "sleep", sleepNode, 20260826L);
		fbds.addRecord(wearable, "activity", activityNode, 20260826L);

		fbds.updateRecord(wearable, "sleep", sleepNode, 20260826L);
		fbds.updateRecord(wearable, "activity", activityNode, 20260826L);
	}

	@Test
	public void testGoogleFitDSLengthConstraints() {
		Dataset ds = datasetConnector.create("GoogleFit Length DS", DatasetType.GOOGLEFIT, project, "Desc", "Target", "true");
		ds.save();

		GoogleFitDS gfds = (GoogleFitDS) datasetConnector.getDatasetDS(ds);

		Wearable wearable = new Wearable();
		wearable.setName("GoogleFit Test");
		wearable.setBrand(Wearable.GOOGLEFIT);
		wearable.setUserId("user_id_".repeat(10)); // > 50 chars
		wearable.setProject(project);
		wearable.setRefId("w" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		wearable.save();

		String[][] scopeList = { { "activity", "100" }, { "calories", "200.5" }, { "speed", "5.2" },
				{ "heart_rate", "75.0" }, { "step_count", "10000" }, { "distance", "8.5" }, { "weight", "70.0" },
				{ "sleep", "480" } };

		// Should not throw SQL exception
		gfds.addRecord(wearable, scopeList, 20260826L);
	}
}

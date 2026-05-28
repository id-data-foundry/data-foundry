package models;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import datasets.DatasetConnector;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;
import models.ds.TimeseriesDS;

public class DatasetTest extends WithApplication {

	private DatasetConnector datasetConnector;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder().configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true).build();
	}

	@Before
	public void setUp() {
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
	}

	@Test
	public void testGetItemCount() {
		Person user = new Person();
		user.setFirstname("Test");
		user.setLastname("User");
		user.setEmail("test@example.com");
		user.setUser_id(UUID.randomUUID().toString());
		user.save();

		Project project = Project.create("Test Project", user, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Test DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();

		assertEquals(0, ds.getItemCount());

		// Add some records
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity1", play.libs.Json.newObject());
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity2", play.libs.Json.newObject());

		assertEquals(2, ds.getItemCount());
	}
}

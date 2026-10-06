package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import datasets.DatasetConnector;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;
import models.ds.EntityDS;
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

	private Person createPerson(String prefix) {
		Person person = new Person();
		person.setUser_id(UUID.randomUUID().toString());
		person.setFirstname(prefix);
		person.setLastname("User");
		person.setEmail(prefix.toLowerCase() + "_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		person.save();
		return person;
	}

	@Test
	public void testGetItemCount() {
		Person user = createPerson("Count");
		Project project = Project.create("Test Project", user, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Test DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();

		assertEquals(0, ds.getItemCount());

		// Add some records
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity1", play.libs.Json.newObject());
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity2", play.libs.Json.newObject());

		// flush queues
		TimeseriesDS.flushQueue();

		assertEquals(2, ds.getItemCount());
	}

	@Test
	public void testDatasetPermissions() {
		Person owner = createPerson("Owner");
		Person collaborator = createPerson("Collab");
		Person subscriber = createPerson("Sub");
		Person other = createPerson("Other");

		Project project = Project.create("Private Project", owner, "Intro", false, false);
		project.save();

		Collaboration collab = new Collaboration(collaborator, project);
		collab.save();
		project.getCollaborators().add(collab);

		Subscription sub = new Subscription(subscriber, project);
		sub.save();
		project.getSubscribers().add(sub);
		project.update();

		Dataset ds = datasetConnector.create("Perm DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();

		// belongsTo tests
		assertTrue(ds.belongsTo(owner.getEmail()));
		assertFalse(ds.belongsTo(collaborator.getEmail()));
		assertFalse(ds.belongsTo(other.getEmail()));
		assertFalse(ds.belongsTo(null));

		// collaboratesWith tests
		assertTrue(ds.collaboratesWith(collaborator.getEmail()));
		assertFalse(ds.collaboratesWith(other.getEmail()));
		assertFalse(ds.collaboratesWith(null));

		// subscribedBy tests
		assertTrue(ds.subscribedBy(subscriber.getEmail()));
		assertFalse(ds.subscribedBy(other.getEmail()));
		assertFalse(ds.subscribedBy(null));

		// visibleFor tests (String username)
		assertTrue(ds.visibleFor(owner.getEmail()));
		assertTrue(ds.visibleFor(collaborator.getEmail()));
		assertTrue(ds.visibleFor(subscriber.getEmail()));
		assertFalse(ds.visibleFor(other.getEmail()));
		assertFalse(ds.visibleFor((String) null));

		// visibleFor tests (Person user)
		assertTrue(ds.visibleFor(owner));
		assertTrue(ds.visibleFor(collaborator));
		assertTrue(ds.visibleFor(subscriber));
		assertFalse(ds.visibleFor(other));
		assertFalse(ds.visibleFor((Person) null));

		// editableBy tests (String username)
		assertTrue(ds.editableBy(owner.getEmail()));
		assertTrue(ds.editableBy(collaborator.getEmail()));
		assertFalse(ds.editableBy(subscriber.getEmail()));
		assertFalse(ds.editableBy(other.getEmail()));
		assertFalse(ds.editableBy((String) null));

		// editableBy tests (Person user)
		assertTrue(ds.editableBy(owner));
		assertTrue(ds.editableBy(collaborator));
		assertFalse(ds.editableBy(subscriber));
		assertFalse(ds.editableBy(other));
		assertFalse(ds.editableBy((Person) null));

		// Null project safety
		Dataset detachedDs = new Dataset();
		assertFalse(detachedDs.belongsTo(owner.getEmail()));
		assertFalse(detachedDs.collaboratesWith(collaborator.getEmail()));
		assertFalse(detachedDs.subscribedBy(subscriber.getEmail()));
		assertFalse(detachedDs.visibleFor(owner.getEmail()));
		assertFalse(detachedDs.visibleFor(owner));
		assertFalse(detachedDs.editableBy(owner.getEmail()));
		assertFalse(detachedDs.editableBy(owner));
	}

	@Test
	public void testProjectIsActiveWithNullDatasetStart() {
		Person owner = createPerson("ActiveOwner");
		Project project = Project.create("Active Project", owner, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Active DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.setStart(null); // start is explicitly null
		ds.setEnd(null);
		ds.save();

		project.getDatasets().add(ds);
		project.update();

		// Should not throw or crash, and ds.isActive() is true when start & end are null
		assertTrue(project.isActive());
	}

	@Test
	public void testTimeseriesQueueTracking() {
		Person owner = createPerson("TsQueueOwner");
		Project project = Project.create("TsQueue Project", owner, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("TsQueue DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();

		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);

		// ensure clean start
		TimeseriesDS.flushQueue();
		assertEquals(0, TimeseriesDS.getQueueSize());

		// add one record
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity1", play.libs.Json.newObject());
		assertEquals(1, TimeseriesDS.getQueueSize());

		// add another record
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity2", play.libs.Json.newObject());
		assertEquals(2, TimeseriesDS.getQueueSize());

		// flush and check size resets to 0
		TimeseriesDS.flushQueue();
		assertEquals(0, TimeseriesDS.getQueueSize());
	}

	@Test
	public void testEntityDSProjectionUpdateGuard() {
		Person owner = createPerson("EntityOwner");
		Project project = Project.create("Entity Project", owner, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Entity DS", DatasetType.ENTITY, project, "Desc", "Target", "true");
		ds.save();

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// Initially, no projection
		assertFalse(ds.getConfiguration().containsKey(Dataset.DATA_PROJECTION));

		// First record: introduces fieldA and fieldB
		eds.addItem("res1", Optional.empty(), play.libs.Json.newObject().put("fieldA", 1).put("fieldB", "val1"));
		assertTrue(ds.getConfiguration().containsKey(Dataset.DATA_PROJECTION));
		String proj1 = ds.getConfiguration().get(Dataset.DATA_PROJECTION);
		assertTrue(proj1.contains("fieldA"));
		assertTrue(proj1.contains("fieldB"));

		// Second record with identical fields: should not change projection or throw
		eds.addItem("res2", Optional.empty(), play.libs.Json.newObject().put("fieldA", 2).put("fieldB", "val2"));
		assertEquals(proj1, ds.getConfiguration().get(Dataset.DATA_PROJECTION));

		// Third record introducing a new field: fieldC
		eds.addItem("res3", Optional.empty(), play.libs.Json.newObject().put("fieldA", 3).put("fieldC", true));
		String proj2 = ds.getConfiguration().get(Dataset.DATA_PROJECTION);
		assertTrue(proj2.contains("fieldA"));
		assertTrue(proj2.contains("fieldB"));
		assertTrue(proj2.contains("fieldC"));
	}
}

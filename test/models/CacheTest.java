package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.junit.Test;

import io.ebean.DB;
import io.ebean.cache.ServerCacheManager;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;

public class CacheTest extends WithApplication {

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.build();
	}

	@Test
	public void testPersonCacheAndNaturalKey() {
		String email = "cache_test_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com";
		Person p = new Person();
		p.setUser_id(UUID.randomUUID().toString());
		p.setFirstname("Cache");
		p.setLastname("User");
		p.setEmail(email);
		p.save();

		Long id = p.getId();
		assertNotNull("Person id should not be null", id);

		// Lookup via ID
		Person pById = Person.find.byId(id);
		assertNotNull(pById);
		assertEquals("Cache", pById.getFirstname());

		// Lookup via Natural Key (email)
		Optional<Person> pByEmail = Person.findByEmail(email);
		assertTrue(pByEmail.isPresent());
		assertEquals(id, pByEmail.get().getId());

		// Update person and verify cache reflects changes
		p.setFirstname("UpdatedCache");
		p.update();

		Person pUpdated = Person.find.byId(id);
		assertEquals("UpdatedCache", pUpdated.getFirstname());

		Optional<Person> pByEmailUpdated = Person.findByEmail(email);
		assertTrue(pByEmailUpdated.isPresent());
		assertEquals("UpdatedCache", pByEmailUpdated.get().getFirstname());
	}

	@Test
	public void testProjectAndDatasetCacheByRefId() {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Owner");
		owner.setLastname("Test");
		owner.setEmail("owner_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Cached Project", owner, "Intro", false, false);
		project.save();

		assertNotNull(project.getId());
		assertNotNull(project.getRefId());

		// Lookup project by ID
		Project cachedProject = Project.find.byId(project.getId());
		assertNotNull(cachedProject);
		assertEquals("Cached Project", cachedProject.getName());

		// Lookup project by refId
		Project projectByRefId = Project.find.query().where().eq("refId", project.getRefId()).findOne();
		assertNotNull(projectByRefId);
		assertEquals(project.getId(), projectByRefId.getId());

		// Create Dataset
		Dataset ds = new Dataset();
		ds.setName("Cached Dataset");
		ds.setRefId("d" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		ds.setProject(project);
		ds.setDsType(DatasetType.COMPLETE);
		ds.save();

		assertNotNull(ds.getId());
		assertNotNull(ds.getRefId());

		// Lookup dataset by ID
		Dataset dsById = Dataset.find.byId(ds.getId());
		assertNotNull(dsById);
		assertEquals("Cached Dataset", dsById.getName());

		// Lookup dataset by refId
		Dataset dsByRefId = Dataset.find.query().where().eq("refId", ds.getRefId()).findOne();
		assertNotNull(dsByRefId);
		assertEquals(ds.getId(), dsByRefId.getId());
	}

	@Test
	public void testResourceEntitiesCache() {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("ResourceOwner");
		owner.setLastname("Test");
		owner.setEmail("res_owner_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Resource Project", owner, "Intro", false, false);
		project.save();

		// Device
		Device device = new Device();
		device.setName("Cached Sensor");
		device.setProject(project);
		device.create();
		device.save();

		assertNotNull(device.getId());
		assertNotNull(device.getRefId());

		Device devById = Device.find.byId(device.getId());
		assertNotNull(devById);
		assertEquals("Cached Sensor", devById.getName());

		// Participant
		Participant participant = new Participant("Alice", "Doe");
		participant.setProject(project);
		participant.setEmail("alice_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		participant.save();

		assertNotNull(participant.getId());
		Participant partById = Participant.find.byId(participant.getId());
		assertNotNull(partById);
		assertEquals("Alice", partById.getFirstname());

		// Wearable
		Wearable wearable = new Wearable();
		wearable.setName("Fitbit Sense");
		wearable.setBrand(Wearable.FITBIT);
		wearable.setProject(project);
		wearable.setRefId("w" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		wearable.save();

		assertNotNull(wearable.getId());
		Wearable wById = Wearable.find.byId(wearable.getId());
		assertNotNull(wById);
		assertEquals("Fitbit Sense", wById.getName());

		// Cluster
		Cluster cluster = new Cluster("Test Cluster");
		cluster.setProject(project);
		cluster.setRefId("c" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		cluster.save();

		assertNotNull(cluster.getId());
		Cluster cById = Cluster.find.byId(cluster.getId());
		assertNotNull(cById);
		assertEquals("Test Cluster", cById.getName());
	}

	@Test
	public void testServerCacheManagerIsActive() {
		ServerCacheManager cacheManager = DB.getDefault().cacheManager();
		assertNotNull("Ebean ServerCacheManager should be available", cacheManager);
		
		// Clear cache to verify cache manager operates properly
		cacheManager.clearAll();
	}
}

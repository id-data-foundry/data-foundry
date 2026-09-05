package models;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Test;

import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;

public class ProjectMembershipTest extends WithApplication {

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
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

	@Test
	public void testBelongsTo() {
		Person owner = createPerson("owner");
		Project project = Project.create("Test BelongsTo", owner, "Intro", false, false);
		project.save();

		// Exact email
		assertTrue(project.belongsTo(owner.getEmail()));

		// Case-insensitive email
		assertTrue(project.belongsTo(owner.getEmail().toUpperCase()));

		// Non-owner
		Person other = createPerson("other");
		assertFalse(project.belongsTo(other.getEmail()));

		// Null and empty
		assertFalse(project.belongsTo((String) null));
		assertFalse(project.belongsTo((Person) null));
		assertFalse(project.belongsTo(""));
	}

	@Test
	public void testCollaboratesWith() {
		Person owner = createPerson("collab_owner");
		Project project = Project.create("Test CollaboratesWith", owner, "Intro", false, false);
		project.save();

		Person collaborator = createPerson("collaborator");
		Collaboration collab = new Collaboration(collaborator, project);
		collab.save();
		project.getCollaborators().add(collab);
		project.update();

		// Test with in-memory project
		assertTrue(project.collaboratesWith(collaborator.getEmail()));
		assertTrue(project.collaboratesWith(collaborator.getEmail().toUpperCase()));
		assertTrue(project.collaboratesWith(collaborator));

		// Test after reload from DB via Project.find.byId
		Project loadedProject = Project.find.byId(project.getId());
		assertNotNull(loadedProject);
		assertTrue(loadedProject.collaboratesWith(collaborator.getEmail()));
		assertTrue(loadedProject.collaboratesWith(collaborator.getEmail().toUpperCase()));
		assertTrue(loadedProject.collaboratesWith(collaborator));

		// Non-collaborator
		Person nonCollab = createPerson("non_collab");
		assertFalse(loadedProject.collaboratesWith(nonCollab.getEmail()));
		assertFalse(loadedProject.collaboratesWith(nonCollab));

		// Null safety
		assertFalse(loadedProject.collaboratesWith((String) null));
		assertFalse(loadedProject.collaboratesWith((Person) null));
	}

	@Test
	public void testSubscribedBy() {
		Person owner = createPerson("sub_owner");
		Project project = Project.create("Test SubscribedBy", owner, "Intro", false, false);
		project.save();

		Person subscriber = createPerson("subscriber");
		Subscription sub = new Subscription(subscriber, project);
		sub.save();
		project.getSubscribers().add(sub);
		project.update();

		// Test with in-memory project
		assertTrue(project.subscribedBy(subscriber.getEmail()));
		assertTrue(project.subscribedBy(subscriber));

		// Test after reload from DB
		Project loadedProject = Project.find.byId(project.getId());
		assertNotNull(loadedProject);
		assertTrue(loadedProject.subscribedBy(subscriber.getEmail()));
		assertTrue(loadedProject.subscribedBy(subscriber));

		// Non-subscriber
		Person nonSub = createPerson("non_sub");
		assertFalse(loadedProject.subscribedBy(nonSub.getEmail()));
		assertFalse(loadedProject.subscribedBy(nonSub));

		// Null safety
		assertFalse(loadedProject.subscribedBy((String) null));
		assertFalse(loadedProject.subscribedBy((Person) null));
	}

	@Test
	public void testHasParticipant() {
		Person owner = createPerson("part_owner");
		Project project = Project.create("Test HasParticipant", owner, "Intro", false, false);
		project.save();

		Participant participant = new Participant("Test", "Participant");
		participant.setProject(project);
		String email = "part_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com";
		participant.setEmail(email);
		participant.save();

		// Test with freshly loaded project
		Project loadedProject = Project.find.byId(project.getId());
		assertNotNull(loadedProject);

		assertTrue(loadedProject.hasParticipant(participant));
		assertTrue(loadedProject.hasParticipantWithEmail(email));
		assertTrue(loadedProject.hasParticipantWithEmail(email.toUpperCase()));

		// Other participant not in this project
		Participant otherParticipant = new Participant("Other", "Participant");
		otherParticipant.setEmail("other_part@example.com");
		otherParticipant.save();

		assertFalse(loadedProject.hasParticipant(otherParticipant));
		assertFalse(loadedProject.hasParticipantWithEmail("other_part@example.com"));

		// Null safety
		assertFalse(loadedProject.hasParticipant(null));
		assertFalse(loadedProject.hasParticipantWithEmail(null));
	}

	@Test
	public void testHasDevice() {
		Person owner = createPerson("dev_owner");
		Project project = Project.create("Test HasDevice", owner, "Intro", false, false);
		project.save();

		Device device = new Device();
		device.setName("Test Sensor");
		device.setProject(project);
		device.create();
		device.save();

		// Test with freshly loaded project
		Project loadedProject = Project.find.byId(project.getId());
		assertNotNull(loadedProject);

		assertTrue(loadedProject.hasDevice(device));
		assertTrue(loadedProject.hasDevice(device.getRefId()));

		// Other device not in this project
		Device otherDevice = new Device();
		otherDevice.setName("Other Sensor");
		otherDevice.create();
		otherDevice.save();

		assertFalse(loadedProject.hasDevice(otherDevice));
		assertFalse(loadedProject.hasDevice(otherDevice.getRefId()));

		// Null safety
		assertFalse(loadedProject.hasDevice((Device) null));
		assertFalse(loadedProject.hasDevice((String) null));
	}

	@Test
	public void testHasWearable() {
		Person owner = createPerson("wear_owner");
		Project project = Project.create("Test HasWearable", owner, "Intro", false, false);
		project.save();

		Wearable wearable = new Wearable();
		wearable.setName("Test Watch");
		wearable.setBrand(Wearable.FITBIT);
		wearable.setProject(project);
		wearable.setRefId("w" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		wearable.save();

		// Test with freshly loaded project
		Project loadedProject = Project.find.byId(project.getId());
		assertNotNull(loadedProject);

		assertTrue(loadedProject.hasWearable(wearable));

		// Other wearable not in this project
		Wearable otherWearable = new Wearable();
		otherWearable.setName("Other Watch");
		otherWearable.setRefId("w" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		otherWearable.save();

		assertFalse(loadedProject.hasWearable(otherWearable));

		// Null safety
		assertFalse(loadedProject.hasWearable(null));
	}

	@Test
	public void testVisibleFor() {
		Person owner = createPerson("vis_owner");
		Project project = Project.create("Private Project", owner, "Intro", false, false);
		project.setPublicProject(false);
		project.save();

		Person collaborator = createPerson("vis_collab");
		Collaboration collab = new Collaboration(collaborator, project);
		collab.save();
		project.getCollaborators().add(collab);

		Person subscriber = createPerson("vis_sub");
		Subscription sub = new Subscription(subscriber, project);
		sub.save();
		project.getSubscribers().add(sub);
		project.update();

		Project loaded = Project.find.byId(project.getId());
		assertNotNull(loaded);

		// Owner, collaborator, subscriber have visibility
		assertTrue(loaded.visibleFor(owner));
		assertTrue(loaded.visibleFor(owner.getEmail()));
		assertTrue(loaded.visibleFor(collaborator));
		assertTrue(loaded.visibleFor(collaborator.getEmail()));
		assertTrue(loaded.visibleFor(subscriber));
		assertTrue(loaded.visibleFor(subscriber.getEmail()));

		// Random third-party does not have visibility
		Person stranger = createPerson("stranger");
		assertFalse(loaded.visibleFor(stranger));
		assertFalse(loaded.visibleFor(stranger.getEmail()));
		assertFalse(loaded.visibleFor((Person) null));
		assertFalse(loaded.visibleFor((String) null));

		// Make public and verify stranger now has visibility
		loaded.setPublicProject(true);
		loaded.update();

		Project publicProject = Project.find.byId(project.getId());
		assertTrue(publicProject.visibleFor(stranger));
		assertTrue(publicProject.visibleFor(stranger.getEmail()));
		assertTrue(publicProject.visibleFor((Person) null));
		assertTrue(publicProject.visibleFor((String) null));
	}
}

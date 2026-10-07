package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import models.Collaboration;
import models.Subscription;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;

public class ParticipantTimelineQueryOptimizationTest extends WithApplication {

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.base_url", "http://localhost:9000")
				.configure("df.keys.project", "test-project-secret")
				.configure("df.keys.v2.user.api", "test-api-key")
				.configure("df.keys.v2.multi.api", "test-api-key")
				.configure("df.keys.registration", java.util.Collections.singletonList("test-access-code"))
				.build();
	}

	@Test
	public void testParticipantClusterDevicesAndWearablesWithoutRefresh() {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Timeline");
		owner.setLastname("Tester");
		owner.setEmail("timeline_test_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Timeline Test Project", owner, "Intro", false, false);
		project.save();
		assertNotNull(project.getId());

		Participant participant = Participant.createInstance("Timeline", "Participant",
				"timeline_part_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com", project);
		participant.save();

		Cluster cluster = new Cluster("single_cluster_" + UUID.randomUUID().toString().substring(0, 8));
		cluster.setProject(project);
		cluster.save();

		Device device = new Device();
		device.setName("Test Device");
		device.setProject(project);
		device.create();
		device.save();

		Wearable wearable = new Wearable();
		wearable.setName("Test Wearable");
		wearable.setProject(project);
		wearable.create();
		wearable.save();

		cluster.add(participant);
		cluster.add(device);
		cluster.add(wearable);

		// 1. Verify getClusterDevices() returns device without throwing or failing on unneeded refresh()
		List<Device> clusterDevices = participant.getClusterDevices();
		assertEquals(1, clusterDevices.size());
		assertEquals(device.getId(), clusterDevices.get(0).getId());

		// 2. Verify getClusterWearables() returns wearable without throwing or failing on unneeded refresh()
		List<Wearable> clusterWearables = participant.getClusterWearables();
		assertEquals(1, clusterWearables.size());
		assertEquals(wearable.getId(), clusterWearables.get(0).getId());

		// 3. Verify wearable.getClusterParticipants() returns participant without unneeded refresh()
		List<Participant> clusterParticipants = wearable.getClusterParticipants();
		assertEquals(1, clusterParticipants.size());
		assertEquals(participant.getId(), clusterParticipants.get(0).getId());
	}

	@Test
	public void testPersonCollaboratesOnAndSubscribesToWithoutRefresh() {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Collab");
		owner.setLastname("Owner");
		owner.setEmail("collab_owner_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Collab Test Project", owner, "Intro", false, false);
		project.save();

		Person member = new Person();
		member.setUser_id(UUID.randomUUID().toString());
		member.setFirstname("Collab");
		member.setLastname("Member");
		member.setEmail("collab_member_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		member.save();

		// Check null safety
		assertFalse("Null project should safely return false", member.collaboratesOn(null));
		assertFalse("Null project should safely return false", member.subscribesTo(null));

		// Initial state
		assertFalse("Should not collaborate initially", member.collaboratesOn(project));
		assertFalse("Should not subscribe initially", member.subscribesTo(project));

		// Add collaboration
		Collaboration collab = new Collaboration(member, project);
		collab.save();
		member.getCollaborations().add(collab);

		assertTrue("Should collaborate after adding collaboration", member.collaboratesOn(project));

		// Add subscription
		Subscription sub = new Subscription(member, project);
		sub.save();
		member.getSubscriptions().add(sub);

		assertTrue("Should subscribe after adding subscription", member.subscribesTo(project));
	}
}

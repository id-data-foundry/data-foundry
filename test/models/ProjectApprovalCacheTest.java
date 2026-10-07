package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import models.LabNotesEntry.LabNotesEntryType;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;

public class ProjectApprovalCacheTest extends WithApplication {

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
	public void testProjectApprovalAndModificationCacheCycle() throws InterruptedException {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Approval");
		owner.setLastname("Tester");
		owner.setEmail("approval_test_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("Approval Test Project", owner, "Intro", false, false);
		project.save();
		assertNotNull(project.getId());

		// 1. Initial state: not approved
		assertFalse("Initial project should not be approved", project.isApproved());
		assertFalse("Initial project should not have once been approved", project.hasOnceBeenApproved());

		// 2. Repeated calls hit cache and return false
		assertFalse("Cached check should still be false", project.isApproved());
		assertFalse("Cached check should still be false", project.hasOnceBeenApproved());

		// 3. Log an APPROVAL
		Thread.sleep(10); // ensure distinct timestamp
		LabNotesEntry.log(Project.class, LabNotesEntryType.APPROVE, "Project approved", project);

		// Cache should be evicted by LabNotesEntry.log and return true
		assertTrue("Project should be approved after approval log entry", project.isApproved());
		assertTrue("Project should have once been approved", project.hasOnceBeenApproved());

		// Cache hit should still be true
		assertTrue("Cached check should still be true", project.isApproved());

		// 4. Log a MODIFICATION after approval
		Thread.sleep(10); // ensure distinct timestamp
		LabNotesEntry.log(Project.class, LabNotesEntryType.MODIFY, "Project modified", project);

		// Cache should be evicted by LabNotesEntry.log and isApproved should now be false
		assertFalse("Project should no longer be approved after modification post-approval", project.isApproved());
		assertTrue("Project should still have once been approved", project.hasOnceBeenApproved());

		// 5. Log another APPROVAL
		Thread.sleep(10);
		LabNotesEntry.log(Project.class, LabNotesEntryType.APPROVE, "Project re-approved", project);

		assertTrue("Project should be re-approved", project.isApproved());
	}

	@Test
	public void testLastUpdatedCachingAndEviction() throws InterruptedException {
		Person owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Updated");
		owner.setLastname("Tester");
		owner.setEmail("updated_test_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		Project project = Project.create("LastUpdated Test Project", owner, "Intro", false, false);
		project.save();
		assertNotNull(project.getId());

		long initialUpdated = project.getLastUpdated();

		Thread.sleep(50);
		LabNotesEntry.log(Project.class, LabNotesEntryType.CREATE, "Initial creation note", project);

		long afterFirstLog = project.getLastUpdated();
		assertTrue("lastUpdated should advance after logging an entry", afterFirstLog >= initialUpdated);

		// Second call hits cache and returns identical timestamp
		assertEquals("Repeated call should return identical cached timestamp", afterFirstLog, project.getLastUpdated());

		Thread.sleep(50);
		LabNotesEntry.log(Project.class, LabNotesEntryType.MODIFY, "Modification note", project);

		long afterSecondLog = project.getLastUpdated();
		assertTrue("lastUpdated should update after cache eviction on new note", afterSecondLog > afterFirstLog);
	}
}

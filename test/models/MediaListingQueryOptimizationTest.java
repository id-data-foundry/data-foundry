package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.ExpSamplingDS;
import models.ds.MediaDS;
import models.ds.MovementDS;
import models.sr.Participant;
import models.vm.TimedMedia;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;

public class MediaListingQueryOptimizationTest extends WithApplication {

	private DatasetConnector datasetConnector;
	private Person owner;
	private Project project;
	private Participant participant1;
	private Participant participant2;

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
				.configure("df.keys.registration", Collections.singletonList("test-access-code"))
				.build();
	}

	@Before
	public void setUp() {
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);

		owner = new Person();
		owner.setUser_id(UUID.randomUUID().toString());
		owner.setFirstname("Media");
		owner.setLastname("Tester");
		owner.setEmail("media_tester_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		owner.save();

		project = Project.create("Media Test Project", owner, "Description", false, false);
		project.save();

		participant1 = Participant.createInstance("Alice", "One",
				"alice_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com", project);
		participant1.save();

		participant2 = Participant.createInstance("Bob", "Two",
				"bob_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com", project);
		participant2.save();
	}

	private File createTempTestFile(String content) throws IOException {
		File temp = File.createTempFile("media_test_", ".dat");
		Files.write(temp.toPath(), content.getBytes(StandardCharsets.UTF_8));
		return temp;
	}

	@Test
	public void testMediaDSGetFilesOptimizationAndResolution() throws Exception {
		Dataset ds = datasetConnector.create("Media DS Test", DatasetType.MEDIA, project, "Desc", "Target", "true");
		ds.save();
		MediaDS mediaDS = (MediaDS) datasetConnector.getDatasetDS(ds);
		assertNotNull(mediaDS);

		Date ts1 = new Date(1000000000L);
		Date ts2 = new Date(2000000000L);
		Date ts3 = new Date(3000000000L);

		// 1. Direct exact filename
		File f1 = createTempTestFile("file 1 content");
		mediaDS.storeFile(f1, "photo1.jpg");
		mediaDS.addRecord(participant1, "photo1.jpg", "First Photo", ts1, "active");
		f1.delete();

		// 2. Timestamp-prefixed file on disk
		File f2 = createTempTestFile("file 2 content");
		mediaDS.storeFile(f2, ts2.getTime() + "_photo2.jpg");
		mediaDS.addRecord(participant2, "photo2.jpg", "Second Photo", ts2, "active");
		f2.delete();

		// 3. Sanitized name: file on disk has spaces, DB has sanitized name
		File folder = mediaDS.getFolder();
		File diskLegacy = new File(folder, "photo legacy.jpg");
		Files.write(diskLegacy.toPath(), "legacy content".getBytes(StandardCharsets.UTF_8));
		mediaDS.addRecord(participant1, "photo_legacy.jpg", "Legacy Photo", ts3, "archived");

		// 4. Missing file on disk (should be filtered out by getFiles)
		mediaDS.addRecord(participant2, "ghost.jpg", "Ghost Photo", new Date(4000000000L), "active");

		// 5. File without participant (empty participant)
		File f5 = createTempTestFile("file 5 content");
		mediaDS.storeFile(f5, "orphan.jpg");
		mediaDS.addRecord(null, "orphan.jpg", "Orphan Photo", new Date(5000000000L), "active");
		f5.delete();

		// Verify getNFiles(10): should find exactly 4 existing files (orphan, legacy, photo2, photo1), ordered by ts DESC
		List<TimedMedia> allFiles = mediaDS.getNFiles(10);
		assertEquals(4, allFiles.size());

		// Verify ghost.jpg is excluded
		boolean hasGhost = allFiles.stream().anyMatch(tm -> "ghost.jpg".equals(tm.getLink()));
		assertTrue("Ghost file must be excluded", !hasGhost);

		// Verify participant resolution
		TimedMedia tmOrphan = allFiles.stream().filter(tm -> "orphan.jpg".equals(tm.getLink())).findFirst().orElse(null);
		assertNotNull(tmOrphan);
		// Orphan record has no valid participant (id -1), participant should be null
		assertNull(tmOrphan.participant);

		TimedMedia tmPhoto1 = allFiles.stream().filter(tm -> "photo1.jpg".equals(tm.getLink())).findFirst().orElse(null);
		assertNotNull(tmPhoto1);
		assertEquals(participant1.getId(), tmPhoto1.participant.getId());

		// Verify timestamp prefix resolution: in getNFiles, filename becomes ts_photo2.jpg
		TimedMedia tmPhoto2 = allFiles.stream().filter(tm -> tm.getLink().contains("photo2.jpg")).findFirst().orElse(null);
		assertNotNull(tmPhoto2);
		assertEquals(participant2.getId(), tmPhoto2.participant.getId());
		assertEquals(ts2.getTime() + "_photo2.jpg", tmPhoto2.getLink());

		// Verify getNFilesByStatus: status "archived" should return only legacy photo
		List<TimedMedia> archivedFiles = mediaDS.getNFilesByStatus(10, "archived");
		assertEquals(1, archivedFiles.size());
		assertEquals("photo_legacy.jpg", archivedFiles.get(0).getLink());
		assertEquals(participant1.getId(), archivedFiles.get(0).participant.getId());

		// Verify getFiles(participants) filter for participant1
		List<TimedMedia> p1Files = mediaDS.getFiles(Collections.singletonList(participant1));
		assertEquals(2, p1Files.size()); // photo1 and photo_legacy
		assertTrue(p1Files.stream().allMatch(tm -> participant1.getId().equals(tm.participant.getId())));
	}

	@Test
	public void testMovementDSGetFilesOptimization() throws Exception {
		Dataset ds = datasetConnector.create("Movement DS Test", DatasetType.MOVEMENT, project, "Desc", "Target", "true");
		ds.save();
		MovementDS movementDS = (MovementDS) datasetConnector.getDatasetDS(ds);
		assertNotNull(movementDS);

		Date ts = new Date(1500000000L);
		File f1 = createTempTestFile("track content");
		movementDS.storeFile(f1, "track1.gpx");
		movementDS.addRecord(participant1, "track1.gpx", "Morning Run", ts, "imported");
		f1.delete();

		// Add missing file
		movementDS.addRecord(participant2, "missing_track.gpx", "Missing Track", ts, "imported");

		List<TimedMedia> files = movementDS.getFiles();
		assertEquals(1, files.size());
		assertEquals("track1.gpx", files.get(0).getLink());
		assertEquals(participant1.getId(), files.get(0).participant.getId());

		List<TimedMedia> p1Files = movementDS.getFiles(Collections.singletonList(participant1));
		assertEquals(1, p1Files.size());
		assertEquals("track1.gpx", p1Files.get(0).getLink());

		List<TimedMedia> p2Files = movementDS.getFiles(Collections.singletonList(participant2));
		assertEquals(0, p2Files.size()); // missing track excluded
	}

	@Test
	public void testExpSamplingDSGetFilesOptimization() throws Exception {
		Dataset ds = datasetConnector.create("ExpSampling DS Test", DatasetType.ES, project, "Desc", "Target", "true");
		ds.save();
		ExpSamplingDS expDS = (ExpSamplingDS) datasetConnector.getDatasetDS(ds);
		assertNotNull(expDS);

		Date ts = new Date(1600000000L);
		File f1 = createTempTestFile("sample content");
		File expFolder = expDS.getFolder();
		expFolder.mkdirs();
		File diskFile = new File(expFolder, "sample1.json");
		Files.copy(f1.toPath(), diskFile.toPath());
		f1.delete();

		expDS.addRecord(participant1.getId(), "sample1.json", "Sample 1", ts, "imported");
		// Add record with missing file
		expDS.addRecord(participant2.getId(), "missing_sample.json", "Missing Sample", ts, "imported");

		List<TimedMedia> files = expDS.getFiles();
		assertEquals(1, files.size());
		assertEquals("sample1.json", files.get(0).getLink());
		assertEquals(participant1.getId(), files.get(0).participant.getId());

		List<TimedMedia> p1Files = expDS.getFiles(Collections.singletonList(participant1));
		assertEquals(1, p1Files.size());
		assertEquals(participant1.getId(), p1Files.get(0).participant.getId());

		List<TimedMedia> p2Files = expDS.getFiles(Collections.singletonList(participant2));
		assertEquals(0, p2Files.size());
	}
}

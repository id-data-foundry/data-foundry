package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import play.Application;
import play.cache.SyncCacheApi;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import play.test.WithApplication;

public class ProjectExportTest extends WithApplication {

	private ProjectsController projectsController;
	private DatasetsController datasetsController;
	private DatasetConnector datasetConnector;
	private SessionStore sessionStore;
	private SyncCacheApi cache;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.build();
	}

	@Before
	public void setUp() {
		projectsController = app.injector().instanceOf(ProjectsController.class);
		datasetsController = app.injector().instanceOf(DatasetsController.class);
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);
		cache = app.injector().instanceOf(SyncCacheApi.class);
	}

	private Person createPerson(String email) {
		Person person = new Person();
		person.setUser_id(UUID.randomUUID().toString());
		person.setFirstname("Export");
		person.setLastname("Tester");
		person.setEmail(email);
		person.save();
		return person;
	}

	private Http.Request authenticateRequest(Http.RequestBuilder requestBuilder, Person person) {
		Http.Request request = requestBuilder.build();
		PlayWebContext context = new PlayWebContext(request);
		ProfileManager manager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(person.getEmail());
		profile.addAttribute(Person.USER_NAME, person.getEmail());
		profile.addAttribute(Person.USER_ID, person.getId());
		manager.save(true, profile, false);
		return context.supplementRequest(request);
	}

	@Test
	public void testProjectExportZipSuccess() throws Exception {
		Person owner = createPerson("exporter_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		Project project = Project.create("Test Export Project", owner, "Intro for export", false, false);
		project.save();

		Dataset entityDs = datasetConnector.create("My Entity Dataset", DatasetType.ENTITY, project, "Entity test description", "", "", "MIT");
		entityDs.save();

		project.getDatasets().add(entityDs);
		project.update();

		Map<String, String> formMap = new HashMap<>();
		formMap.put("exportOption", "export");
		formMap.put("exportDataset" + entityDs.getId(), entityDs.getId().toString());
		formMap.put("intro", "Project introduction text");
		formMap.put("license", "MIT");

		Http.RequestBuilder requestBuilder = Helpers.fakeRequest("POST", "/projects/" + project.getId() + "/publish")
				.bodyForm(formMap);
		Http.Request request = authenticateRequest(requestBuilder, owner);

		Result publishResult = projectsController.publishProject(request, project.getId()).toCompletableFuture().get();
		assertEquals(Helpers.OK, publishResult.status());

		String body = Helpers.contentAsString(publishResult);
		assertTrue(body.contains("All clear, download should be starting"));
		assertTrue(body.contains("click here to download"));

		// Extract token from download URL in the response
		Pattern pattern = Pattern.compile("/download/([a-f0-9\\-]+)");
		Matcher matcher = pattern.matcher(body);
		assertTrue("Response body must contain /download/{token}", matcher.find());
		String token = matcher.group(1);
		assertNotNull(token);

		// Authenticated request to download the exported zip
		Http.RequestBuilder downloadBuilder = Helpers.fakeRequest("GET", "/download/" + token);
		Http.Request downloadRequest = authenticateRequest(downloadBuilder, owner);

		Result downloadResult = datasetsController.downloadTemporaryFile(downloadRequest, token);
		assertEquals(Helpers.OK, downloadResult.status());
		assertEquals("application/zip", downloadResult.contentType().orElse(""));

		String contentDisposition = downloadResult.header(Http.HeaderNames.CONTENT_DISPOSITION)
				.orElse(downloadResult.header("Content-Disposition").orElse(""));
		assertTrue("Content-Disposition should contain attachment: " + contentDisposition, contentDisposition.contains("attachment"));
		assertTrue(contentDisposition.contains(".zip"));

		// Verify zip content
		org.apache.pekko.stream.Materializer mat = app.injector().instanceOf(org.apache.pekko.stream.Materializer.class);
		byte[] zipBytes = Helpers.contentAsBytes(downloadResult, mat).toArray();
		assertTrue("Zip file must not be empty", zipBytes.length > 0);

		Set<String> entries = new HashSet<>();
		try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				entries.add(entry.getName());
				zis.closeEntry();
			}
		}

		boolean hasMetadata = entries.stream().anyMatch(e -> e.endsWith("metadata.json"));
		boolean hasLicense = entries.stream().anyMatch(e -> e.endsWith("LICENSE"));
		boolean hasReadme = entries.stream().anyMatch(e -> e.endsWith("README.md"));
		boolean hasLogbook = entries.stream().anyMatch(e -> e.endsWith("logbook.csv"));
		boolean hasDatasetJson = entries.stream().anyMatch(e -> e.endsWith(".json") && !e.endsWith("metadata.json"));
		boolean hasDatasetCsv = entries.stream().anyMatch(e -> e.endsWith(".csv") && !e.endsWith("logbook.csv"));

		assertTrue("Archive must contain metadata.json", hasMetadata);
		assertTrue("Archive must contain LICENSE", hasLicense);
		assertTrue("Archive must contain README.md", hasReadme);
		assertTrue("Archive must contain logbook.csv", hasLogbook);
		assertTrue("Archive must contain dataset json export", hasDatasetJson);
		assertTrue("Archive must contain dataset csv export", hasDatasetCsv);
	}

	@Test
	public void testDownloadTemporaryFileUnknownTokenReturnsNotFound() {
		Person user = createPerson("user_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		Http.RequestBuilder downloadBuilder = Helpers.fakeRequest("GET", "/download/non-existent-token");
		Http.Request downloadRequest = authenticateRequest(downloadBuilder, user);

		Result downloadResult = datasetsController.downloadTemporaryFile(downloadRequest, "non-existent-token");
		assertEquals(Helpers.NOT_FOUND, downloadResult.status());
	}

	@Test
	public void testDownloadTemporaryFileMissingPhysicalFileReturnsNotFound() {
		Person user = createPerson("user_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		String token = UUID.randomUUID().toString();
		cache.set("cachedTemporaryFile_" + token, "/tmp/definitely_not_existing_" + token + ".zip", 60);

		Http.RequestBuilder downloadBuilder = Helpers.fakeRequest("GET", "/download/" + token);
		Http.Request downloadRequest = authenticateRequest(downloadBuilder, user);

		Result downloadResult = datasetsController.downloadTemporaryFile(downloadRequest, token);
		assertEquals(Helpers.NOT_FOUND, downloadResult.status());
	}
}

package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.FORBIDDEN;
import static play.mvc.Http.Status.SEE_OTHER;

import java.util.Collections;

import org.junit.Test;

import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import play.test.WithApplication;
import utils.validators.FileTypeUtils;

public class LingeringSecurityTest extends WithApplication {

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.keys.project", "test-project-secret-lingering")
				.configure("df.keys.registration", Collections.singletonList("test-reg-key"))
				.build();
	}

	@Test
	public void testAdminDatabaseEndpointsRejectGet() {
		Http.RequestBuilder backupGet = Helpers.fakeRequest("GET", "/admin/backupDB");
		Result backupResult = Helpers.route(app, backupGet);
		// In non-dev environment, unrouted requests trigger onNotFound which redirects home
		assertEquals("GET /admin/backupDB must not be routable", SEE_OTHER, backupResult.status());
		assertTrue(backupResult.redirectLocation().orElse("").endsWith("/"));

		Http.RequestBuilder compactGet = Helpers.fakeRequest("GET", "/admin/shutdownCompactDB");
		Result compactResult = Helpers.route(app, compactGet);
		assertEquals("GET /admin/shutdownCompactDB must not be routable", SEE_OTHER, compactResult.status());
		assertTrue(compactResult.redirectLocation().orElse("").endsWith("/"));
	}

	@Test
	public void testEntityTableAddEndpointsRejectGet() {
		Http.RequestBuilder participantsGet = Helpers.fakeRequest("GET", "/datasets/entity/table/addParticipants/1");
		Result participantsResult = Helpers.route(app, participantsGet);
		assertEquals("GET /datasets/entity/table/addParticipants/1 must not be routable", SEE_OTHER, participantsResult.status());

		Http.RequestBuilder wearablesGet = Helpers.fakeRequest("GET", "/datasets/entity/table/addWearables/1");
		Result wearablesResult = Helpers.route(app, wearablesGet);
		assertEquals("GET /datasets/entity/table/addWearables/1 must not be routable", SEE_OTHER, wearablesResult.status());

		Http.RequestBuilder devicesGet = Helpers.fakeRequest("GET", "/datasets/entity/table/addDevices/1");
		Result devicesResult = Helpers.route(app, devicesGet);
		assertEquals("GET /datasets/entity/table/addDevices/1 must not be routable", SEE_OTHER, devicesResult.status());
	}

	@Test
	public void testStudyManagementRemoveFileRejectsGet() {
		Http.RequestBuilder removeGet = Helpers.fakeRequest("GET", "/projects/1/studyManagement/removeFile?fileId=1&filename=doc.pdf");
		Result removeResult = Helpers.route(app, removeGet);
		assertEquals("GET /projects/1/studyManagement/removeFile must not be routable", SEE_OTHER, removeResult.status());
	}

	@Test
	public void testFileTypeUtilsExecutableChecks() {
		assertTrue(FileTypeUtils.looksLikeExecutableFile("test.exe"));
		assertTrue(FileTypeUtils.looksLikeExecutableFile("script.sh"));
		assertTrue(FileTypeUtils.looksLikeExecutableFile("payload.bat"));
		assertTrue(FileTypeUtils.looksLikeExecutableFile("run.cmd"));
		assertTrue(FileTypeUtils.looksLikeExecutableFile("app.jar"));

		assertFalse(FileTypeUtils.looksLikeExecutableFile("paper.pdf"));
		assertFalse(FileTypeUtils.looksLikeExecutableFile("notes.txt"));
		assertFalse(FileTypeUtils.looksLikeExecutableFile("diagram.svg"));

		assertTrue(FileTypeUtils.looksLikeDocumentFile("paper.pdf"));
		assertTrue(FileTypeUtils.looksLikeDocumentFile("notes.txt"));
		assertFalse(FileTypeUtils.looksLikeDocumentFile("test.exe"));
	}
}

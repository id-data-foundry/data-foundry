package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;
import static play.mvc.Http.Status.SEE_OTHER;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.apache.pekko.stream.Materializer;
import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import models.Person;
import play.Application;
import play.Environment;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import play.test.WithApplication;
import utils.rendering.FileUtil;

public class HomeControllerEnvironmentFileTest extends WithApplication {

	private HomeController homeController;
	private ProjectsController projectsController;
	private Environment environment;
	private SessionStore sessionStore;
	private Person testUser;

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
		homeController = app.injector().instanceOf(HomeController.class);
		projectsController = app.injector().instanceOf(ProjectsController.class);
		environment = app.injector().instanceOf(Environment.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);

		testUser = new Person();
		testUser.setFirstname("Test");
		testUser.setLastname("User");
		testUser.setEmail("test_" + UUID.randomUUID() + "@example.com");
		testUser.setUser_id(UUID.randomUUID().toString());
		testUser.save();
	}

	private Http.Request createAuthenticatedRequest(String method, String uri) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method(method).uri(uri);
		Http.Request request = requestBuilder.build();
		PlayWebContext context = new PlayWebContext(request);
		ProfileManager manager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(testUser.getEmail());
		profile.addAttribute(Person.USER_NAME, testUser.getEmail());
		profile.addAttribute(Person.USER_ID, testUser.getId());
		manager.save(true, profile, false);
		return context.supplementRequest(request);
	}

	// =========================================================================
	// 1. FileUtil Environment Folder Resolution
	// =========================================================================

	@Test
	public void testGetEnvironmentFolderWhitelistAndDiscovery() {
		// Valid whitelisted folders must resolve
		Optional<File> docFolder = FileUtil.getEnvironmentFolder(environment, "documentation");
		assertTrue("Documentation folder should be resolved", docFolder.isPresent());
		assertTrue("Documentation folder should exist and be directory", docFolder.get().isDirectory());

		Optional<File> contentFolder = FileUtil.getEnvironmentFolder(environment, "content");
		assertTrue("Content folder should be resolved", contentFolder.isPresent());
		assertTrue("Content folder should exist and be directory", contentFolder.get().isDirectory());

		Optional<File> templatesFolder = FileUtil.getEnvironmentFolder(environment, "templates");
		assertTrue("Templates folder should be resolved", templatesFolder.isPresent());
		assertTrue("Templates folder should exist and be directory", templatesFolder.get().isDirectory());

		Optional<File> announcementsFolder = FileUtil.getEnvironmentFolder(environment, "announcements");
		assertTrue("Announcements folder should be resolved", announcementsFolder.isPresent());
		assertTrue("Announcements folder should exist and be directory", announcementsFolder.get().isDirectory());

		// Unauthorized folders must be rejected
		Optional<File> confFolder = FileUtil.getEnvironmentFolder(environment, "conf");
		assertFalse("Unauthorized 'conf' folder must be rejected", confFolder.isPresent());

		Optional<File> etcFolder = FileUtil.getEnvironmentFolder(environment, "etc");
		assertFalse("Unauthorized 'etc' folder must be rejected", etcFolder.isPresent());

		Optional<File> traversalFolder = FileUtil.getEnvironmentFolder(environment, "../conf");
		assertFalse("Traversal folder must be rejected", traversalFolder.isPresent());
	}

	@Test
	public void testProjectTemplatesDiscovery() throws Exception {
		Method getFileTemplatesMethod = ProjectsController.class.getDeclaredMethod("getFileTemplates");
		getFileTemplatesMethod.setAccessible(true);
		@SuppressWarnings("unchecked")
		List<String> templates = (List<String>) getFileTemplatesMethod.invoke(projectsController);

		assertNotNull("Templates list should not be null", templates);
		assertFalse("Templates list should not be empty in test environment", templates.isEmpty());
		assertTrue("Templates list should contain 'Starboard Notebook Tutorial'",
				templates.contains("Starboard Notebook Tutorial"));
	}

	// =========================================================================
	// 2. Public Content Retrieval (/content/*)
	// =========================================================================

	@Test
	public void testPublicContentFiles() {
		Http.Request request = new Http.RequestBuilder().method("GET").uri("/content/contact").build();

		// /content/contact (resolves contact.md)
		Result result = homeController.publicContent(request, "contact");
		assertEquals("Accessing /content/contact should return 200 OK", OK, result.status());

		// /content/contact.md
		Result resultMd = homeController.publicContent(request, "contact.md");
		assertEquals("Accessing /content/contact.md should return 200 OK", OK, resultMd.status());

		// /content/data-protection.md
		Result resultDp = homeController.publicContent(request, "data-protection.md");
		assertEquals("Accessing /content/data-protection.md should return 200 OK", OK, resultDp.status());
	}

	@Test
	public void testPublicContentPathTraversalBlocked() {
		Http.Request request = new Http.RequestBuilder().method("GET").uri("/content/test").build();

		// Directory traversal with dots
		Result r1 = homeController.publicContent(request, "../conf/application.conf");
		assertEquals(NOT_FOUND, r1.status());

		// URL encoded dots
		Result r2 = homeController.publicContent(request, "%2e%2e/conf/application.conf");
		assertEquals(NOT_FOUND, r2.status());

		// Double URL encoded dots
		Result r3 = homeController.publicContent(request, "%252e%252e/conf/application.conf");
		assertEquals(NOT_FOUND, r3.status());

		// Absolute path
		Result r4 = homeController.publicContent(request, "/etc/passwd");
		assertEquals(NOT_FOUND, r4.status());

		// Backslashes
		Result r5 = homeController.publicContent(request, "..\\..\\conf\\application.conf");
		assertEquals(NOT_FOUND, r5.status());
	}

	// =========================================================================
	// 3. Documentation Retrieval (/documentation/*)
	// =========================================================================

	@Test
	public void testDocumentationPages() {
		Http.Request authRequest = createAuthenticatedRequest("GET", "/documentation/index.html");

		// /documentation/index.html
		Result rIndex = homeController.docusite(authRequest, "index.html");
		assertEquals("Accessing /documentation/index.html should return 200 OK", OK, rIndex.status());

		// Footer link: /documentation/Learning/DataFoundry/DataProtection.html
		Result rDp = homeController.docusite(authRequest, "Learning/DataFoundry/DataProtection.html");
		assertEquals("Accessing DataProtection.html should return 200 OK", OK, rDp.status());

		// /documentation/Guides/Examples/index.html
		Result rExamples = homeController.docusite(authRequest, "Guides/Examples/index.html");
		assertEquals("Accessing Guides/Examples/index.html should return 200 OK", OK, rExamples.status());

		// /documentation/Guides/developerGuide.html
		Result rDev = homeController.docusite(authRequest, "Guides/developerGuide.html");
		assertEquals("Accessing Guides/developerGuide.html should return 200 OK", OK, rDev.status());
	}

	@Test
	public void testDocumentationDirectoryRedirect() {
		Http.Request authRequest = createAuthenticatedRequest("GET", "/documentation/Guides");

		// Directory navigation /documentation/Guides redirects to /documentation/Guides/index.html
		Result rGuides = homeController.docusite(authRequest, "Guides");
		assertEquals("Navigating to directory should redirect", SEE_OTHER, rGuides.status());
		assertTrue("Redirect header should point to Guides/index.html",
				rGuides.redirectLocation().orElse("").contains("Guides/index.html"));

		// Nested directory /documentation/Guides/Examples redirects to Guides/Examples/index.html
		Result rNested = homeController.docusite(authRequest, "Guides/Examples");
		assertEquals("Navigating to nested directory should redirect", SEE_OTHER, rNested.status());
		assertTrue("Redirect header should preserve parent path",
				rNested.redirectLocation().orElse("").contains("Guides/Examples/index.html"));
	}

	@Test
	public void testDocumentationPathTraversalBlocked() {
		Http.Request authRequest = createAuthenticatedRequest("GET", "/documentation/test");

		// Directory traversal with dots
		Result r1 = homeController.docusite(authRequest, "../conf/application.conf");
		assertEquals(NOT_FOUND, r1.status());

		// URL encoded dots
		Result r2 = homeController.docusite(authRequest, "%2e%2e/conf/application.conf");
		assertEquals(NOT_FOUND, r2.status());

		// Double URL encoded dots
		Result r3 = homeController.docusite(authRequest, "%252e%252e/conf/application.conf");
		assertEquals(NOT_FOUND, r3.status());

		// Absolute path
		Result r4 = homeController.docusite(authRequest, "/etc/passwd");
		assertEquals(NOT_FOUND, r4.status());

		// Backslashes
		Result r5 = homeController.docusite(authRequest, "..\\..\\conf\\application.conf");
		assertEquals(NOT_FOUND, r5.status());
	}

	// =========================================================================
	// 4. Configurable Links (Footer Redirection)
	// =========================================================================

	@Test
	public void testConfigurableFooterLinks() {
		Http.Request request = new Http.RequestBuilder().method("GET").uri("/links/test").build();

		// About -> redirects to configured link (or /documentation)
		Result rAbout = homeController.configurableLink(request, "about");
		assertEquals(SEE_OTHER, rAbout.status());
		assertFalse("About redirect location should not be empty", rAbout.redirectLocation().orElse("").isEmpty());

		// Data Protection -> redirects to /documentation/Learning/DataFoundry/DataProtection.html
		Result rDp = homeController.configurableLink(request, "data-protection");
		assertEquals(SEE_OTHER, rDp.status());
		assertEquals("/documentation/Learning/DataFoundry/DataProtection.html", rDp.redirectLocation().orElse(""));

		// Contact -> redirects to configured link (or /content/contact)
		Result rContact = homeController.configurableLink(request, "contact");
		assertEquals(SEE_OTHER, rContact.status());
		assertFalse("Contact redirect location should not be empty", rContact.redirectLocation().orElse("").isEmpty());
	}
}

package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.NOT_FOUND;
import static play.mvc.Http.Status.OK;

import org.junit.Test;

import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import play.test.WithApplication;

public class IconRoutingTest extends WithApplication {

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.build();
	}

	@Test
	public void testFaviconIcoRoute() {
		Http.RequestBuilder request = Helpers.fakeRequest("GET", "/favicon.ico");
		Result result = Helpers.route(app, request);
		assertNotNull("Route /favicon.ico should be handled", result);
		assertEquals(OK, result.status());
		assertTrue("Content-Type should be present", result.contentType().isPresent());
		String contentType = result.contentType().get().toLowerCase();
		assertTrue("Content-Type should be icon or image", contentType.contains("icon") || contentType.contains("image"));
	}

	@Test
	public void testAppleTouchIconRoute() {
		Http.RequestBuilder request = Helpers.fakeRequest("GET", "/apple-touch-icon.png");
		Result result = Helpers.route(app, request);
		assertNotNull("Route /apple-touch-icon.png should be handled", result);
		assertEquals(OK, result.status());
		assertTrue("Content-Type should be present", result.contentType().isPresent());
		assertEquals("image/png", result.contentType().get());
	}

	@Test
	public void testAppleTouchIconPrecomposedRoute() {
		Http.RequestBuilder request = Helpers.fakeRequest("GET", "/apple-touch-icon-precomposed.png");
		Result result = Helpers.route(app, request);
		assertNotNull("Route /apple-touch-icon-precomposed.png should be handled", result);
		assertEquals(OK, result.status());
		assertTrue("Content-Type should be present", result.contentType().isPresent());
		assertEquals("image/png", result.contentType().get());
	}

	@Test
	public void testProdStaticAssetNotFound() {
		Application prodApp = new GuiceApplicationBuilder()
				.in(play.Mode.PROD)
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.build();

		try {
			// A non-existent static image file should return 404, not redirect
			Http.RequestBuilder request = Helpers.fakeRequest("GET", "/missing-file.png");
			Result result = Helpers.route(prodApp, request);
			assertNotNull("Missing static file should be handled", result);
			assertEquals(NOT_FOUND, result.status());

			// An API endpoint not found should return 404, not redirect
			Http.RequestBuilder apiRequest = Helpers.fakeRequest("GET", "/api/v2/nonexistent");
			Result apiResult = Helpers.route(prodApp, apiRequest);
			assertNotNull("Missing API route should be handled", apiResult);
			assertEquals(NOT_FOUND, apiResult.status());
		} finally {
			Helpers.stop(prodApp);
		}
	}
}

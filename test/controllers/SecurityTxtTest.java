package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.OK;

import org.junit.Test;

import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import play.test.WithApplication;

public class SecurityTxtTest extends WithApplication {

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.base_url", "https://data-foundry.net")
				.build();
	}

	@Test
	public void testWellKnownSecurityTxtRoute() {
		Http.RequestBuilder request = Helpers.fakeRequest("GET", "/.well-known/security.txt");
		Result result = Helpers.route(app, request);
		assertNotNull("Route /.well-known/security.txt should be handled", result);
		assertEquals(OK, result.status());
		assertTrue("Content-Type should be present", result.contentType().isPresent());
		assertTrue("Content-Type should be text/plain", result.contentType().get().contains("text/plain"));

		String content = Helpers.contentAsString(result);
		assertTrue(content.contains("Contact: mailto:security@data-foundry.net"));
		assertTrue("Expires should match RFC 3339 format",
				content.matches("(?s).*Expires: \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z.*"));
		assertTrue(content.contains("Canonical: https://data-foundry.net/.well-known/security.txt"));
		assertTrue(content.contains("Preferred-Languages: en, nl"));
		assertTrue(content.contains("Policy: https://github.com/id-data-foundry/data-foundry/blob/main/SECURITY.md"));
		assertTrue(content.contains("Acknowledgments: https://github.com/id-data-foundry/data-foundry/blob/main/SECURITY.md#hall-of-fame"));
	}

	@Test
	public void testSecurityTxtRoute() {
		Http.RequestBuilder request = Helpers.fakeRequest("GET", "/security.txt");
		Result result = Helpers.route(app, request);
		assertNotNull("Route /security.txt should be handled", result);
		assertEquals(OK, result.status());
		assertTrue("Content-Type should be present", result.contentType().isPresent());
		assertTrue("Content-Type should be text/plain", result.contentType().get().contains("text/plain"));

		String content = Helpers.contentAsString(result);
		assertTrue(content.contains("Contact: mailto:security@data-foundry.net"));
		assertTrue("Expires should match RFC 3339 format",
				content.matches("(?s).*Expires: \\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z.*"));
	}

	@Test
	public void testExplicitConfigurationOverrides() {
		Application customApp = new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.securitytxt.expires", "2029-01-01T00:00:00.000Z")
				.configure("df.securitytxt.contact", "mailto:custom-sec@data-foundry.net")
				.configure("df.securitytxt.policy", "https://example.com/custom-policy")
				.configure("df.securitytxt.acknowledgments", "https://example.com/custom-hall-of-fame")
				.build();

		try {
			Http.RequestBuilder request = Helpers.fakeRequest("GET", "/.well-known/security.txt");
			Result result = Helpers.route(customApp, request);
			assertNotNull("Route should be handled", result);
			assertEquals(OK, result.status());

			String content = Helpers.contentAsString(result);
			assertTrue(content.contains("Expires: 2029-01-01T00:00:00.000Z"));
			assertTrue(content.contains("Contact: mailto:custom-sec@data-foundry.net"));
			assertTrue(content.contains("Policy: https://example.com/custom-policy"));
			assertTrue(content.contains("Acknowledgments: https://example.com/custom-hall-of-fame"));
		} finally {
			Helpers.stop(customApp);
		}
	}
}

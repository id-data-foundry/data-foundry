package controllers.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.GET;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import com.google.common.hash.Hashing;

import models.Person;
import play.Application;
import play.cache.SyncCacheApi;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Security;
import play.test.WithApplication;
import utils.tools.QRCodeUtil;

public class QRCodeSecurityTest extends WithApplication {

	private QRCode qrCodeController;
	private SyncCacheApi cache;
	private SessionStore sessionStore;
	private Person user;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.keys.project", "test-project-token-secret-123456")
				.configure("df.keys.registration", Collections.singletonList("reg-key"))
				.build();
	}

	@Before
	public void setUp() {
		qrCodeController = app.injector().instanceOf(QRCode.class);
		cache = app.injector().instanceOf(SyncCacheApi.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);

		user = new Person();
		user.setUser_id(UUID.randomUUID().toString());
		user.setFirstname("QR");
		user.setLastname("Tester");
		user.setEmail("qr_tester_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		user.save();
	}

	private Http.Request createAuthenticatedRequest(String uri) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method(GET).uri(uri);
		requestBuilder.attr(Security.USERNAME, user.getEmail());
		Http.Request request = requestBuilder.build();

		PlayWebContext context = new PlayWebContext(request);
		ProfileManager manager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(user.getEmail());
		profile.addAttribute(Person.USER_NAME, user.getEmail());
		profile.addAttribute(Person.USER_ID, user.getId());
		manager.save(true, profile, false);
		return context.supplementRequest(request);
	}

	@Test
	public void testShortKeyNamespacingPreventsCachePoisoning() {
		// DF-19: Previously, key.length() < 6 used raw url as cachingKey, enabling arbitrary cache injection/overwrite.
		// Verify that a short key now hashes the URL and prefixes with "cached_qrcode_".
		String shortKey = "123";
		String targetUrl = "https://example.com/target-resource";

		Http.Request request = createAuthenticatedRequest("/qr/" + shortKey + "/" + targetUrl);
		Result result = qrCodeController.qrCode(request, shortKey, targetUrl);
		assertEquals(OK, result.status());

		// Verify the raw URL was NOT used as a cache key
		assertTrue("Raw URL must not exist in cache", !cache.get(targetUrl).isPresent());

		// Verify the prefixed hashed key was used
		String expectedHashedKey = "cached_qrcode_" + Hashing.sha256().hashString(targetUrl, StandardCharsets.UTF_8).toString();
		assertTrue("Expected namespaced cache key must be present", cache.get(expectedHashedKey).isPresent());
	}

	@Test
	public void testLongKeyNamespacingPrefixed() {
		String validKey = "sufficiently_long_key_12345";
		String targetUrl = "https://example.com/other-resource";

		Http.Request request = createAuthenticatedRequest("/qr/" + validKey + "/" + targetUrl);
		Result result = qrCodeController.qrCode(request, validKey, targetUrl);
		assertEquals(OK, result.status());

		String expectedKey = "cached_qrcode_" + validKey;
		assertTrue("Expected namespaced cache key must be present", cache.get(expectedKey).isPresent());
	}
}

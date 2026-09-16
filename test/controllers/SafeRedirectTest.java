package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class SafeRedirectTest {

	@Test
	public void testSafeRedirectValidation() {
		// Valid relative paths
		assertEquals("/projects", AbstractAsyncController.sanitizeRedirectUrl("/projects"));
		assertEquals("/projects/123", AbstractAsyncController.sanitizeRedirectUrl("/projects/123"));
		assertEquals("/projects/123", AbstractAsyncController.sanitizeRedirectUrl("projects/123"));
		assertEquals("/projects?view=all", AbstractAsyncController.sanitizeRedirectUrl("/projects?view=all"));
		assertEquals("/projects#section1", AbstractAsyncController.sanitizeRedirectUrl("/projects#section1"));

		// Null and empty
		assertNull(AbstractAsyncController.sanitizeRedirectUrl(null));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl(""));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("   "));

		// Protocol-relative URLs (open redirect)
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("//evil.com"));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("//evil.com/path"));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("/\\evil.com"));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("\\evil.com"));

		// Absolute URLs with scheme
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("https://evil.com"));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("http://evil.com"));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("javascript:alert(1)"));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("data:text/html,evil"));

		// Control characters / CRLF injection
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("/projects\r\nHeader: Injection"));
		assertNull(AbstractAsyncController.sanitizeRedirectUrl("/projects\nSet-Cookie: test=1"));
	}
}

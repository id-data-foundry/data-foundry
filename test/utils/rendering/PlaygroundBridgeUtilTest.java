package utils.rendering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class PlaygroundBridgeUtilTest {

	@Test
	public void testBridgeScriptContainsEssentialPolyfillsAndForwarders() {
		String script = PlaygroundBridgeUtil.getBridgeScript();
		assertNotNull(script);
		assertTrue("Script must start with <script>", script.startsWith("<script>"));
		assertTrue("Script must end with </script>\n", script.trim().endsWith("</script>"));
		assertTrue("Must polyfill localStorage", script.contains("window.localStorage"));
		assertTrue("Must polyfill sessionStorage", script.contains("window.sessionStorage"));
		assertTrue("Must support Proxy for property assignment", script.contains("typeof Proxy !== 'undefined'"));
		assertTrue("Must wrap in Proxy", script.contains("new Proxy(api,"));
		assertTrue("Must check prototype property safely", script.contains("Object.prototype.hasOwnProperty.call"));
		assertTrue("Must forward console messages to parent window", script.contains("df-playground-console"));
		assertTrue("Must call postMessage", script.contains("window.parent.postMessage"));
		assertTrue("Must intercept window.onerror", script.contains("window.onerror"));
	}

	@Test
	public void testInjectIntoHeadLowercase() {
		String original = "<!DOCTYPE html><html><head><title>Test</title></head><body>Hi</body></html>";
		String injected = PlaygroundBridgeUtil.injectHtmlBridge(original);
		assertNotNull(injected);
		int headIdx = injected.indexOf("<head>");
		int scriptIdx = injected.indexOf("<script>");
		int titleIdx = injected.indexOf("<title>");
		assertTrue(headIdx < scriptIdx);
		assertTrue(scriptIdx < titleIdx);
	}

	@Test
	public void testInjectIntoHeadUppercase() {
		String original = "<!DOCTYPE html><html><HEAD><TITLE>Test</TITLE></HEAD><BODY>Hi</BODY></html>";
		String injected = PlaygroundBridgeUtil.injectHtmlBridge(original);
		assertNotNull(injected);
		int headIdx = injected.indexOf("<HEAD>");
		int scriptIdx = injected.indexOf("<script>");
		int titleIdx = injected.indexOf("<TITLE>");
		assertTrue(headIdx < scriptIdx);
		assertTrue(scriptIdx < titleIdx);
	}

	@Test
	public void testInjectIntoHeadWithAttributes() {
		String original = "<!DOCTYPE html><html><head lang=\"en\" class=\"dark-mode\" data-theme=\"green\"><title>Attribute Test</title></head><body>Hi</body></html>";
		String injected = PlaygroundBridgeUtil.injectHtmlBridge(original);
		assertNotNull(injected);
		int headEndIdx = injected.indexOf("data-theme=\"green\">") + "data-theme=\"green\">".length();
		int scriptIdx = injected.indexOf("<script>");
		int titleIdx = injected.indexOf("<title>");
		assertTrue(headEndIdx <= scriptIdx);
		assertTrue(scriptIdx < titleIdx);
	}

	@Test
	public void testInjectIntoHeadUppercaseWithAttributes() {
		String original = "<!DOCTYPE html><html><HEAD LANG=\"EN\"><TITLE>Attribute Test</TITLE></HEAD><BODY>Hi</BODY></html>";
		String injected = PlaygroundBridgeUtil.injectHtmlBridge(original);
		assertNotNull(injected);
		int headEndIdx = injected.indexOf("LANG=\"EN\">") + "LANG=\"EN\">".length();
		int scriptIdx = injected.indexOf("<script>");
		int titleIdx = injected.indexOf("<TITLE>");
		assertTrue(headEndIdx <= scriptIdx);
		assertTrue(scriptIdx < titleIdx);
	}

	@Test
	public void testInjectWithoutHeadPrepends() {
		String original = "<div>No head fragment</div>";
		String injected = PlaygroundBridgeUtil.injectHtmlBridge(original);
		assertNotNull(injected);
		assertTrue(injected.startsWith("<script>"));
		assertTrue(injected.endsWith(original));
	}

	@Test
	public void testInjectNullReturnsNull() {
		assertNull(PlaygroundBridgeUtil.injectHtmlBridge((String) null));
		assertNull(PlaygroundBridgeUtil.injectHtmlBridge((File) null));
		assertNull(PlaygroundBridgeUtil.injectHtmlBridge(new File("/non/existent/file.html")));
	}

	@Test
	public void testInjectFileSuccess() throws IOException {
		File tempFile = File.createTempFile("bridge_test_", ".html");
		tempFile.deleteOnExit();
		try (FileOutputStream fos = new FileOutputStream(tempFile)) {
			fos.write("<html><head></head><body>OK</body></html>".getBytes(StandardCharsets.UTF_8));
		}

		String result = PlaygroundBridgeUtil.injectHtmlBridge(tempFile);
		assertNotNull(result);
		assertTrue(result.contains(PlaygroundBridgeUtil.getBridgeScript()));
	}

	@Test
	public void testInjectFileExceedsLimitReturnsNull() throws IOException {
		File tempFile = File.createTempFile("bridge_large_", ".html");
		tempFile.deleteOnExit();
		// Set file length without writing 5MB of actual disk bytes if possible, or sparse write
		try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(tempFile, "rw")) {
			raf.setLength(PlaygroundBridgeUtil.MAX_BRIDGE_FILE_SIZE + 1024L);
		}

		String result = PlaygroundBridgeUtil.injectHtmlBridge(tempFile);
		assertNull("Files exceeding size limit must return null", result);
	}

	@Test
	public void testBridgeScriptAsJsLiteralDoesNotContainClosingScriptTag() {
		String jsLiteral = PlaygroundBridgeUtil.getBridgeScriptAsJsLiteral();
		assertNotNull(jsLiteral);
		assertTrue("Must start with double quote", jsLiteral.startsWith("\""));
		assertTrue("Must end with double quote", jsLiteral.endsWith("\""));
		assertFalse("Must NOT contain literal </script> which prematurely ends HTML <script> elements", jsLiteral.contains("</script>"));
		assertFalse("Must NOT contain literal <script>", jsLiteral.contains("<script>"));
		assertTrue("Must escape '<' as '\\u003c'", jsLiteral.contains("\\u003cscript>"));
		assertTrue("Must escape '</' as '\\u003c/'", jsLiteral.contains("\\u003c/script>"));

		// When unescaped/parsed by JSON parser, it must equal the original BRIDGE_SCRIPT
		String unescaped = play.libs.Json.parse(jsLiteral).asText();
		assertEquals("Unescaped literal must equal original BRIDGE_SCRIPT", PlaygroundBridgeUtil.BRIDGE_SCRIPT, unescaped);
	}

	@Test
	public void testBridgeRawJsDoesNotContainScriptTags() {
		String rawJs = PlaygroundBridgeUtil.getBridgeRawJs();
		assertNotNull(rawJs);
		assertFalse("Must not contain <script>", rawJs.contains("<script>"));
		assertFalse("Must not contain </script>", rawJs.contains("</script>"));
		assertTrue("Must contain localStorage polyfill", rawJs.contains("window.localStorage"));
		assertTrue("Must contain sessionStorage polyfill", rawJs.contains("window.sessionStorage"));
		assertTrue("Must contain console forwarder", rawJs.contains("df-playground-console"));
	}
}

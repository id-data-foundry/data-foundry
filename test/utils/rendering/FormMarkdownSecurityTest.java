package utils.rendering;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class FormMarkdownSecurityTest {

	@Test
	public void testRawHtmlIsEscaped() {
		String markdown = "Hello <script>alert('xss')</script> <iframe src='http://evil.com'></iframe>";
		String rendered = new FormMarkdown().renderForm(markdown);

		assertFalse("Script tag must not be emitted unescaped", rendered.contains("<script>"));
		assertFalse("Iframe tag must not be emitted unescaped", rendered.contains("<iframe"));
		assertTrue("Script tag must be escaped", rendered.contains("&lt;script&gt;"));
	}

	@Test
	public void testUnsafeUrlsAreSanitized() {
		String markdown = "[click](javascript:alert(1)) and ![img](javascript:alert(2)) and [vb](vbscript:msgbox(1))";
		String rendered = new FormMarkdown().renderForm(markdown);

		assertFalse("javascript: URL must be neutralized", rendered.contains("javascript:alert(1)"));
		assertFalse("javascript: URL must be neutralized", rendered.contains("javascript:alert(2)"));
		assertFalse("vbscript: URL must be neutralized", rendered.contains("vbscript:msgbox(1)"));
	}

	@Test
	public void testRenderHtmlSanitizesUnsafeUrls() {
		String markdown = "[safe](https://example.com) [unsafe](javascript:steal())";
		String rendered = FormMarkdown.renderHtml(markdown);

		assertTrue("Safe URL should be preserved", rendered.contains("href=\"https://example.com\""));
		assertFalse("Unsafe javascript: link should be neutralized", rendered.contains("javascript:steal()"));
	}
}

package utils.rendering;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MarkdownRendererTest {

	@Test
	public void testRawHtmlIsEscaped() {
		MarkdownRenderer renderer = new MarkdownRenderer();
		String markdownWithScript = "Hello world\n\n<script>alert('xss')</script>\n\n<iframe src=\"http://evil.com\"></iframe>";
		String rendered = renderer.render(markdownWithScript);

		// Raw script or iframe tags must NOT be present unescaped
		assertFalse("Raw script tag should not be emitted", rendered.contains("<script>"));
		assertFalse("Raw iframe tag should not be emitted", rendered.contains("<iframe"));

		// Escaped versions should be emitted
		assertTrue("Script tag must be escaped", rendered.contains("&lt;script&gt;"));
		assertTrue("Iframe tag must be escaped", rendered.contains("&lt;iframe"));
	}

	@Test
	public void testMarkdownFormattingWorks() {
		MarkdownRenderer renderer = new MarkdownRenderer();
		String markdown = "# Title\n\n**bold text** and *italic text*";
		String rendered = renderer.render(markdown);

		assertTrue("Heading should be rendered", rendered.contains("<h1>Title</h1>"));
		assertTrue("Bold should be rendered", rendered.contains("<strong>bold text</strong>"));
		assertTrue("Italic should be rendered", rendered.contains("<em>italic text</em>"));
	}
}

package utils.rendering;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Locale;

import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;

import com.google.inject.Singleton;

import models.Dataset;

/**
 * Plain Markdown renderer
 * 
 * @author matsfunk
 */
@Singleton
public class MarkdownRenderer {

	private final Parser parser;
	private final HtmlRenderer renderer;

	public MarkdownRenderer() {
		this(null, true);
	}

	public MarkdownRenderer(boolean escapeHTML) {
		this(null, escapeHTML);
	}

	public MarkdownRenderer(Dataset ds, boolean escapeHTML) {
		parser = Parser.builder().extensions(Arrays.asList(TablesExtension.create())).build();
		renderer = HtmlRenderer.builder().escapeHtml(escapeHTML)
				.attributeProviderFactory(context -> (node, tagName, attributes) -> {
					if ("a".equals(tagName)) {
						String href = attributes.get("href");
						if (href != null && isUnsafeUrl(href)) {
							attributes.put("href", "");
						}
					} else if ("img".equals(tagName)) {
						String src = attributes.get("src");
						if (src != null && isUnsafeUrl(src)) {
							attributes.put("src", "");
						}
					}
				})
				.extensions(Arrays.asList(TablesExtension.create()))
				.build();
	}

	private static boolean isUnsafeUrl(String url) {
		if (url == null) {
			return false;
		}
		String cleaned = url.replaceAll("[\\x00-\\x20]+", "").toLowerCase(Locale.ROOT);
		if (cleaned.isEmpty()) {
			return false;
		}
		if (cleaned.startsWith("/") || cleaned.startsWith("#") || cleaned.startsWith("?") || !cleaned.contains(":")) {
			return false;
		}
		return !(cleaned.startsWith("http://") || cleaned.startsWith("https://") || cleaned.startsWith("mailto:"));
	}

	public String render(String input) {
		String output = renderer.render(parser.parse(input));
		return output;
	}

	public String render(Reader input) throws IOException {
		String output = renderer.render(parser.parseReader(input));
		return output;
	}

}

package utils.rendering;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

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
		this(null);
	}

	public MarkdownRenderer(Dataset ds) {
		parser = Parser.builder().extensions(Arrays.asList(TablesExtension.create())).build();
		renderer = HtmlRenderer.builder().extensions(Arrays.asList(TablesExtension.create())).build();
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

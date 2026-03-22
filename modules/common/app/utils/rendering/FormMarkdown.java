package utils.rendering;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TableBlock;
import org.commonmark.ext.gfm.tables.TableBody;
import org.commonmark.ext.gfm.tables.TableCell;
import org.commonmark.ext.gfm.tables.TableHead;
import org.commonmark.ext.gfm.tables.TableRow;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.ext.gfm.tables.internal.TableHtmlNodeRenderer;
import org.commonmark.node.BlockQuote;
import org.commonmark.node.BulletList;
import org.commonmark.node.HtmlBlock;
import org.commonmark.node.ListItem;
import org.commonmark.node.Node;
import org.commonmark.node.OrderedList;
import org.commonmark.node.Text;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.NodeRenderer;
import org.commonmark.renderer.html.HtmlNodeRendererContext;
import org.commonmark.renderer.html.HtmlNodeRendererFactory;
import org.commonmark.renderer.html.HtmlRenderer;
import org.commonmark.renderer.html.HtmlRenderer.Builder;
import org.commonmark.renderer.html.HtmlWriter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import models.Dataset;
import play.libs.Json;
import utils.DataUtils;

/**
 * Translates MarkDown with form fields to HTML and also a data projection
 *
 * 
 * --- Library for MD parsing: https://github.com/atlassian/commonmark-java
 *
 * --- Syntax idea: http://checkbox.io/researchers.html
 *
 * @author matsfunk
 */
public class FormMarkdown {

	private final Map<String, List<String>> itemMap = new HashMap<String, List<String>>();
	private final List<Extension> renderExtensions = Arrays.asList(new TableFormRenderer(), TablesExtension.create());

	private final Parser parser;
	private final HtmlRenderer renderer;
	private final HtmlRenderer visualizer;

	private int itemCount = 0;

	public FormMarkdown() {
		this(null);
	}

	public FormMarkdown(Dataset ds) {
		final List<Extension> visualizationExtensions = Arrays.asList(new VisualizationTableRenderer(ds),
				TablesExtension.create());
		parser = Parser.builder().extensions(renderExtensions).build();
		renderer = HtmlRenderer.builder().extensions(renderExtensions)
				.nodeRendererFactory(new HtmlNodeRendererFactory() {
					public NodeRenderer create(HtmlNodeRendererContext context) {
						return new FormNodeRenderer(context, false);
					}
				}).build();
		visualizer = HtmlRenderer.builder().extensions(visualizationExtensions)
				.nodeRendererFactory(new HtmlNodeRendererFactory() {
					public NodeRenderer create(HtmlNodeRendererContext context) {
						return new VisualizationRenderer(context, ds);
					}
				}).build();
	}

	/**
	 * one-off render markdown as html
	 * 
	 * @param md
	 * @return
	 */
	public static String renderHtml(String md) {
		Node n = Parser.builder().build().parse(md);
		return HtmlRenderer.builder().build().render(n);
	}

	/**
	 * one-off render html-escaped markdown as html <br>
	 * note: this is used in some templates directly
	 * 
	 * @param md
	 * @return
	 */
	public static String renderEscapedHtml(String md) {
		// escape any tags
		md = md.replace("<", "&lt;").replace(">", "&gt;");
		// unescape the textual \n
		md = md.replace("\\\n", "\n");
		// then transform markdown with softbreak <br>
		Node n = Parser.builder().build().parse(md);
		return HtmlRenderer.builder().softbreak("<br>").build().render(n);
	}

	public String renderForm(String input) {
		itemCount = 0;
		itemMap.clear();
		String output = renderer.render(parser.parse(input));

		return output;
	}

	public String renderFormPreview(String input) {
		itemCount = 0;
		HtmlRenderer previewRenderer = HtmlRenderer.builder().extensions(renderExtensions)
				.nodeRendererFactory(new HtmlNodeRendererFactory() {
					public NodeRenderer create(HtmlNodeRendererContext context) {
						return new FormNodeRenderer(context, true);
					}
				}).build();
		return previewRenderer.render(parser.parse(input));
	}

	public String renderVisualization(String input) {
		itemCount = 0;
		String output = visualizer.render(parser.parse(input));

		return output;
	}

	public int itemCount(String input) {
		itemCount = 0;
		itemMap.clear();
		renderer.render(parser.parse(input));

		return itemCount;
	}

	public String getProjection() {
		return itemMap.keySet().stream().sorted(Comparator.comparingLong(k -> extractNumber(k)))
				.collect(Collectors.joining(","));
	}

	public static long extractNumber(String str) {
		return DataUtils.parseLong(str.replaceAll("\\D", ""));
	}

	public Map<String, List<String>> getItemList() {
		return itemMap;
	}

	class FormNodeRenderer implements NodeRenderer {

		private final HtmlWriter html;
		private final boolean filterHTML;

		FormNodeRenderer(HtmlNodeRendererContext context, boolean filterHTML) {
			this.html = context.getWriter();
			this.filterHTML = filterHTML;
		}

		@Override
		public Set<Class<? extends Node>> getNodeTypes() {
			Set<Class<? extends Node>> set = new HashSet<>();
			set.add(BlockQuote.class);
			set.add(BulletList.class);
			set.add(OrderedList.class);
			set.add(HtmlBlock.class);
			return set;
		}

		@Override
		public void render(Node node) {

			if (node instanceof BlockQuote) {
				BlockQuote bq = (BlockQuote) node;

				String text = getText(bq);

				try {
					// read json inside the blockquote
					ObjectMapper mapper = Json.mapper();
					mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
					JsonNode jn = mapper.readTree(text);
					if (jn.has("rows")) {
						JsonNode jn2 = jn.get("rows");
						int rows = jn2.asInt();
						if (rows > 1) {
							// multiple row --> textarea
							html.raw(views.html.elements.forms.multiLine.render(itemCount, rows).toString());
						} else {
							// no rows or just one --> single line
							html.raw(views.html.elements.forms.singleLine.render(itemCount, false, "").toString());
						}

						itemMap.put("text_" + itemCount++, Collections.<String>emptyList());
					} else if (jn.has("numerical")) {
						JsonNode num = jn.get("numerical");
						if (num.has("start") && num.has("end")) {
							int start = num.get("start").asInt();
							int end = num.get("end").asInt();

							String startLabel = num.has("startLabel") ? num.get("startLabel").asText("") : "";
							String endLabel = num.has("endLabel") ? num.get("endLabel").asText("") : "";

							// start and end --> scale
							html.raw(views.html.elements.forms.scale.render(itemCount, start, end, startLabel, endLabel)
									.toString());
							itemMap.put("numerical_" + itemCount++, Collections.<String>emptyList());
						} else {
							// unbounded --> spinner via HTML5
							html.raw(views.html.elements.forms.numerical.render(itemCount).toString());
							itemMap.put("numerical_" + itemCount++, Collections.<String>emptyList());
						}
					} else if (jn.has("optional")) {
						// if available extract placeholder text
						String placeholder = "";
						if (jn.has("placeholder")) {
							placeholder = jn.get("placeholder").asText();
						}

						// no rows or just one --> single line
						html.raw(views.html.elements.forms.singleLine.render(itemCount, true, placeholder).toString());
						itemMap.put("text_" + itemCount++, Collections.<String>emptyList());
					} else {
						// if available extract placeholder text
						String placeholder = "";
						if (jn.has("placeholder")) {
							placeholder = jn.get("placeholder").asText();
						}

						// no rows or just one --> single line
						html.raw(views.html.elements.forms.singleLine.render(itemCount, false, placeholder).toString());
						itemMap.put("text_" + itemCount++, Collections.<String>emptyList());
					}
				} catch (RuntimeException e) {
					throw e;
				} catch (Exception e) {
					// continue without rows
					html.raw(views.html.elements.forms.singleLine.render(itemCount, false, text).toString());
					itemMap.put("text_" + itemCount++, Collections.<String>emptyList());
				}

			}

			if (node instanceof BulletList) {
				BulletList bl = (BulletList) node;
				String itemKey = "choice_" + itemCount;

				html.raw("""
						<div class="row" data="%s">
								<div class="input-field col s12">
									<fieldset class="validate" required>\n
						""".formatted(itemKey));

				int choiceCount = 0;
				List<String> items = new LinkedList<String>();
				Node n = bl.getFirstChild();
				do {
					if (n instanceof ListItem) {
						String itemText = getText(n);
						html.raw(views.html.elements.forms.multiChoice.render(itemCount, itemText, choiceCount++)
								.toString());
						html.line();
						items.add(itemText);
					} else {
						break;
					}
				} while ((n = n.getNext()) != null);

				html.raw("""
									</fieldset>
						   </div>
						</div>
						""");
				itemMap.put(itemKey, items);
				itemCount++;
			}

			if (node instanceof OrderedList) {
				OrderedList ol = (OrderedList) node;
				String itemKey = "choice_" + itemCount;

				html.raw("""
						<div class="row" data="%s">
								<div class="input-field col s12">
						""".formatted(itemKey));

				int choiceCount = 0;
				List<String> items = new LinkedList<String>();
				Node n = ol.getFirstChild();
				do {
					if (n instanceof ListItem) {
						String itemText = getText(n);
						html.raw(views.html.elements.forms.singleChoice.render(itemCount, itemText, choiceCount++)
								.toString());
						html.line();
						items.add(itemText);
					} else {
						break;
					}
				} while ((n = n.getNext()) != null);

				html.raw("""
						   </div>
						</div>
						""");
				itemMap.put(itemKey, items);
				itemCount++;
			}

			if (node instanceof HtmlBlock) {
				if (filterHTML) {
					html.raw("""
							<p>
									<code>Filtered HTML</code>
								</p>""");
				} else {
					html.raw(((HtmlBlock) node).getLiteral());
				}
			}
		}

	}

	class VisualizationRenderer implements NodeRenderer {

		private final HtmlWriter html;
		private final Dataset dataset;

		VisualizationRenderer(HtmlNodeRendererContext context, Dataset ds) {
			this.html = context.getWriter();
			this.dataset = ds;
		}

		@Override
		public Set<Class<? extends Node>> getNodeTypes() {
			Set<Class<? extends Node>> set = new HashSet<>();
			set.add(BlockQuote.class);
			set.add(BulletList.class);
			set.add(OrderedList.class);
			set.add(HtmlBlock.class);
			return set;
		}

		@Override
		public void render(Node node) {

			if (node instanceof BlockQuote) {
				BlockQuote bq = (BlockQuote) node;

				String text = getText(bq);

				// read json inside the blockquote
				ObjectMapper mapper = Json.mapper();
				mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
				JsonNode jn;
				try {
					jn = mapper.readTree(text);
					if (jn.has("numerical")) {
						String choice = "numerical_" + itemCount++;
						html.raw(views.html.elements.forms.visualizeNumerical.render(dataset, choice).toString());
					} else {
						String choice = "text_" + itemCount++;
						html.raw(views.html.elements.forms.visualizeText.render(dataset, choice).toString());
					}
				} catch (Exception e) {
					String choice = "text_" + itemCount++;
					html.raw(views.html.elements.forms.visualizeText.render(dataset, choice).toString());
				}
			}

			if (node instanceof BulletList) {
				// BulletList bl = (BulletList) node;

				// int choiceCount = 0;
				// Node n = bl.getFirstChild();
				// do {
				// if (n instanceof ListItem) {
				// ListItem item = (ListItem) n;
				// String itemText = ((Text) item.getFirstChild().getFirstChild()).getLiteral();
				// } else {
				// break;
				// }
				// } while ((n = n.getNext()) != null);

				String choice = "choice_" + itemCount++;
				html.raw(views.html.elements.forms.visualizeMultiChoice
						.render(dataset, choice, getJsonItems(itemMap.get(choice))).toString());
			}
			if (node instanceof OrderedList) {
				// OrderedList ol = (OrderedList) node;

				// int choiceCount = 0;
				// Node n = ol.getFirstChild();
				// do {
				// if (n instanceof ListItem) {
				// ListItem item = (ListItem) n;
				// String itemText = ((Text) item.getFirstChild().getFirstChild()).getLiteral();
				// } else {
				// break;
				// }
				// } while ((n = n.getNext()) != null);

				String choice = "choice_" + itemCount++;
				html.raw(views.html.elements.forms.visualizeChoice
						.render(dataset, choice, getJsonItems(itemMap.get(choice))).toString());
			}

			// note: we registered for HtmlBlock but we don't render anything here.
		}

	}

	class TableFormRenderer implements Extension, HtmlRenderer.HtmlRendererExtension {

		@Override
		public void extend(Builder rendererBuilder) {
			rendererBuilder.nodeRendererFactory(new HtmlNodeRendererFactory() {
				@Override
				public NodeRenderer create(HtmlNodeRendererContext context) {
					return new FormTableHtmlNodeRenderer(context);
				}
			});
		}
	}

	class VisualizationTableRenderer implements Extension, HtmlRenderer.HtmlRendererExtension {

		private final Dataset dataset;

		public VisualizationTableRenderer(Dataset ds) {
			this.dataset = ds;
		}

		@Override
		public void extend(Builder rendererBuilder) {
			rendererBuilder.nodeRendererFactory(new HtmlNodeRendererFactory() {
				@Override
				public NodeRenderer create(HtmlNodeRendererContext context) {
					return new VisualizationTableHtmlRenderer(context, dataset);
				}
			});
		}
	}

	class FormTableHtmlNodeRenderer extends TableHtmlNodeRenderer {

		private final HtmlWriter html;
		private int choiceCount = 0;
		private List<String> items = new LinkedList<String>();

		FormTableHtmlNodeRenderer(HtmlNodeRendererContext context) {
			super(context);
			this.html = context.getWriter();
		}

		@Override
		protected void renderBlock(TableBlock tableBlock) {
			items.clear();
			super.renderBlock(tableBlock);
		}

		@Override
		protected void renderRow(TableRow tableRow) {
			choiceCount = 0;

			if (items.isEmpty()) {
				super.renderRow(tableRow);
			} else {
				super.renderRow(tableRow);
				itemMap.put("choice_" + itemCount++, items);
			}
		}

		@Override
		protected void renderCell(TableCell tb) {
			if (tb.isHeader() || choiceCount == 0) {
				items.add(getText(tb));
				super.renderCell(tb);
			} else {
				html.raw(views.html.elements.forms.singleChoiceTC.render(itemCount, "", items.get(choiceCount))
						.toString());
			}
			choiceCount++;
		}
	}

	class VisualizationTableHtmlRenderer extends TableHtmlNodeRenderer {

		private final HtmlWriter html;
		private final HtmlNodeRendererContext context;
		private final Dataset dataset;

		private int choiceCount = 0;
		private List<String> gridColumns = new LinkedList<String>();
		private boolean renderingHead = false;

		VisualizationTableHtmlRenderer(HtmlNodeRendererContext context, Dataset ds) {
			super(context);
			this.context = context;
			this.html = context.getWriter();
			this.dataset = ds;
		}

		protected void renderBlock(TableBlock tableBlock) {
			gridColumns.clear();
			renderChildren(tableBlock);
		}

		protected void renderHead(TableHead tableHead) {
			renderingHead = true;
			renderChildren(tableHead);
		}

		protected void renderBody(TableBody tableBody) {
			renderingHead = false;
			renderChildren(tableBody);
		}

		@Override
		protected void renderRow(TableRow tableRow) {

			// only for header
			if (renderingHead) {
				choiceCount = 0;
				renderChildren(tableRow);
			} else {
				String choice = "choice_" + itemCount++;
				html.raw(views.html.elements.forms.visualizeChoice.render(dataset, choice, getJsonItems(gridColumns))
						.toString());
			}
		}

		protected void renderCell(TableCell tableCell) {
			if (renderingHead) {
				if (choiceCount++ > 0) {
					gridColumns.add(getText(tableCell));
				}
			}
		}

		private void renderChildren(Node parent) {
			Node node = parent.getFirstChild();
			while (node != null) {
				Node next = node.getNext();
				context.render(node);
				node = next;
			}
		}
	}

	public static String getJsonItems(List<String> items) {
		return items.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
	}

	public static String getText(Node n) {
		while (n.getFirstChild() != null) {
			n = n.getFirstChild();
		}
		if (n instanceof Text) {
			Text text = (Text) n;
			return text.getLiteral();
		} else {
			return n.toString();
		}
	}
}

package utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class StringUtilsGanttSanitizeTest {

	@Test
	public void testNullAndEmptyLabels() {
		assertEquals("Fallback", StringUtils.sanitizeGanttLabel(null, "Fallback"));
		assertEquals("Fallback", StringUtils.sanitizeGanttLabel("", "Fallback"));
		assertEquals("Fallback", StringUtils.sanitizeGanttLabel("   ", "Fallback"));
		assertEquals("", StringUtils.sanitizeGanttLabel(null, null));
		assertEquals("Item", StringUtils.sanitizeGanttLabel("", ""));
	}

	@Test
	public void testSpecialCharactersStripping() {
		// Non-allowed characters such as :, ;, #, quotes, brackets should be stripped
		assertEquals("Dataset 123", StringUtils.sanitizeGanttLabel("Dataset: #123!", "Fallback"));
		assertEquals("My - Dataset + 1", StringUtils.sanitizeGanttLabel("My - Dataset + 1?", "Fallback"));
		// If string contains only special characters, should fall back
		assertEquals("Fallback", StringUtils.sanitizeGanttLabel("???:::###", "Fallback"));
	}

	@Test
	public void testNormalLabelsUnmodified() {
		assertEquals("HomeofmyAI", StringUtils.sanitizeGanttLabel("HomeofmyAI", "Fallback"));
		assertEquals("User Study 2026", StringUtils.sanitizeGanttLabel("User Study 2026", "Fallback"));
		assertEquals("My_Project-Data", StringUtils.sanitizeGanttLabel("My_Project-Data", "Fallback"));
	}

	@Test
	public void testReservedGanttKeywordsPrefixedWithZeroWidthSpace() {
		// "click data" collided with the 'click' directive in Pele
		String clickLabel = StringUtils.sanitizeGanttLabel("click data", "Fallback");
		assertTrue(clickLabel.startsWith("\u200B"));
		assertEquals("\u200Bclick data", clickLabel);

		// Case-insensitive check
		String clickUpper = StringUtils.sanitizeGanttLabel("Click Data", "Fallback");
		assertTrue(clickUpper.startsWith("\u200B"));
		assertEquals("\u200BClick Data", clickUpper);

		// Other reserved Gantt keywords
		String callLabel = StringUtils.sanitizeGanttLabel("call center", "Fallback");
		assertEquals("\u200Bcall center", callLabel);

		String ganttLabel = StringUtils.sanitizeGanttLabel("gantt chart", "Fallback");
		assertEquals("\u200Bgantt chart", ganttLabel);

		String sectionLabel = StringUtils.sanitizeGanttLabel("section one", "Fallback");
		assertEquals("\u200Bsection one", sectionLabel);

		String titleLabel = StringUtils.sanitizeGanttLabel("title test", "Fallback");
		assertEquals("\u200Btitle test", titleLabel);

		String accTitleLabel = StringUtils.sanitizeGanttLabel("accTitle test", "Fallback");
		assertEquals("\u200BaccTitle test", accTitleLabel);

		String accDescrLabel = StringUtils.sanitizeGanttLabel("accDescr test", "Fallback");
		assertEquals("\u200BaccDescr test", accDescrLabel);

		String accDescriptionLabel = StringUtils.sanitizeGanttLabel("accDescription test", "Fallback");
		assertEquals("\u200BaccDescription test", accDescriptionLabel);

		String weekdayLabel = StringUtils.sanitizeGanttLabel("weekday monday", "Fallback");
		assertEquals("\u200Bweekday monday", weekdayLabel);

		String weekendLabel = StringUtils.sanitizeGanttLabel("weekend saturday", "Fallback");
		assertEquals("\u200Bweekend saturday", weekendLabel);
	}

	@Test
	public void testWordsContainingKeywordsAsSubstringsAreNotPrefixed() {
		// "clicks" or "clicking" is not word-boundary keyword "click"
		String clicksLabel = StringUtils.sanitizeGanttLabel("clicks count", "Fallback");
		assertFalse(clicksLabel.startsWith("\u200B"));
		assertEquals("clicks count", clicksLabel);

		String callingLabel = StringUtils.sanitizeGanttLabel("calling service", "Fallback");
		assertFalse(callingLabel.startsWith("\u200B"));
		assertEquals("calling service", callingLabel);
	}
}

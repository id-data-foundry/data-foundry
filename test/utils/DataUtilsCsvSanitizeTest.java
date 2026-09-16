package utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import utils.telegrambot.TelegramBotUtils;

public class DataUtilsCsvSanitizeTest {

	@Test
	public void testNullAndEmptyCells() {
		assertEquals("\"\"", DataUtils.sanitizeCsvCell(null));
		assertEquals("\"\"", DataUtils.sanitizeCsvCell(""));
		assertEquals("\"\"", StringUtils.cf(null));
		assertEquals("\"\"", StringUtils.cf(""));
	}

	@Test
	public void testNormalCells() {
		assertEquals("\"normal text\"", DataUtils.sanitizeCsvCell("normal text"));
		assertEquals("\"simple\"", StringUtils.cf("simple"));
	}

	@Test
	public void testQuotesEscaping() {
		assertEquals("\"hello \"\"world\"\"\"", DataUtils.sanitizeCsvCell("hello \"world\""));
	}

	@Test
	public void testFormulaInjectionSanitization() {
		// Formula trigger characters: =, @, \t, \r
		assertEquals("\"'=cmd|' /C calc'!A0\"", DataUtils.sanitizeCsvCell("=cmd|' /C calc'!A0"));
		assertEquals("\"'=1+1\"", DataUtils.sanitizeCsvCell("=1+1"));
		assertEquals("\"'@SUM(A1:A10)\"", DataUtils.sanitizeCsvCell("@SUM(A1:A10)"));
		assertEquals("\"'\ttab\"", DataUtils.sanitizeCsvCell("\ttab"));
		assertEquals("\"'\rcarriage\"", DataUtils.sanitizeCsvCell("\rcarriage"));

		// Text starting with + or - should be escaped
		assertEquals("\"'+cmd|' /C calc'!A0\"", DataUtils.sanitizeCsvCell("+cmd|' /C calc'!A0"));
		assertEquals("\"'-cmd|' /C calc'!A0\"", DataUtils.sanitizeCsvCell("-cmd|' /C calc'!A0"));

		// But valid numeric values starting with + or - should remain intact
		assertEquals("\"-123.45\"", DataUtils.sanitizeCsvCell("-123.45"));
		assertEquals("\"+42\"", DataUtils.sanitizeCsvCell("+42"));
	}

	@Test
	public void testTelegramBotUtilsSecureRandomPINs() {
		for (int i = 0; i < 20; i++) {
			String pin = TelegramBotUtils.generateTelegramPersonalPIN();
			assertNotNull(pin);
			assertEquals(5, pin.length());
			int pinVal = Integer.parseInt(pin);
			assertTrue(pinVal >= 10000 && pinVal <= 99999);

			long projectId = 42L;
			String projectPin = TelegramBotUtils.generateTelegramProjectPIN(projectId);
			assertNotNull(projectPin);
			assertTrue(TelegramBotUtils.isValidTelegramProjectCode(projectPin));
			long[] ids = TelegramBotUtils.extractIdsFromtelegramProjectCode(projectPin);
			assertEquals(2, ids.length);
			assertEquals(projectId, ids[0]);
		}
	}
}

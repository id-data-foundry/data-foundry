package models.ds;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MovementDSSecurityTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testSafeGpxPassesCheck() throws IOException {
		File safeFile = tempFolder.newFile("safe.gpx");
		String safeXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<gpx version=\"1.1\"><trk></trk></gpx>";
		try (FileOutputStream fos = new FileOutputStream(safeFile)) {
			fos.write(safeXml.getBytes(StandardCharsets.UTF_8));
		}
		assertFalse("Safe GPX file should not be flagged", MovementDS.containsXmlEntityOrDocType(safeFile));
	}

	@Test
	public void testUtf8DocTypeAndEntityFlagged() throws IOException {
		File docTypeFile = tempFolder.newFile("doctype_utf8.gpx");
		String xxe = "<?xml version=\"1.0\"?>\n<!DOCTYPE gpx [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>\n<gpx></gpx>";
		try (FileOutputStream fos = new FileOutputStream(docTypeFile)) {
			fos.write(xxe.getBytes(StandardCharsets.UTF_8));
		}
		assertTrue("UTF-8 DOCTYPE/ENTITY must be detected", MovementDS.containsXmlEntityOrDocType(docTypeFile));
	}

	@Test
	public void testUtf16LeDocTypeAndEntityFlagged() throws IOException {
		File utf16LeFile = tempFolder.newFile("doctype_utf16le.gpx");
		String xxe = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<!DOCTYPE gpx [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>\n<gpx></gpx>";
		try (FileOutputStream fos = new FileOutputStream(utf16LeFile)) {
			// Write UTF-16LE with BOM
			fos.write(new byte[] { (byte) 0xFF, (byte) 0xFE });
			fos.write(xxe.getBytes(StandardCharsets.UTF_16LE));
		}
		assertTrue("UTF-16LE DOCTYPE/ENTITY must be detected", MovementDS.containsXmlEntityOrDocType(utf16LeFile));
	}

	@Test
	public void testUtf16BeDocTypeAndEntityFlagged() throws IOException {
		File utf16BeFile = tempFolder.newFile("doctype_utf16be.gpx");
		String xxe = "<?xml version=\"1.0\" encoding=\"UTF-16\"?>\n<!DOCTYPE gpx [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>\n<gpx></gpx>";
		try (FileOutputStream fos = new FileOutputStream(utf16BeFile)) {
			// Write UTF-16BE with BOM
			fos.write(new byte[] { (byte) 0xFE, (byte) 0xFF });
			fos.write(xxe.getBytes(StandardCharsets.UTF_16BE));
		}
		assertTrue("UTF-16BE DOCTYPE/ENTITY must be detected", MovementDS.containsXmlEntityOrDocType(utf16BeFile));
	}
}

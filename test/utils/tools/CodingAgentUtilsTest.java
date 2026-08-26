package utils.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import models.DatasetType;

public class CodingAgentUtilsTest {

	@Test
	public void testCleanThinkingTags() {
		// Clean standard thinking block
		assertEquals("Hello world", CodingAgentUtils.cleanThinkingTags("<think>Some internal thought</think>Hello world"));
		
		// Multi-line thinking block
		assertEquals("Result here", CodingAgentUtils.cleanThinkingTags("<think>\nStep 1\nStep 2\n</think>\nResult here"));
		
		// Unclosed thinking tag
		assertEquals("", CodingAgentUtils.cleanThinkingTags("<think>Incomplete thought..."));
		
		// Missing opening tag
		assertEquals("Finished text", CodingAgentUtils.cleanThinkingTags("Unopened thought\n</think>\nFinished text"));
		
		// Null and empty
		assertEquals("", CodingAgentUtils.cleanThinkingTags(null));
		assertEquals("", CodingAgentUtils.cleanThinkingTags(""));
		assertEquals("Regular text without think tags", CodingAgentUtils.cleanThinkingTags("Regular text without think tags"));
	}

	@Test
	public void testFilterDatasetPath() {
		File datasetFolder = new File("/workspace/DataFoundry/public/uploads/datasets/p100__d200");

		// Absolute file path
		String text1 = "I have updated the file in /workspace/DataFoundry/public/uploads/datasets/p100__d200/index.html successfully.";
		assertEquals("I have updated the file in PATH/index.html successfully.",
				CodingAgentUtils.filterDatasetPath(text1, datasetFolder));

		// Absolute directory path without trailing slash
		String text2 = "Files saved in /workspace/DataFoundry/public/uploads/datasets/p100__d200.";
		assertEquals("Files saved in PATH.", CodingAgentUtils.filterDatasetPath(text2, datasetFolder));

		// Agentscope workspace directory
		String text3 = "Checked /workspace/DataFoundry/public/uploads/datasets/p100__d200/.agentscope/knowledge/OOCSI.md.";
		assertEquals("Checked PATH/knowledge/OOCSI.md.", CodingAgentUtils.filterDatasetPath(text3, datasetFolder));

		// Multiple occurrences
		String text4 = "Copied from /workspace/DataFoundry/public/uploads/datasets/p100__d200/a.js to /workspace/DataFoundry/public/uploads/datasets/p100__d200/b.js.";
		assertEquals("Copied from PATH/a.js to PATH/b.js.", CodingAgentUtils.filterDatasetPath(text4, datasetFolder));

		// Clean text without paths
		String text5 = "Here is the code for your index.html.";
		assertEquals(text5, CodingAgentUtils.filterDatasetPath(text5, datasetFolder));

		// Null and empty inputs
		assertNull(CodingAgentUtils.filterDatasetPath(null, datasetFolder));
		assertEquals("", CodingAgentUtils.filterDatasetPath("", datasetFolder));

		// Project root path (new File(""))
		String projectRoot = new File("").getAbsolutePath();
		if (projectRoot != null && !projectRoot.isEmpty()) {
			String textWithProjectRoot = "Project root is located at " + projectRoot + "/conf/application.conf";
			assertEquals("Project root is located at PATH/conf/application.conf",
					CodingAgentUtils.filterDatasetPath(textWithProjectRoot, datasetFolder));

			String textWithNullDataset = "Look at " + projectRoot + "/build.sbt";
			assertEquals("Look at PATH/build.sbt", CodingAgentUtils.filterDatasetPath(textWithNullDataset, null));
		}
	}

	@Test
	public void testBotConstantsAndSummoning() {
		// Constants
		assertEquals("boter", CodingAgentUtils.BOT_NAME);
		assertEquals("@boter", CodingAgentUtils.BOT_SUMMON_TAG);
		assertEquals("@boter 🧈", CodingAgentUtils.BOT_DISPLAY_NAME);

		// Direct summoning
		org.junit.Assert.assertTrue(CodingAgentUtils.isAgentSummoned("@boter"));
		org.junit.Assert.assertTrue(CodingAgentUtils.isAgentSummoned("@boter please help"));
		org.junit.Assert.assertTrue(CodingAgentUtils.isAgentSummoned("Hello @boter, what is this?"));
		org.junit.Assert.assertTrue(CodingAgentUtils.isAgentSummoned("Can you check this @boter?"));
		org.junit.Assert.assertTrue(CodingAgentUtils.isAgentSummoned("@boter: create an index.html"));

		// Case-insensitive summoning
		org.junit.Assert.assertTrue(CodingAgentUtils.isAgentSummoned("@Boter can you help?"));
		org.junit.Assert.assertTrue(CodingAgentUtils.isAgentSummoned("@BOTER status report"));
		org.junit.Assert.assertTrue(CodingAgentUtils.isAgentSummoned("Hey @BoTeR!"));

		// Non-summoning human chat
		org.junit.Assert.assertFalse(CodingAgentUtils.isAgentSummoned("Hello team!"));
		org.junit.Assert.assertFalse(CodingAgentUtils.isAgentSummoned("Hey Bob, what do you think?"));
		org.junit.Assert.assertFalse(CodingAgentUtils.isAgentSummoned("The robotics club meets tomorrow"));
		org.junit.Assert.assertFalse(CodingAgentUtils.isAgentSummoned("Pass the water bottle"));
		org.junit.Assert.assertFalse(CodingAgentUtils.isAgentSummoned(""));
		org.junit.Assert.assertFalse(CodingAgentUtils.isAgentSummoned(null));
	}

	@Test
	public void testFormatUserMessageForAgent() {
		assertEquals("Alice S.: Hello everyone", CodingAgentUtils.formatUserMessageForAgent("Alice S.", "Hello everyone"));
		assertEquals("Bob K.: @bot please edit index.html",
				CodingAgentUtils.formatUserMessageForAgent("Bob K.", "@bot please edit index.html"));
		assertEquals("Hello world", CodingAgentUtils.formatUserMessageForAgent(null, "Hello world"));
		assertEquals("Hello world", CodingAgentUtils.formatUserMessageForAgent("", "Hello world"));
		assertEquals("Alice S.: ", CodingAgentUtils.formatUserMessageForAgent("Alice S.", null));
	}

	@Test
	public void testSubAgentConstantsAndMessages() {
		assertEquals("comrade roomboter", CodingAgentUtils.SUBAGENT_NAME);

		String startMsg = CodingAgentUtils.getRandomSubAgentStartMessage();
		org.junit.Assert.assertNotNull(startMsg);
		org.junit.Assert.assertTrue(startMsg.contains(CodingAgentUtils.SUBAGENT_NAME));

		String completionMsg = CodingAgentUtils.getRandomSubAgentCompletionMessage();
		org.junit.Assert.assertNotNull(completionMsg);
		org.junit.Assert.assertTrue(completionMsg.contains(CodingAgentUtils.SUBAGENT_NAME));
	}

	@Test
	public void testDatasetTypeMapping() {
		// COMPLETE must be named EXISTING
		assertEquals("EXISTING", CodingAgentUtils.getDatasetType(DatasetType.COMPLETE));

		// Other types retain their standard naming
		assertEquals("IOT", CodingAgentUtils.getDatasetType(DatasetType.IOT));
		assertEquals("TIMESERIES", CodingAgentUtils.getDatasetType(DatasetType.TIMESERIES));
		assertEquals("ENTITY", CodingAgentUtils.getDatasetType(DatasetType.ENTITY));
		assertEquals("MEDIA", CodingAgentUtils.getDatasetType(DatasetType.MEDIA));
		assertEquals("ANNOTATION", CodingAgentUtils.getDatasetType(DatasetType.ANNOTATION));
		assertEquals("DIARY", CodingAgentUtils.getDatasetType(DatasetType.DIARY));
		assertEquals("FORM", CodingAgentUtils.getDatasetType(DatasetType.FORM));
		assertEquals("SURVEY", CodingAgentUtils.getDatasetType(DatasetType.SURVEY));
		assertEquals("MOVEMENT", CodingAgentUtils.getDatasetType(DatasetType.MOVEMENT));
		assertEquals("ES", CodingAgentUtils.getDatasetType(DatasetType.ES));
		assertEquals("FITBIT", CodingAgentUtils.getDatasetType(DatasetType.FITBIT));
		assertEquals("GOOGLEFIT", CodingAgentUtils.getDatasetType(DatasetType.GOOGLEFIT));
		assertEquals("LINKED", CodingAgentUtils.getDatasetType(DatasetType.LINKED));

		// Null check
		assertEquals("", CodingAgentUtils.getDatasetType(null));
	}

	@Test
	public void testDatasetApiRoutes() {
		// IoT / Timeseries routes
		ObjectNode iotRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.IOT, 101L);
		assertNotNull(iotRoutes);
		assertEquals("POST /api/v1/datasets/ts/101", iotRoutes.get("log").asText());
		assertEquals("GET /datasets/download/json/101", iotRoutes.get("downloadJson").asText());
		assertEquals("GET /datasets/download/101", iotRoutes.get("downloadCsv").asText());

		// Entity routes
		ObjectNode entityRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.ENTITY, 102L);
		assertNotNull(entityRoutes);
		assertEquals("GET /api/v1/datasets/entity/102", entityRoutes.get("getItem").asText());
		assertEquals("POST /api/v1/datasets/entity/102", entityRoutes.get("addItem").asText());
		assertEquals("PUT /api/v1/datasets/entity/102", entityRoutes.get("updateItem").asText());
		assertEquals("DELETE /api/v1/datasets/entity/102", entityRoutes.get("deleteItem").asText());
		assertEquals("GET /datasets/download/json/102", entityRoutes.get("downloadJson").asText());
		assertEquals("GET /datasets/download/102", entityRoutes.get("downloadCsv").asText());

		// Media routes
		ObjectNode mediaRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.MEDIA, 103L);
		assertNotNull(mediaRoutes);
		assertEquals("POST /api/v1/datasets/media/103", mediaRoutes.get("uploadMedia").asText());
		assertEquals("GET /api/v1/datasets/media/103/{filename}", mediaRoutes.get("getMedia").asText());
		assertEquals("PUT /api/v1/datasets/media/103/{itemId}", mediaRoutes.get("updateMedia").asText());

		// Complete / Existing routes
		ObjectNode completeRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.COMPLETE, 104L);
		assertNotNull(completeRoutes);
		assertEquals("POST /api/v1/datasets/existing/104", completeRoutes.get("uploadFile").asText());
		assertEquals("GET /datasets/existing/downloadLatest/104/{fileName}",
				completeRoutes.get("downloadLatestFile").asText());
		assertEquals("GET /datasets/existing/download/104/{fileId}", completeRoutes.get("downloadFile").asText());
		assertEquals("GET /datasets/web/104/{filepath}", completeRoutes.get("web").asText());

		// Annotation routes
		ObjectNode annotationRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.ANNOTATION, 105L);
		assertNotNull(annotationRoutes);
		assertEquals("POST /api/v2/datasets/annotation/105", annotationRoutes.get("addRecord").asText());

		// Diary routes
		ObjectNode diaryRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.DIARY, 106L);
		assertNotNull(diaryRoutes);
		assertEquals("POST /api/v2/datasets/diary/106/{participant_id}", diaryRoutes.get("addRecord").asText());

		// Form and Survey routes
		ObjectNode formRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.FORM, 107L);
		assertEquals("POST /datasets/form/record/107", formRoutes.get("record").asText());
		assertEquals("GET /datasets/form/raw/107.csv", formRoutes.get("downloadCsv").asText());

		ObjectNode surveyRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.SURVEY, 108L);
		assertEquals("POST /datasets/survey/record/108/{invite_token}", surveyRoutes.get("record").asText());
		assertEquals("GET /datasets/survey/raw/108.csv", surveyRoutes.get("downloadCsv").asText());

		// Movement and ES routes
		ObjectNode movementRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.MOVEMENT, 109L);
		assertEquals("POST /api/v2/datasets/upload/109", movementRoutes.get("uploadFile").asText());

		// Wearable routes
		ObjectNode fitbitRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.FITBIT, 110L);
		assertEquals("GET /datasets/fitbit/heartrate/110", fitbitRoutes.get("heartrate").asText());

		// Null safety
		ObjectNode nullTypeRoutes = CodingAgentUtils.getDatasetApiRoutes(null, 111L);
		assertEquals(0, nullTypeRoutes.size());

		ObjectNode nullIdRoutes = CodingAgentUtils.getDatasetApiRoutes(DatasetType.IOT, null);
		assertEquals(0, nullIdRoutes.size());
	}
}

package utils.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.junit.Test;

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
}

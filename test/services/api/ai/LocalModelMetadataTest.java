package services.api.ai;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.Test;
import services.api.ai.LocalModelMetadata.ModelMetadata;

public class LocalModelMetadataTest {

	@Test
	public void testParseOpenAIAPIModels() throws IOException {
		String jsonContent = Files.readString(Path.of("test/services/api/ai/openai-api-format.json"));
		LocalModelMetadata localModelMetadata = new LocalModelMetadata();
		localModelMetadata.updateModels(jsonContent);

		List<ModelMetadata> models = localModelMetadata.getModels();
		assertNotNull(models);
		assertEquals(1, models.size());

		ModelMetadata model = models.get(0);
		assertEquals("openai/gpt-oss-20b", model.id());
		assertEquals("openai/gpt-oss-20b", model.name());
		// Verify capability type is parsed if present or not guessed
		// In openai-api-models.json, type is not present
		assertEquals(null, model.type());
	}

	@Test
	public void testParseLiteLLMFormat() throws IOException {
		String jsonContent = Files.readString(Path.of("test/services/api/ai/litellm-format.json"));
		LocalModelMetadata localModelMetadata = new LocalModelMetadata();
		localModelMetadata.updateModels(jsonContent);

		List<ModelMetadata> models = localModelMetadata.getModels();
		assertNotNull(models);
		assertEquals(1, models.size());

		ModelMetadata model = models.get(0);
		assertEquals("gpt-4", model.id());
		assertEquals("gpt-4", model.name());
		// Verify alias contains the model_info.id "e889baacd17f591cce4c63639275ba5e8dc60765d6c553e6ee5a504b19e50ddc"
		assertNotNull(model.alias());
		assertTrue(model.alias().contains("e889baacd17f591cce4c63639275ba5e8dc60765d6c553e6ee5a504b19e50ddc"));
		
		// In litellm-format.json, type is not present under model_info or root
		assertEquals(null, model.type());
	}
}

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

	private static Path resolveTestFile(String relativePath) {
		Path[] candidates = new Path[] {
			Path.of(relativePath),
			Path.of("DataFoundry", relativePath),
			Path.of("../..", relativePath),
			Path.of("..", relativePath)
		};
		for (Path p : candidates) {
			if (Files.exists(p)) {
				return p;
			}
		}
		return Path.of(relativePath);
	}

	@Test
	public void testParseOpenAIAPIModels() throws IOException {
		String jsonContent = Files.readString(resolveTestFile("test/services/api/ai/openai-api-format.json"));
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
		String jsonContent = Files.readString(resolveTestFile("test/services/api/ai/litellm-format.json"));
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

	@Test
	public void testResolvePlaceholdersFromConfig() {
		com.typesafe.config.Config config = com.typesafe.config.ConfigFactory.parseMap(java.util.Map.of(
				"df.processing.ai.models.default", "my-default-model",
				"df.processing.ai.models.chat", "my-chat-model",
				"df.processing.ai.models.vision", "my-vision-model",
				"df.processing.ai.models.coding", "my-coding-model",
				"df.processing.ai.models.translate", "my-translate-model",
				"df.processing.ai.models.image", "my-image-model"
		));

		LocalModelMetadata metadata = new LocalModelMetadata(config);

		assertEquals("my-default-model", metadata.mapModelId("default"));
		assertEquals("my-default-model", metadata.mapModelId("DEFAULT"));
		assertEquals("my-default-model", metadata.mapModelId(null));
		assertEquals("my-default-model", metadata.mapModelId(""));
		assertEquals("my-chat-model", metadata.mapModelId("chat"));
		assertEquals("my-chat-model", metadata.mapModelId("CHAT"));
		assertEquals("my-vision-model", metadata.mapModelId("vision"));
		assertEquals("my-vision-model", metadata.mapModelId("VISION"));
		assertEquals("my-vision-model", metadata.mapModelId("image-to-text"));
		assertEquals("my-coding-model", metadata.mapModelId("coding"));
		assertEquals("my-coding-model", metadata.mapModelId("code"));
		assertEquals("my-translate-model", metadata.mapModelId("translate"));
		assertEquals("my-translate-model", metadata.mapModelId("translation"));
		assertEquals("my-image-model", metadata.mapModelId("image"));
		assertEquals("my-image-model", metadata.mapModelId("text-to-image"));

		// Non-placeholder literal model IDs pass through untouched
		assertEquals("custom-unknown-model", metadata.mapModelId("custom-unknown-model"));
		assertEquals("openai/gpt-4o", metadata.mapModelId("openai/gpt-4o"));

		assertTrue(metadata.isPlaceholder("default"));
		assertTrue(metadata.isPlaceholder("vision"));
		assertTrue(metadata.isPlaceholder("coding"));
		org.junit.Assert.assertFalse(metadata.isPlaceholder("openai/gpt-4o"));
	}

	@Test
	public void testFallbackWhenSpecificPlaceholderOmitted() {
		com.typesafe.config.Config config = com.typesafe.config.ConfigFactory.parseMap(java.util.Map.of(
				"df.processing.ai.models.default", "fallback-default"
		));

		LocalModelMetadata metadata = new LocalModelMetadata(config);

		// Text/LLM placeholders fall back to default
		assertEquals("fallback-default", metadata.mapModelId("chat"));
		assertEquals("fallback-default", metadata.mapModelId("coding"));
		assertEquals("fallback-default", metadata.mapModelId("text"));
		assertEquals("fallback-default", metadata.mapModelId("translate"));

		// Modality-specific placeholders strictly DO NOT fall back to text models!
		assertEquals("vision", metadata.mapModelId("vision"));
		assertEquals("image", metadata.mapModelId("image"));
		assertEquals("stt", metadata.mapModelId("stt"));
		assertEquals("tts", metadata.mapModelId("tts"));
	}

	@Test
	public void testPlaceholdersCombinedWithDiscoveredModelAliases() {
		com.typesafe.config.Config config = com.typesafe.config.ConfigFactory.parseMap(java.util.Map.of(
				"df.processing.ai.models.default", "alias-default",
				"df.processing.ai.models.vision", "alias-vision"
		));

		LocalModelMetadata metadata = new LocalModelMetadata(config);

		String mockModelsJson = "[{\"id\": \"canonical-default\", \"alias\": [\"alias-default\"]},"
				+ "{\"id\": \"canonical-vision\", \"alias\": [\"alias-vision\"]}]";
		metadata.updateModels(mockModelsJson);

		// Placeholder resolves to alias, which then resolves to canonical model ID
		assertEquals("canonical-default", metadata.mapModelId("default"));
		assertEquals("canonical-vision", metadata.mapModelId("vision"));
	}
}

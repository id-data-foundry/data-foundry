package services.api.ai;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.inject.Singleton;
import play.Logger;
import play.libs.F.Tuple;

@Singleton
public class LocalModelMetadata {

	private static final Logger.ALogger logger = Logger.of(LocalModelMetadata.class);

	private Map<String, ModelMetadata> modelmapper = new ConcurrentHashMap<>();

	// Capability flags
	private boolean textToTextAvailable = false;
	private boolean textToImageAvailable = false;
	private boolean speechToTextAvailable = false;
	private boolean textToSpeechAvailable = false;

	/**
	 * clear all models
	 */
	public void clearModels() {
		modelmapper = new ConcurrentHashMap<>();
		textToTextAvailable = false;
		textToImageAvailable = false;
		speechToTextAvailable = false;
		textToSpeechAvailable = false;
		logger.info("Cleared all models.");
	}

	public boolean isTextToTextAvailable() {
		return textToTextAvailable;
	}

	public void setTextToTextAvailable(boolean available) {
		this.textToTextAvailable = available;
	}

	public boolean isTextToImageAvailable() {
		return textToImageAvailable;
	}

	public void setTextToImageAvailable(boolean available) {
		this.textToImageAvailable = available;
	}

	public boolean isSpeechToTextAvailable() {
		return speechToTextAvailable;
	}

	public void setSpeechToTextAvailable(boolean available) {
		this.speechToTextAvailable = available;
	}

	public boolean isTextToSpeechAvailable() {
		return textToSpeechAvailable;
	}

	public void setTextToSpeechAvailable(boolean available) {
		this.textToSpeechAvailable = available;
	}

	/**
	 * map the given model id (from request) to actual model id; this will resolve models also via the alias
	 *
	 * @param modelId
	 * @return
	 */
	public String mapModelId(String modelId) {
		ModelMetadata mmd = modelmapper.get(modelId);
		return mmd != null ? mmd.id() : modelId;
	}

	/**
	 * retrieve the shortened model name
	 *
	 * @param modelId
	 * @return
	 */
	public String getModelName(String modelId) {
		ModelMetadata mmd = modelmapper.get(modelId);
		return mmd != null ? mmd.name() : modelId;
	}

	/**
	 * retrieve a sorted list of models as list of modelmetadata instances
	 *
	 * @return
	 */
	public List<ModelMetadata> getModels() {
		return modelmapper.values().stream().distinct().sorted((a, b) -> a.id().compareTo(b.id()))
				.collect(Collectors.toUnmodifiableList());
	}

	/**
	 * get all available models in a map of short key and display model name, sorted ABC by display model name
	 *
	 * @return
	 */
	public List<Tuple<String, String>> getModelNames() {
		return modelmapper.values().stream().distinct().map(e -> new Tuple<>(e.id(), e.name() + " " + e.type()))
				.sorted((a, b) -> a._2.compareToIgnoreCase(b._2)).collect(Collectors.toList());
	}

	/**
	 * update model mapper from JSON
	 *
	 * @param modelJson
	 */
	public void updateModels(String modelJson) {
		// parse and check if it's an array
		if (modelJson == null || modelJson.isEmpty()) {
			logger.warn("⚠️ Model update failed, JSON empty. Clearing models.");
			clearModels();
			return;
		}

		List<ModelMetadata> models = json2ModelList(modelJson);
		if (models.isEmpty()) {
			logger.warn("⚠️ No models found in JSON. Clearing models.");
			clearModels();
			return;
		}

		// then extract model meta data into a new map
		Map<String, ModelMetadata> newModelMapper = new ConcurrentHashMap<>();
		models.stream().forEach(m -> {
			// first model
			{
				String key = m.id();
				newModelMapper.put(key, m);
			}
			// then alias
			{
				if (m.alias() != null) {
					m.alias().stream().forEach(key -> {
						newModelMapper.put(key, m);
					});
				}
			}
		});

		// Atomically replace the map
		this.modelmapper = newModelMapper;
//		logger.info("Successfully synced 🧪" + models.size() + " models.");
	}

	/**
	 * convert JSON String to list of ModelMetaData objects
	 * 
	 * @param modelJson
	 * @return
	 */
	private List<ModelMetadata> json2ModelList(String modelJson) {
		final ObjectMapper objectMapper = new ObjectMapper();

		List<ModelMetadata> list = null;
		try {
			list = objectMapper.readValue(modelJson, new TypeReference<List<ModelMetadata>>() {
			});
		} catch (Exception e) {
			logger.error("❌ Model update failed, JSON invalid.");
		}
		return list != null ? list : List.of();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@JsonIgnoreProperties(ignoreUnknown = true)
	@JsonInclude(JsonInclude.Include.NON_NULL)
	public record ModelMetadata( //
			String id, //
			String name, //
			String type, //
			String description, //
			String link, //
			String comment, //
			List<String> tags, //
			List<String> alias //
	) {

		/**
		 * compute a hash value for the entire record, so we can compare
		 * 
		 * @return
		 */
		public final int hashValue() {
			String tagsStr = (tags == null) ? "" : tags.stream().collect(Collectors.joining("-"));
			String aliasStr = (alias == null) ? "" : alias.stream().collect(Collectors.joining("-"));
			String stringRepresentation = id + name + type + description + link + comment + tagsStr + aliasStr;
			return stringRepresentation.hashCode();
		}

	}

}

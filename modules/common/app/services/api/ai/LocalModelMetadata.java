package services.api.ai;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser.Feature;
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
	@SuppressWarnings("deprecation")
	private List<ModelMetadata> json2ModelList(String modelJson) {
		final ObjectMapper objectMapper = new ObjectMapper();
		// Configure Jackson to tolerate trailing commas, comments, etc.
		objectMapper.configure(Feature.ALLOW_TRAILING_COMMA, true);
		objectMapper.configure(Feature.ALLOW_SINGLE_QUOTES, true);
		objectMapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		objectMapper.configure(Feature.ALLOW_COMMENTS, true);

		try {
			com.fasterxml.jackson.databind.JsonNode rootNode = objectMapper.readTree(modelJson);
			if (rootNode.isArray()) {
				// Existing format: JSON Array of ModelMetadata objects
				return objectMapper.readValue(modelJson, new TypeReference<List<ModelMetadata>>() {
				});
			} else if (rootNode.isObject()) {
				// LiteLLM / OpenAI format: JSON Object with a "data" array
				com.fasterxml.jackson.databind.JsonNode dataNode = rootNode.get("data");
				if (dataNode != null && dataNode.isArray()) {
					java.util.ArrayList<ModelMetadata> list = new java.util.ArrayList<>();
					for (com.fasterxml.jackson.databind.JsonNode item : dataNode) {
						String id = null;
						String name = null;
						String type = null;
						String description = null;
						String link = null;
						String comment = null;
						List<String> tags = null;
						List<String> alias = null;

						if (item.has("id")) {
							id = item.get("id").asText();
							name = item.has("name") ? item.get("name").asText() : id;
							type = item.has("type") ? item.get("type").asText() : null;
							description = item.has("description") ? item.get("description").asText() : null;
							link = item.has("link") ? item.get("link").asText() : null;
							comment = item.has("comment") ? item.get("comment").asText() : null;
							if (item.has("tags") && item.get("tags").isArray()) {
								tags = objectMapper.convertValue(item.get("tags"), new TypeReference<List<String>>() {
								});
							}
							if (item.has("alias") && item.get("alias").isArray()) {
								alias = objectMapper.convertValue(item.get("alias"), new TypeReference<List<String>>() {
								});
							}
						} else if (item.has("model_name")) {
							// LiteLLM model/info format
							id = item.get("model_name").asText();
							name = id;

							java.util.ArrayList<String> aliasList = new java.util.ArrayList<>();
							if (item.has("model_info")) {
								com.fasterxml.jackson.databind.JsonNode info = item.get("model_info");
								if (info.has("id")) {
									String infoId = info.get("id").asText();
									if (!infoId.equals(id)) {
										aliasList.add(infoId);
									}
								}
								if (info.has("name")) {
									name = info.get("name").asText();
								} else if (info.has("model_name")) {
									name = info.get("model_name").asText();
								}
								if (info.has("type")) {
									type = info.get("type").asText();
								}
								if (info.has("description")) {
									description = info.get("description").asText();
								}
								if (info.has("link")) {
									link = info.get("link").asText();
								}
								if (info.has("comment")) {
									comment = info.get("comment").asText();
								}
								if (info.has("tags") && info.get("tags").isArray()) {
									tags = objectMapper.convertValue(info.get("tags"),
											new TypeReference<List<String>>() {
											});
								}
								if (info.has("alias") && info.get("alias").isArray()) {
									List<String> jsonAliases = objectMapper.convertValue(info.get("alias"),
											new TypeReference<List<String>>() {
											});
									aliasList.addAll(jsonAliases);
								}
							}
							if (!aliasList.isEmpty()) {
								alias = aliasList;
							}
						}

						if (id != null) {
							list.add(new ModelMetadata(id, name, type, description, link, comment, tags, alias));
						}
					}
					return list;
				}
			}
		} catch (Exception e) {
			logger.error("❌ Model update failed, JSON invalid: " + e.getMessage());
		}
		return List.of();
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

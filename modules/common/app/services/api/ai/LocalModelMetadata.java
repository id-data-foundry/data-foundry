package services.api.ai;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.inject.Singleton;
import play.Logger;
import play.libs.F.Tuple;

@Singleton
public class LocalModelMetadata {

	private static final Logger.ALogger logger = Logger.of(LocalModelMetadata.class);

	private Map<String, ModelMetadata> modelmapper = new HashMap<>();

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
			logger.error("❌ Model update failed, JSON empty.");
			return;
		}

		// then extract model meta data
		json2ModelList(modelJson).stream().forEach(m -> {
			// first model
			{
				String key = m.id();
				if (modelmapper.containsKey(key)) {
					ModelMetadata existingMM = modelmapper.get(key);
					if (existingMM.hashValue() != m.hashValue()) {
						modelmapper.put(key, m);
						logger.info("Replaced model 🧪" + key);
					} else {
//						logger.trace("Same model 🧪" + key);
					}
				} else {
					modelmapper.put(key, m);
					logger.info("Added model 🧪" + key);
				}
			}
			// then alias
			{
				if (m.alias() != null) {
					m.alias().stream().forEach(key -> {
						if (modelmapper.containsKey(key)) {
							ModelMetadata existingMM = modelmapper.get(key);
							if (existingMM.hashValue() != m.hashValue()) {
								modelmapper.put(key, m);
								logger.info("Replaced alias 🏷 ️" + key);
							} else {
//								logger.trace("Same alias 🏷 ️" + key);
							}
						} else {
							modelmapper.put(key, m);
							logger.info("Added alias 🏷️ " + key);
						}
					});
				}
			}
		});
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
		} catch (JsonProcessingException e) {
			logger.error("❌ Model update failed, JSON invalid.", e);
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

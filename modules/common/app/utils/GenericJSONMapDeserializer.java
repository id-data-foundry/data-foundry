package utils;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class GenericJSONMapDeserializer {

	public static Map<String, Object> deserialize(JsonNode root) {
		return internalDeserialize(root);
	}

	private static Map<String, Object> internalDeserialize(JsonNode root) {
		Map<String, Object> om = new HashMap<String, Object>();

		if (root != null && root.isObject()) {
			root.fields().forEachRemaining(e -> {
				JsonNode node = e.getValue();
				if (node.isNumber()) {
					om.put(e.getKey(), e.getValue().asDouble());
				} else if (node.isBoolean()) {
					om.put(e.getKey(), e.getValue().asBoolean());
				} else if (node.isTextual()) {
					om.put(e.getKey(), e.getValue().asText());
				} else if (node.isObject()) {
					// recursive descent into the json object
					om.put(e.getKey(), deserialize(e.getValue()));
				} else if (node.isArray()) {
					// recursive descent into the json array
					om.put(e.getKey(), deserializeArray((ArrayNode) node));
				} else {
					om.put(e.getKey(), e.getValue().toString());
				}
			});
		}

		return om;
	}

	private static Object deserializeArray(ArrayNode jsonArray) {
		final Object result;
		// array is non-empty and contains values
		if (jsonArray != null && jsonArray.size() > 0) {
			if (jsonArray.get(0).isValueNode()) {
				// boolean
				if (jsonArray.get(0).isBoolean()) {
					boolean[] array = new boolean[jsonArray.size()];
					for (int i = 0; i < array.length; i++) {
						JsonNode jsonElement = jsonArray.get(i);
						if (jsonElement.isBoolean()) {
							array[i] = jsonElement.asBoolean();
						}
					}
					result = array;
				}
				// number
				else if (jsonArray.get(0).isNumber()) {
					double[] array = new double[jsonArray.size()];
					for (int i = 0; i < array.length; i++) {
						JsonNode jsonElement = jsonArray.get(i);
						if (jsonElement.isNumber()) {
							array[i] = jsonElement.asDouble();
						}
					}
					result = array;
				}
				// String
				else if (jsonArray.get(0).isTextual()) {
					String[] array = new String[jsonArray.size()];
					for (int i = 0; i < array.length; i++) {
						JsonNode jsonElement = jsonArray.get(i);
						if (jsonElement.isTextual()) {
							array[i] = jsonElement.asText("");
						}
					}
					result = array;
				} else {
					result = new Object();
				}
			} else if (jsonArray.get(0).isObject()) {
				// object
				Object[] array = new Object[jsonArray.size()];
				for (int i = 0; i < array.length; i++) {
					JsonNode jsonElement = jsonArray.get(i);
					if (jsonElement.isObject()) {
						array[i] = deserialize(jsonElement);
					}
				}
				result = array;
			} else {
				result = new Object();
			}
		} else {
			result = new Object();
		}

		return result;
	}
}

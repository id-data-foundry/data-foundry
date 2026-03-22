package utils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.lang3.math.NumberUtils;

import com.fasterxml.jackson.databind.node.ObjectNode;

import play.libs.Json;

public class DataUtils {

	/**
	 * parse a Boolean value from String, with default "false"
	 * 
	 * @param valueStr
	 * @return
	 */
	public static boolean parseBoolean(String valueStr) {
		return parseBoolean(valueStr, false);
	}

	/**
	 * parse a Boolean value from String, with given default value
	 * 
	 * @param valueStr
	 * @param defaultValue
	 * @return
	 */
	public static boolean parseBoolean(String valueStr, boolean defaultValue) {
		try {
			return Boolean.parseBoolean(valueStr.trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * parse int value from String, with default -1
	 * 
	 * @param valueStr
	 * @return
	 */
	public static int parseInt(String valueStr) {
		return parseInt(valueStr, -1);
	}

	/**
	 * parse int value from String, with given default value
	 * 
	 * @param valueStr
	 * @param defaultValue
	 * @return
	 */
	public static int parseInt(String valueStr, int defaultValue) {
		return NumberUtils.toInt(valueStr, defaultValue);
	}

	/**
	 * parse long value from String, with default -1
	 * 
	 * @param valueStr
	 * @return
	 */
	public static long parseLong(String valueStr) {
		return parseLong(valueStr, -1);
	}

	/**
	 * parse long value from String, with given default value
	 * 
	 * @param valueStr
	 * @param defaultValue
	 * @return
	 */
	public static long parseLong(String valueStr, long defaultValue) {
		return NumberUtils.toLong(valueStr, defaultValue);
	}

	/**
	 * parse float value from String, with default -1
	 * 
	 * @param valueStr
	 * @return
	 */
	public static float parseFloat(String valueStr) {
		return parseFloat(valueStr, -1f);
	}

	/**
	 * parse float value from String, with given default value
	 * 
	 * @param valueStr
	 * @param defaultValue
	 * @return
	 */
	public static float parseFloat(String valueStr, float defaultValue) {
		return NumberUtils.toFloat(valueStr, defaultValue);
	}

	/**
	 * parse double value from String, with default -1
	 * 
	 * @param valueStr
	 * @return
	 */
	public static double parseDouble(String valueStr) {
		return parseDouble(valueStr, -1d);
	}

	/**
	 * parse double value from String, with given default value
	 * 
	 * @param valueStr
	 * @param defaultValue
	 * @return
	 */
	public static double parseDouble(String valueStr, double defaultValue) {
		return NumberUtils.toDouble(valueStr, defaultValue);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * extract the file name from a relative path or URL
	 * 
	 * @param path
	 * @return
	 */
	public static String extractFileNameFromPath(String path) {
		final int lastIndexOf = path.lastIndexOf("/");
		final String fileName;
		if (lastIndexOf == -1) {
			fileName = path.trim();
		} else if (path.length() > lastIndexOf) {
			fileName = path.substring(lastIndexOf + 1);
		} else {
			fileName = path;
		}
		return fileName;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static ObjectNode toJson(Object obj) {
		ObjectNode on = Json.newObject();
		try {
			Field[] fields = obj.getClass().getFields();
			for (Field field : fields) {
				if (!Modifier.isFinal(field.getModifiers())
						&& (field.getType().isPrimitive() || field.getType().equals(String.class))) {
					on.putPOJO(field.getName(), field.get(obj));
				}
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

		return on;
	}

}

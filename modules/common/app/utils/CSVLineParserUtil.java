package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.sisyphsu.dateparser.DateParser;

public class CSVLineParserUtil {

	private final Map<String, Integer> headerIndex = new HashMap<String, Integer>();
	public final List<String> headerData = new ArrayList<String>();
	private SimpleDateFormat sdf;
	private DateParser parser;

	public CSVLineParserUtil() {
		parser = DateParser.newBuilder().build();
	}

	public void setTimestampPattern(String pattern) {
		sdf = new SimpleDateFormat(pattern);
		parser.setPreferMonthFirst(false);
	}

	public Date parseTimestamp(String timestamp, Date defaultTimestamp) throws ParseException {
		if (sdf != null) {
			try {
				return sdf.parse(timestamp);
			} catch (ParseException e) {
				if (defaultTimestamp != null) {
					return defaultTimestamp;
				} else {
					throw e;
				}
			}
		} else {
			return parser.parseDate(timestamp);
		}
	}

	public void putIndex(String attribute, int index) {
		if (!headerIndex.containsKey(attribute)) {
			headerIndex.put(attribute, index);
		} else {
			headerIndex.put("data", index);
			putDataColumn(attribute);
		}
	}

	public void putDataColumn(String attribute) {
		headerData.add(attribute.trim());
	}

	public String extract(String[] line, String attribute) {
		attribute = attribute.trim();
		if (headerIndex.containsKey(attribute)) {
			int index = headerIndex.get(attribute);
			if (index >= 0 && index < line.length) {
				return line[index].trim();
			}
		}

		return "";
	}
}

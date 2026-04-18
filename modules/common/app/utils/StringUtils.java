package utils;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Collectors;

import com.google.common.html.HtmlEscapers;

public class StringUtils {

	public static final String VERSION = "0.9.18";

	/**
	 * null safe string
	 */
	static public final String nss(String s) {
		return s != null ? s : "";
	}

	/**
	 * null safe string with max length
	 */
	static public final String nss(String s, int maxLength) {
		String result = s != null ? s : "";
		return result.length() > maxLength ? result.substring(0, maxLength) : result;
	}

	/**
	 * return a pluralized string if number != 1
	 * 
	 * @param s
	 * @param number
	 * @return
	 */
	static public final String pluralize(String s, int number) {
		return number + " " + (number == 1 ? s : s + "s");
	}

	/**
	 * escape HTML meta characters in String
	 * 
	 * @param s
	 * @return
	 */
	static public final String htmlEscape(String s) {
		return HtmlEscapers.htmlEscaper().escape(s);
	}

	/**
	 * valid text = Not Null and Not Empty
	 */
	static public final boolean nnne(String text) {
		return text != null && text.length() > 0;
	}

	/**
	 * format / escape string for CSV output
	 */
	static public final String cf(String s) {
		return s != null ? "\"" + s + "\"" : "\"\"";
	}

	/**
	 * encode a string for use in a URL
	 * 
	 * @param s
	 * @return
	 */
	static public final String s2url(String s) {
		return URLEncoder.encode(s, StandardCharsets.UTF_8);
	}

	/**
	 * decode a string from a URL
	 * 
	 * @param s
	 * @return
	 */
	static public final String url2s(String url) {
		return URLDecoder.decode(url, StandardCharsets.UTF_8);
	}

	/**
	 * js block to scramble a string for transport; counterpart to unscrambleTransport
	 * 
	 * @param contentVariable
	 * @return
	 */
	static public final String scrambleTransport(String contentVariable) {
		return """
				{
					const src = %s;
				    const key = 3;
				    let scrambled = "";
				    for (let i = 0; i < src.length; i++) {
				    	scrambled += String.fromCharCode(src.charCodeAt(i) ^ key);
					}
					%s = "TRANSPORT_SCRAMBLE__" + btoa(unescape(encodeURIComponent(scrambled)))
				}""".formatted(contentVariable, contentVariable);
	}

	/**
	 * unscramble transport string
	 * 
	 * @param scrambled
	 * @return
	 */
	static public final String unscrambleTransport(String scrambled) {
		final int key = 3;
		final String prefix = "TRANSPORT_SCRAMBLE__";
		return scrambled.startsWith(prefix)
				? new String(Base64.getDecoder().decode(scrambled.substring(prefix.length()))).chars().map(c -> c ^ key)
						.mapToObj(c -> String.valueOf((char) c)).collect(Collectors.joining())
				: scrambled;
	}

}

package utils.rendering;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Utility for providing and injecting the in-memory storage polyfill and console
 * postMessage bridge into user-hosted dataset web pages.
 */
public class PlaygroundBridgeUtil {

	public static final long MAX_BRIDGE_FILE_SIZE = 5 * 1024 * 1024L;

	public static final String BRIDGE_SCRIPT = """
			<script>
			(function() {
			  function createStore() {
			    var store = {};
			    var api = {
			      getItem: function(k) { return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
			      setItem: function(k, v) { store[k] = String(v); },
			      removeItem: function(k) { delete store[k]; },
			      clear: function() { for (var k in store) if (Object.prototype.hasOwnProperty.call(store, k)) delete store[k]; },
			      key: function(i) { return Object.keys(store)[i] || null; },
			      get length() { return Object.keys(store).length; }
			    };
			    if (typeof Proxy !== 'undefined') {
			      return new Proxy(api, {
			        get: function(target, prop) {
			          if (typeof prop === 'string' && !(prop in target)) {
			            return target.getItem(prop);
			          }
			          return target[prop];
			        },
			        set: function(target, prop, value) {
			          if (typeof prop === 'string' && !(prop in target)) {
			            target.setItem(prop, value);
			            return true;
			          }
			          target[prop] = value;
			          return true;
			        },
			        deleteProperty: function(target, prop) {
			          if (typeof prop === 'string' && !(prop in target)) {
			            target.removeItem(prop);
			            return true;
			          }
			          delete target[prop];
			          return true;
			        }
			      });
			    }
			    return api;
			  }
			  try { window.localStorage.getItem('__test'); } catch (e) {
			    try { Object.defineProperty(window, 'localStorage', { value: createStore(), configurable: true, writable: true }); } catch (ex) {}
			  }
			  try { window.sessionStorage.getItem('__test'); } catch (e) {
			    try { Object.defineProperty(window, 'sessionStorage', { value: createStore(), configurable: true, writable: true }); } catch (ex) {}
			  }
			  if (window.parent && window.parent !== window) {
			    function fwd(t, a) {
			      try {
			        var m = Array.prototype.slice.call(a).map(function(item) {
			          return (typeof item === 'object') ? JSON.stringify(item) : String(item);
			        }).join(' ');
			        window.parent.postMessage({ source: 'df-playground-console', type: t, message: m }, '*');
			      } catch (err) {}
			    }
			    var origLog = console.log, origErr = console.error;
			    console.log = function() { fwd('log', arguments); origLog.apply(console, arguments); };
			    console.error = function() { fwd('error', arguments); origErr.apply(console, arguments); };
			    window.onerror = function(msg, url, line, col) {
			      fwd('error', [msg + ' at ' + (url ? url.substring(url.lastIndexOf('/')) : '') + ' line ' + line + ':' + col]);
			    };
			  }
			})();
			</script>
			""";

	/**
	 * Returns the raw bridge script including script tags.
	 *
	 * @return bridge script string
	 */
	public static String getBridgeScript() {
		return BRIDGE_SCRIPT;
	}

	/**
	 * Reads an HTML file up to MAX_BRIDGE_FILE_SIZE and injects the bridge script.
	 *
	 * @param file the HTML file to read
	 * @return bridged HTML string or null if file is missing, exceeds size, or cannot be read
	 */
	public static String injectHtmlBridge(File file) {
		if (file == null || !file.exists() || !file.isFile() || file.length() > MAX_BRIDGE_FILE_SIZE) {
			return null;
		}
		try {
			String html = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
			return injectHtmlBridge(html);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Injects the bridge script into the provided HTML string.
	 * If &lt;head...&gt; is present (case-insensitive, with optional attributes), the script is injected immediately after &lt;head...&gt;.
	 * Otherwise, the script is prepended to the document.
	 *
	 * @param html original HTML
	 * @return bridged HTML string or null if html is null
	 */
	public static String injectHtmlBridge(String html) {
		if (html == null) {
			return null;
		}
		java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(?i)<head[^>]*>").matcher(html);
		if (matcher.find()) {
			int insertIdx = matcher.end();
			return html.substring(0, insertIdx) + "\n" + BRIDGE_SCRIPT + html.substring(insertIdx);
		}
		return BRIDGE_SCRIPT + html;
	}
}

package controllers.tools;

import java.io.File;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import controllers.auth.UserAuth;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import play.Logger;
import play.mvc.Controller;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.tools.QRCodeUtil;

@Singleton
@Authenticated(UserAuth.class)
public class QRCode extends Controller {

	@Inject
	QRCodeUtil qrCodeUtil;

	private static final Logger.ALogger logger = Logger.of(QRCode.class);

	/**
	 * QR code configuration view
	 * 
	 * @return
	 */
	public Result index() {
		return ok(views.html.tools.qrcode.index.render());
	}

	/**
	 * generate QR code; note: this is only visible for authenticated users to prevent misuse
	 *
	 * @param request
	 * @param key
	 * @param url
	 * @return
	 */
	public Result qrCode(Request request, String key, String url) {

		// check attributes and append them if available
		String queryString = request.queryString().entrySet().stream().filter(e -> e.getValue().length == 1)
				.map(e -> e.getKey() + "=" + e.getValue()[0]).collect(Collectors.joining("&"));
		url += queryString.isEmpty() ? "" : ("?" + queryString);

		// ensure URLs have the right protocol
		if (Pattern.compile("^https:/(?!/).*").matcher(url).matches()) {
			logger.warn("Broken URL submitted to QRCode gen: " + url);
			url = url.replace("https:/", "https://");
		} else if (Pattern.compile("^http:/(?!/).*").matcher(url).matches()) {
			logger.warn("Broken URL submitted to QRCode gen: " + url);
			url = url.replace("http:/", "http://");
		}

		// generate caching key (strictly namespaced to prevent Play cache poisoning)
		String keyStr = (key != null && key.trim().length() >= 6) ? key.trim()
				: com.google.common.hash.Hashing.sha256().hashString(url, java.nio.charset.StandardCharsets.UTF_8)
						.toString();
		String cachingKey = "cached_qrcode_" + keyStr;

		// generate QR code and cache for 5 minutes
		String pathStr = qrCodeUtil.generateCachedQRCode(url, cachingKey, 300);
		return pathStr != null && !pathStr.isEmpty() ? ok(new File(pathStr)).as("image/png") : notFound();
	}

}

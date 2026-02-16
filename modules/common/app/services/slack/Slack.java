package services.slack;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.inject.Singleton;
import com.typesafe.config.ConfigFactory;

import play.Logger;
import services.slack.api.SlackApi;
import services.slack.api.SlackMessage;
import utils.conf.ConfigurationUtils;

@Singleton
public class Slack {

	private static final Logger.ALogger logger = Logger.of(Slack.class);

	private static SlackApi sa;
	private static String hostname;

	static {
		try {
			String slackChannel = ConfigFactory.load().getString(ConfigurationUtils.DF_VENDOR_SLACK_CHANNEL);
			sa = new SlackApi(slackChannel);
		} catch (Exception e) {
			logger.error("Slack unavailable");
			sa = null;
		}

		// determine the hostname
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			logger.error("hostname unavailable");
			hostname = "<server unknown>";
		}
	}

	public static void call(String title, String message) {
		title = title != null ? title : "";
		message = message != null ? message : "";

		logger.info(hostname + ": [" + title + "] " + message);
		if (sa != null) {
			try {
				sa.call(new SlackMessage(hostname + ": " + title, message));
			} catch (Exception e) {
				logger.error("Slack unavailable");
			}
		}
	}
}

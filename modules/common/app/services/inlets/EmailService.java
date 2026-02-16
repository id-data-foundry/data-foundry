package services.inlets;

import java.util.Properties;

import javax.inject.Inject;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Session;
import javax.mail.Store;

import com.google.inject.Singleton;
import com.typesafe.config.Config;

import play.Logger;
import services.slack.Slack;
import utils.conf.ConfigurationUtils;

@Singleton
public class EmailService implements ScheduledService {

	private static final Logger.ALogger logger = Logger.of(EmailService.class);

	private final String mailhost;
	private final String username;
	private final String password;

	@Inject
	public EmailService(Config config) {
		if (config.hasPath(ConfigurationUtils.DF_MAIL_HOST) && config.hasPath(ConfigurationUtils.DF_MAIL_USERNAME)
		        && config.hasPath(ConfigurationUtils.DF_MAIL_PASSWORD)) {
			mailhost = config.getString(ConfigurationUtils.DF_MAIL_HOST);
			username = config.getString(ConfigurationUtils.DF_MAIL_USERNAME);
			password = config.getString(ConfigurationUtils.DF_MAIL_PASSWORD);
		} else {
			mailhost = null;
			username = null;
			password = null;
		}
	}

	@Override
	public void refresh() {
		try {
			if (mailhost != null && username != null && password != null) {
				// Calling checkMail method to check received emails
				checkMail(mailhost, username, password);
			}
		} catch (Exception e) {
			// do nothing
		}
	}

	private void checkMail(String hostval, String uname, String pwd) {
		try {
			// Set property values
			Properties propvals = new Properties();
			propvals.put("mail.pop3.host", hostval);
			propvals.put("mail.pop3.port", "995");
			propvals.put("mail.pop3.starttls.enable", "true");
			Session emailSessionObj = Session.getDefaultInstance(propvals);

			// Create POP3 store object and connect with the server
			Store storeObj = emailSessionObj.getStore("pop3s");
			storeObj.connect(hostval, uname, pwd);

			// Create folder object and open it in read-only mode
			Folder emailFolderObj = storeObj.getFolder("INBOX");
			emailFolderObj.open(Folder.READ_ONLY);

			// Fetch messages from the folder and print in a loop
			Message[] messageobjs = emailFolderObj.getMessages();

			for (int i = 0, n = messageobjs.length; i < n; i++) {
				Message individualmsg = messageobjs[i];
				logger.trace("Printing individual messages");
				logger.trace("No# " + (i + 1));
				logger.trace("Email Subject: " + individualmsg.getSubject());
				logger.trace("Sender: " + individualmsg.getFrom()[0]);
				logger.trace("Content: " + individualmsg.getContent().toString());
			}
			// Now close all the objects
			emailFolderObj.close(false);
			storeObj.close();
		} catch (NoSuchProviderException e) {
			logger.error("Email sending problem: provider", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (MessagingException e) {
			logger.error("Email sending problem: message", e);
			Slack.call("Exception", e.getLocalizedMessage());
		} catch (Exception e) {
			logger.error("Email sending problem: general", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}
	}

	@Override
	public void stop() {
	}
}

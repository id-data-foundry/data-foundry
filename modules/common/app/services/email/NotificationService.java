package services.email;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.apache.pekko.actor.ActorSystem;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.microsoft.graph.authentication.TokenCredentialAuthProvider;
import com.microsoft.graph.models.BodyType;
import com.microsoft.graph.models.EmailAddress;
import com.microsoft.graph.models.ItemBody;
import com.microsoft.graph.models.Message;
import com.microsoft.graph.models.Recipient;
import com.microsoft.graph.models.UserSendMailParameterSet;
import com.microsoft.graph.requests.GraphServiceClient;
import com.typesafe.config.Config;

import play.Logger;
import play.libs.mailer.Email;
import play.libs.mailer.MailerClient;
import play.twirl.api.Html;
import scala.concurrent.duration.Duration;
import services.slack.Slack;
import utils.conf.ConfigurationUtils;

@Singleton
public class NotificationService {

	private final ActorSystem system;

	private final MailerClient javaMailClient;
	private final GraphServiceClient<?> msGraphClient;
	private final String mailFrom;

	private static final Logger.ALogger logger = Logger.of(NotificationService.class);

	@Inject
	public NotificationService(final Config config, MailerClient mailerClient, ActorSystem system) {
		this.system = system;

		// check config for MS Graph configuration
		if (ConfigurationUtils.checkConfiguration(config, ConfigurationUtils.DF_MSGRAPH_FROM,
		        ConfigurationUtils.DF_MSGRAPH_CLIENT, ConfigurationUtils.DF_MSGRAPH_SECRET,
		        ConfigurationUtils.DF_MSGRAPH_TENANT)) {
			this.mailFrom = config.getString(ConfigurationUtils.DF_MSGRAPH_FROM);
			ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
			        .clientId(config.getString(ConfigurationUtils.DF_MSGRAPH_CLIENT))
			        .clientSecret(config.getString(ConfigurationUtils.DF_MSGRAPH_SECRET))
			        .tenantId(config.getString(ConfigurationUtils.DF_MSGRAPH_TENANT)).build();
			TokenCredentialAuthProvider tokenCredentialAuthProvider = new TokenCredentialAuthProvider(
			        Collections.singletonList("https://graph.microsoft.com/.default"), clientSecretCredential);
			this.msGraphClient = GraphServiceClient.builder().authenticationProvider(tokenCredentialAuthProvider)
			        .buildClient();

			// disable Java Mail
			this.javaMailClient = null;

			logger.info("Email configuration: MS Graph via " + this.mailFrom);
		}
		// check config for Java Mail configuration
		else if (ConfigurationUtils.checkConfiguration(config, ConfigurationUtils.DF_MAIL_FROM)) {
			this.mailFrom = config.getString(ConfigurationUtils.DF_MAIL_FROM);
			this.javaMailClient = mailerClient;

			// disable MS Graph mail
			this.msGraphClient = null;

			logger.info("Email configuration: Java Mail via " + this.mailFrom);
		} else {

			// disable both clients
			this.mailFrom = null;
			this.javaMailClient = null;
			this.msGraphClient = null;

			logger.info("Email configuration: NONE");
		}
	}

	public void sendMail(String receiver, String textBody, Html htmlBody, String actionLink, String subject,
	        String replyTo, String senderName, String consoleOutput) {
		internalSendMailDispatch(receiver, textBody, htmlBody.body(), actionLink, subject, replyTo, senderName,
		        consoleOutput);
	}

	public void sendMail(String receiver, String textBody, Html htmlBody, String actionLink, String subject,
	        String consoleOutput) {
		internalSendMailDispatch(receiver, textBody, htmlBody.body(), actionLink, subject, null, "ID Data Foundry",
		        consoleOutput);
	}

	/**
	 * dispatch mail sending (or tmp storing) via a different thread
	 * 
	 * @param receiver
	 * @param textBody
	 * @param htmlBody
	 * @param actionLink
	 * @param subject
	 * @param replyTo
	 * @param senderName
	 * @param consoleOutput
	 */
	private void internalSendMailDispatch(String receiver, String textBody, String htmlBody, String actionLink,
	        String subject, String replyTo, String senderName, String consoleOutput) {
		// schedule sending the email from a different thread
		system.scheduler().scheduleOnce(Duration.create(0, TimeUnit.MILLISECONDS), () -> {
			// Java Mail is configuration -> send email there
			if (javaMailClient != null) {
				mailWithJavaMail(receiver, textBody, htmlBody, subject, replyTo, senderName, consoleOutput);
			}
			// MS Graph is configuration -> send email there
			else if (msGraphClient != null) {
				mailWithMSGraphClient(receiver, textBody, htmlBody, subject, replyTo, senderName, consoleOutput);
			}
			// no email sender configured -> store email in tmp folder
			else {
				mailIntoTempFolder(receiver, textBody, htmlBody, subject, senderName);
			}
		}, system.dispatcher());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private void mailWithJavaMail(String receiver, String textBody, String htmlBody, String subject, String replyTo,
	        String senderName, String consoleOutput) {
		try {
			logger.info(String.format("Emailing (%s) from (%s) with subject (%s)", receiver, this.mailFrom, subject));
			Email email = new Email();
			email.setSubject(subject);
			if (replyTo != null) {
				email.setReplyTo(Arrays.asList(replyTo));
			}
			email.setFrom(mailFrom);
			email.addTo(receiver);
			email.setBodyText(textBody);
			email.setBodyHtml(htmlBody);
			javaMailClient.send(email);

			// log the email if successful
			logger.info(
			        String.format("[EMAIL] from (%s) to (%s) '%s': %s", senderName, receiver, subject, consoleOutput));
			Slack.call("Email sent out", String.format("From %s to %s", senderName, receiver));
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
	}

	private void mailWithMSGraphClient(String receiver, String textBody, String htmlBody, String subject,
	        String replyTo, String senderName, String consoleOutput) {
		try {
			Message message = new Message();
			message.subject = subject;
			ItemBody body = new ItemBody();
			body.contentType = BodyType.HTML;
			body.content = htmlBody;
			message.body = body;
			LinkedList<Recipient> toRecipientsList = new LinkedList<Recipient>();
			Recipient toRecipients = new Recipient();
			EmailAddress emailAddress = new EmailAddress();
			emailAddress.address = receiver;
			toRecipients.emailAddress = emailAddress;
			toRecipientsList.add(toRecipients);
			message.toRecipients = toRecipientsList;
			msGraphClient.users(this.mailFrom).sendMail(
			        UserSendMailParameterSet.newBuilder().withMessage(message).withSaveToSentItems(false).build())
			        .buildRequest().post();

			// log the email if successful
			logger.info(
			        String.format("[EMAIL] from (%s) to (%s) '%s': %s", senderName, receiver, subject, consoleOutput));
			Slack.call("Email sent out", String.format("From %s to %s", senderName, receiver));
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
	}

	private void mailIntoTempFolder(String receiver, String textBody, String htmlBody, String subject,
	        String senderName) {
		new File("tmp").mkdir();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_hhmm");
		File tf = new File("tmp", sdf.format(new Date()) + "renderedEmail_" + receiver + ".html");
		FileWriter fw;
		try {
			fw = new FileWriter(tf);
			fw.append(htmlBody);
			fw.close();
		} catch (IOException e) {
			logger.error("Error in storing email on tmp.", e);
		}

		// log the failed attempt
		logger.error(String.format("[EMAIL] [FAIL] from (%s) to (%s) '%s'", senderName, receiver, subject));
	}
}

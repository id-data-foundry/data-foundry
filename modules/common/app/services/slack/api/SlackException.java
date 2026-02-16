package services.slack.api;

@SuppressWarnings("serial")
public class SlackException extends RuntimeException {

	public SlackException(Throwable cause) {
		super(cause);
	}
}

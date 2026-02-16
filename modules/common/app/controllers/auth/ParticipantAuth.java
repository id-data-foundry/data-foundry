package controllers.auth;

import java.util.Optional;

import play.mvc.Http.Request;
import play.mvc.Result;

/**
 * Specific authentication for participants in a project
 * 
 * @author mathias
 *
 */
public class ParticipantAuth extends play.mvc.Security.Authenticator {

	public static final String PARTICIPANT_ID = "participant_id";

	@Override
	public Optional<String> getUsername(Request request) {
		Optional<String> participantId = request.session().get(PARTICIPANT_ID);
		return participantId.isPresent() && participantId.get().length() > 0 ? participantId : null;
	}

	@Override
	public Result onUnauthorized(Request req) {
		return redirect("/logout");
	}
}

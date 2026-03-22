package controllers.swagger;

import java.util.Optional;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.auth.V2UserApiAuth;
import datasets.DatasetConnector;
import jakarta.inject.Singleton;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.sr.Participant;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.data.validation.Constraints.EmailValidator;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import play.twirl.api.Html;
import services.email.NotificationService;
import utils.DataUtils;
import utils.auth.TokenResolverUtil;

@Singleton
public class ParticipantApiController extends AbstractApiController {

	private final NotificationService notificationService;

	@Inject
	public ParticipantApiController(FormFactory formFactory, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolverUtil, NotificationService notificationService) {
		super(formFactory, datasetConnector, tokenResolverUtil);

		this.notificationService = notificationService;
	}

	@Authenticated(V2UserApiAuth.class)
	public Result participants(Request request) {

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return redirect(controllers.routes.HomeController.logout());
		}

		ObjectNode on = okJSONResponseObject();
		ObjectNode data = on.putObject("data");
		{
			ArrayNode participantNode = data.putArray("participants");
			user.projects().stream().forEach(p -> {
				p.getParticipants().stream().forEach(pt -> participantNode.add(pt.getId()));
			});
		}

		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result participant(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether participant exists
		Participant pt = Participant.find.byId(id);
		if (pt == null) {
			return notFound(errorJSONResponseObject("Not valid participant."));
		}

		// check the relation of the participant and the researcher
		if (!pt.getProject().belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and participant mismatch."));
		}

		return ok(toJSON(pt));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result add(Request request) {

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("Not valid user."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		// find project
		Project p = null;
		try {
			String projectId = df.get("project_id");
			if (!nnne(projectId)) {
				// does user have a project at all? if one, then automatically add to that one
				if (user.projects().size() == 1) {
					projectId = "" + user.projects().get(0).getId();
				}
				// does user have a project at all? if two, then find the one that matches the public switch
				else if (user.projects().size() == 2) {
					boolean searchFlag = df.get("isPublic") != null;
					Optional<Project> op = user.projects().stream().filter(up -> up.isPublicProject() == searchFlag)
							.findFirst();
					// choose project
					if (op.isPresent()) {
						projectId = "" + user.projects().get(0).getId();
					} else {
						return badRequest(errorJSONResponseObject("Wrong format of project ID."));
					}
				} else {
					return badRequest(errorJSONResponseObject("Wrong format of project ID."));
				}
			}

			Long id = DataUtils.parseLong(projectId);
			p = Project.find.byId(id);
		} catch (Exception e) {
			return badRequest(errorJSONResponseObject("Wrong format of project ID."));
		}

		// check ownership and existence of the project
		if (p == null || !p.belongsTo(user) && !p.collaboratesWith(user)) {
			return forbidden(
					errorJSONResponseObject("Given project not available or not owned by / collaborate with user."));
		}

		final String email = nss(df.get("email")).toLowerCase();
		if (!new EmailValidator().isValid(email)) {
			return badRequest(errorJSONResponseObject("The email seems not correct, please check."));
		}
		// try to find existing
		if (Participant.findByEmailAndProject(email, p.getId()).isPresent()) {
			return notAcceptable(errorJSONResponseObject("We have found the email already in the project."));
		}

		String firstName = nss(df.get("first_name"));
		String lastName = nss(df.get("last_name"));
		if (!nnne(firstName) || !nnne(lastName)) {
			return badRequest(errorJSONResponseObject("Wrong format of the names, please check."));
		}

		Participant participant = Participant.createInstance(firstName, lastName, email, p);
		participant.save();

		p.getParticipants().add(participant);
		p.update();

		LabNotesEntry.log(ParticipantApiController.class, LabNotesEntryType.CREATE,
				"Participant created: " + participant.getName(), participant.getProject());

		ObjectNode on = okJSONResponseObject();
		on.put("id", participant.getId());
		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result edit(Request request, long id) {

		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return notFound(errorJSONResponseObject("Not valid participant."));
		}

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		// check ownership and existence of the project
		if (!participant.getProject().belongsTo(user) && !participant.getProject().collaboratesWith(user)) {
			return forbidden(
					errorJSONResponseObject("Given participant is not in a project which is editable by user."));
		}

		String email = nss(df.get("email")).toLowerCase();
		if (nnne(email) && !participant.getEmail().equals(email)) {
			if (!new EmailValidator().isValid(email)) {
				return notAcceptable(errorJSONResponseObject("The email seems not correct, please check."));
			}

			// try to find existing
			if (Participant.findByEmailAndProject(email, participant.getProject().getId()).isPresent()) {
				return forbidden(errorJSONResponseObject("This email has been used. Please enter another one."));
			}

			participant.setEmail(email);
		}

		participant.setFirstname(df.get("first_name"));
		participant.setLastname(df.get("last_name"));
		if (df.get("gender") != null) {
			participant.setGender(DataUtils.parseInt(df.get("gender")));
		}
		if (df.get("career") != null) {
			participant.setCareer(df.get("career"));
		}
		if (df.get("age_range") != null) {
			participant.setAgeRange(DataUtils.parseInt(df.get("age_range")));
		}
		if (df.get("public_parameter1") != null) {
			participant.setPublicParameter1(df.get("public_parameter1"));
		}
		if (df.get("public_parameter2") != null) {
			participant.setPublicParameter2(df.get("public_parameter2"));
		}
		if (df.get("public_parameter3") != null) {
			participant.setPublicParameter3(df.get("public_parameter3"));
		}
		// update the dataset
		participant.update();

		LabNotesEntry.log(ParticipantApiController.class, LabNotesEntryType.MODIFY,
				"participant edited: " + participant.getEmail(), participant.getProject());

		return ok(okJSONResponseObject("Information updated."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result delete(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether participant exists
		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return notFound(errorJSONResponseObject("Not valid participant."));
		}

		// check the relation of the participant and the researcher
		if (!participant.getProject().belongsTo(user) && !participant.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and participant mismatch."));
		}

		Project project = participant.getProject();
		String name = participant.getName();

		participant.delete();

		LabNotesEntry.log(ParticipantApiController.class, LabNotesEntryType.DELETE, "Participant deleted: " + name,
				project);

		return ok(okJSONResponseObject("Participant deleted."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result getInviteToken(long id) {
		return ok(errorJSONResponseObject("This API has been deprecated."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result sendInvitationLink(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether participant exists
		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return notFound(errorJSONResponseObject("Not valid participant."));
		} else if (participant.getEmail().isEmpty()) {
			return notFound(errorJSONResponseObject("No email address for participant."));
		}

		// check the relation of the participant and the researcher
		if (!participant.getProject().belongsTo(user) && !participant.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and participant mismatch."));
		}

		// send email to participant with invite link
		Html htmlBody = views.html.emails.invite.render("Participation link", String.format("Hi "
				+ participant.getRealName() + ","
				+ "\n\nThe following is the participation link for you to join the project:"
				+ participant.getProject().getName() + ","
				+ "please finish the consent form and your basic information to complete the participation process."
				+ "If you change your mind not to join this project, feel free to DECLINE this invitation by the link."),
				getParticipantViewLink(participant.getProject(), participant.getId(), request.host()));
		String textBody = "Hello! \n\nWe send you this email for your participation in DataFondry."
				+ "Just click the link below to be forwarded to a form where you can finish the participation process."
				+ "If you change your mind not to join this project, feel free to DECLINE this invitation by the link. \n\n"
				+ getParticipantViewLink(participant.getProject(), participant.getId(), request.host()) + "\n\n";

		notificationService.sendMail(participant.getEmail(), textBody, htmlBody,
				getParticipantViewLink(participant.getProject(), participant.getId(), request.host()),
				"[ID Data Foundry] Invitation", "Invitation link: "
						+ getParticipantViewLink(participant.getProject(), participant.getId(), request.host()));

		return ok(okJSONResponseObject("Invitation mail sent."));
	}

	////////////////////////////////////////////////////////////////////////////////////////////

	private String getParticipantViewLink(Project project, Long participant_id, String host) {
		return controllers.routes.ParticipationController
				.view(tokenResolverUtil.getParticipationToken(project.getId(), participant_id)).absoluteURL(true, host);
	}

	private ObjectNode toJSON(Participant participant) {
		ObjectNode newObject = Json.newObject();
		newObject.put("id", participant.getId());
		newObject.put("name", participant.getName());
		newObject.put("firstname", participant.getFirstname());
		newObject.put("lastname", participant.getLastname());
		newObject.put("email", participant.getEmail());
		newObject.put("status", participant.getStatus().name());
		newObject.put("gender", participant.gender());
		newObject.put("career", participant.getCareer());
		newObject.put("age_range", participant.ageRange());
		newObject.put("publicParameter1", participant.getPublicParameter1());
		newObject.put("publicParameter2", participant.getPublicParameter2());
		newObject.put("publicParameter3", participant.getPublicParameter3());
		newObject.put("invite_token",
				tokenResolverUtil.getParticipationToken(participant.getProject().getId(), participant.getId()));

		// lists
		{
			// add a list for the clusters which the participant is involved in
			final ArrayNode an = newObject.putArray("clusters");
			participant.getClusters().stream().forEach(c -> an.add(c.getId()));
		}
		{
			// add a list for the devices which are in the clusters that there is only one
			// participant in the clusters
			// and the participant is this participant
			final ArrayNode an = newObject.putArray("devices");
			participant.getClusterDevices().stream().forEach(d -> an.add(d.getId()));
		}
		{
			// add a list for the wearables which are in the clusters that there is only one
			// participant in the clusters
			// and the participant is this participant
			final ArrayNode an = newObject.putArray("wearables");
			participant.getClusterWearables().stream().forEach(w -> an.add(w.getId()));
		}

		return newObject;
	}

}
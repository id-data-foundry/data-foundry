package controllers.swagger;

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
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.auth.TokenResolverUtil;

@Singleton
public class ClusterApiController extends AbstractApiController {

	@Inject
	public ClusterApiController(FormFactory formFactory, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolverUtil) {
		super(formFactory, datasetConnector, tokenResolverUtil);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result clusters(Request request) {

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return forbidden(errorJSONResponseObject("Not valid user."));
		}

		ObjectNode on = okJSONResponseObject();
		ObjectNode data = on.putObject("data");
		{
			ArrayNode deviceNode = data.putArray("clusters");
			user.projects().stream().forEach(p -> {
				p.getClusters().stream().forEach(c -> deviceNode.add(c.getId()));
			});
		}

		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result cluster(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether cluster exists
		Cluster c = Cluster.find.byId(id);
		if (c == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		// check the relation of the cluster and the researcher
		if (!c.getProject().belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and cluster mismatch."));
		}

		return ok(toJSON(c));
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

		// check project
		String projectId = df.get("project_id");
		if (!nnne(projectId)) {
			return badRequest(errorJSONResponseObject("Wrong format of project ID."));
		}
		Project p = Project.find.byId(Long.parseLong(projectId));
		// check ownership and existence of the project
		if (p == null || !p.belongsTo(user) && !p.collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Given project not available or owned by user account."));
		}

		// check device name
		String clusterName = df.get("name");
		if (!nnne(clusterName)) {
			return badRequest(errorJSONResponseObject("Wrong format of the names, please check."));
		}

		Cluster cluster = new Cluster(clusterName);
		cluster.create();
		cluster.setProject(p);
		cluster.save();

		p.getClusters().add(cluster);
		p.update();

		LabNotesEntry.log(ClusterApiController.class, LabNotesEntryType.CREATE, "Cluster created: " + cluster.getName(),
				cluster.getProject());

		ObjectNode on = okJSONResponseObject();
		on.put("id", cluster.getId());
		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result rename(Request request, long id, String name) {
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check project and ownership of user
		if (!cluster.getProject().belongsTo(user) && !cluster.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Given cluster is not in a project which is editable by user."));
		}

		// check cluster name
		if (!nnne(name)) {
			return badRequest(errorJSONResponseObject("Wrong format of the name, please check."));
		}

		// check/set data
		cluster.setName(name);
		cluster.update();

		LabNotesEntry.log(ClusterApiController.class, LabNotesEntryType.MODIFY, "cluster updated: " + cluster.getName(),
				cluster.getProject());

		return ok(okJSONResponseObject("Information updated."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result delete(Request request, long id) {
		// load cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null || !cluster.getProject().belongsTo(user) && !cluster.getProject().collaboratesWith(user)) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		cluster.remove();

		return ok(okJSONResponseObject("Cluster removed."));
	}

	// ----------------------------------------------------------------------------------------------------
	//
	// CLUSTER COMPONENTS
	//
	// ----------------------------------------------------------------------------------------------------

	@Authenticated(V2UserApiAuth.class)
	public Result addDevice(Request request, long id, long deviceId) {

		// check cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("Not valid user."));
		}
		if (!cluster.getProject().belongsTo(user) && !cluster.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("The user is not allowed to perform this operation."));
		}

		// check device
		Device device = Device.find.byId(deviceId);
		if (device == null || !cluster.getProject().hasDevice(device)) {
			return notFound(errorJSONResponseObject("Not valid device"));
		}
		if (cluster.hasDevice(device)) {
			return forbidden(errorJSONResponseObject("This device is already in teh cluster."));
		}

		cluster.add(device);

		return ok(okJSONResponseObject("Device added."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result removeDevice(Request request, long id, long deviceId) {

		// check cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		// check user and ownership
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("Not valid user."));
		}
		if (!cluster.getProject().belongsTo(user) && !cluster.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("The user is not allowed to perform this operation."));
		}

		// check device
		Device device = Device.find.byId(deviceId);
		if (device == null || !cluster.hasDevice(device) || !cluster.getProject().hasDevice(device)) {
			return notFound(errorJSONResponseObject("Not valid device"));
		}

		cluster.remove(device);

		return ok(okJSONResponseObject("Device removed."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result addParticipant(Request request, long id, long participantId) {

		// check cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("Not valid user."));
		}
		if (!cluster.getProject().belongsTo(user) && !cluster.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("The user is not allowed to perform this operation."));
		}

		// check participant
		Participant participant = Participant.find.byId(participantId);
		if (participant == null || !cluster.getProject().hasParticipant(participant)) {
			return notFound(errorJSONResponseObject("Not valid participant"));
		}
		if (cluster.hasParticipant(participant)) {
			return forbidden(errorJSONResponseObject("This participant is already in the cluster"));
		}

		cluster.add(participant);

		return ok(okJSONResponseObject("Participant added."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result removeParticipant(Request request, long id, long participantId) {

		// check cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		// check user and ownership
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("Not valid user."));
		}
		if (!cluster.getProject().belongsTo(user) && !cluster.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("The user is not allowed to perform this operation."));
		}

		// check participant
		Participant participant = Participant.find.byId(participantId);
		if (participant == null || !cluster.hasParticipant(participant)
				|| !cluster.getProject().hasParticipant(participant)) {
			return notFound(errorJSONResponseObject("Not valid participant"));
		}

		cluster.remove(participant);

		return ok(okJSONResponseObject("Participant removed."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result addWearable(Request request, long id, long wearableId) {

		// check cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("Not valid user."));
		}
		if (!cluster.getProject().belongsTo(user) && !cluster.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("The user is not allowed to perform this operation."));
		}

		// check wearable
		Wearable wearable = Wearable.find.byId(wearableId);
		if (wearable == null || !cluster.getProject().hasWearable(wearable)) {
			return notFound(errorJSONResponseObject("Not valid wearable"));
		}
		if (cluster.hasWearable(wearable)) {
			return forbidden(errorJSONResponseObject("This wearable is already in the cluster"));
		}

		cluster.add(wearable);

		return ok(okJSONResponseObject("Wearable added."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result removeWearable(Request request, long id, long wearableId) {

		// check cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return notFound(errorJSONResponseObject("Not valid cluster."));
		}

		// check user and ownership
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("Not valid user."));
		}
		if (!cluster.getProject().belongsTo(user) && !cluster.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("The user is not allowed to perform this operation."));
		}

		// check wearable
		Wearable wearable = Wearable.find.byId(wearableId);
		if (wearable == null || !cluster.hasWearable(wearable) || !cluster.getProject().hasWearable(wearable)) {
			return notFound(errorJSONResponseObject("Not valid wearable"));
		}

		cluster.remove(wearable);

		return ok(okJSONResponseObject("Wearable removed."));
	}

	////////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode toJSON(Cluster cluster) {
		ObjectNode newObject = Json.newObject();
		newObject.put("id", cluster.getId());
		newObject.put("refId", cluster.getRefId());
		newObject.put("name", cluster.getName());
		newObject.put("project", cluster.getProject().getId());

		// lists
		{
			// add a list for the participants in the cluster
			final ArrayNode an = newObject.putArray("participants");
			cluster.getParticipants().stream().forEach(c -> an.add(c.getId()));
		}
		{
			// add a list for the wearables in the cluster
			final ArrayNode an = newObject.putArray("wearables");
			cluster.getWearables().stream().forEach(c -> an.add(c.getId()));
		}
		{
			// add a list for the devices in the cluster
			final ArrayNode an = newObject.putArray("devices");
			cluster.getDevices().stream().forEach(c -> an.add(c.getId()));
		}

		return newObject;
	}

}
package controllers.swagger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.auth.V2UserApiAuth;
import datasets.DatasetConnector;
import jakarta.inject.Singleton;
import models.Dataset;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.sr.Wearable;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.DataUtils;
import utils.DateUtils;
import utils.auth.TokenResolverUtil;

@Singleton
public class WearableApiController extends AbstractApiController {

	@Inject
	public WearableApiController(FormFactory formFactory, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolverUtil) {
		super(formFactory, datasetConnector, tokenResolverUtil);
	}

	/**
	 * list all wearables relevant to the user
	 * 
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result wearables(Request request) {

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return redirect(controllers.routes.HomeController.logout());
		}

		ObjectNode on = okJSONResponseObject();
		ObjectNode data = on.putObject("data");
		{
			ArrayNode wearableNode = data.putArray("wearables");
			user.projects().stream().forEach(p -> {
				p.getWearables().stream().forEach(dv -> wearableNode.add(dv.getId()));
			});
		}

		return ok(on);
	}

	/**
	 * view the information of a specific wearable which is included in one of the projects is owned or collaborated by
	 * the user
	 * 
	 * @param id
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public Result wearable(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether wearable exists
		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return notFound(errorJSONResponseObject("Not valid wearable."));
		}

		// check the relation of the wearable and the researcher
		if (!wearable.getProject().belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and wearable mismatch."));
		}

		return ok(toJSON(wearable));
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

		// check dataset
		String datasetId = df.get("dataset_id");
		if (!nnne(datasetId)) {
			return badRequest(errorJSONResponseObject("Dataset ID is not given."));
		}

		Long ds_id = DataUtils.parseLong(df.get("dataset_id"), -1L);
		Dataset ds = Dataset.find.byId(ds_id);

		// check ownership and existence of the dataset
		if (ds == null || !ds.getProject().belongsTo(user) && !ds.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Given dataset id is not available or owned by other user."));
		}

		String brand = df.get("brand") == null ? "" : df.get("brand");
		if (brand == null || !brand.equals(ds.getDsType().name())) {
			return badRequest(errorJSONResponseObject("Mismatch between the wearable and dataset."));
		}

		// check wearable name
		String wearableName = df.get("name");
		if (!nnne(wearableName)) {
			return badRequest(errorJSONResponseObject("Wrong format of the names, please check."));
		}

		Wearable wearable = new Wearable();
		wearable.create();
		wearable.setName(wearableName);
		wearable.setProject(ds.getProject());
		wearable.setBrand(brand);
		wearable.setScopes(df.get("dataset_id"));
		wearable.setExpiry(DateUtils.getMillisFromDS(ds.getId())[0]);
		wearable.setPublicParameter1(nss(df.get("public_parameter1")));
		wearable.setPublicParameter2(nss(df.get("public_parameter2")));
		wearable.setPublicParameter3(nss(df.get("public_parameter3")));
		wearable.save();

		ds.getProject().getWearables().add(wearable);
		ds.getProject().update();

		LabNotesEntry.log(WearableApiController.class, LabNotesEntryType.CREATE,
				"wearable created: " + wearable.getName(), wearable.getProject());

		ObjectNode on = okJSONResponseObject();
		on.put("id", wearable.getId());
		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result edit(Request request, long id) {
		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return notFound(errorJSONResponseObject("Not valid wearable."));
		}

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return badRequest(errorJSONResponseObject("Not valid user."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		// check project and ownership of user
		if (!wearable.getProject().belongsTo(user) && !wearable.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Given wearable is not in a project which is editable by user."));
		}

		// check dataset
		String datasetId = df.get("dataset_id");
		if (nnne(datasetId) && !wearable.getScopeFromDataset().equals(datasetId)) {
			Long ds_id = DataUtils.parseLong(df.get("dataset_id"), -1L);
			Dataset ds = Dataset.find.byId(ds_id);

			// check ownership and existence of the dataset
			if (ds == null || !ds.getProject().belongsTo(user)) {
				return forbidden(errorJSONResponseObject("Given dataset not available or owned by user account."));
			}

			// make sure the wearable and the dataset is in the same project
			if (!wearable.getProject().getId().equals(ds.getProject().getId())) {
				return forbidden(errorJSONResponseObject("Given wearable and dataset are not in the same project."));
			}

			// check the brand of the wearable and the dataset
			if (!wearable.getBrand().equals(ds.getDsType().name())) {
				return badRequest(errorJSONResponseObject("Mismatch between the wearable and dataset."));
			}

			wearable.setScopes(ds_id + "");
		}

		// check wearable name
		String wearableName = df.get("name");
		if (nnne(wearableName) && !wearable.getName().equals(wearableName)) {
			wearable.setName(wearableName);
		}

		// check/set data
		if (df.get("public_parameter1") != null) {
			wearable.setPublicParameter1(df.get("public_parameter1"));
		}
		if (df.get("public_parameter2") != null) {
			wearable.setPublicParameter2(df.get("public_parameter2"));
		}
		if (df.get("public_parameter3") != null) {
			wearable.setPublicParameter3(df.get("public_parameter3"));
		}
		wearable.update();

		LabNotesEntry.log(WearableApiController.class, LabNotesEntryType.MODIFY,
				"wearable updated: " + wearable.getName(), wearable.getProject());

		return ok(okJSONResponseObject("Information updated."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result reset(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether wearable exists
		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return notFound(errorJSONResponseObject("Not valid wearable."));
		}

		// check the relation of the wearable and the researcher
		if (!wearable.getProject().belongsTo(user) && !wearable.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and wearable mismatch."));
		}

		wearable.reset();

		return ok(okJSONResponseObject("Wearable reset completed."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result delete(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether wearable exists
		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return notFound(errorJSONResponseObject("Not valid wearable."));
		}

		// check the relation of the wearable and the researcher
		if (!wearable.getProject().belongsTo(user) && !wearable.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and wearable mismatch."));
		}

		Project project = wearable.getProject();
		String name = wearable.getName();

		wearable.delete();

		LabNotesEntry.log(WearableApiController.class, LabNotesEntryType.DELETE, "wearable deleted: " + name, project);

		return ok(okJSONResponseObject("Wearable deleted."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result signup(Request request, long id) {

		return ok(errorJSONResponseObject("This API has been deprecated."));

//		// check wearable
//		Wearable wearable = Wearable.find.byId(id);
//		if (wearable == null) {
//			return notFound(errorJSONResponseObject("Not valid wearable."));
//		}
//
//		DynamicForm df = formFactory.form().bindFromRequest(request);
//		if (df == null) {
//			return badRequest(errorJSONResponseObject("No data given."));
//		}
//
//		// check inviteToken
//		String inviteToken = df.get("inviteToken");
//		if (!nnne(inviteToken)) {
//			return badRequest(errorJSONResponseObject("Invite token is required."));
//		}
//
//		// check participant has no wearables in on-going projects
//		Long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(inviteToken);
//		Participant participant = Participant.find.byId(participant_id);
//		Participant clusterParticipant = wearable.getClusterParticipant();
//
//		// check whether participant and wearable is in the same cluster or the wearable is unregistered
//		if (clusterParticipant != null && participant_id.equals(clusterParticipant.id)
//		        || (!participant.clusters.isEmpty() && !wearable.isConnected())) {
//
//			String signupStatus = "new", scopes = "", returnUrl = "", redirectUrl = "";
//			if (wearable.isConnected()) {
//				signupStatus = "registered";
//			}
//
//			switch (wearable.brand) {
//			case Wearable.FITBIT:
//				returnUrl = controllers.routes.FitbitWearablesController.finishWearableRegistration(0).absoluteURL(true,
//				        request.host());
//
//				// scopes = wearable.scopes.split("::")[1];
//				scopes = "activity weight sleep heartrate settings location";
//
//				// https://www.fitbit.com/oauth2/authorize?response_type=code&client_id=@client_id&scope=@scopes
//				// &redirect_uri=@redirectUrl&state=@w.id
//
//				// https://www.fitbit.com/oauth2/authorize?response_type=code&client_id=22BWV3&scope=activity weight
//				// sleep heartrate settings
//				// location&redirect_uri=https://datafoundry.com:9443/wearables/signup/fitbit&state=37
//
//				redirectUrl = "https://www.fitbit.com/oauth2/authorize?response_type=code" + "&client_id="
//				        + fitbitService.APP_CLIENT_ID + "&scope=" + scopes + "&redirect_uri=" + returnUrl + "&state="
//				        + wearable.id;
//				return ok(okJSONResponseObject(redirectUrl));
//			case Wearable.GOOGLEFIT:
//				// need url which domain is authorized by google as redirectURL
//				returnUrl = controllers.routes.GoogleFitWearablesController.finishWearableRegistration(0)
//				        .absoluteURL(true, request.host());
//
//				// scopes = wearable.scopes.split("::")[1] + " openid";
//				scopes = "https://www.googleapis.com/auth/fitness.activity.read"
//				        + " https://www.googleapis.com/auth/fitness.body.read"
//				        + " https://www.googleapis.com/auth/fitness.location.read" + " openid";
//
//				// https://accounts.google.com/o/oauth2/v2/auth?response_type=code&client_id=@client_id&prompt=consent
//				// &redirect_uri=@redirectUrl&scope=@scopes&access_type=offline&state=@w.id"
//				redirectUrl = "https://accounts.google.com/o/oauth2/v2/auth?response_type=code" + "&client_id="
//				        + googlefitService.APP_CLIENT_ID + "&prompt=consent" + "&redirect_uri" + returnUrl + "&scope="
//				        + scopes + "&access_type=offline" + "&state=" + wearable.id;
//				return redirect(redirectUrl);
//			default:
//				return badRequest(errorJSONResponseObject("Unknown wearable brand."));
//			}
//		}
//
//		return ok(okJSONResponseObject());
	}

	////////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode toJSON(Wearable wearable) {
		ObjectNode newObject = Json.newObject();
		newObject.put("id", wearable.getId());
		newObject.put("name", wearable.getName());
		newObject.put("brand", wearable.getBrand());
		newObject.put("project", wearable.getProject().getId());
		newObject.put("dataset", DataUtils.parseLong(wearable.getScopes()));

		// cluster list
		{
			// add a list for the clusters which the participant is involved in
			final ArrayNode an = newObject.putArray("clusters");
			wearable.getClusters().stream().forEach(c -> an.add(c.getId()));
		}

		long participant = wearable.getClusterParticipant() == null ? -1l : wearable.getClusterParticipant().getId();
		newObject.put("participant", participant);
		newObject.put("status", wearable.isConnected() ? true : false);

		newObject.put("publicParameter1", wearable.getPublicParameter1());
		newObject.put("publicParameter2", wearable.getPublicParameter2());
		newObject.put("publicParameter3", wearable.getPublicParameter3());

		return newObject;
	}

}
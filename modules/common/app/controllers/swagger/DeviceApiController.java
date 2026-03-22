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
import models.sr.Device;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.DataUtils;
import utils.auth.TokenResolverUtil;

@Singleton
public class DeviceApiController extends AbstractApiController {

	@Inject
	public DeviceApiController(FormFactory formFactory, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolverUtil) {
		super(formFactory, datasetConnector, tokenResolverUtil);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result devices(Request request) {

		// check if user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return redirect(controllers.routes.HomeController.logout());
		}

		ObjectNode on = okJSONResponseObject();
		ObjectNode data = on.putObject("data");
		{
			ArrayNode deviceNode = data.putArray("devices");
			user.projects().stream().forEach(p -> {
				p.getDevices().stream().forEach(dv -> deviceNode.add(dv.getId()));
			});
		}

		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result device(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether device exists
		Device dv = Device.find.byId(id);
		if (dv == null) {
			return notFound(errorJSONResponseObject("Not valid device."));
		}

		// check the relation of the device and the researcher
		if (!dv.getProject().belongsTo(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and device mismatch."));
		}

		return ok(toJSON(dv));
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
		Project p = Project.find.byId(DataUtils.parseLong(projectId));
		// check ownership and existence of the project
		if (p == null || !p.belongsTo(user) && !p.collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Given project not available or owned by user account."));
		}

		// check device name
		String deviceName = df.get("name");
		if (!nnne(deviceName)) {
			return badRequest(errorJSONResponseObject("Wrong format of the names, please check."));
		}

		Device device = new Device();
		device.create();
		device.setName(deviceName);
		device.setProject(p);
		if (df.get("category") != null) {
			device.setCategory(df.get("category"));
		}
		if (df.get("subtype") != null) {
			device.setSubtype(df.get("subtype"));
		}
		if (df.get("ip_address") != null) {
			device.setIpAddress(df.get("ip_address"));
		}
		if (df.get("location") != null) {
			device.setLocation(df.get("location"));
		}
		if (df.get("configuration") != null) {
			device.setConfiguration(df.get("configuration"));
		}
		if (df.get("public_parameter1") != null) {
			device.setPublicParameter1(df.get("public_parameter1"));
		}
		if (df.get("public_parameter2") != null) {
			device.setPublicParameter2(df.get("public_parameter2"));
		}
		if (df.get("public_parameter3") != null) {
			device.setPublicParameter3(df.get("public_parameter3"));
		}
		device.save();

		p.getDevices().add(device);
		p.update();

		LabNotesEntry.log(DeviceApiController.class, LabNotesEntryType.CREATE, "Device created: " + device.getName(),
				device.getProject());

		ObjectNode on = okJSONResponseObject();
		on.put("id", device.getId());
		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result edit(Request request, long id) {
		Device device = Device.find.byId(id);
		if (device == null) {
			return notFound(errorJSONResponseObject("Not valid device."));
		}

		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		// check project and ownership of user
		if (!device.getProject().belongsTo(user) && !device.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Given device is not in a project which is editable by user."));
		}

		// check device name
		String deviceName = df.get("name");
		if (!nnne(deviceName)) {
			return badRequest(errorJSONResponseObject("Wrong format of the names, please check."));
		}

		// check/set data
		device.setName(deviceName);
		if (df.get("category") != null) {
			device.setCategory(df.get("category"));
		}
		if (df.get("subtype") != null) {
			device.setSubtype(df.get("subtype"));
		}
		if (df.get("ip_address") != null) {
			device.setIpAddress(df.get("ip_address"));
		}
		if (df.get("location") != null) {
			device.setLocation(df.get("location"));
		}
		if (df.get("configuration") != null) {
			device.setConfiguration(df.get("configuration"));
		}
		if (df.get("public_parameter1") != null) {
			device.setPublicParameter1(df.get("public_parameter1"));
		}
		if (df.get("public_parameter2") != null) {
			device.setPublicParameter2(df.get("public_parameter2"));
		}
		if (df.get("public_parameter3") != null) {
			device.setPublicParameter3(df.get("public_parameter3"));
		}
		device.update();

		LabNotesEntry.log(DeviceApiController.class, LabNotesEntryType.MODIFY, "Device updated: " + device.getName(),
				device.getProject());

		return ok(okJSONResponseObject("Information updated."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result delete(Request request, long id) {

		// check whether user exists
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check whether device exists
		Device device = Device.find.byId(id);
		if (device == null) {
			return notFound(errorJSONResponseObject("Not valid device."));
		}

		// check the relation of the device and the researcher
		if (!device.getProject().belongsTo(user) && !device.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Not available, user and device mismatch."));
		}

		Project project = device.getProject();
		String name = device.getName();

		device.delete();

		LabNotesEntry.log(DeviceApiController.class, LabNotesEntryType.DELETE, "Device deleted: " + name, project);

		return ok(okJSONResponseObject("Device deleted."));
	}

	////////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode toJSON(Device device) {
		ObjectNode newObject = Json.newObject();
		newObject.put("id", device.getId());
		newObject.put("name", device.getName());
		newObject.put("project", device.getProject().getId());
		newObject.put("category", device.getCategory());
		newObject.put("subtype", device.getSubtype());
		newObject.put("ip_address", device.getIpAddress());
		newObject.put("location", device.getLocation());
		newObject.put("configuration", device.getConfiguration());
		newObject.put("publicParameter1", device.getPublicParameter1());
		newObject.put("publicParameter2", device.getPublicParameter2());
		newObject.put("publicParameter3", device.getPublicParameter3());

		// cluster list
		{
			// add a list for the clusters which the participant is involved in
			final ArrayNode an = newObject.putArray("clusters");
			device.getClusters().stream().forEach(c -> an.add(c.getId()));
		}

		return newObject;
	}

}
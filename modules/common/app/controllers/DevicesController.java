package controllers;

import java.util.Optional;

import javax.inject.Inject;

import controllers.auth.UserAuth;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.DataUtils;
import utils.components.OnboardingSupport;

/**
 * This controller contains an action to handle HTTP requests to the application's home page.
 */
public class DevicesController extends AbstractAsyncController {

	private final OnboardingSupport onboardingSupport;
	private final FormFactory formFactory;

	@Inject
	public DevicesController(OnboardingSupport onboardingSupport, FormFactory formFactory) {
		this.onboardingSupport = onboardingSupport;
		this.formFactory = formFactory;
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {

		// check user
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// load device
		Device device = Device.find.byId(id);
		if (device == null) {
			return redirect(HOME);
		}

		// check user permissions
		Project project = device.getProject();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// show device overview
		project.refresh();
		return ok(views.html.sources.device.view.render(device, project.getIoTDatasets()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id, Long participantId) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(username)) {
			return redirect(HOME);
		}

		if (participantId != -1l) {
			Participant participant = Participant.find.byId(participantId);
			if (participant == null || !project.hasParticipant(participant)) {
				return redirect(HOME).addingToSession(request, "error", "This participant not found in this project.");
			}
		}

		// display the add form page
		return ok(views.html.sources.device.add.render(csrfToken(request), project, participantId));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(username)) {
			return redirect(HOME);
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(project.getId())).addingToSession(request, "error", "Expecting some data");
		}

		// create wearable in project
		Device device = new Device();
		device.create();
		device.setName(df.get("name"));
		device.setCategory(df.get("category"));
		device.setSubtype(df.get("subtype"));
		device.setIpAddress(df.get("ip_address") == null ? "" : df.get("ip_address"));
		device.setLocation(df.get("location") == null ? "" : df.get("location"));
		device.setConfiguration(df.get("configuration") == null ? "" : df.get("configuration"));
		device.setProject(project);
		device.setPublicParameter1(df.get("public_parameter1") == null ? "" : df.get("public_parameter1"));
		device.setPublicParameter2(df.get("public_parameter2") == null ? "" : df.get("public_parameter2"));
		device.setPublicParameter3(df.get("public_parameter3") == null ? "" : df.get("public_parameter3"));
		device.save();

		project.getDevices().add(device);

		// check whether the request is from participant page or not
		final long participant_id = DataUtils.parseLong(df.get("participantId"), -1L);
		if (participant_id > -1L) {
			final Participant participant = Participant.find.byId(participant_id);

			if (participant == null || !participant.getProject().getId().equals(project.getId())) {
				return redirect(PROJECT(id)).addingToSession(request, "error", "Participant not exists");
			}

			Optional<Cluster> cluster = project.getClusters().stream()
			        .filter(c -> c.hasParticipant(participant) && c.getParticipants().size() == 1).findFirst();

			if (cluster.isPresent()) {
				// if cluster is existed, add new device directly
				cluster.get().add(device);
			} else {
				// else create a new cluster and add the enw device
				Cluster newCluster = new Cluster(participant.getName());
				newCluster.setProject(project);
				newCluster.create();
				newCluster.getDevices().add(device);
				newCluster.getParticipants().add(participant);

				project.getClusters().add(newCluster);
			}
		}

		project.update();

		onboardingSupport.updateAfterDone(username, "new_device");

		LabNotesEntry.log(Device.class, LabNotesEntryType.CREATE, "Device created: " + device.getName(), project);

		return redirect(routes.DevicesController.view(device.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long device_id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Device device = Device.find.byId(device_id);
		if (device == null) {
			return redirect(HOME).addingToSession(request, "message", "The device was not found.");
		}

		// check project
		Project project = device.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// display the add form page
		return ok(views.html.sources.device.edit.render(csrfToken(request), device));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long device_id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// get device
		Device device = Device.find.byId(device_id);
		if (device == null) {
			return redirect(HOME).addingToSession(request, "error", "The device was not found.");
		}

		// check authorization
		Project project = device.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error", "The project is not editable.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(project.getId())).addingToSession(request, "error", "Expecting some data.");
		}

		// get device data from edit page and write to database
		device.setName(df.get("name"));
		device.setCategory(df.get("category"));
		device.setSubtype(df.get("subtype"));
		device.setIpAddress(df.get("ip_address"));
		device.setLocation(df.get("location"));
		device.setConfiguration(df.get("configuration"));
		device.setPublicParameter1(df.get("public_parameter1") == null ? "" : df.get("public_parameter1"));
		device.setPublicParameter2(df.get("public_parameter2") == null ? "" : df.get("public_parameter2"));
		device.setPublicParameter3(df.get("public_parameter3") == null ? "" : df.get("public_parameter3"));
		device.update();

		LabNotesEntry.log(Device.class, LabNotesEntryType.MODIFY, "Device updated: " + device.getName(), project);

		return redirect(routes.DevicesController.view(device.getId()));
	}

	/**
	 * delete a device
	 * 
	 * @param request
	 * @param device_id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result delete(Request request, Long device_id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check device
		Device device = Device.find.byId(device_id);
		if (device == null) {
			return redirect(HOME).addingToSession(request, "message", "The device was not found.");
		}

		// check project
		Project project = device.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// remove from clusters
		for (int i = 0; i < device.getClusters().size(); i++) {
			Cluster cluster = device.getClusters().get(0);
			cluster.refresh();
			cluster.remove(device);
			cluster.update();
		}

		// remove from project
		project.getDevices().removeIf(d -> d.getId().equals(device.getId()));
		project.update();

		// delete device
		device.delete();

		LabNotesEntry.log(Device.class, LabNotesEntryType.DELETE, "Device deleted: " + device.getName(), project);

		return redirect(PROJECT(project.getId()));
	}

}
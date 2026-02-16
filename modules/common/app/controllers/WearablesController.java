package controllers;

import controllers.auth.UserAuth;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.sr.Wearable;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;

/**
 * This controller contains an action to handle HTTP requests to the application's home page.
 */
public class WearablesController extends AbstractAsyncController {

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return redirect(routes.ProjectsController.index());
		}

		Project project = Project.find.byId(wearable.getProject().getId());
		if (project == null) {
			return redirect(routes.ProjectsController.index());
		}

		if (!project.editableBy(username)) {
			return redirect(routes.ProjectsController.view(project.getId()));
		}

		// dispatch to the correct controller for the wearable brand
		if (wearable.isFitbit()) {
			return redirect(routes.FitbitWearablesController.view(id));
		} else {
			return redirect(routes.GoogleFitWearablesController.view(id));
		}
	}

	@Authenticated(UserAuth.class)
	public Result delete(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return redirect(routes.ProjectsController.index());
		}

		Project project = Project.find.byId(wearable.getProject().getId());
		if (project == null) {
			return redirect(routes.ProjectsController.index());
		}

		if (!project.editableBy(username)) {
			return redirect(routes.ProjectsController.view(project.getId()));
		}

		project.getWearables().removeIf(d -> wearable.getId().equals(d.getId()));
		project.update();
		wearable.delete();

		LabNotesEntry.log(Wearable.class, LabNotesEntryType.DELETE, "Wearable deleted: " + wearable.getName(), project);

		return redirect(PROJECT(project.getId()));
	}
}
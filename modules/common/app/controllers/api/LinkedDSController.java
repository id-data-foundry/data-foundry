package controllers.api;

import java.util.Date;

import javax.inject.Inject;

import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.components.OnboardingSupport;

public class LinkedDSController extends AbstractDSController {

	@Inject
	public LinkedDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
	        OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		if (!ds.visibleFor(username)) {
			return redirect(controllers.routes.ProjectsController.view(ds.getProject().getId()));
		}

		return ok(views.html.datasets.linked.view.render(ds, ds.getProject().getParticipants(), username, request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		return ok(views.html.datasets.linked.add.render(csrfToken(request), p));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data");
		}

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.LINKED, p, df.get("description"),
		        df.get("target_object"), df.get("isPublic"), df.get("license"));
		ds.setStart(new Date());
		ds.save();

		onboardingSupport.updateAfterDone(username, "new_dataset");

		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		if (!ds.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		return ok(views.html.datasets.linked.edit.render(csrfToken(request), ds));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		if (!ds.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
			        "Expecting some data");
		}

		ds.setName(htmlTagEscape(nss(df.get("dataset_name"), 64)));
		ds.setDescription(htmlTagEscape(nss(df.get("description"))));
		ds.setTargetObject(nss(df.get("target_object")));

		// metadata
		storeMetadata(ds, df);
		ds.update();

		LabNotesEntry.log(LinkedDSController.class, LabNotesEntryType.MODIFY, "Dataset edited: " + ds.getName(),
		        ds.getProject());
		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}
}

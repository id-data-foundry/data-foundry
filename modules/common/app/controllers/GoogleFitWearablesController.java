package controllers;

import java.util.Optional;

import com.google.inject.Inject;

import controllers.auth.UserAuth;
import models.Dataset;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.sr.Cluster;
import models.sr.Participant;
import models.sr.Wearable;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.inlets.GoogleFitService;
import utils.DataUtils;
import utils.DateUtils;
import utils.auth.TokenResolverUtil;
import utils.components.OnboardingSupport;
import utils.conf.Configurator;

public class GoogleFitWearablesController extends AbstractAsyncController {

	private final Configurator configurator;
	private final FormFactory formFactory;
	private final GoogleFitService googleFitService;
	private final TokenResolverUtil tokenResolverUtil;
	private final OnboardingSupport onboardingSupport;

	@Inject
	public GoogleFitWearablesController(Configurator configurator, FormFactory formFactory,
			GoogleFitService googleFitService, TokenResolverUtil tokenResolverUtil,
			OnboardingSupport onboardingSupport) {
		this.configurator = configurator;
		this.formFactory = formFactory;
		this.googleFitService = googleFitService;
		this.tokenResolverUtil = tokenResolverUtil;
		this.onboardingSupport = onboardingSupport;
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return redirect(HOME);
		}

		Project p = wearable.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not editable.");
		}

		Dataset ds = null;
		Optional<Dataset> ods = p.findDatasetById(wearable.getDatasetId());
		if (ods.isPresent()) {
			ds = ods.get();
		}

		// show wearable overview
		return ok(views.html.sources.wearable.googlefit.view.render(wearable, ds, p.belongsTo(username)));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id, Long participantId) {
		// check configurations
		if (!configurator.isGoogleFitAvailable()) {
			return redirect(HOME).addingToSession(request, "error", "GoogleFit service is not available.");
		}

		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not found or not editable.");
		}

		if (participantId != -1l) {
			Participant participant = Participant.find.byId(participantId);
			if (participant == null || !p.hasParticipant(participant)) {
				return redirect(HOME).addingToSession(request, "error", "Participant does not exist in this project.");
			}
		}

		return ok(views.html.sources.wearable.googlefit.add.render(csrfToken(request), p, p.getGoogleFitDatasets(),
				participantId));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(username)) {
			return redirect(HOME);
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(id));
		}

		// check whether the request is from participant page or not
		final long participant_id = DataUtils.parseLong(df.get("participantId"), -1L);
		final long ds_id = DataUtils.parseLong(df.get("ds_id"), -1L);
		final Dataset dataset = Dataset.find.byId(ds_id);
		if (dataset == null || !id.equals(dataset.getProject().getId())) {
			return redirect(routes.ProjectsController.viewResources(project.getId())).addingToSession(request, "error",
					"This dataset is not in the project");
		}

		Wearable wearable = new Wearable();
		wearable.setName(df.get("name"));
		wearable.setBrand(Wearable.GOOGLEFIT);
		wearable.setProject(project);
		wearable.setScopes(nss(df.get("ds_id")));
		wearable.setExpiry(DateUtils.getMillisFromDS(ds_id)[0]);
		wearable.setPublicParameter1(nss(df.get("public_parameter1")));
		wearable.setPublicParameter2(nss(df.get("public_parameter2")));
		wearable.setPublicParameter3(nss(df.get("public_parameter3")));
		wearable.create();
		wearable.save();

		project.getWearables().add(wearable);

		if (participant_id > -1L) {
			// check participant: existance and in the correct project
			Participant participant = Participant.find.byId(participant_id);

			if (participant == null || !participant.getProject().getId().equals(project.getId())) {
				return redirect(PROJECT(id)).addingToSession(request, "error", "Participant not exists");
			}

			Optional<Cluster> cluster = project.getClusters().stream()
					.filter(c -> c.hasParticipant(participant) && c.getParticipants().size() == 1).findFirst();

			if (cluster.isPresent()) {
				// if cluster is existed, add new wearable directly
				cluster.get().add(wearable);
			} else {
				// else create a new cluster and add the enw wearable
				Cluster newCluster = new Cluster(participant.getName());
				newCluster.setProject(project);
				newCluster.create();
				newCluster.getWearables().add(wearable);
				newCluster.getParticipants().add(participant);

				project.getClusters().add(newCluster);
			}
		}
		project.update();

		onboardingSupport.updateAfterDone(username, "new_gf_wearable");

		LabNotesEntry.log(Wearable.class, LabNotesEntryType.CREATE, "Wearable created: " + wearable.getName(), project);

		return redirect(routes.ProjectsController.viewResources(project.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long wearable_id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Wearable wearable = Wearable.find.byId(wearable_id);
		if (wearable == null) {
			return redirect(HOME);
		}

		Project project = wearable.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		Dataset ds = null;
		Optional<Dataset> ods = project.findDatasetById(wearable.getDatasetId());
		if (ods.isPresent()) {
			ds = ods.get();
		}

		return ok(views.html.sources.wearable.googlefit.edit.render(csrfToken(request), wearable,
				project.getGoogleFitDatasets(), ds));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long wearable_id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Wearable wearable = Wearable.find.byId(wearable_id);
		if (wearable == null) {
			return redirect(HOME);
		}

		Project project = wearable.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(project.getId()));
		}

		final long ds_id = DataUtils.parseLong(df.get("ds_id"), -1L);
		final Optional<Dataset> dataset = project.findDatasetById(ds_id);
		if (dataset.isPresent()) {

			if (wearable.getBrand() == null) {
				wearable.setBrand(Wearable.GOOGLEFIT);
			}

			// scopes and expiry should be changed only when it needed
			if (wearable.getScopeFromDataset() == null || !wearable.getScopeFromDataset().equals(ds_id + "")) {
				wearable.setScopes(ds_id + "");
				wearable.setExpiry(DateUtils.getMillisFromDS(ds_id)[0]);
			}

			wearable.setName(nss(df.get("name")));

			// wearable.configuration = df.get("configuration");
			wearable.setPublicParameter1(nss(df.get("public_parameter1")));
			wearable.setPublicParameter2(nss(df.get("public_parameter2")));
			wearable.setPublicParameter3(nss(df.get("public_parameter3")));
			wearable.update();
		} else {
			return redirect(PROJECT(project.getId())).addingToSession(request, "error",
					"Edit GoogleFit wearable:" + wearable.getName() + " failed.");
		}

		LabNotesEntry.log(Wearable.class, LabNotesEntryType.MODIFY, "Wearable updated: " + wearable.getName(), project);

		return redirect(routes.GoogleFitWearablesController.view(wearable.getId()));
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Result finishWearableRegistration(Request request, Long state) {

		String invite_token = session(request, "participant_id");

		// check invite_token
		if (invite_token.isEmpty()) {
			return redirect(LANDING).addingToSession(request, "error", "Participant token not found.");
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id < 0) {
			return redirect(LANDING).addingToSession(request, "error", "Participant id not found.");
		}

		// check participant object
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null) {
			return redirect(LANDING).addingToSession(request, "error", "Participant not found.");
		}

		// check participant in the correct project
		long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
		if (project_id != participant.getProject().getId()) {
			return redirect(LANDING).addingToSession(request, "error", "Participant token is corrupt.");
		}

		// check wearable in correct project
		Wearable wearable = Wearable.find.byId(state);
		if (!wearable.getProject().getId().equals(project_id)) {
			return redirect(LANDING).addingToSession(request, "error", "Wearable not in project.");
		}
		if (participant.getClusterWearables().stream().noneMatch(cw -> cw.getId().equals(wearable.getId()))) {
			return redirect(LANDING).addingToSession(request, "error",
					"Wearable not associated to participant in project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(routes.ParticipationController.view(invite_token)).addingToSession(request, "error",
					"Expecting some data.");
		}

		if (df.get("error") != null) {
			return redirect(routes.ParticipationController.view(invite_token)).addingToSession(request, "error",
					"Access denied.");
		}

		// access code
		String code = df.get("code");
		String redirectUrl = controllers.routes.GoogleFitWearablesController.finishWearableRegistration("0")
				.absoluteURL(true, request.host());

		// start the second step of the process
		googleFitService.authorizationRequest(wearable, code, redirectUrl);

		return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
				"message", "GoogleFit wearable successfully connected.");
	}

	public Result finishWearableRegistration(Request request, String state) {
		// split state with "==" into wearable_id and invite_token
		if (!nnne(state)) {
			return redirect(LANDING).addingToSession(request, "error", "Participant token not found.");
		}

		String states[] = state.split("==");
		if (states.length < 2) {
			return redirect(routes.HomeController.index()).addingToSession(request, "error", "Insufficient info!");
		}

		long wearable_id = DataUtils.parseLong(states[0]);
		if (wearable_id == -1L) {
			return redirect(routes.HomeController.index()).addingToSession(request, "error",
					"Fail to parse wearable id!");
		}

		Wearable wearable;
		wearable = Wearable.find.byId(wearable_id);
		if (wearable == null) {
			return redirect(routes.HomeController.index()).addingToSession(request, "error", "Wearable not found!");
		}

		String invite_token = states[1];

		// check invite_token
		if (invite_token.isEmpty()) {
			return redirect(LANDING).addingToSession(request, "error", "Participant token not found.");
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id < 0) {
			return redirect(LANDING).addingToSession(request, "error", "Participant id not found.");
		}

		// check participant object
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null) {
			return redirect(LANDING).addingToSession(request, "error", "Participant not found.");
		}

		// check participant in the correct project
		long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
		if (project_id != participant.getProject().getId()) {
			return redirect(LANDING).addingToSession(request, "error", "Participant token is corrupt.");
		}

		// check wearable in correct project
		if (!wearable.getProject().getId().equals(project_id)) {
			return redirect(LANDING).addingToSession(request, "error", "Wearable not in project.");
		}
		if (participant.getClusterWearables().stream().noneMatch(cw -> cw.getId().equals(wearable.getId()))) {
			return redirect(LANDING).addingToSession(request, "error",
					"Wearable not associated to participant in project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(routes.ParticipationController.view(invite_token)).addingToSession(request, "error",
					"Expecting some data.");
		}

		if (df.get("error") != null) {
			return redirect(routes.ParticipationController.view(invite_token)).addingToSession(request, "error",
					"Access denied.");
		}

		// access code
		String code = df.get("code");
		String redirectUrl = controllers.routes.GoogleFitWearablesController.finishWearableRegistration("0")
				.absoluteURL(request, true);

		// start the second step of the process
		googleFitService.authorizationRequest(wearable, code, redirectUrl);

		return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
				"message", "GoogleFit wearable successfully connected.");
	}

	@Authenticated(UserAuth.class)
	public Result resetMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return redirect(HOME);
		}

		Project project = wearable.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		wearable.reset();

		return redirect(controllers.routes.ProjectsController.viewResources(project.getId())).addingToSession(request,
				"message", "GoogleFit wearable successfully reset.");
	}

	/**
	 * delete a GoogleFit wearable
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result delete(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check wearable
		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return redirect(HOME);
		}

		// check ownership
		Project project = wearable.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// remove from clusters
		for (int i = 0; i < wearable.getClusters().size(); i++) {
			Cluster cluster = wearable.getClusters().get(0);
			cluster.refresh();
			cluster.remove(wearable);
			cluster.update();
		}

		// remove from project
		project.getWearables().removeIf(d -> d.getId().equals(wearable.getId()));
		project.update();

		// delete wearable
		wearable.delete();

		LabNotesEntry.log(Wearable.class, LabNotesEntryType.DELETE, "Wearable deleted: " + wearable.getName(), project);

		// back to project
		return redirect(routes.ProjectsController.view(project.getId()));
	}

	/**
	 * delete a GoogleFit wearable
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result delete(Request request, String string_id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		long id = DataUtils.parseLong(string_id.trim());
		if (id == -1L) {
			return redirect(HOME).addingToSession(request, "error", "fail to parse GoogleFit wearable id.");
		}

		// check wearable
		Wearable wearable = Wearable.find.byId(id);
		if (wearable == null) {
			return redirect(HOME);
		}

		// check ownership
		Project project = wearable.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// remove from clusters
		for (int i = 0; i < wearable.getClusters().size(); i++) {
			Cluster cluster = wearable.getClusters().get(0);
			cluster.refresh();
			cluster.remove(wearable);
			cluster.update();
		}

		// remove from project
		project.getWearables().removeIf(d -> d.getId().equals(wearable.getId()));
		project.update();

		// delete wearable
		wearable.delete();

		LabNotesEntry.log(Wearable.class, LabNotesEntryType.DELETE, "Wearable deleted: " + wearable.getName(), project);

		// back to project
		return

		redirect(routes.ProjectsController.view(project.getId()));
	}
}

package controllers.tools;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.google.inject.Inject;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.vm.TimedMedia;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.MimeTypes;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.jsexecutor.JSActor;
import services.jsexecutor.JSExecutorService;
import utils.DataUtils;
import utils.StringUtils;

@Authenticated(UserAuth.class)
public class ActorController extends AbstractAsyncController {

	private final JSExecutorService jsExecService;
	private final FormFactory formFactory;
	private final DatasetConnector datasetConnector;

	@Inject
	public ActorController(JSExecutorService jsExecService, FormFactory formFactory,
			DatasetConnector datasetConnector) {
		this.jsExecService = jsExecService;
		this.formFactory = formFactory;
		this.datasetConnector = datasetConnector;
	}

	@AddCSRFToken
	public Result index(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		if (user.projects().size() + user.collaborations().size() == 0) {
			return redirect(controllers.routes.ProjectsController.index()).addingToSession(request, "message",
					"No project scripts to show.");
		}

		return ok(views.html.tools.actor.index.render(user, csrfToken(request)));
	}

	@RequireCSRFCheck
	public Result add(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data.");
		}

		// check project id, replace by form project id, if necessary
		if (id == -1l) {
			String projectId = nss(df.get("project"));
			id = DataUtils.parseLong(projectId);
		}
		if (id == -1l) {
			return redirect(HOME).addingToSession(request, "error", "Project id not valid.");
		}

		// scripts can only be added by the project owner
		Project p = Project.find.byId(id);
		if (p == null || !p.belongsTo(user)) {
			return redirect(routes.ActorController.index());
		}

		// create new dataset for the actor
		Dataset ds = datasetConnector.create(nss(df.get("name")), DatasetType.COMPLETE, p, "Script dataset",
				"Data Foundry scripting", null, df.get("license"));
		ds.setCollectorType(Dataset.ACTOR);
		ds.save();

		// create actor
		jsExecService.addActor(ds);

		return redirect(routes.ActorController.view(ds.getId())).addingToSession(request, "message",
				"Script " + ds.getName() + " created in project " + p.getName());
	}

	public Result view(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.editableBy(user.getEmail())) {
			return redirect(routes.ActorController.index());
		}

		return ok(views.html.tools.actor.view.render(user, ds));
	}

	public CompletionStage<Result> execute(Request request, long id) {

		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.editableBy(username)) {
			return redirectCS(routes.ActorController.index());
		}

		// check actor
		JSActor actor = jsExecService.getTrialActor(ds.getId());
		if (actor == null) {
			// actor is not available, craete new one
			actor = jsExecService.addTrialActor(ds);
		}

		String codeTmp = nss(request.body().asText());
		// unscramble the contents if necessary
		String code = StringUtils.unscrambleTransport(codeTmp);

		final JSActor finalActor = actor;
		return CompletableFuture.supplyAsync(() -> finalActor.runTrial(code)).thenApplyAsync(output -> ok(output));
	}

	public Result save(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.editableBy(user.getEmail())) {
			return redirect(routes.ActorController.index());
		}

		String code = nss(request.body().asText());
		// unscramble the contents if necessary
		code = StringUtils.unscrambleTransport(code);

		// update dataset
		ds.getConfiguration().put(Dataset.ACTOR_CODE, code);
		ds.update();

		// check actor
		JSActor actor = jsExecService.getActor(ds.getId());
		if (actor == null) {
			// actor is not available, craete new one
			actor = jsExecService.addActor(ds);
		}

		// try to set an compile the code, return depending on compilation result
		if (actor.setCode(code, user)) {
			return ok();
		} else {
			return badRequest("Problem in code.");
		}
	}

	/**
	 * install this actor on an OOCSI channel or Telegram input
	 * 
	 * @param request
	 * @param id
	 * @param channelName
	 * @return
	 */
	public Result install(Request request, long id, String channelName) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.editableBy(user.getEmail())) {
			return forbidden("This did not work.");
		}

		// check that this actor is active
		if (!ds.isActive()) {
			String output = "This script dataset is inactive and cannot be registered on channel. Change start and end to install.";
			return badRequest(output).as(MimeTypes.HTML);
		}

		// ensure that users don't have too many scripts running at a time
		if (!channelName.isEmpty() && !ds.isScriptLive() && user.getLiveUserActors().size() > 4) {
			String output = "You have more than five scripts running live. Deactivate one to activate this script.";
			return badRequest(output).as(MimeTypes.HTML);
		}

		// modify channel name for Telegram subscriptions
		channelName = channelName.replace("\"", "");
		if (channelName.toLowerCase().startsWith("telegram")) {
			channelName = channelName + "_" + ds.getProject().getId();
		}

		String code = nss(request.body().asText());
		// unscramble the contents if necessary
		code = StringUtils.unscrambleTransport(code);

		// update dataset
		ds.getConfiguration().put(Dataset.ACTOR_CHANNEL, channelName);
		ds.getConfiguration().put(Dataset.ACTOR_CODE, code);
		ds.update();

		// check actor
		JSActor actor = jsExecService.getActor(ds.getId());
		if (actor == null) {
			// actor is not available, craete new one
			actor = jsExecService.addActor(ds);
		}

		actor.messages.clear();
		actor.setCode(code, user);

		return ok();
	}

	public Result log(Request request, long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript() || !ds.editableBy(username)) {
			return ok("error: dataset not found").as(MimeTypes.HTML);
		}

		if (!ds.isActive()) {
			String output = "<span style=\"color:red;\">This script dataset is inactive and cannot be registered on channel. Change start and end to install.</span>";
			return ok(output).as(MimeTypes.HTML);
		}

		// check actor
		JSActor actor = jsExecService.getActor(ds.getId());
		if (actor == null) {
			// actor is not available, create new one
			actor = jsExecService.addActor(ds);
		}

		return ok(actor.messages.stream().collect(Collectors.joining())).as(MimeTypes.HTML);
	}

	public Result fileList(Request request, long id) {

		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript() || !ds.editableBy(username)) {
			return ok("").as(MimeTypes.HTML);
		}

		final CompleteDS cpsc = (CompleteDS) datasetConnector.getDatasetDS(ds);
		final List<TimedMedia> fileList = cpsc.getFiles();
		return ok(views.html.tools.actor.files.render(ds, fileList));
	}

	public Result file(Request request, long id, String filename) {

		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript() || !ds.editableBy(username)) {
			return notFound("file not found").as(MimeTypes.HTML);
		}

		final CompleteDS cpsc = (CompleteDS) datasetConnector.getDatasetDS(ds);
		Optional<File> file = cpsc.getFile(filename);
		if (!file.isPresent()) {
			return notFound("file not found").as(MimeTypes.HTML);
		}

		return ok(file.get()).as(MimeTypes.HTML);
	}
}

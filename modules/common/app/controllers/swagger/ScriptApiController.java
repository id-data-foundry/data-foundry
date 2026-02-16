
package controllers.swagger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.auth.V2UserApiAuth;
import datasets.DatasetConnector;
import jakarta.inject.Singleton;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.jsexecutor.JSActor;
import services.jsexecutor.JSExecutorService;
import utils.auth.TokenResolverUtil;
import utils.export.MetaDataUtils;

@Singleton
public class ScriptApiController extends AbstractApiController {

	private final JSExecutorService jsExecService;

	@Inject
	public ScriptApiController(FormFactory formFactory, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolverUtil, JSExecutorService jsExecService) {
		super(formFactory, datasetConnector, tokenResolverUtil);
		this.jsExecService = jsExecService;
	}

	@Authenticated(V2UserApiAuth.class)
	public Result view(Request request, long id) {
		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript()) {
			return notFound(errorJSONResponseObject("Not valid script."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check user role
		if (!ds.getProject().belongsTo(user) && !ds.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Invalid user or script."));
		}

		return ok(toJSON(ds));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result add(Request request, long id) {

		// find project
		Project p = Project.find.byId(id);

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check project and user role
		if (p == null || !p.belongsTo(user) && !p.collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Invalid user or project."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		// check dataset name
		String dsName = df.get("script_name") == null ? null : df.get("script_name");
		if (dsName == null) {
			return badRequest(errorJSONResponseObject("The name for this new script is required."));
		}

		// always for COMPLETE dataset
		String openParticipation = df.get("isOpenParticipation");
		if (nnne(openParticipation)) {
			if (openParticipation.equals("true")) {
				openParticipation = "true";
			} else {
				openParticipation = null;
			}
		}
		Dataset ds = datasetConnector.create(dsName, DatasetType.COMPLETE, p, df.get("description"),
				df.get("target_object"), openParticipation, df.get("license"));
		ds.setCollectorType(Dataset.ACTOR);
		ds.save();

		// create actor
		jsExecService.addActor(ds);

		LabNotesEntry.log(ScriptApiController.class, LabNotesEntryType.CREATE, "Script created: " + ds.getName(),
				ds.getProject());

		ObjectNode on = okJSONResponseObject();
		on.put("id", ds.getId());
		return ok(on);
	}

	@Authenticated(V2UserApiAuth.class)
	public Result edit(Request request, long id) {

		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript()) {
			return notFound(errorJSONResponseObject("Not valid script."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check user role
		if (!ds.getProject().belongsTo(user) && !ds.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Invalid user or script."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		boolean changed = false;
		// check data
		if (df.get("script_name") != null) {
			ds.setName(df.get("script_name"));
			changed = true;
		}

		if (df.get("description") != null) {
			ds.setDescription(df.get("description"));
			changed = true;
		}

		if (df.get("target_object") != null) {
			ds.setTargetObject(df.get("target_object"));
			changed = true;
		}

		if (df.get("isOpenParticipation") != null) {
			if (df.get("isOpenParticipation").equals("true")) {
				ds.setOpenParticipation(true);
			} else {
				ds.setOpenParticipation(false);
			}
			changed = true;
		}

		if (changed) {
			ds.save();
		}

		LabNotesEntry.log(ScriptApiController.class, LabNotesEntryType.MODIFY, "Script edited: " + ds.getName(),
				ds.getProject());

		return ok(okJSONResponseObject("Information updated."));
	}

	/**
	 * execute the scripting code in a trial actor that is not connected to incoming data
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(V2UserApiAuth.class)
	public CompletionStage<Result> execute(Request request, long id) {

		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript()) {
			return notFoundCS(errorJSONResponseObject("Not valid script."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFoundCS(errorJSONResponseObject("Not valid user."));
		}

		// check user role
		if (!ds.getProject().belongsTo(user) && !ds.getProject().collaboratesWith(user)) {
			return forbiddenCS(errorJSONResponseObject("Invalid user or script."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequestCS(errorJSONResponseObject("No data given."));
		}
		if (df.get("data") == null) {
			return badRequestCS(errorJSONResponseObject("No test data given."));
		}

		String code = df.get("code") == null ? ds.getConfiguration().get(Dataset.ACTOR_CODE) : df.get("code");
		if (!nnne(code)) {
			return badRequestCS(errorJSONResponseObject("No code given."));
		}

		// check actor
		JSActor actor = jsExecService.getTrialActor(ds.getId());
		if (actor == null) {
			// actor is not available, craete new one
			actor = jsExecService.addTrialActor(ds);
		}

		final JSActor finalActor = actor;
		return CompletableFuture.supplyAsync(() -> finalActor.runTrial(df.get("data") + "\n" + code))
				.thenApplyAsync(output -> ok(output));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result saveCode(Request request, long id) {

		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript()) {
			return notFound(errorJSONResponseObject("Not valid script."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check user role
		if (!ds.getProject().belongsTo(user) && !ds.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Invalid user or script."));
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest(errorJSONResponseObject("No data given."));
		}

		String code = df.get("code");
		if (code == null) {
			return badRequest(errorJSONResponseObject("No code given."));
		}

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
			return ok(okJSONResponseObject("Code saved."));
		} else {
			return badRequest("Problem in code.");
		}
	}

	@Authenticated(V2UserApiAuth.class)
	public Result install(Request request, long id, String channelName) {

		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript()) {
			return notFound(errorJSONResponseObject("Not valid script."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check user role
		if (!ds.getProject().belongsTo(user) && !ds.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Invalid user or script."));
		}

		// modify channel name for Telegram subscriptions
		if (channelName == null) {
			return badRequest(errorJSONResponseObject("No channel name given."));
		} else if (channelName.toLowerCase().startsWith("telegram")) {
			channelName = channelName + "_" + ds.getProject().getId();
		}

		// update dataset
		ds.getConfiguration().put(Dataset.ACTOR_CHANNEL, channelName);
		// ds.configuration.put(Dataset.ACTOR_CODE, code);
		ds.update();

		// check actor
		JSActor actor = jsExecService.getActor(ds.getId());
		if (actor == null) {
			// actor is not available, craete new one
			actor = jsExecService.addActor(ds);
		}

		// actor.setCode(code);
		actor.messages.clear();

		return ok(okJSONResponseObject("Done."));
	}

	@Authenticated(V2UserApiAuth.class)
	public Result unInstall(Request request, long id) {
		return install(request, id, "");
	}

	@Authenticated(V2UserApiAuth.class)
	public Result log(Request request, long id) {

		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript()) {
			return notFound(errorJSONResponseObject("Not valid script."));
		}

		// check user
		Person user = Person.find.byId(getAuthenticatedAPIUserId(request));
		if (user == null) {
			return notFound(errorJSONResponseObject("Not valid user."));
		}

		// check user role
		if (!ds.getProject().belongsTo(user) && !ds.getProject().collaboratesWith(user)) {
			return forbidden(errorJSONResponseObject("Invalid user or script."));
		}

		// check actor
		JSActor actor = jsExecService.getActor(ds.getId());
		if (actor == null) {
			// actor is not available, craete new one
			actor = jsExecService.addActor(ds);
		}

		// compile output HTML string
		String output = actor.messages.stream().collect(Collectors.joining());
		return ok(output);
	}

	//////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode toJSON(Dataset dataset) {
		ObjectNode newObject = MetaDataUtils.toJson(dataset);

		// add addtitional information
		newObject.put("isOpenParticipation", dataset.isOpenParticipation());
		newObject.put("project_id", dataset.getProject().getId());
		newObject.put("code", dataset.getConfiguration().get(Dataset.ACTOR_CODE));

		return newObject;
	}

}
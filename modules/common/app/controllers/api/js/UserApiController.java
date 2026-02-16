package controllers.api.js;

import java.util.Optional;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.DatasetsController;
import controllers.swagger.AbstractApiController;
import datasets.DatasetConnector;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.EntityDS;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import utils.auth.TokenResolverUtil;

public class UserApiController extends AbstractApiController {

	final DatasetsController datasetsController;
	final TokenResolverUtil tokenResolverUtil;
	final DatasetConnector datasetConnector;

	@Inject
	public UserApiController(DatasetsController datasetsController, FormFactory formFactory,
	        TokenResolverUtil tokenResolverUtil, DatasetConnector datasetConnector) {
		super(formFactory, datasetConnector, tokenResolverUtil);

		this.datasetsController = datasetsController;
		this.tokenResolverUtil = tokenResolverUtil;
		this.datasetConnector = datasetConnector;
	}

	public Result jsAPI(Request request, long projectId) {
		Person user = getAuthenticatedUserOrReturn(request, notFound("").as(TEXT_JAVASCRIPT));

		// compute user id as stable participation token
		String userEntityToken = tokenResolverUtil.getStableParticipationToken(projectId, user.getId());

		// set default empty profile
		ObjectNode on = Json.newObject();

		// check project to acquire the user profile
		Project project = Project.find.byId(projectId);
		if (project != null && project.visibleFor(user)) {
			// from first entity dataset
			Dataset ds = project.getEntityDataset();
			if (ds != Dataset.EMPTY_DATASET) {
				EntityDS entityDS = (EntityDS) datasetConnector.getDatasetDS(ds);
				on = entityDS.getItem(userEntityToken, Optional.empty()).orElse(Json.newObject());
			}
		}

		String token = tokenResolverUtil.getParticipationToken(projectId, user.getId());
		String output = views.html.elements.api.userJSAPI
		        .render(userEntityToken, projectId, token, on.toString(), request).toString().replace("</script>", "");
		return ok(output).as(TEXT_JAVASCRIPT);
	}

	public Result setItem(Request request, long id, String token) {

		Person user = getAuthenticatedUserOrReturn(request, notFound("").as(TEXT_JAVASCRIPT));

		long projectId = tokenResolverUtil.getProjectIdFromParticipationToken(token);
		long participantId = tokenResolverUtil.getParticipantIdFromParticipationToken(token);

		if (projectId == -1 || participantId == -1 || !user.getId().equals(participantId)) {
			return notFound();
		}

		// compute user id
		String user_id = tokenResolverUtil.getParticipationToken(projectId, user.getId());

		// check project to acquire the user profile
		Project project = Project.find.byId(projectId);
		if (project == null || !project.visibleFor(user)) {
			return notFound();
		}

		// setitem
		Dataset ds = project.getEntityDataset();
		if (ds == Dataset.EMPTY_DATASET) {
			return notFound();
		}
		// get the request data
		DynamicForm df = formFactory.form().bindFromRequest(request);
		String key = (String) df.value("key").orElse("");
		String value = (String) df.value("value").orElse("");

		if (key.isEmpty()) {
			return badRequest();
		}

		// update profile
		EntityDS entityDS = (EntityDS) datasetConnector.getDatasetDS(ds);
		ObjectNode on = entityDS.getItem(user_id, Optional.empty()).orElse(Json.newObject());
		on.put(key, value);
		entityDS.updateItem(user_id, Optional.empty(), on);

		return ok();
	}

}

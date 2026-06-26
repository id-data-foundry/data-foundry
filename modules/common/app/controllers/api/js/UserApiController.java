package controllers.api.js;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.DatasetsController;
import controllers.swagger.AbstractApiController;
import datasets.DatasetConnector;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.ds.EntityDS;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.cache.SyncCacheApi;
import utils.auth.TokenResolverUtil;
import utils.validators.FileTypeUtils;

public class UserApiController extends AbstractApiController {

	final DatasetsController datasetsController;
	final TokenResolverUtil tokenResolverUtil;
	final DatasetConnector datasetConnector;
	final SyncCacheApi cache;

	private static final play.Logger.ALogger logger = play.Logger.of(UserApiController.class);

	@Inject
	public UserApiController(DatasetsController datasetsController, FormFactory formFactory,
			TokenResolverUtil tokenResolverUtil, DatasetConnector datasetConnector, SyncCacheApi cache) {
		super(formFactory, datasetConnector, tokenResolverUtil);

		this.datasetsController = datasetsController;
		this.tokenResolverUtil = tokenResolverUtil;
		this.datasetConnector = datasetConnector;
		this.cache = cache;
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

		long completeDsId = -1L;
		if (project != null) {
			Dataset completeDs = project.getCompleteDataset();
			if (completeDs != null) {
				completeDsId = completeDs.getId();
			}
		}

		String token = tokenResolverUtil.getParticipationToken(projectId, user.getId());
		String output = views.html.elements.api.userJSAPI
				.render(userEntityToken, projectId, token, on.toString(), completeDsId, request).toString()
				.replace("</script>", "");
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
		String user_id = tokenResolverUtil.getStableParticipationToken(projectId, user.getId());

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

	public Result uploadFile(Request request, long id, String token) {
		Person user = getAuthenticatedUserOrReturn(request, forbidden(Json.newObject().put("error", "Not logged in")));

		long projectId = tokenResolverUtil.getProjectIdFromParticipationToken(token);
		long participantId = tokenResolverUtil.getParticipantIdFromParticipationToken(token);

		if (projectId == -1 || participantId == -1 || !user.getId().equals(participantId) || projectId != id) {
			return forbidden(Json.newObject().put("error", "Invalid token."));
		}

		// check project
		Project project = Project.find.byId(projectId);
		if (project == null || !project.visibleFor(user)) {
			return notFound(Json.newObject().put("error", "Project not found."));
		}

		// check write/edit access
		if (!project.editableBy(user)) {
			return forbidden(Json.newObject().put("error",
					"You need to be either project owner or collaborator to perform this action."));
		}

		// get COMPLETE dataset
		Dataset ds = project.getCompleteDataset();
		if (ds == null || ds == Dataset.EMPTY_DATASET) {
			return notFound(
					Json.newObject().put("error", "No complete dataset available for file upload in this project."));
		}

		if (!ds.canAppend()) {
			return forbidden(Json.newObject().put("error", "Dataset is closed (adjust start and end dates to open)."));
		}

		try {
			Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
			if (body == null) {
				return badRequest(Json.newObject().put("error", "Body malformed."));
			}

			DynamicForm df = formFactory.form().bindFromRequest(request);
			if (df == null) {
				return badRequest(Json.newObject().put("error", "Data malformed."));
			}

			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
			if (!fileParts.isEmpty()) {
				final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

				List<String> savedFiles = new ArrayList<>();

				for (int i = 0; i < fileParts.size(); i++) {
					FilePart<TemporaryFile> filePart = fileParts.get(i);
					TemporaryFile tempfile = filePart.getRef();
					String fileName = nss(filePart.getFilename());

					// filename-based quick check
					if (FileTypeUtils.looksLikeExecutableFile(fileName)) {
						logger.error("API upload attempt blocked due to executable-like filename: " + fileName);
						continue;
					}

					// content-based validation
					if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.ANY)) {
						continue;
					}

					// store file, add record
					Optional<String> storeFile = cpds.storeFile(tempfile.path().toFile(), fileName);
					if (storeFile.isPresent()) {
						String description = nss(df.get("description"));
						savedFiles.add(storeFile.get());
						cpds.addRecord(storeFile.get(), description, new Date());
					}
				}

				// invalidate cache
				cache.remove(controllers.api.CompleteDSController.CACHE_FILES + ds.getId());

				models.LabNotesEntry.log(UserApiController.class, models.LabNotesEntry.LabNotesEntryType.DATA,
						"Files uploaded to dataset: " + ds.getName(), ds.getProject());

				ObjectNode result = Json.newObject();
				result.set("files", Json.toJson(savedFiles));
				return ok(result).as("application/json");
			}
		} catch (Exception e) {
			logger.error("Error in uploading dataset file via UserApiController", e);
		}

		return badRequest(Json.newObject().put("error", "Missing files.")).as("application/json");
	}

}

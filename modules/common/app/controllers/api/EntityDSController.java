package controllers.api;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.auth.DatasetApiAuth;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Project;
import models.ds.EntityDS;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.StringUtils;
import utils.components.OnboardingSupport;

public class EntityDSController extends AbstractDSController {

	private static final Logger.ALogger logger = Logger.of(EntityDSController.class);

	@Inject
	public EntityDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
			OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);
	}

    private Optional<String> validateApiToken(
        Request request,
        Dataset ds,
        EntityDS eds,
        String resource_id,
        String dsApiToken
    ) {
        final String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);

        // Try parameter token first
        if (dsApiToken != null && internalToken.equals(dsApiToken)) {
            return Optional.ofNullable(dsApiToken);
        }

        // Try header token
        final String headerToken = request.header("api_token").orElse("");
        if (!headerToken.isEmpty() && internalToken.equals(headerToken)) {
            return Optional.ofNullable(headerToken);
        }

        // Invalid or missing token - check user permissions for item-level access
        if (!headerToken.isEmpty()) {
            String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));
            if (username != null && !username.isEmpty() && ds.editableBy(username)) {
                return eds.internalGetItemToken(resource_id);
            } else {
                throw new RuntimeException("No token given, no user logged in with edit permissions for this dataset.");
            }
        }

        throw new RuntimeException("Api token is not correct");
    }
	
	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// find dataset
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		// check permissions
		if (!ds.visibleFor(username)) {
			return redirect(PROJECT(ds.getProject().getId())).addingToSession(request, "error",
					"Dataset is not visible.");
		}

		return ok(views.html.datasets.entity.view.render(ds, username, request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"Project not valid or you don't have permissions for this action. Need to be the owner or "
							+ "a collaborator of the project.");
		}

		// display the add form page
		return ok(views.html.datasets.entity.add.render(csrfToken(request), p));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"Project not valid or you don't have permissions for this action. Need to be the owner or "
							+ "a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data");
		}

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.ENTITY, p, df.get("description"),
				df.get("target_object"), df.get("isPublic"), df.get("license"));

		// dates
		storeDates(ds, df);
		ds.save();

		// auto-generate the API token for sending data to the dataset
		ds.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds.getId()));
		ds.update();

		onboardingSupport.updateAfterDone(username, "new_dataset");

		// display the add form page
		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You don't have permissions for this action. Need to be the owner or a collaborator of the "
							+ "project.");
		}

		// display the add form page
		return ok(views.html.datasets.entity.edit.render(csrfToken(request), ds));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You don't have permissions for this action. Need to be the owner or a collaborator of the "
							+ "project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Expecting some data");
		}

		ds.setName(htmlTagEscape(nss(df.get("dataset_name"), 64)));
		ds.setDescription(htmlTagEscape(nss(df.get("description"))));
		ds.setTargetObject(nss(df.get("target_object")));
		ds.setOpenParticipation(df.get("isPublic") == null ? false : true);
		ds.setLicense(df.get("license"));

		// dates
		storeDates(ds, df);

		// metadata
		storeMetadata(ds, df);
		ds.update();

		// display the add form page
		return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "message",
				"Changes saved.");
	}

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> getItemsNested(Request request, Long id) {
		// check user
		String username = getAuthenticatedUserName(request).orElse(null);
		if (username == null) {
			return CompletableFuture.completedFuture(forbidden());
		}

		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(errorJSONResponseObject("Dataset not found."));
			}

			// check access
			if (!ds.visibleFor(username)) {
				return forbidden(errorJSONResponseObject("Project is not accessible."));
			}

			// get items
			final EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);
			return ok(eds.getItemsNested()).as("application/json");
		}).exceptionally(e -> {
			logger.error("Entity dataset index problem", e);
			return badRequest();
		});
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> getItems(Long id) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(errorJSONResponseObject("Dataset not found."));
			}

			// only allow this access if the project is public
			if (!ds.getProject().isPublicProject()) {
				return forbidden(errorJSONResponseObject("Project is not public."));
			}

			// get items
			final EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);
			return ok(eds.getItems()).as("application/json");
		}).exceptionally(e -> {
			logger.error("Entity dataset index problem", e);
			return badRequest();
		});
	}

	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> getItemsMatching(Request request, Long id) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(errorJSONResponseObject("Dataset not found."));
			}

			// check all data
			final String resource_id = request.header("resource_id").orElse("");
			String token = request.header("token").orElse(request.header("api_token").orElse(""));
			if (resource_id.length() == 0 || token.length() == 0) {
				return badRequest(errorJSONResponseObject("No proper resource_id or token given."));
			}

			// get items
			final EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);
			return ok(eds.getItemsMatching(resource_id, Optional.of(token))).as("application/json");
		}).exceptionally(e -> {
			logger.error("Entity dataset index problem", e);
			return badRequest();
		});
	}

	public CompletionStage<Result> addItemApi(Request request, Long id, final String dsApiToken) {
		return addItem(request, id, dsApiToken);
	}

	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> addItem(Request request, Long id, final String dsApiToken) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(errorJSONResponseObject("Dataset not found."));
			}

			// check all data
			final JsonNode jn = request.body().asJson();
			if (jn == null || jn.size() == 0) {
				return badRequest(errorJSONResponseObject("No data given."));
			}
			if (!jn.isObject()) {
				return badRequest(errorJSONResponseObject("Data is not a valid JSON object."));
			}
			final String resource_id = request.header("resource_id").orElse("");
			if (resource_id.isEmpty()) {
				return badRequest(errorJSONResponseObject("No resource_id given."));
			}

			// check API token (both internal and configuration)
            final EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
            String token;
            final String checkToken = dsApiToken;
            final String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
            if (checkToken == null || (!checkToken.equals(internalToken))) {
                // try to get token from request body if dsApiToken is empty or invalid
                final String checkBodyToken = request.header("api_token").orElse("");
                if (
                    checkBodyToken.isEmpty() ||
                    (!checkBodyToken.equals(internalToken))
                ) {
                    return forbidden("Api token is not correct");
                } else {
                    token = checkBodyToken;
                }
            } else {
                token = checkToken;
            }

			// ADD operation
			Optional<ObjectNode> data = eds.addItem(resource_id, Optional.of(token), jn);
			if (!data.isPresent()) {
				return badRequest("JSON data invalid");
			} else {
				return ok(data.orElse(Json.newObject())).as("application/json");
			}
		}).exceptionally(e -> {
			Throwable cause = e.getCause() != null ? e.getCause() : e;
			if (cause instanceof ResponseException) {
				return ((ResponseException) cause).getResponse();
			}

			logger.error("Entity dataset add problem", e);
			return badRequest();
		});
	}

	public CompletionStage<Result> getItemApi(Request request, Long id, final String dsApiToken) {
		return getItem(request, id, dsApiToken);
	}

	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> getItem(Request request, Long id, final String dsApiToken) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(errorJSONResponseObject("Dataset not found."));
			}

			// check all data
			final String resource_id = request.header("resource_id").orElse("");
			if (resource_id.isEmpty()) {
				return badRequest(errorJSONResponseObject("No resource_id given."));
			}

			final EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
            String tokenOpt;
            // check API token (both internal and configuration)
            final String checkToken = dsApiToken;
            final String internalToken = ds.getConfiguration().get(Dataset.API_TOKEN);
            if (checkToken == null || (!ds.getApiToken().equals(checkToken) && !checkToken.equals(internalToken))) {
                // try to get token from request body if dsApiToken is empty or invalid
                final String checkBodyToken = request.header("api_token").orElse("");
                if (
                    checkBodyToken.isEmpty() ||
                    (!checkBodyToken.equals(internalToken))
                ) {
                    return forbidden("Api token is not correct");
                } else {
                    tokenOpt = checkBodyToken;
                }
            } else {
                tokenOpt = checkToken;
            }

			// GET operation
			Optional<ObjectNode> data = eds.getItem(resource_id, Optional.of(tokenOpt));
			if (data.isEmpty()) {
				return notFound("Item not found or incorrect token given.");
			} else {
				return ok(data.get()).as("application/json");
			}
		}).exceptionally(e -> {
			Throwable cause = e.getCause() != null ? e.getCause() : e;
			if (cause instanceof ResponseException) {
				return ((ResponseException) cause).getResponse();
			}

			logger.error("Entity dataset get problem", e);
			return badRequest();
		});
	}

	public CompletionStage<Result> updateItemApi(Request request, Long id, final String dsApiToken) {
		return updateItem(request, id, dsApiToken);
	}

	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> updateItem(Request request, Long id, final String dsApiToken) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(errorJSONResponseObject("Dataset not found."));
			}

			// check all data
			final JsonNode jn = request.body().asJson();
			if (jn == null || jn.size() == 0) {
				return badRequest(errorJSONResponseObject("No data given."));
			}
			if (!jn.isObject()) {
				return badRequest(errorJSONResponseObject("Data is not a valid JSON object."));
			}
			final String resource_id = request.header("resource_id").orElse("");
			if (resource_id.isEmpty()) {
				return badRequest(errorJSONResponseObject("No resource_id given."));
			}

			final EntityDS eds = datasetConnector.getTypedDatasetDS(ds);

            // Validate API token and get item-level token if needed
            final Optional<String> tokenOpt;
            try {
                tokenOpt = validateApiToken(request, ds, eds, resource_id, dsApiToken);
            } catch (RuntimeException e) {
                return badRequest(errorJSONResponseObject(e.getMessage()));
            }

			// UPDATE operation
			Optional<ObjectNode> data = eds.updateItem(resource_id, tokenOpt, jn);
			if (!data.isPresent()) {
				return notFound("Item not found or incorrect token given.");
			} else {
				return ok(data.orElse(Json.newObject())).as("application/json");
			}
		}).exceptionally(e -> {
			Throwable cause = e.getCause() != null ? e.getCause() : e;
			if (cause instanceof ResponseException) {
				return ((ResponseException) cause).getResponse();
			}

			logger.error("Entity dataset update problem", e);
			return badRequest();
		});
	}

	public CompletionStage<Result> deleteItemApi(Request request, Long id, final String dsApiToken) {
		return deleteItem(request, id, dsApiToken);
	}

	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> deleteItem(Request request, Long id, final String dsApiToken) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound(errorJSONResponseObject("Dataset not found."));
			}

			// check resourcE_id and token data
			final String resource_id = request.header("resource_id").orElse("");
			if (resource_id.isEmpty()) {
				return badRequest(errorJSONResponseObject("No resource_id given."));
			}

			final EntityDS eds = datasetConnector.getTypedDatasetDS(ds);
            // Validate API token and get item-level token if needed
            final Optional<String> tokenOpt;
            try {
                tokenOpt = validateApiToken(request, ds, eds, resource_id, dsApiToken);
            } catch (RuntimeException e) {
                return badRequest(errorJSONResponseObject(e.getMessage()));
            }

			// DELETE operation
			Optional<ObjectNode> data = eds.deleteItem(resource_id, tokenOpt);
			if (!data.isPresent()) {
				return notFound("Item not found");
			} else {
				return ok(data.get()).as("application/json");
			}
		}).exceptionally(e -> {
			Throwable cause = e.getCause() != null ? e.getCause() : e;
			if (cause instanceof ResponseException) {
				return ((ResponseException) cause).getResponse();
			}

			logger.error("Entity dataset delete problem", e);
			return badRequest();
		});
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result table(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		}

		if (!ds.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not accessible.");
		}

		return ok(views.html.datasets.entity.table.render(ds, request));
	}

	@Authenticated(UserAuth.class)
	public Result tableTree(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		}

		if (!ds.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not accessible.");
		}

		return ok(views.html.datasets.entity.tableTree.render(ds, request));
	}

	@Authenticated(UserAuth.class)
	public Result tableAddParticipants(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not accessible.");
		}

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// add participant objects into the data table
		p.getParticipants().stream().forEach(participant -> {
			eds.addResourceItem(participant, new Date(), Json.newObject());
		});

		return redirect(routes.EntityDSController.table(ds.getId())).addingToSession(request, "message",
				StringUtils.pluralize("participant", p.getParticipants().size()) + " added.");
	}

	@Authenticated(UserAuth.class)
	public Result tableAddWearables(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not accessible.");
		}

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// add participant objects into the data table
		p.getWearables().stream().forEach(wearable -> {
			eds.addResourceItem(wearable, new Date(), Json.newObject());
		});

		return redirect(routes.EntityDSController.table(ds.getId())).addingToSession(request, "message",
				StringUtils.pluralize("wearable", p.getWearables().size()) + " added.");
	}

	@Authenticated(UserAuth.class)
	public Result tableAddDevices(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not accessible.");
		}

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// add participant objects into the data table
		p.getDevices().stream().forEach(device -> {
			eds.addResourceItem(device, new Date(), Json.newObject());
		});

		return redirect(routes.EntityDSController.table(ds.getId())).addingToSession(request, "message",
				StringUtils.pluralize("device", p.getDevices().size()) + " added.");
	}

	@Authenticated(UserAuth.class)
	public Result recordForm(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project not accessible.");
		}

		return ok(views.html.datasets.entity.record.render(ds, request));
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletionStage<Result> downloadExternal(Dataset ds, long limit, long start, long end) {
		return CompletableFuture.supplyAsync(() -> internalExport(ds, limit, start, end))
				.thenApplyAsync(chunks -> ok().chunked(chunks)
						.withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
						.as("text/csv"));
	}

	public CompletionStage<Result> downloadInternal(Dataset ds, long limit, long start, long end) {
		return CompletableFuture.supplyAsync(() -> internalExport(ds, limit, start, end))
				.thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadRaw(Request request, Long id, long limit, long start, long end) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		Project project = ds.getProject();
		project.refresh();
		if (!project.visibleFor(username)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "Project not accessible."));
		}

		if (acceptLicenseFirst(request, username, project)) {
			// redirect to accept license first
			return redirectCS(controllers.routes.ProjectsController.license(project.getId(),
					routes.EntityDSController.downloadRaw(id, limit, start, end).relativeTo("/")));
		}

		return CompletableFuture.supplyAsync(() -> internalExportRaw(ds, limit, start, end))
				.thenApplyAsync(chunks -> ok().chunked(chunks)
						.withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
						.as("text/csv"));
	}

	public CompletionStage<Result> downloadRawPublic(Request request, String token) {

		// retrieve id from token
		Long id = tokenResolverUtil.getDatasetIdFromToken(token);

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check if the token is current
		String pat = ds.getConfiguration().get(Dataset.PUBLIC_ACCESS_TOKEN);
		if (!token.equals(pat)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "Dataset access token is incorrect."));
		}

		return CompletableFuture.supplyAsync(() -> internalExportRaw(ds, -1, -1, -1))
				.thenApplyAsync(chunks -> ok().chunked(chunks)
						.withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
						.as("text/csv"));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * internal export projected data (if projection is available); exports all items in their last states, but not the
	 * item tokens
	 *
	 * @param ds
	 * @param end
	 * @param start
	 * @return
	 */
	private Source<ByteString, ?> internalExport(Dataset ds, long limit, long start, long end) {
		EntityDS tssc = (EntityDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> tssc.exportProjected(sourceActor, start, end));
			return sourceActor;
		});
	}

	/**
	 * internal export just the raw data; exports all items and all updates in the entity, but not the item tokens
	 *
	 * @param ds
	 * @param end
	 * @param start
	 * @return
	 */
	private Source<ByteString, ?> internalExportRaw(Dataset ds, long limit, long start, long end) {
		EntityDS tssc = (EntityDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> tssc.exportLogProjected(sourceActor, start, end));
			return sourceActor;
		});
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode errorJSONResponseObject(String message) {
		ObjectNode on = Json.newObject();
		on.put("result", "error");
		on.put("message", message);
		return on;
	}

}

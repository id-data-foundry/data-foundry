package services.api;

import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.EntityDS;
import models.ds.LinkedDS;
import play.Logger;
import play.libs.Json;
import services.slack.Slack;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;

abstract public class GenericApiService implements ApiServiceConstants {

	public static final String SYSTEM_OPEN_AI_API_SERVICE = "SYSTEM_OPEN_AI_API_SERVICE";

	private static final int DEFAULT_STARTING_CREDITS = 2000;

	private static final Logger.ALogger logger = Logger.of(GenericApiService.class);

	static final String CURRENT_TOKEN = "currentToken";

	static final String CREATED = "created";

	protected final TokenResolverUtil tokenResolver;
	protected final long datastoreDSId;
	protected EntityDS datastore;

	protected GenericApiService(Config configuration, AdminUtils adminUtils, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolver) {

		this.tokenResolver = tokenResolver;

		// check dataset availability
		final Dataset datastoreDataset;
		Optional<Dataset> dsOpt = Dataset.find.query().where().eq("refId", SYSTEM_OPEN_AI_API_SERVICE).findOneOrEmpty();
		if (dsOpt.isEmpty()) {
			// no dataset available yet for services

			// is admin user available?
			if (adminUtils.getFirstAdminUser().isEmpty()) {
				// not available -> don't create one now
				datastoreDSId = -1L;
				datastore = null;
				return;
			}

			// is there a system project?
			Optional<Project> systemProjectOpt = Project.find.query().where().eq("refId", AdminUtils.SYSTEM_PROJECT)
			        .findOneOrEmpty();

			Project systemProject;
			if (systemProjectOpt.isEmpty()) {
				// first create a project
				systemProject = Project.create(AdminUtils.SYSTEM_PROJECT, adminUtils.getFirstAdminUser().get(), "",
				        false, false);
				systemProject.setRefId(AdminUtils.SYSTEM_PROJECT);
				systemProject.save();
				logger.info("Created system project.");
			} else {
				systemProject = systemProjectOpt.get();
			}

			// then the dataset in that project
			final Dataset tempDataset = new Dataset();
			tempDataset.setName(SYSTEM_OPEN_AI_API_SERVICE);
			tempDataset.setDsType(DatasetType.ENTITY);
			tempDataset.setRefId(SYSTEM_OPEN_AI_API_SERVICE);
			tempDataset.setApiToken(UUID.randomUUID().toString());
			tempDataset.setProject(systemProject);
			tempDataset.setDescription("");
			tempDataset.setTargetObject("");
			tempDataset.setOpenParticipation(false);
			tempDataset.start();
			tempDataset.end();
			tempDataset.save();

			// create an instance of the linked data set
			LinkedDS lds = datasetConnector.getDatasetDS(tempDataset);
			lds.createInstance();

			systemProject.getDatasets().add(tempDataset);
			systemProject.update();

			tempDataset.refresh();
			datastoreDataset = tempDataset;

			logger.info("Created entity table for the OpenAI API service.");
		} else {
			datastoreDataset = dsOpt.get();
		}

		datastoreDSId = datastoreDataset.getId();

		// store instance of datastore
		datastore = datasetConnector.getTypedDatasetDS(datastoreDataset);
	}

	/**
	 * generic check credits function
	 * 
	 * @param apiKey
	 * @return
	 */
	protected Optional<String> checkCredits(String apiKey) {
		int tokens = -1, maxTokens = -1;

		// return if API functionality needs to be blocked because the token DB is not available
		if (datastoreDSId == -1L || datastore == null) {
			return Optional.empty();
		}

		// this is needed to isolate database access in case multiple requests come in at the same time
		synchronized (datastore) {
			Optional<ObjectNode> profileOpt = datastore.getItem(apiKey, Optional.empty());
			if (profileOpt.isEmpty() || profileOpt.get().isEmpty()) {
				return Optional.of(Json.newObject().put(RESPONSE_ERROR, "No valid API key provided.").toString());
			}

			// profile key provided, let's look up the credits...

			// check profile for available credits
			ObjectNode profile = profileOpt.get();
			if (!profile.has(TOKENS_USED) || !profile.has(TOKENS_MAX)) {
				return Optional.of(Json.newObject().put(RESPONSE_ERROR, "No token credits remaining.").toString());
			}

			// assign tokens from profile
			tokens = profile.path(TOKENS_USED).asInt(0);
			maxTokens = profile.path(TOKENS_MAX).asInt(2000);

			if (tokens > -1 && maxTokens > -1) {
				return Optional.of(Json.newObject().put(TOKENS_USED, tokens).put(TOKENS_MAX, maxTokens).toString());
			} else {
				return Optional.of(Json.newObject()
				        .put(RESPONSE_ERROR, "Please ensure a correct DF API key is provided.").toString());
			}
		}
	}

	/**
	 * generic check and update credits function
	 * 
	 * @param apiToken
	 * @param requestedTokens
	 * @return
	 */
	protected Optional<String> checkAndUpdateCredits(String apiToken, int requestedTokens) {
		int tokens = -1, maxTokens = -1;

		// return if API functionality needs to be blocked because the token DB is not available
		if (datastoreDSId == -1L || datastore == null) {
			return Optional.empty();
		}

		// this is needed to isolate database access in case multiple requests come in at the same time
		synchronized (datastore) {
			Optional<ObjectNode> profileOpt = datastore.getItem(apiToken, Optional.empty());
			if (profileOpt.isEmpty() || profileOpt.get().isEmpty()) {
				return Optional.of(Json.newObject().put(RESPONSE_ERROR, "No valid API key provided.").toString());
			}
			// profile key provided, let's look up the credits
			else {
				// check profile for available credits
				ObjectNode profile = profileOpt.get();
				if (!profile.has(TOKENS_USED) || !profile.has(TOKENS_MAX)) {
					return Optional.of(Json.newObject().put(RESPONSE_ERROR, "No token credits remaining.").toString());
				}

				// assign tokens from profile
				tokens = profile.path(TOKENS_USED).asInt(0);
				maxTokens = profile.path(TOKENS_MAX).asInt(2000);

				if (maxTokens <= 0 || tokens + requestedTokens > maxTokens) {
					return Optional
					        .of(Json.newObject().put(RESPONSE_ERROR, "Not enough token credits remaining.").toString());
				}
			}

			// update datastore
			datastore.updateItem(apiToken, Optional.empty(),
			        Json.newObject().put(TOKENS_USED, tokens + requestedTokens));

			// notify admin via slack if credits are running out
			if (tokens + requestedTokens > DEFAULT_STARTING_CREDITS - 200
			        && tokens + requestedTokens < DEFAULT_STARTING_CREDITS - 150) {
				Slack.call("system", "Tokens are running out for " + apiToken);
			}

			return Optional.empty();
		}
	}

	/**
	 * retrieve the datastore dataset id
	 * 
	 * @return
	 */
	public long getDataStoreDatasetId() {
		return datastoreDSId;
	}

	public ProjectAPIInfo getProjectAPIAccess(Person user, Project project) {
		String userProjectKey = tokenResolver.getStableParticipationToken(project.getId(), user.getId());

		// return if API functionality needs to be blocked because the token DB is not available
		if (datastoreDSId == -1L || datastore == null) {
			return new ProjectAPIInfo("", 0, 0, 0, Optional.of("No API access configured. Please contact support."));
		}

		// find entry with token
		Optional<ObjectNode> on = datastore.getItem(userProjectKey, Optional.empty());
		if (on.isEmpty() || on.get().isEmpty()) {
			return new ProjectAPIInfo("", 0, 0, 0);
		}

		// get current api-token from entry
		ObjectNode profile = on.get();
		String apiKey = profile.get(CURRENT_TOKEN).asText();
		long created = profile.get(CREATED).asLong();

		// get metrics from api-token
		on = datastore.getItem(apiKey, Optional.empty());
		if (on.isEmpty() || on.get().isEmpty()) {
			return new ProjectAPIInfo("", 0, 0, 0);
		}
		profile = on.get();

		int tokensUsed = profile.path(TOKENS_USED).asInt(0);
		int tokensMax = profile.path(TOKENS_MAX).asInt(2000);

		return new ProjectAPIInfo(apiKey, created, tokensUsed, tokensMax);
	}

	public ObjectNode getProjectAPIUsage(String apiToken) {

		// return if API functionality needs to be blocked because the token DB is not available
		if (datastoreDSId == -1L || datastore == null) {
			return Json.newObject();
		}

		// find entry with token
		Optional<ObjectNode> on = datastore.getItem(apiToken, Optional.empty());
		if (on.isEmpty() || on.get().isEmpty()) {
			return Json.newObject();
		}

		// get current api-token from entry
		ObjectNode profile = on.get();
		int tokensUsed = profile.path(TOKENS_USED).asInt(0);
		int tokensMax = profile.path(TOKENS_MAX).asInt(2000);

		return Json.newObject().put(TOKENS_USED, tokensUsed).put(TOKENS_MAX, tokensMax);
	}

	public synchronized ProjectAPIInfo activateProjectAPIAccess(Person user, Project project) {
		String userProjectKey = tokenResolver.getStableParticipationToken(project.getId(), user.getId());

		// return if API functionality needs to be blocked because the token DB is not available
		if (datastoreDSId == -1L || datastore == null) {
			return new ProjectAPIInfo("", 0, 0, 0, Optional.of("No API access configured. Please contact support."));
		}

		// find entry with token
		Optional<ObjectNode> on = datastore.getItem(userProjectKey, Optional.empty());
		if (on.isEmpty() || on.get().isEmpty()) {
			// create new api key and metrics
			return updateApiKey(user, project, userProjectKey, 0, DEFAULT_STARTING_CREDITS);
		}

		// recreate token
		// get current api-token from entry
		ObjectNode profile = on.get();
		String oldApiKey = profile.get(CURRENT_TOKEN).asText();

		// get metrics from api-token
		on = datastore.getItem(oldApiKey, Optional.empty());
		if (on.isEmpty() || on.get().isEmpty()) {
			// recreate api key and original metrics
			return updateApiKey(user, project, userProjectKey, 0, DEFAULT_STARTING_CREDITS);
		}
		profile = on.get();
		int tokensUsed = profile.path(TOKENS_USED).asInt(0);
		int tokensMax = profile.path(TOKENS_MAX).asInt(2000);

		// migrate api key and metrics
		datastore.deleteItem(oldApiKey, Optional.empty());
		return updateApiKey(user, project, userProjectKey, tokensUsed, tokensMax);
	}

	ProjectAPIInfo updateApiKey(Person user, Project project, String userProjectKey, int tokensUsed, int tokensMax) {
		// return if API functionality needs to be blocked because the token DB is not available
		if (datastoreDSId == -1L || datastore == null) {
			return new ProjectAPIInfo("", 0, 0, 0, Optional.of("No API access configured. Please contact support."));
		}

		String newApiKey = ApiServiceConstants.DF_API_KEY_PREFIX
		        + tokenResolver.getParticipationToken(project.getId(), user.getId());
		long now = System.currentTimeMillis();
		datastore.updateItem(userProjectKey, Optional.empty(), //
		        Json.newObject().put(CURRENT_TOKEN, newApiKey).put(CREATED, now) //
		                .put("user", user.getId()).put("email", user.getEmail()).put("project", project.getId()));
		datastore.addItem(newApiKey, Optional.empty(),
		        Json.newObject().put(TOKENS_USED, tokensUsed).put(TOKENS_MAX, tokensMax));
		return new ProjectAPIInfo(newApiKey, now, tokensUsed, tokensMax);
	}

	static public class ProjectAPIInfo {
		public String apiKey;
		public long created;
		public int tokensUsed;
		public int tokensMax;
		public Optional<String> error;

		public ProjectAPIInfo(String key, long created, int tokensUsed, int tokensMax) {
			this(key, created, tokensUsed, tokensMax, Optional.empty());
		}

		public ProjectAPIInfo(String key, long created, int tokensUsed, int tokensMax, Optional<String> error) {
			this.apiKey = key;
			this.created = created;
			this.tokensUsed = tokensUsed;
			this.tokensMax = tokensMax;
			this.error = error;
		}
	}

}

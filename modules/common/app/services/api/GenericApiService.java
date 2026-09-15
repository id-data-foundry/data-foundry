package services.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import io.ebean.DB;
import io.ebean.Transaction;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.EntityDS;
import models.ds.LinkedDS;
import models.ds.TimeseriesDS;
import play.Logger;
import play.libs.Json;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;

abstract public class GenericApiService implements ApiServiceConstants {

	public static final String SYSTEM_OPEN_AI_API_SERVICE = "SYSTEM_OPEN_AI_API_SERVICE";
	public static final String SYSTEM_LOCAL_AI_USAGE = "SYSTEM_LOCAL_AI_USAGE";

	private static final int DEFAULT_STARTING_CREDITS = 2000;

	private static final Logger.ALogger logger = Logger.of(GenericApiService.class);

	static final String CURRENT_TOKEN = "currentToken";

	static final String CREATED = "created";

	/**
	 * Number of striped locks used for concurrency control over per-token credit checks and updates. Must be a power of
	 * two so that bitwise AND masking {@code (hash & (NUM_STRIPES - 1))} distributes uniformly across the stripe array.
	 */
	private static final int CREDIT_LOCK_STRIPES = 64;

	/**
	 * Bitmask used for mapping positive token hashcodes to lock stripe indices.
	 */
	private static final int CREDIT_LOCK_MASK = CREDIT_LOCK_STRIPES - 1;

	/**
	 * Array of reentrant locks for striped concurrency control over credit checks and balance updates.
	 * <p>
	 * <b>Architecture & Rationale:</b><br>
	 * In high-throughput AI proxy environments, multiple concurrent API requests need to inspect and deduct user token
	 * credits stored in the backing {@code datastore} (an {@link EntityDS} instance backed by JDBC). Historically, this
	 * was protected by a coarse-grained {@code synchronized (datastore)} monitor. Under load, that JVM-wide monitor
	 * caused severe thread serialization: all requests across unrelated users, projects, and concurrent AI lanes were
	 * forced to wait sequentially for database reads and writes to finish, creating an artificial bottleneck and
	 * leading to queue timeouts.<br>
	 * <br>
	 * <b>Lock Striping Mechanism:</b><br>
	 * Lock striping partitions the token key space across an array of 64 independent {@link ReentrantLock} instances.
	 * When a request arrives, its API token is mapped to a stripe lock via:
	 * 
	 * <pre>{@code
	 * int idx = (token.hashCode() & 0x7FFFFFFF) & CREDIT_LOCK_MASK;
	 * }</pre>
	 * <ul>
	 * <li><b>Per-Token Thread Safety:</b> Concurrent requests presenting the <i>same</i> API token deterministically
	 * map to the identical stripe lock. This guarantees strict serial isolation for that token's read-modify-write
	 * credit cycle, preventing lost updates and balance race conditions.</li>
	 * <li><b>Cross-Token Parallelism:</b> Requests presenting <i>different</i> API tokens distribute uniformly across
	 * the 64 stripes (with ~98.4% probability of disjoint locks for any two keys), enabling up to 64 credit updates to
	 * execute concurrently in parallel without blocking each other.</li>
	 * <li><b>Safe Over-Synchronization:</b> In the event of a hash collision where two different tokens map to the same
	 * stripe index, correctness is 100% maintained: the colliding requests simply serialize temporarily, which is
	 * harmless and safe.</li>
	 * <li><b>Fault Tolerance & Null Safety:</b> If a token is null or empty, it maps safely to stripe 0. Every lock
	 * acquisition is wrapped in a standard {@code try ... finally { lock.unlock(); }} idiom to ensure locks are never
	 * leaked even if database operations throw runtime exceptions.</li>
	 * </ul>
	 */
	private final ReentrantLock[] creditLocks = new ReentrantLock[CREDIT_LOCK_STRIPES];

	protected final TokenResolverUtil tokenResolver;
	protected final Config configuration;
	protected final AdminUtils adminUtils;
	protected final DatasetConnector datasetConnector;

	protected long datastoreDSId = -1L;
	protected EntityDS datastore;

	protected long localAiUsageDSId = -1L;
	protected TimeseriesDS localAiUsageStore;

	protected static final Map<String, ApiKeyDetails> apiKeyCache = new java.util.concurrent.ConcurrentHashMap<>();

	public record ApiKeyDetails(String username, String email, long projectId) {
	}

	@Inject
	protected ThrottlingService throttlingService;

	protected GenericApiService(Config configuration, AdminUtils adminUtils, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolver) {

		this.configuration = configuration;
		this.adminUtils = adminUtils;
		this.datasetConnector = datasetConnector;
		this.tokenResolver = tokenResolver;

		for (int i = 0; i < CREDIT_LOCK_STRIPES; i++) {
			this.creditLocks[i] = new ReentrantLock();
		}
	}

	/**
	 * Resolves the striped {@link ReentrantLock} assigned to the specified API token.
	 *
	 * @param token the API token or key; may be null or empty
	 * @return the striped {@link ReentrantLock} guarding operations on this token
	 */
	protected ReentrantLock getLockForToken(String token) {
		if (token == null || token.isEmpty()) {
			return creditLocks[0];
		}
		int hash = token.hashCode();
		int idx = (hash & 0x7FFFFFFF) & CREDIT_LOCK_MASK;
		return creditLocks[idx];
	}

	protected synchronized void initDatastoreIfNeeded() {
		if (datastore != null && localAiUsageStore != null) {
			return;
		}

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

			logger.info("Created entity table for the AI API service.");
		} else {
			datastoreDataset = dsOpt.get();
		}

		datastoreDSId = datastoreDataset.getId();

		// store instance of datastore
		datastore = datasetConnector.getTypedDatasetDS(datastoreDataset);

		// Initialize SYSTEM_LOCAL_AI_USAGE dataset if needed
		final Dataset localAiUsageDataset;
		Optional<Dataset> laOpt = Dataset.find.query().where().eq("refId", SYSTEM_LOCAL_AI_USAGE).findOneOrEmpty();
		if (laOpt.isEmpty()) {
			if (adminUtils.getFirstAdminUser().isPresent()) {
				Optional<Project> systemProjectOpt = Project.find.query().where().eq("refId", AdminUtils.SYSTEM_PROJECT)
						.findOneOrEmpty();

				Project systemProject;
				if (systemProjectOpt.isEmpty()) {
					systemProject = Project.create(AdminUtils.SYSTEM_PROJECT, adminUtils.getFirstAdminUser().get(), "",
							false, false);
					systemProject.setRefId(AdminUtils.SYSTEM_PROJECT);
					systemProject.save();
					logger.info("Created system project.");
				} else {
					systemProject = systemProjectOpt.get();
				}

				final Dataset tempDataset = new Dataset();
				tempDataset.setName(SYSTEM_LOCAL_AI_USAGE);
				tempDataset.setDsType(DatasetType.IOT);
				tempDataset.setRefId(SYSTEM_LOCAL_AI_USAGE);
				tempDataset.setApiToken(UUID.randomUUID().toString());
				tempDataset.setProject(systemProject);
				tempDataset.setDescription("Central IoT dataset to track local AI model invocations.");
				tempDataset.setTargetObject("");
				tempDataset.setOpenParticipation(false);
				tempDataset.start();
				tempDataset.end();
				tempDataset.save();

				LinkedDS lds = datasetConnector.getDatasetDS(tempDataset);
				lds.createInstance();

				systemProject.getDatasets().add(tempDataset);
				systemProject.update();

				tempDataset.refresh();
				localAiUsageDataset = tempDataset;
				logger.info("Created IOT table for the Local AI usage tracking.");
			} else {
				localAiUsageDataset = null;
			}
		} else {
			localAiUsageDataset = laOpt.get();
		}

		if (localAiUsageDataset != null) {
			localAiUsageDSId = localAiUsageDataset.getId();
			localAiUsageStore = datasetConnector.getTypedDatasetDS(localAiUsageDataset);
		}
	}

	public ApiKeyDetails getApiKeyDetails(String apiKey) {
		if (apiKey == null || apiKey.isEmpty()) {
			return null;
		}

		// 1. Check cache first
		ApiKeyDetails cached = apiKeyCache.get(apiKey);
		if (cached != null) {
			return cached;
		}

		initDatastoreIfNeeded();
		if (datastore == null || datastore.getDataTableName() == null) {
			return null;
		}
		String sql = "SELECT data FROM " + datastore.getDataTableName() + " WHERE data LIKE ? ORDER BY id DESC LIMIT 1";
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, "%\"currentToken\":\"" + apiKey + "\"%");
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String data = rs.getString("data");
				JsonNode jn = Json.parse(data);
				if (jn.isObject()) {
					ObjectNode on = (ObjectNode) jn;
					long userId = on.path("user").asLong(-1L);
					long projectId = on.path("project").asLong(-1L);
					String email = on.path("email").asText("");
					String username = "";
					if (userId != -1L) {
						Person p = Person.find.byId(userId);
						if (p != null) {
							username = p.getUser_id();
						}
					}
					transaction.commit();

					// 2. Cache result on success
					ApiKeyDetails details = new ApiKeyDetails(username, email, projectId);
					apiKeyCache.put(apiKey, details);
					return details;
				}
			}
			transaction.commit();
		} catch (Exception e) {
			logger.error("Error retrieving API key details: ", e);
		}
		return null;
	}

	public long getLocalAiUsageDatasetId() {
		initDatastoreIfNeeded();
		return localAiUsageDSId;
	}

	/**
	 * generic check credits function
	 * 
	 * @param apiKey
	 * @return
	 */
	protected Optional<String> checkCredits(String apiKey) {
		initDatastoreIfNeeded();
		int tokens = -1, maxTokens = -1;

		// return if API functionality needs to be blocked because the token DB is not available
		if (datastoreDSId == -1L || datastore == null) {
			return Optional.empty();
		}

		// isolate access per-token via lock striping to allow concurrent requests for different tokens
		ReentrantLock lock = getLockForToken(apiKey);
		lock.lock();
		try {
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
		} finally {
			lock.unlock();
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
		initDatastoreIfNeeded();
		// enforce rate limiting per token
		if (throttlingService != null && !throttlingService.tryConsume(apiToken)) {
			return Optional.of(Json.newObject()
					.put(RESPONSE_ERROR,
							"Too many requests. Please slow down. (Max 1 request per second with a burst of 20)")
					.toString());
		}

		int tokens = -1;

		// return if API functionality needs to be blocked because the token DB is not available
		if (datastoreDSId == -1L || datastore == null) {
			return Optional.empty();
		}

		// isolate access per-token via lock striping to allow concurrent requests for different tokens
		ReentrantLock lock = getLockForToken(apiToken);
		lock.lock();
		try {
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
			}

			// update datastore
			datastore.updateItem(apiToken, Optional.empty(),
					Json.newObject().put(TOKENS_USED, tokens + requestedTokens));

			return Optional.empty();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * retrieve the datastore dataset id
	 * 
	 * @return
	 */
	public long getDataStoreDatasetId() {
		initDatastoreIfNeeded();
		return datastoreDSId;
	}

	public ProjectAPIInfo getProjectAPIAccess(Person user, Project project) {
		initDatastoreIfNeeded();
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
		initDatastoreIfNeeded();

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
		initDatastoreIfNeeded();
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
		apiKeyCache.remove(oldApiKey);
		return updateApiKey(user, project, userProjectKey, tokensUsed, tokensMax);
	}

	ProjectAPIInfo updateApiKey(Person user, Project project, String userProjectKey, int tokensUsed, int tokensMax) {
		initDatastoreIfNeeded();
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

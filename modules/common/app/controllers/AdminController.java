package controllers;

import java.io.File;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.pekko.actor.ActorSystem;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.typesafe.config.Config;

import controllers.auth.UserAuth;
import io.ebean.ExpressionList;
import io.ebean.PagedList;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.TelegramSession;
import models.sr.Wearable;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Json;
import play.mvc.Http.MimeTypes;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.api.ai.ManagedAIApiService;
import services.maintenance.DatabaseBackupService;
import services.maintenance.ProjectLifecycleService;
import utils.DataUtils;
import utils.DateUtils;
import utils.admin.AdminUtils;
import utils.auth.TokenResolverUtil;
import utils.conf.Configurator;

@Authenticated(UserAuth.class)
public class AdminController extends AbstractAsyncController {

	private final SyncCacheApi cache;
	private final Configurator configurator;
	private final AdminUtils adminUtils;
	private final TokenResolverUtil tokenResolverUtil;
	private final DatabaseBackupService backupService;
	private final ManagedAIApiService managedAIApiService;
	private final FormFactory formFactory;
	private final ActorSystem actorSystem;
	private final ProjectLifecycleService lifeCycleService;

	@Inject
	public AdminController(SyncCacheApi cache, Config config, Configurator configurator, AdminUtils adminUtils,
			ActorSystem actorSystem, TokenResolverUtil tokenResolverUtil, DatabaseBackupService backupService,
			ManagedAIApiService openAIApiService, FormFactory formFactory, ProjectLifecycleService lifeCycleService) {
		this.cache = cache;
		this.configurator = configurator;
		this.adminUtils = adminUtils;
		this.tokenResolverUtil = tokenResolverUtil;
		this.backupService = backupService;
		this.managedAIApiService = openAIApiService;
		this.formFactory = formFactory;
		this.actorSystem = actorSystem;
		this.lifeCycleService = lifeCycleService;
	}

	@AddCSRFToken
	public Result index(Request request) {

		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		int totalUsers = Person.find.query().findCount();
		int activeUsers = Person.find.query().where().gt("lastAction", DateUtils.moveDays(new Date(), -30)).findCount();

		int totalWearables = Wearable.find.query().findCount();
		int activeWearables = Wearable.find.query().where().ne("apiKey", "").findCount();

		// admin!
		return ok(views.html.admin.index.render(activeUsers, totalUsers, activeWearables, totalWearables,
				csrfToken(request)));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@AddCSRFToken
	public Result users(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		List<Person> users = Person.find.all();
		return ok(views.html.admin.users.render(users, csrfToken(request)));
	}

	@AddCSRFToken
	public Result projects(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		return ok(views.html.admin.projects.render(csrfToken(request)));
	}

	@AddCSRFToken
	public Result api(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		// api access
		long datastoreDatasetId = managedAIApiService.getDataStoreDatasetId();

		return ok(views.html.admin.api.render(datastoreDatasetId, csrfToken(request)));
	}

	@AddCSRFToken
	public Result database(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		return ok(views.html.admin.database.render(csrfToken(request)));
	}

	public Result configuration(Request request) {
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		return ok(views.html.admin.configuration.render(this.configurator));
	}

	@AddCSRFToken
	public Result publicDatasets(Request request, String type, int page) {
		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		ExpressionList<Dataset> query = Dataset.find.query().where();
		if (type != null && !type.isEmpty()) {
			try {
				query.eq("dsType", DatasetType.valueOf(type));
			} catch (IllegalArgumentException e) {
				// ignore
			}
		}
		query = query.eq("project.publicProject", true).or().isNull("project.relation").not()
				.contains("project.relation", "$").endOr();

		PagedList<Dataset> pagedDatasets = query.orderBy("id desc").setMaxRows(20).setFirstRow(page * 20)
				.findPagedList();
		return ok(views.html.admin.publicDatasets.render(pagedDatasets, type, csrfToken(request)));
	}

	@AddCSRFToken
	public Result publicWebsites(Request request, int page) {
		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		ExpressionList<Dataset> query = Dataset.find.query().where();
		// filter by COMPLETE type and presence of WEB_ACCESS_TOKEN in configuration
		query = query.eq("dsType", DatasetType.COMPLETE)
				.raw("configuration like ?", "%" + Dataset.WEB_ACCESS_TOKEN + "%");

		PagedList<Dataset> pagedDatasets = query.orderBy("id desc").setMaxRows(20).setFirstRow(page * 20)
				.findPagedList();
		return ok(views.html.admin.publicWebsites.render(pagedDatasets, csrfToken(request)));
	}

	@AddCSRFToken
	public Result chatbots(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		PagedList<Dataset> pagedChatbots = Dataset.find.query().where().eq("collectorType", Dataset.CHATBOT)
				.setMaxRows(20).findPagedList();
		return ok(views.html.admin.chatbots.render(pagedChatbots.getList(), csrfToken(request)));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@RequireCSRFCheck
	public Result userRole(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return forbidden();
		}

		JsonNode jn = request.body().asJson();
		if (!jn.isObject()) {
			return badRequest();
		}

		ObjectNode on = (ObjectNode) jn;
		if (!on.has("user") || !on.has("role") || !on.has("value")) {
			return badRequest();
		}

		long userId = on.get("user").asLong(-1);
		String role = on.get("role").asText("");
		boolean active = on.get("value").asBoolean(false);
		if (userId == -1L || role.isEmpty()) {
			return badRequest();
		}

		Person user = Person.find.byId(userId);
		if (user == null) {
			return notFound();
		}

		// set property
		user.setRole(role, active);
		user.update();

		return noContent();
	}

	/**
	 * run a live database backup; this is the safest before updating if you don't want to shutdown, then do a cold
	 * backup
	 * 
	 * @param request
	 * @return
	 */
	@RequireCSRFCheck
	public Result backupDB(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		// trigger backup
		actorSystem.dispatcher().execute(() -> {
			backupService.adminBackup();
		});

		// return to admin
		return redirect(routes.AdminController.index()).addingToSession(request, "message", "DB backup scheduled.");
	}

	/**
	 * generate statistics
	 * 
	 * @param request
	 * @return
	 */
	public Result generateStats(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return forbidden();
		}

		// trigger admin db stats
		actorSystem.dispatcher().execute(() -> {
			backupService.adminDBStats();
		});

		return ok("Generating stats...");
	}

	/**
	 * request all statistics
	 * 
	 * @param request
	 * @return
	 */
	public Result allStats(Request request) {

		// check permissions
		if (!isAdmin(request)) {
			return forbidden();
		}

		// return results
		File statsFile = new File("current_stats.json");
		return (statsFile.exists() ? ok(statsFile) : ok("[]")).as(MimeTypes.JSON);
	}

	/**
	 * get a password reset link for the given username / email
	 * 
	 * @param request
	 * @return
	 */
	@RequireCSRFCheck
	public Result resetPasswordTokenLink(Request request) {

		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		String username = nss(df.get("username"));

		// cache for three hours
		String token = tokenResolverUtil.createEmailResetToken(username);
		cache.set("email_reset_" + username, token, 25 * 60 * 60);

		// switch to link directly (because admin permissions)
		return ok(routes.UsersController.resetPW(token).absoluteURL(request, true));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * search the database for an entity with the given ID or refId
	 * 
	 * @param request
	 * @return
	 */
	@RequireCSRFCheck
	public Result searchEntity(Request request) {
		// check permissions
		if (!isAdmin(request)) {
			return forbidden();
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		String query = df.get("query");
		String type = df.get("type");
		long id = DataUtils.parseLong(query.replace("id:", "").trim(), -1);
		if (id > -1) {
			// search for entity by id
			switch (type) {
			case "wearable": {
				Wearable e = Wearable.find.byId(id);
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "device": {
				Device e = Device.find.byId(id);
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "participant": {
				Participant e = Participant.find.byId(id);
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "cluster": {
				Cluster e = Cluster.find.byId(id);
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "dataset": {
				Dataset e = Dataset.find.byId(id);
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "project": {
				Project e = Project.find.byId(id);
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "person": {
				Person e = Person.find.byId(id);
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "telegram": {
				TelegramSession e = TelegramSession.find.byId(id);
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			default:
				return notFound(Json.newObject().toPrettyString());
			}
		} else {
			// search for entity by refId
			switch (type) {
			case "wearable": {
				Wearable e = Wearable.find.query().where().eq("refId", query.trim()).findOne();
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "device": {
				Device e = Device.find.query().where().eq("refId", query.trim()).findOne();
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "participant": {
				Participant e = Participant.find.query().where().eq("refId", query.trim()).findOne();
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "cluster": {
				Cluster e = Cluster.find.query().where().eq("refId", query.trim()).findOne();
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "dataset": {
				Dataset e = Dataset.find.query().where().eq("refId", query.trim()).findOne();
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "project": {
				Project e = Project.find.query().where().eq("refId", query.trim()).findOne();
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "person": {
				Person e = Person.findByEmail(query.trim()).get();
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			case "telegram": {
				TelegramSession e = TelegramSession.find.query().where().eq("chatId", query.trim()).findOne();
				return ok(e != null ? e.toJson() : Json.newObject().toPrettyString());
			}
			default:
				return notFound(Json.newObject().toPrettyString());
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@AddCSRFToken
	public Result actionableProjects(Request request, String filter) {
		// check permissions
		if (!isAdmin(request)) {
			return forbidden();
		}

		// one year ago
		Date threeMonthsAgo = DateUtils.moveMonths(new Date(), -3);

		List<ArchivableItem> items = new LinkedList<>();
		AtomicInteger totalOpenProjects = new AtomicInteger();
		AtomicInteger totalFreezableProjects = new AtomicInteger();
		// find project that have ended for about a month
		Project.find.query().where().le("end", threeMonthsAgo).findIterate().forEachRemaining(p -> {
			// filter projects to retain only DF native projects
			if (!p.isDFNativeProject()) {
				return;
			}

			// filter system project
			if (p.getRefId().equals(AdminUtils.SYSTEM_PROJECT)) {
				return;
			}

			ArchivableItem ai = new ArchivableItem();
			ai.isArchived = p.isArchivedProject();
			ai.oneYearAgo = p.isOutdated();
			ai.projectId = p.getId();
			ai.projectName = p.getName();
			ai.projectTeam = p.getTeam().stream().map(tm -> tm.getName()).collect(Collectors.joining(", "));

			// filter this project to retain only the participants which need to be deidentified
			ai.numberParticipantsToDeidentify = (int) p.getParticipants().stream()
					.filter(participant -> participant.canDeidentify()).count();

			// filter this project to retain only the devices which need to be deidentified
			ai.numberDevicesToDeidentify = (int) p.getDevices().stream().filter(device -> device.canDeidentify())
					.count();

			// filter this project to retain only the wearables that need to be unlinked
			ai.numberWearablesToUnlink = (int) p.getWearables().stream().filter(w -> w.canDeidentify()).count();

			if (!ai.isArchived || ai.numberParticipantsToDeidentify + ai.numberDevicesToDeidentify
					+ ai.numberWearablesToUnlink > 0) {
				items.add(ai);
				totalOpenProjects.incrementAndGet();
			} else if (ai.isArchived && !p.isFrozen() && ai.oneYearAgo) {
				items.add(ai);
				totalFreezableProjects.incrementAndGet();
			}
		});
		return ok(views.html.admin.components.actionableProjects.render(items, totalOpenProjects.get(),
				totalFreezableProjects.get(), csrfToken(request)));
	}

	@AddCSRFToken
	public Result actionableProject(Request request, long id) {
		// check permissions
		if (!isAdmin(request)) {
			return forbidden();
		}

		// find project that have ended for about a month
		Project p = Project.find.byId(id);
		if (p == null || !p.isDFNativeProject()) {
			return notFound("");
		}

		// filter system project
		if (p.getRefId().equals(AdminUtils.SYSTEM_PROJECT)) {
			return forbidden("");
		}

		ArchivableItem ai = new ArchivableItem();
		ai.isArchived = p.isArchivedProject();
		ai.oneYearAgo = p.isOutdated();
		ai.projectId = p.getId();
		ai.projectName = p.getName();
		ai.projectTeam = p.getTeam().stream().map(tm -> tm.getName()).collect(Collectors.joining(", "));

		// filter this project to retain only the participants which need to be deidentified
		ai.numberParticipantsToDeidentify = (int) p.getParticipants().stream()
				.filter(participant -> participant.canDeidentify()).count();

		// filter this project to retain only the devices which need to be deidentified
		ai.numberDevicesToDeidentify = (int) p.getDevices().stream().filter(device -> device.canDeidentify()).count();

		// filter this project to retain only the wearables that need to be unlinked
		ai.numberWearablesToUnlink = (int) p.getWearables().stream().filter(w -> w.canDeidentify()).count();

		if (!ai.isArchived || ai.numberParticipantsToDeidentify > 0 || ai.numberDevicesToDeidentify > 0
				|| ai.numberWearablesToUnlink > 0) {
			return ok(views.html.admin.components.actionableProject.render(ai, csrfToken(request)));
		} else if (ai.isArchived && !p.isFrozen() && ai.oneYearAgo) {
			return ok(views.html.admin.components.actionableProject.render(ai, csrfToken(request)));
		}

		return ok("");
	}

	@RequireCSRFCheck
	public CompletionStage<Result> deidentifyProject(Request request, long id) {
		// check permissions
		if (!isAdmin(request)) {
			return CompletableFuture.completedFuture(forbidden());
		}

		return CompletableFuture.<Result>supplyAsync(() -> {
			// find project that have ended for about a month
			Project p = Project.find.byId(id);
			if (p == null || !p.isDFNativeProject()) {
				return notFound("");
			}

			// filter system project
			if (p.getRefId().equals(AdminUtils.SYSTEM_PROJECT)) {
				return forbidden("");
			}

			lifeCycleService.deidentifyProject(p);

			return redirect(routes.AdminController.actionableProject(id));
		});
	}

	@RequireCSRFCheck
	public CompletionStage<Result> archiveProject(Request request, long id) {
		// check permissions
		if (!isAdmin(request)) {
			return CompletableFuture.completedFuture(forbidden());
		}

		return CompletableFuture.<Result>supplyAsync(() -> {
			// find project that have ended for about a month
			Project p = Project.find.byId(id);
			if (p == null || !p.isDFNativeProject()) {
				return notFound("");
			}

			// filter system project
			if (p.getRefId().equals(AdminUtils.SYSTEM_PROJECT)) {
				return forbidden("");
			}

			lifeCycleService.archiveProject(p);

			return redirect(routes.AdminController.actionableProject(id));
		});
	}

	@RequireCSRFCheck
	public CompletionStage<Result> freezeProject(Request request, long id) {
		// check permissions
		if (!isAdmin(request)) {
			return CompletableFuture.completedFuture(forbidden());
		}

		return CompletableFuture.<Result>supplyAsync(() -> {
			// find project that have ended for about a month
			Project p = Project.find.byId(id);
			if (p == null || !p.isDFNativeProject()) {
				return notFound("");
			}

			// filter system project
			if (p.getRefId().equals(AdminUtils.SYSTEM_PROJECT)) {
				return forbidden("");
			}

			// freeze the project
			lifeCycleService.freezeProject(Optional.of(request), p);

			return redirect(routes.AdminController.actionableProject(id));
		});
	}

	@RequireCSRFCheck
	public Result runFullRetentionChecks(Request request) {

		// check permissions
		if (!isAdmin(request)) {
			return redirect(routes.HomeController.index());
		}

		// trigger backup
		actorSystem.dispatcher().execute(() -> {
			lifeCycleService.refresh();
		});

		return ok("").addingToSession(request, "message", "Retention checks started.");
	}

	static public class ArchivableItem {
		public long projectId;
		public String projectName;
		public String projectTeam;
		public boolean oneYearAgo;
		public boolean isArchived;
		public int numberParticipantsToDeidentify;
		public int numberDevicesToDeidentify;
		public int numberWearablesToUnlink;

	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * is the current request issued by a user who is also administrator?
	 * 
	 * @param request
	 * @return
	 */
	private boolean isAdmin(Request request) {
		// check if account exists
		Optional<Person> userOpt = getAuthenticatedUser(request);
		if (!userOpt.isPresent()) {
			return false;
		}

		return adminUtils.isAdmin(userOpt);
	}

}

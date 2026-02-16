package controllers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import controllers.auth.ParticipantAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.ds.DiaryDS;
import models.ds.MediaDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.ParticipationStatus;
import models.sr.TelegramSession;
import models.sr.TelegramSession.TelegramSessionState;
import models.sr.Wearable;
import models.vm.TimedAnnotatedMedia;
import models.vm.TimedText;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.CSRF;
import play.filters.csrf.RequireCSRFCheck;
import play.i18n.Langs;
import play.libs.Files.TemporaryFile;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Http.Cookie;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.twirl.api.Html;
import services.email.NotificationService;
import services.inlets.FitBitService;
import services.inlets.GoogleFitService;
import services.slack.Slack;
import services.telegrambot.TelegramBotService;
import utils.DataUtils;
import utils.StringUtils;
import utils.auth.TokenResolverUtil;
import utils.telegrambot.TelegramBotUtils;
import utils.validators.FileTypeUtils;

public class ParticipationController extends Controller {

	private final Langs langs;
	private final FormFactory formFactory;
	private final SyncCacheApi cache;
	private final DatasetConnector datasetConnector;
	private final TokenResolverUtil tokenResolverUtil;
	private final FitBitService fitbitService;
	private final GoogleFitService googlefitService;
	private final NotificationService notificationService;
	private final TelegramBotService telegramBotUtils;
	private static final Logger.ALogger logger = Logger.of(ParticipationController.class);

	@Inject
	public ParticipationController(Langs langs, FormFactory formFactory, SyncCacheApi cache,
			DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil, FitBitService fitbitService,
			GoogleFitService googlefitService, NotificationService notificationService,
			TelegramBotService telegramBotService) {
		this.langs = langs;
		this.formFactory = formFactory;
		this.cache = cache;
		this.datasetConnector = datasetConnector;
		this.tokenResolverUtil = tokenResolverUtil;
		this.fitbitService = fitbitService;
		this.googlefitService = googlefitService;
		this.notificationService = notificationService;
		this.telegramBotUtils = telegramBotService;
	}

	@AddCSRFToken
	public Result view(Request request, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id < 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant object
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null) {
			return redirect(routes.HomeController.index());
		}

		// check participant in the correct project
		long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
		if (project_id != participant.getProject().getId()) {
			return redirect(routes.HomeController.index());
		}

		if (participant.declined()) {
			return redirect(routes.ParticipationController.declinedStatus(invite_token));
		} else if (participant.pending()) {
			return redirect(routes.ParticipationController.confirm(invite_token));
		}

		// accepted participant!

		final Project project = participant.getProject();
		project.refresh();

		final Dataset diaryDS = project.getDiaryDataset();

		final List<TimedAnnotatedMedia> annotatedMedia;
		final Dataset mediaDS = project.getMediaDataset();
		if (mediaDS != null && mediaDS.getId() > -1) {
			MediaDS mds = (MediaDS) datasetConnector.getDatasetDS(mediaDS);
			annotatedMedia = mds.getMediaForParticipant(participant_id);

			// merge annotations into the media files
			if (diaryDS != null && diaryDS.getId() > -1) {
				DiaryDS dds = (DiaryDS) datasetConnector.getDatasetDS(diaryDS);
				List<TimedText> tts = dds.getDiaryForParticipant(participant_id);
				for (TimedText tt : tts) {
					final Optional<TimedAnnotatedMedia> otam = annotatedMedia.stream()
							.filter(am -> am.matchTime(tt.timestamp)).findAny();
					final TimedAnnotatedMedia tam;
					if (!otam.isPresent()) {
						tam = new TimedAnnotatedMedia(-1L, tt.timestamp, "", "", "", mediaDS.getId());
						annotatedMedia.add(tam);
					} else {
						tam = otam.get();
					}
					tam.addAnnotation(tt.text);
				}
			}
		} else {
			annotatedMedia = Collections.emptyList();
		}

		// set token

		// check if participant is already connected via Telegram
		Optional<TelegramSession> telegramSession = TelegramSession.find.query().setMaxRows(1).where()
				.eq("activeProjectId", project.getId()).eq("email", participant.getEmail())
				.eq("state", TelegramSessionState.PARTICIPANT.name()).findOneOrEmpty();
		final String telegramPIN = telegramSession.isPresent() ? "" : TelegramBotUtils.generateTelegramPersonalPIN();
		// set Telegram PIN in cache (valid for one hour)
		cache.set(TelegramBotUtils.TG_PARTICIPANT_CACHE_PREFIX + participant.getId(),
				telegramPIN + "," + participant.getProject().getId(), 3600);
		String telegramBotName = telegramBotUtils.getBotUsername();

		final List<Device> devices = participant.getClusterDevices();
		final List<Wearable> wearables = participant.getClusterWearables();

		// participant home, with language selection
		switch (determineLanguage(request)) {
		case "nl":
			return ok(views.html.participation.nl.view.render(invite_token, csrfToken(request), telegramBotName,
					telegramPIN, participant, project, diaryDS, annotatedMedia, devices, wearables))
					.addingToSession(request, ParticipantAuth.PARTICIPANT_ID, invite_token);
		case "de":
			return ok(views.html.participation.de.view.render(invite_token, csrfToken(request), telegramBotName,
					telegramPIN, participant, project, diaryDS, annotatedMedia, devices, wearables))
					.addingToSession(request, ParticipantAuth.PARTICIPANT_ID, invite_token);
		case "en":
		default:
			return ok(views.html.participation.en.view.render(invite_token, csrfToken(request), telegramBotName,
					telegramPIN, participant, project, diaryDS, annotatedMedia, devices, wearables))
					.addingToSession(request, ParticipantAuth.PARTICIPANT_ID, invite_token);
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@AddCSRFToken
	public Result recordForm(Request request, Long id, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id < 0) {
			return redirect(routes.HomeController.index());
		}

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.canAppend()) {
			return redirect(routes.HomeController.index());
		}

		Project project = ds.getProject();
		project.refresh();

		// check participant object
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null || !project.hasParticipant(participant)) {
			return redirect(routes.HomeController.index());
		}

		switch (determineLanguage(request)) {
		case "nl":
			return ok(views.html.participation.nl.record.render(csrfToken(request), invite_token, ds));
		case "de":
			return ok(views.html.participation.de.record.render(csrfToken(request), invite_token, ds));
		case "en":
		default:
			return ok(views.html.participation.en.record.render(csrfToken(request), invite_token, ds));
		}
	}

	@RequireCSRFCheck
	public Result recordDiary(Request request, Long id, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id < 0) {
			return redirect(routes.HomeController.index());
		}

		final Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
					"error", "Dataset not found.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
					"error", "Dataset inaccessible.");
		}

		// check dataset type
		if (ds.getDsType() != DatasetType.DIARY) {
			return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
					"error", "Wrong dataset type.");
		}

		Project project = ds.getProject();
		project.refresh();

		// check participant access
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null || !project.hasParticipant(participant)) {
			return redirect(routes.HomeController.index());
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest("Expecting some data");
		}

		// record the information in the database for this data set
		DiaryDS dysc = (DiaryDS) datasetConnector.getDatasetDS(ds);

		// title
		String title = nss(df.get("title")).replace(",", ";");

		// text
		// TODO Markdown formatting? into transformation into HTML?
		String text = nss(df.get("text")).replace("\n", "<br>").replaceAll("[\\p{Cntrl}\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}]",
				"");

		// timestamp
		String date = nss(df.get("ts-date"));
		String time = nss(df.get("ts-time"));

		Date parsed = null;
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			parsed = format.parse(date + " " + time);
		} catch (ParseException pe) {
			// do nothing: if parsing does not, we insert the current date
		}

		// store data
		dysc.addRecord(participant, parsed != null ? parsed : new Date(), title, text);

		// flash and redirect
		return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
				"message", "Diary entry recorded, thanks!");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@RequireCSRFCheck
	public Result uploadFile(Request request, Long id, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id < 0) {
			return redirect(routes.HomeController.index());
		}

		final Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
					"error", "Dataset not found.");
		} else if (!ds.canAppend() || !ds.isOpenParticipation()) {
			return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
					"error", "Dataset inaccessible.");
		}

		// check dataset type
		if (ds.getDsType() != DatasetType.MEDIA) {
			return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
					"error", "Wrong dataset type.");
		}

		Project project = ds.getProject();
		project.refresh();

		// check participant access
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null || !project.hasParticipant(participant)) {
			return redirect(controllers.routes.ParticipationController.view(invite_token));
		}

		try {
			Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
			if (body == null) {
				return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
						"error", "Bad request");
			}

			DynamicForm df = formFactory.form().bindFromRequest(request);
			if (df == null) {
				return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request,
						"error", "Expecting some data");
			}

			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
			if (!fileParts.isEmpty()) {
				final MediaDS cpds = (MediaDS) datasetConnector.getDatasetDS(ds);

				for (int i = 0; i < fileParts.size(); i++) {
					FilePart<TemporaryFile> filePart = fileParts.get(i);
					TemporaryFile file = filePart.getRef();
					String fileName = filePart.getFilename();
					String fileType = filePart.getContentType();
					String timestamp = df.get(fileName);

					// restrict file type
					if (!FileTypeUtils.looksLikeImageFile(fileName)) {
						continue;
					}

					// content-based validation
					if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.IMAGE)) {
						continue;
					}

					Date now = new Date();

					// ensure that filename is unique on disk
					fileName = participant.getId() + "_" + now.getTime() + "_" + fileName;

					// store file, add record
					Optional<String> storeFile = cpds.storeFile(file.path().toFile(), fileName);
					if (storeFile.isPresent()) {
						long ts = -1;
						try {
							ts = DataUtils.parseLong(timestamp, 0);
						} catch (Exception e) {
						}

						String description = df.get("description");
						cpds.addRecord(participant, storeFile.get(), description, now, "imported fully");
						cpds.importFileContents(
								participant, controllers.api.routes.MediaDSController.image(id, storeFile.get())
										.absoluteURL(request, true),
								fileType, description, ts != -1 ? new Date(ts) : now);
					}
				}

				LabNotesEntry.log(ParticipationController.class, LabNotesEntryType.DATA,
						"Files uploaded to dataset: " + ds.getName(), ds.getProject());
			}

			return redirect(controllers.routes.ParticipationController.view(invite_token));
		} catch (NullPointerException e) {
			logger.error("Error in uploading dataset file.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return redirect(controllers.routes.ParticipationController.view(invite_token)).addingToSession(request, "error",
				"Invalid request, no files have been included in the request.");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@AddCSRFToken
	public Result recruit(Request request, String recruitmentToken) {

		// if language cookie is missing, redirect to self with cookie
		if (request.cookie("PLAY_LANG").isEmpty()) {
			return redirect(routes.ParticipationController.recruit(recruitmentToken)).withCookies(Http.Cookie
					.builder("PLAY_LANG", determineLanguage(request)).withMaxAge(Duration.ofDays(365)).build());
		}

		// get and check project id
		long id = tokenResolverUtil.getProjectIdFromParticipationToken(recruitmentToken);
		if (id == -1) {
			return redirect(routes.HomeController.index());
		}

		// check invite_token again
		Project project = Project.find.byId(id);
		if (project == null || !project.isSignupOpen()) {
			return redirect(routes.HomeController.index());
		}

		// create a new invite_token without a participant, yet
		switch (determineLanguage(request)) {
		case "nl":
			return ok(views.html.participation.nl.recruit.render(null, project, recruitmentToken, csrfToken(request)));
		case "de":
			return ok(views.html.participation.de.recruit.render(null, project, recruitmentToken, csrfToken(request)));
		case "en":
		default:
			return ok(views.html.participation.en.recruit.render(null, project, recruitmentToken, csrfToken(request)));
		}
	}

	@AddCSRFToken
	public Result recruitAnon(Request request, String recruitmentToken) {

		// if language cookie is missing, redirect to self with cookie
		if (request.cookie("PLAY_LANG").isEmpty()) {
			return redirect(routes.ParticipationController.recruit(recruitmentToken)).withCookies(Http.Cookie
					.builder("PLAY_LANG", determineLanguage(request)).withMaxAge(Duration.ofDays(365)).build());
		}

		// get and check project id
		long id = tokenResolverUtil.getProjectIdFromParticipationToken(recruitmentToken);
		if (id == -1) {
			return redirect(routes.HomeController.index());
		}

		// check invite_token again
		Project project = Project.find.byId(id);
		if (project == null || !project.isSignupOpen()) {
			return redirect(routes.HomeController.index());
		}

		final Participant participant;
		participant = Participant.createInstance("", "", "", project);
		participant.setStatus(ParticipationStatus.ACCEPT);
		participant.save();
		project.getParticipants().add(participant);
		project.update();
		participant.refresh();

		// create new token, because the participant might be new
		String invite_token = tokenResolverUtil.getParticipationToken(id, participant.getId());

		// redirect to participant view
		return redirect(routes.ParticipationController.view(invite_token)).addingToSession(request,
				ParticipantAuth.PARTICIPANT_ID, invite_token);
	}

	@AddCSRFToken
	public Result confirm(Request request, String invite_token) {

		// if language cookie is missing, redirect to self with cookie
		if (request.cookie("PLAY_LANG").isEmpty()) {
			return redirect(routes.ParticipationController.recruit(invite_token)).withCookies(Http.Cookie
					.builder("PLAY_LANG", determineLanguage(request)).withMaxAge(Duration.ofDays(365)).build());
		}

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id < 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant object
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null) {
			return redirect(routes.HomeController.index());
		}

		// check signup status
		if (participant.accepted()) {
			return redirect(routes.ParticipationController.view(invite_token));
		}

		// check participant in the correct project
		long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
		if (project_id != participant.getProject().getId()) {
			return redirect(routes.HomeController.index());
		}

		Project project = participant.getProject();
		project.refresh();

		switch (determineLanguage(request)) {
		case "nl":
			return ok(
					views.html.participation.nl.recruit.render(participant, project, invite_token, csrfToken(request)));
		case "de":
			return ok(
					views.html.participation.de.recruit.render(participant, project, invite_token, csrfToken(request)));
		case "en":
		default:
			return ok(
					views.html.participation.en.recruit.render(participant, project, invite_token, csrfToken(request)));
		}
	}

	@RequireCSRFCheck
	@AddCSRFToken
	public Result signup(Request request, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
		Project project = Project.find.byId(project_id);
		if (project == null) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id <= 0 && !project.isSignupOpen()) {
			return redirect(routes.HomeController.index());
		}

		// check tick boxes
		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest("Expecting some data");
		}

		// check for double registration in the same project with the same email
		final String isDataProcessingAgreed = nss(df.get("isDataProcessingAgreed")).toLowerCase();
		final String isStudyParticipationAgreed = nss(df.get("isStudyParticipationAgreed")).toLowerCase();
		if (!isDataProcessingAgreed.equals("on") || !isStudyParticipationAgreed.equals("on")) {
			return redirect(routes.ParticipationController.recruit(invite_token));
		}

		// create or retrieve participant object
		final Participant participant;
		if (participant_id <= 0 && project.isSignupOpen()) {
			participant = Participant.createInstance("", "", "", project);
			participant.save();
			project.getParticipants().add(participant);
			project.update();
			participant.refresh();

			// if there is a template cluster in the project, also create a new cluster according to the template with
			// devices and wearables
			Optional<Cluster> templateClusterOpt = project.getClusters().stream()
					.filter(c -> c.getName().equalsIgnoreCase("template")).findFirst();
			if (templateClusterOpt.isPresent()) {
				Cluster templateCluster = templateClusterOpt.get();

				Cluster newCluster = new Cluster(participant.getName());
				newCluster.create();
				newCluster.setProject(project);
				newCluster.getParticipants().add(participant);
				newCluster.save();
				project.getClusters().add(newCluster);
				project.update();

				// add devices according to template
				for (Device templateDevice : templateCluster.getDevices()) {
					Device device = new Device();
					device.setName(participant.getName() + "_" + templateDevice.getName());
					device.setCategory(templateDevice.getCategory());
					device.setConfiguration(templateDevice.getConfiguration());
					device.setProject(project);
					device.getClusters().add(newCluster);
					device.save();
					newCluster.getDevices().add(device);
					project.getDevices().add(device);
				}

				// add wearables according to template
				for (Wearable templateWearable : templateCluster.getWearables()) {
					Wearable wearable = new Wearable();
					wearable.setName(participant.getName() + "_" + templateWearable.getName());
					wearable.setBrand(templateWearable.getBrand());
					wearable.setProject(project);
					wearable.getClusters().add(newCluster);
					wearable.save();
					newCluster.getWearables().add(wearable);
					project.getWearables().add(wearable);
				}

				newCluster.update();
				participant.update();
				project.update();
			}

		} else {
			participant = Participant.find.byId(participant_id);
		}

		// check object
		if (participant == null) {
			return redirect(routes.HomeController.index());
		}

		// check participant in the correct project
		if (project_id != participant.getProject().getId()) {
			return redirect(routes.HomeController.index());
		}

		// create new token, because the participant might be new
		invite_token = tokenResolverUtil.getParticipationToken(project_id, participant.getId());

		switch (determineLanguage(request)) {
		case "nl":
			return ok(views.html.participation.nl.signup.render(csrfToken(request), participant, invite_token));
		case "de":
			return ok(views.html.participation.de.signup.render(csrfToken(request), participant, invite_token));
		case "en":
		default:
			return ok(views.html.participation.en.signup.render(csrfToken(request), participant, invite_token));
		}
	}

	@AddCSRFToken
	public Result edit(Request request, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
		Project project = Project.find.byId(project_id);
		if (project == null) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id <= 0 && !project.isSignupOpen()) {
			return redirect(routes.HomeController.index());
		}

		// retrieve participant object
		final Participant participant = Participant.find.byId(participant_id);

		// check object
		if (participant == null) {
			return redirect(routes.HomeController.index());
		}

		// check participant in the correct project
		if (project_id != participant.getProject().getId()) {
			return redirect(routes.HomeController.index());
		}

		// create new token, because the participant might be new
		invite_token = tokenResolverUtil.getParticipationToken(project_id, participant.getId());

		switch (determineLanguage(request)) {
		case "nl":
			return ok(views.html.participation.nl.signup.render(csrfToken(request), participant, invite_token));
		case "de":
			return ok(views.html.participation.de.signup.render(csrfToken(request), participant, invite_token));
		case "en":
		default:
			return ok(views.html.participation.en.signup.render(csrfToken(request), participant, invite_token));
		}
	}

	@RequireCSRFCheck
	public Result signMeUp(Request request, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token == "") {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id <= 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant object
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null) {
			return redirect(routes.HomeController.index());
		}

		// check participant in the correct project
		long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
		if (project_id != participant.getProject().getId()) {
			return redirect(routes.HomeController.index());
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest("Expecting some data");
		}

		// check for double registration in the same project with the same email
		final String email = nss(df.get("email")).toLowerCase();
		if (nnne(email) && !email.equals(participant.getEmail())) {
			// check for existing participant with same email address
			Optional<Participant> existingParticipant = Participant.findByEmailAndProject(email, project_id);
			if (existingParticipant.isPresent()) {
				return redirect(routes.ProjectsController.view(project_id)).addingToSession(request, "error",
						"We have found another participant with this email address in this project, please choose a different email address.");
			}

			// save the email if email is still empty
			if (participant.getEmail().isEmpty() || participant.getEmail().equals("<fill in>")) {
				participant.setEmail(email);
			}
		}

		// store the participant-provided info
		participant.setFirstname(htmlEscape(nss(df.get("first_name"), 64)));
		participant.setLastname(htmlEscape(nss(df.get("last_name"), 64)));
		participant.setGender(DataUtils.parseInt(df.get("gender")));
		participant.setCareer(htmlEscape(nss(df.get("career"), 64)));
		participant.setAgeRange(DataUtils.parseInt(nss(df.get("age_range"))));

		// change to accept status
		participant.setStatus(ParticipationStatus.ACCEPT);
		participant.update();

		// if participant has a wearable
		final List<Wearable> clusterWearables = participant.getClusterWearables();
		if (!clusterWearables.isEmpty()) {
			// ask for wearable setup IF the wearable is not yet set up
			for (Wearable w : clusterWearables) {
				if (!w.isConnected()) {
					return redirect(routes.ParticipationController.signupWearable(w.getId())).addingToSession(request,
							ParticipantAuth.PARTICIPANT_ID, invite_token);
				}
			}
		}

		// send email
		String redirectLink = routes.ParticipantsController.view(participant.getId()).absoluteURL(request, true);

		Html htmlBody = views.html.emails.confirmed.render("Participation accepted",
				String.format(
						"%s, we would like to inform you, your participant, %s for project: %s,"
								+ " has accepted the invitation.",
						participant.getProject().getOwner().getName(), participant.getName(),
						participant.getProject().getName()),
				redirectLink);

		String textBody = String.format(
				"Hello! \n\n%s, we would like to inform you, your participant, %s for project: %s,"
						+ " has accepted the invitation. You can click the link below to view more details.\n\n%s\n\n",
				participant.getProject().getOwner().getName(), participant.getName(),
				participant.getProject().getName(), redirectLink);

		notificationService.sendMail(participant.getProject().getOwner().getEmail(), textBody, htmlBody, redirectLink,
				"[ID Data Foundry] " + "Participation accepted", null, "ID Data Foundry", "Participation accepted: "
						+ participant.getName() + " in project: " + participant.getProject().getName());

		// refresh project
		Project project = participant.getProject();
		project.refresh();

		// send instant message to project owner if he / she is online in Telegram
		telegramBotUtils.sendMessageToProjectOwner(project.getId(),
				"Your participant, " + participant.getName() + ", has accepted the project: " + project.getName(),
				Executors.newSingleThreadExecutor());

		return redirect(routes.ParticipationController.view(invite_token))
				.addingToSession(request, "message",
						"Thank you for joining this study! &nbsp; <i class=\"material-icons\">mood</i>")
				.addingToSession(request, ParticipantAuth.PARTICIPANT_ID, invite_token);
	}

	public Result decline(Request request, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
		}

		// check invite_token again
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant id from invite_id
		long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		if (participant_id <= 0) {
			return redirect(routes.HomeController.index());
		}

		// check participant object
		Participant participant = Participant.find.byId(participant_id);
		if (participant == null) {
			return redirect(routes.HomeController.index());
		}

		// check participant in the correct project
		long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
		if (project_id != participant.getProject().getId()) {
			return redirect(routes.HomeController.index());
		}

		// get wearables linked to this participant and unlink them
		List<Wearable> wearables = participant.getClusterWearables();
		if (!wearables.isEmpty()) {
			for (Wearable wearable : wearables) {
				wearable.reset();
			}
		}

		// record flag for decline
		participant.setStatus(ParticipationStatus.DECLINE);
		participant.update();

		// refresh project
		Project project = participant.getProject();
		project.refresh();

		// send instant message to project owner if he / she is online in Telegram
		if (telegramBotUtils != null) {
			telegramBotUtils.sendMessageToProjectOwner(project.getId(),
					"Your participant, " + participant.getName() + ", has left your project: " + project.getName(),
					Executors.newSingleThreadExecutor());
		}

		// participant home
		return redirect(routes.ParticipationController.declinedStatus(invite_token));
	}

	public Result declinedStatus(Request request, String invite_token) {
		switch (determineLanguage(request)) {
		case "nl":
			return ok(views.html.participation.nl.decline.render(invite_token));
		case "de":
			return ok(views.html.participation.de.decline.render(invite_token));
		case "en":
		default:
			return ok(views.html.participation.en.decline.render(invite_token));
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Result switchLanguage(Request request, String token, String languageCode) {
		return redirect(routes.ParticipationController.recruit(token))
				.withCookies(Http.Cookie.builder("PLAY_LANG", languageCode).withMaxAge(Duration.ofDays(365)).build());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * first step to get FITBIT and GoogleFit data : get authorization code from participant
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	public Result signupWearable(Request request, Long id) {

		// start
		Wearable wearable = Wearable.find.byId(id);

		if (wearable == null) {
			return redirect(routes.HomeController.index());
		}

		String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);

		// check participant has no wearables in on-going projects
		Long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
		Participant participant = Participant.find.byId(participant_id);
		Participant clusterParticipant = wearable.getClusterParticipant();

		// check whether participant and wearable is in the same cluster or the wearable is unregistered
		if (clusterParticipant != null && participant_id.equals(clusterParticipant.getId())
				|| (!participant.getClusters().isEmpty() && !wearable.isConnected())) {

			String signupStatus = "new";
			String scopes = "";
			if (wearable.isConnected()) {
				signupStatus = "registered";
			}

			switch (wearable.getBrand()) {
			case Wearable.FITBIT:
				String redirectUrl = controllers.routes.FitbitWearablesController.finishWearableRegistration("0")
						.absoluteURL(true, request.host());

				// scopes = wearable.scopes.split("::")[1];
				// scopes = "activity body sleep heartrate settings";
				scopes = wearable.getScopeURLs();

				if (!nnne(scopes)) {
					return redirect(routes.HomeController.index()).addingToSession(request, "error",
							"expect some scopes.");
				}

				switch (determineLanguage(request)) {
				case "nl":
					return ok(views.html.participation.nl.signupFitbit.render(wearable, redirectUrl,
							fitbitService.APP_CLIENT_ID, scopes, signupStatus, invite_token));
				case "de":
					return ok(views.html.participation.de.signupFitbit.render(wearable, redirectUrl,
							fitbitService.APP_CLIENT_ID, scopes, signupStatus, invite_token));
				case "en":
				default:
					return ok(views.html.participation.en.signupFitbit.render(wearable, redirectUrl,
							fitbitService.APP_CLIENT_ID, scopes, signupStatus, invite_token));
				}
			case Wearable.GOOGLEFIT:
				// need url which domain is authorized by google as redirectURL
				String redirectURL = controllers.routes.GoogleFitWearablesController.finishWearableRegistration("0")
						.absoluteURL(true, request.host());

				// scopes = wearable.scopes.split("::")[1] + "+openid";
				// scopes = "https://www.googleapis.com/auth/fitness.activity.read"
				// + "+https://www.googleapis.com/auth/fitness.body.read"
				// + "+https://www.googleapis.com/auth/fitness.location.read"
				// + "+https://www.googleapis.com/auth/fitness.heart_rate.read"
				// + "+https://www.googleapis.com/auth/fitness.sleep.read+openid";
				scopes = wearable.getScopeURLs();

				if (!nnne(scopes)) {
					return redirect(routes.HomeController.index()).addingToSession(request, "error",
							"expect some scopes.");
				}

				switch (determineLanguage(request)) {
				case "nl":
					return ok(views.html.participation.nl.signupGoogleFit.render(wearable, redirectURL,
							googlefitService.APP_CLIENT_ID, scopes, signupStatus, invite_token));
				case "de":
					return ok(views.html.participation.de.signupGoogleFit.render(wearable, redirectURL,
							googlefitService.APP_CLIENT_ID, scopes, signupStatus, invite_token));
				case "en":
				default:
					return ok(views.html.participation.en.signupGoogleFit.render(wearable, redirectURL,
							googlefitService.APP_CLIENT_ID, scopes, signupStatus, invite_token));
				}
			default:
				return redirect(routes.HomeController.index());
			}
		}

		return redirect(routes.HomeController.index());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * determine the selected user language from PLAY_LANG cookie or from the browser settings
	 * 
	 * @param request
	 * @return
	 */
	private String determineLanguage(Request request) {
		Optional<Cookie> cookie = request.cookie("PLAY_LANG");
		if (cookie.isEmpty()) {
			return langs.preferred(request.acceptLanguages()).language();
		} else {
			return cookie.get().value();
		}
	}

	private String csrfToken(Request request) {
		return CSRF.getToken(request).get().value();
	}

	private String session(Request request, String key) {
		return request.session().get(key).orElse("");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * null safe string
	 */
	protected final String nss(String s) {
		return StringUtils.nss(s);
	}

	/**
	 * null safe string with max length
	 */
	protected final String nss(String s, int maxLength) {
		return StringUtils.nss(s, maxLength);
	}

	/**
	 * escape HTML meta characters in String
	 * 
	 * @param s
	 * @return
	 */
	protected final String htmlEscape(String s) {
		return StringUtils.htmlEscape(s);
	}

	/**
	 * valid text = Not Null and Not Empty
	 */
	protected static boolean nnne(String text) {
		return StringUtils.nnne(text);
	}

	/**
	 * format / escape string for CSV output
	 */
	protected final String cf(String s) {
		return StringUtils.cf(s);
	}

}

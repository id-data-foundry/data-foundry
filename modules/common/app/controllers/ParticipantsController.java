package controllers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.inject.Inject;

import controllers.auth.UserAuth;
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
import models.sr.TelegramSession;
import models.sr.Wearable;
import models.vm.TimedAnnotatedMedia;
import models.vm.TimedText;
import play.Logger;
import play.api.mvc.Call;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.ws.WSClient;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import play.twirl.api.Html;
import services.email.NotificationService;
import services.telegrambot.TelegramBotService;
import utils.DataUtils;
import utils.auth.TokenResolverUtil;
import utils.components.OnboardingSupport;
import utils.conf.Configurator;

public class ParticipantsController extends AbstractAsyncController {

	private static final Call PROJECTS = routes.ProjectsController.index();

	private final Configurator configurator;
	private final FormFactory formFactory;
	private final TokenResolverUtil tokenResolverUtil;
	private final OnboardingSupport onboardingSupport;
	private final DatasetConnector datasetConnector;
	private final NotificationService notificationService;
	private final TelegramBotService telegramBotUtils;
	private final WSClient ws;
	private static final Logger.ALogger logger = Logger.of(ParticipantsController.class);

	@Inject
	public ParticipantsController(Configurator configurator, FormFactory formFactory,
	        TokenResolverUtil tokenResolverUtil, OnboardingSupport onboardingSupport, DatasetConnector datasetConnector,
	        NotificationService notificationService, TelegramBotService telegramBotService, WSClient ws) {
		this.configurator = configurator;
		this.formFactory = formFactory;
		this.tokenResolverUtil = tokenResolverUtil;
		this.onboardingSupport = onboardingSupport;
		this.datasetConnector = datasetConnector;
		this.notificationService = notificationService;
		this.telegramBotUtils = telegramBotService;
		this.ws = ws;
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// load participant
		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return redirect(HOME);
		}

		// check authorization
		Project project = participant.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be project owner or collaborator.");
		}

		List<Device> clusterDevices = participant.getClusterDevices();
		List<Wearable> clusterWearables = participant.getClusterWearables();

		final Dataset diaryDS = project.getDiaryDataset();
		final List<TimedAnnotatedMedia> annotatedMedia;
		final Dataset mediaDS = project.getMediaDataset();
		if (mediaDS != null && mediaDS.getId() > -1) {
			MediaDS mds = (MediaDS) datasetConnector.getDatasetDS(mediaDS);
			annotatedMedia = mds.getMediaForParticipant(id);

			// merge annotations into the media files
			if (diaryDS != null && diaryDS.getId() > -1) {
				DiaryDS dds = (DiaryDS) datasetConnector.getDatasetDS(diaryDS);
				List<TimedText> tts = dds.getDiaryForParticipant(id);
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

		List<Dataset> datasets = new LinkedList<>();
		datasets.addAll(project.getDiaryDatasets());
		datasets.addAll(project.getMediaDatasets());
		datasets.addAll(project.getSurveyDatasets());
		datasets.addAll(project.getMovementDatasets());

		// show participant overview: allow for accept and decline options
		return ok(views.html.sources.participant.view.render(participant, datasets, clusterDevices, clusterWearables,
		        getParticipantViewLink(project, participant.getId(), request.host()),
		        getParticipantDiaryEntryLink(project, participant.getId(), request.host()), diaryDS,
		        tokenResolverUtil.getParticipationToken(project.getId(), participant.getId()), csrfToken(request),
		        annotatedMedia, configurator));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Project project = Project.find.byId(id);
		if (project == null || (!project.editableBy(username))) {
			return redirect(PROJECT(id)).addingToSession(request, "error",
			        "You need to be project owner or collaborator.");
		}

		return ok(views.html.sources.participant.add.render(csrfToken(request), project));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Project project = Project.find.byId(id);
		if (project == null || (!project.editableBy(username))) {
			return redirect(PROJECT(id)).addingToSession(request, "error",
			        "You need to be project owner or collaborator.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Expecting some data");
		}

		// check for double registration in the same project with the same email
		final String email = nss(df.get("email")).toLowerCase();
		if (Participant.existsInProject(email, id)) {
			return redirect(routes.ProjectsController.viewResources(id)).addingToSession(request, "error",
			        "We have found a participant with this email address in this project, please choose a different address.");
		}

		Participant participant = new Participant(htmlEscape(nss(df.get("first_name"), 64)),
		        htmlEscape(nss(df.get("last_name"), 64)));
		participant.setEmail(email);
		participant.setGender(df.get("gender") == null ? 4 : DataUtils.parseInt(df.get("gender"), 4));
		participant.setCareer(htmlEscape(nss(df.get("career"))));
		participant.setAgeRange(df.get("age_range") == null ? 5 : DataUtils.parseInt(df.get("age_range"), 5));
		participant.setPublicParameter1(nss(df.get("public_parameter1")));
		participant.setPublicParameter2(nss(df.get("public_parameter2")));
		participant.setPublicParameter3(nss(df.get("public_parameter3")));
		participant.setProject(project);
		participant.create();
		participant.save();

		onboardingSupport.updateAfterDone(username, "new_participant");

		return redirect(routes.ProjectsController.viewResources(id)).addingToSession(request, "message",
		        "New participant:" + participant.getName() + " successfully added.");
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result addInBulk(Request request, Long id, int quantity) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || (!project.editableBy(username))) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be project owner or collaborator.");
		}

		return ok(views.html.sources.participant.addInBulk.render(csrfToken(request), project, quantity));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMeInBulk(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || (!project.editableBy(username))) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be project owner or collaborator.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Expecting some data.");
		}

		for (int quantity = 0; quantity < DataUtils.parseInt(df.get("quantity"), 0); quantity++) {
			// if (Participant.find.query().where().eq("email", df.get("participants_" + (quantity + 1)))
			// .findOne() == null) {
			// }
			Participant participant = new Participant(nss(df.get("participants_fname_" + (quantity + 1))),
			        nss(df.get("participants_lname_" + (quantity + 1))));
			participant.setEmail(nss(df.get("participants_" + (quantity + 1))).toLowerCase());
			participant.setProject(project);
			participant.create();
			participant.save();
		}

		return redirect(PROJECT(id));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result addInBulkSeparately(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || (!project.editableBy(username))) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be project owner or collaborator.");
		}

		return ok(views.html.sources.participant.addInBulkSeparately.render(csrfToken(request), project, configurator));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMeInBulkSeparately(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || (!project.editableBy(username))) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You need to be project owner or collaborator.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Expecting some data.");
		}
		String participants = df.get("participant_quantity");
		int participant_quantity = participants == null || participants.equals("") ? 0
		        : DataUtils.parseInt(participants, 0);

		String devices = df.get("devices_quantity");
		int devices_quantity = devices == null || devices.equals("") ? 0 : DataUtils.parseInt(devices, 0);
		final boolean needClusters = df.get("addNewCluster") == null ? false : true;
		final boolean addFitbitWearable = df.get("addNewFitbitWearable") == null ? false : true;
		final boolean addGoogleFitWearable = df.get("addNewGoogleFitWearable") == null ? false : true;

		for (int j = 0; j < participant_quantity; j++) {

			// create participant if not beenexisted
			Participant participant = new Participant("", "");
			participant.setFirstname("");
			participant.setLastname("");
			participant.setGender(4);
			participant.setAgeRange(5);
			participant.setCareer("");
			participant.setProject(project);
			participant.setPublicParameter1("");
			participant.setPublicParameter2("");
			participant.setPublicParameter3("");
			participant.create();
			participant.save();
			project.getParticipants().add(participant);

			// create cluster if necessary before adding new objects inside
			Cluster cluster = null;
			if (needClusters) {
				cluster = new Cluster(participant.getName());
				cluster.setProject(project);
				cluster.create();
				cluster.getParticipants().add(participant);
			}

			// create devices
			for (int i = 0; i < devices_quantity; i++) {
				Device device = new Device();
				device.setName(participant.getName() + "_device_" + (i + 1));
				device.setProject(project);
				device.create();
				project.getDevices().add(device);
				if (cluster != null) {
					cluster.getDevices().add(device);
				}
			}

			// create wearables
			Wearable fbWearable = null, gfWearable = null;
			if (addFitbitWearable) {
				long fbds_id = df.get("fitbitDataset") == null ? -1l
				        : DataUtils.parseLong(df.get("fitbitDataset"), -1L);
				fbWearable = new Wearable();
				fbWearable.setName(participant.getName() + "_Fitbit_wearable");
				fbWearable.setBrand(Wearable.FITBIT);
				fbWearable.setProject(project);
				fbWearable.setScopes(Long.toString(fbds_id));
				fbWearable.create();
				project.getWearables().add(fbWearable);
				if (cluster != null) {
					cluster.getWearables().add(fbWearable);
				}
			}

			if (addGoogleFitWearable) {
				long gfds_id = df.get("googleFitDataset") == null ? -1l
				        : DataUtils.parseLong(df.get("googleFitDataset"), -1L);
				gfWearable = new Wearable();
				gfWearable.setName(participant.getName() + "_GoogleFit_wearable");
				gfWearable.setBrand(Wearable.GOOGLEFIT);
				gfWearable.setProject(project);
				gfWearable.setScopes(Long.toString(gfds_id));
				gfWearable.create();
				project.getWearables().add(gfWearable);
				if (cluster != null) {
					cluster.getWearables().add(gfWearable);
				}
			}

			if (cluster != null) {
				project.getClusters().add(cluster);
			}

			project.update();

			// console output for manual unlock
			String token = tokenResolverUtil.getParticipationToken(project.getId(), participant.getId());
			logger.info(routes.ParticipationController.confirm(token).absoluteURL(true, request.host()));
		}

		return redirect(routes.ProjectsController.viewResources(id)).addingToSession(request, "message",
		        participant_quantity + " participants have been added successfully.");
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result addByEmail(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || (!project.editableBy(username))) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be project owner or collaborator.");
		}

		return ok(views.html.sources.participant.addByEmail.render(csrfToken(request), project));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMeByEmail(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check authorization
		Project project = Project.find.byId(id);
		if (project == null || (!project.editableBy(username))) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You need to be project owner or collaborator.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Expecting some data.");
		}

		String email_list = df.get("email_list");
		if (email_list == null || email_list.equals("")) {
			return redirect(PROJECT(id)).addingToSession(request, "error", "Expecting some emails.");
		}

		// clean white spaces between emails
		List<String> emails = Arrays.stream(email_list.split(",")).filter(e -> e != null).map(e -> e.trim())
		        .filter(e -> e.length() > 0).collect(Collectors.toList());

		String devices = df.get("devices_quantity");
		int devices_quantity = devices == null || devices.equals("") ? 0 : DataUtils.parseInt(devices, 0);
		final boolean needClusters = df.get("addNewCluster") == null ? false : true;
		final boolean addFitbitWearable = df.get("addNewFitbitWearable") == null ? false : true;
		final boolean addGoogleFitWearable = df.get("addNewGoogleFitWearable") == null ? false : true;

		int quantity = 0;
		for (String email : emails) {
			if (!Participant.existsInProject(email, id)) {

				// create participant if not beenexisted
				Participant participant = new Participant(email);
				participant.setFirstname("");
				participant.setLastname("");
				participant.setGender(4);
				participant.setAgeRange(5);
				participant.setCareer("");
				participant.setProject(project);
				participant.setPublicParameter1("");
				participant.setPublicParameter2("");
				participant.setPublicParameter3("");
				participant.create();
				participant.save();
				project.getParticipants().add(participant);

				// create cluster if necessary before adding new objects inside
				Cluster cluster = null;
				if (needClusters) {
					cluster = new Cluster(email);
					cluster.setProject(project);
					cluster.create();
					cluster.getParticipants().add(participant);
				}

				// create devices
				for (int i = 0; i < devices_quantity; i++) {
					Device device = new Device();
					device.setName(email + "_device_" + (i + 1));
					device.setProject(project);
					device.create();
					project.getDevices().add(device);
					if (cluster != null) {
						cluster.getDevices().add(device);
					}
				}

				// create wearables
				Wearable fbWearable = null, gfWearable = null;
				if (addFitbitWearable) {
					long fbds_id = df.get("fitbitDataset") == null ? -1l
					        : DataUtils.parseLong(df.get("fitbitDataset"), -1L);
					fbWearable = new Wearable();
					fbWearable.setName(email + "_Fitbit_wearable");
					fbWearable.setBrand(Wearable.FITBIT);
					fbWearable.setProject(project);
					fbWearable.setScopes(Long.toString(fbds_id));
					fbWearable.create();
					project.getWearables().add(fbWearable);
					if (cluster != null) {
						cluster.getWearables().add(fbWearable);
					}
				}

				if (addGoogleFitWearable) {
					long gfds_id = df.get("googleFitDataset") == null ? -1l
					        : DataUtils.parseLong(df.get("googleFitDataset"), -1L);
					gfWearable = new Wearable();
					gfWearable.setName(email + "_GoogleFit_wearable");
					gfWearable.setBrand(Wearable.GOOGLEFIT);
					gfWearable.setProject(project);
					gfWearable.setScopes(Long.toString(gfds_id));
					gfWearable.create();
					project.getWearables().add(gfWearable);
					if (cluster != null) {
						cluster.getWearables().add(gfWearable);
					}
				}

				if (cluster != null) {
					project.getClusters().add(cluster);
				}

				project.update();

				// console output for manual unlock
				String token = tokenResolverUtil.getParticipationToken(project.getId(), participant.getId());
				logger.info(routes.ParticipationController.confirm(token).absoluteURL(true, request.host()));
				quantity++;
			}
		}

		return redirect(routes.ProjectsController.viewResources(id)).addingToSession(request, "message",
		        quantity + " emails has been added successfully.");
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return redirect(PROJECTS).addingToSession(request, "error", "Participant not found.");
		}

		// check authorization
		Project project = participant.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You need to be project owner or collaborator.");
		}

		return ok(views.html.sources.participant.edit.render(csrfToken(request), participant));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return redirect(PROJECTS).addingToSession(request, "error", "Participant not found.");
		}

		// check authorization
		Project project = participant.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You need to be project owner or collaborator.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(routes.ParticipantsController.edit(id)).addingToSession(request, "error",
			        "Expecting some data.");
		}

		final String email = nss(df.get("email")).toLowerCase();

		// check for double registration in the same project with a new email
		if (!email.equals(participant.getEmail())
		        && project.getParticipants().stream().anyMatch(pt -> pt.getEmail().equals(email))) {
			return redirect(routes.ParticipantsController.edit(id)).addingToSession(request, "error",
			        "We have found a participant with this email address in this project, please choose a different address.");
		}
		// email cannot be changed into an empty or NULL email
		else if (!email.isEmpty()) {
			// Note: email can be changed by researcher, but not by participant
			participant.setEmail(email);
		}

		participant.setFirstname(nss(df.get("first_name")));
		participant.setLastname(nss(df.get("last_name")));
		participant.setGender(df.get("gender") == null ? 4 : DataUtils.parseInt(df.get("gender"), 4));
		participant.setCareer(nss(df.get("career")));
		participant.setAgeRange(df.get("age_range") == null ? 5 : DataUtils.parseInt(df.get("age_range"), 5));
		participant.setPublicParameter1(nss(df.get("public_parameter1")));
		participant.setPublicParameter2(nss(df.get("public_parameter2")));
		participant.setPublicParameter3(nss(df.get("public_parameter3")));

		participant.update();

		LabNotesEntry.log(Participant.class, LabNotesEntryType.MODIFY, "Participant changed: " + participant.getName(),
		        project);

		return redirect(routes.ParticipantsController.view(participant.getId())).addingToSession(request, "message",
		        "We have changed the participant details.");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result recordForm(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return redirect(PROJECTS).addingToSession(request, "error", "Participant not found.");
		}

		// check authorization
		Project project = participant.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You need to be project owner or collaborator.");
		}

		Dataset ds = project.getDiaryDataset();
		if (ds == null) {
			return redirect(PROJECT(participant.getProject().getId())).addingToSession(request, "error",
			        "Dataset not found.");
		} else if (!ds.canAppend()) {
			return redirect(PROJECT(participant.getProject().getId())).addingToSession(request, "error",
			        "Dataset inaccessible.");
		}

		return ok(views.html.sources.participant.record.render(participant, csrfToken(request), ds));
	}

	/**
	 * Record a diary entry for a participant in a diary dataset
	 * 
	 * @param id        Project id
	 * @param datasetId Dataset id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result recordDiary(Request request, Long id, Long datasetId) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return redirect(PROJECTS).addingToSession(request, "error", "Participant not found.");
		}

		// check authorization
		Project project = participant.getProject();
		project.refresh();
		if (!project.editableBy(username)) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be project owner or collaborator.");
		}

		final Dataset ds = Dataset.find.byId(datasetId);
		if (ds == null) {
			return redirect(PROJECT(participant.getProject().getId())).addingToSession(request, "error",
			        "Dataset not found.");
		} else if (!ds.canAppend()) {
			return redirect(PROJECT(participant.getProject().getId())).addingToSession(request, "error",
			        "Dataset inaccessible.");
		}

		// check dataset type
		if (ds.getDsType() != DatasetType.DIARY) {
			return redirect(PROJECT(participant.getProject().getId())).addingToSession(request, "error",
			        "Wrong dataset type.");
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
		String text = nss(df.get("text")).replace("\n", "<br>").replaceAll("[\\p{Cntrl}\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}]",
		        "");

		// TODO Is Markdown formatting needed?

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
		return redirect(controllers.routes.ParticipantsController.view(id)).addingToSession(request, "message",
		        "Diary entry recorded, thanks!");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * send telegram message
	 * 
	 * @param id Project id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result sendTelegramMessage(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		if (telegramBotUtils == null) {
			return internalServerError();
		}

		// check authorization
		Project project = Project.find.byId(id);
		if (!project.editableBy(username)) {
			return redirect(PROJECTS).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be project owner or collaborator.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return badRequest("Expecting some data");
		}

		// title
		final long pid = DataUtils.parseLong(nss(df.get("participant_id")), -1L);

		// message text
		String message = nss(df.get("message")).replace("\n", " ")
		        .replaceAll("[\\p{Cntrl}\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}]", "");

		if (pid == -1) {
			// send instant message to all participants
			project.getParticipants().forEach(p -> {
				ExecutorService newSingleThreadExecutor = Executors.newSingleThreadExecutor();
				telegramBotUtils.sendMessageToParticipant(project.getId(), p.getEmail(), message,
				        newSingleThreadExecutor);
			});
			return redirect(routes.ProjectsController.viewResources(id)).addingToSession(request, "message",
			        "Telegram message sent out successfully.");
		} else {
			TelegramSession ts = TelegramSession.find.byId(pid);
			if (ts != null) {
				// send instant message to one participant
				ExecutorService newSingleThreadExecutor = Executors.newSingleThreadExecutor();
				telegramBotUtils.sendMessageToParticipant(project.getId(), ts.getEmail(), message,
				        newSingleThreadExecutor);
				return redirect(routes.ProjectsController.viewResources(id)).addingToSession(request, "message",
				        "Telegram message sent out successfully.");
			} else {
				return redirect(routes.ProjectsController.viewResources(id)).addingToSession(request, "error",
				        "Participant could not be found.");
			}
		}
	}

	/**
	 * retrieve an image from Telegram given by the fileId
	 * 
	 * @param fileId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> getTelegramImage(String fileId) {
		// https://api.telegram.org/file/bot<token>/<file_path>
		return ws.url("https://api.telegram.org/file/bot" + telegramBotUtils.getBotToken() + "/" + fileId)
		        .setFollowRedirects(true).get().toCompletableFuture().thenApply(ws -> {
			        return ok(ws.asByteArray());
		        });
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * send email to the participant
	 * 
	 * @param id Participant id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result sendLink(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// check participant object
		Participant participant = Participant.find.byId(id);
		if (participant == null) {
			return redirect(HOME).addingToSession(request, "error", "Participant not found.");
		}
		if (!participant.accepted() || participant.getEmail().isEmpty()) {
			return redirect(HOME).addingToSession(request, "error", "Participant not accepted yet.");
		}

		Project project = participant.getProject();
		project.refresh();

		// check the project permissions
		if (!project.editableBy(username)) {
			logger.info("User, " + username + ", is not available to send invite link to participant in project - "
			        + project.getName() + ".");
			return redirect(routes.ParticipantsController.view(id)).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be project owner or collaborator.");
		}

		// create invite link

		// send email to participant with invite link
		Html htmlBody = views.html.emails.invite.render("Participation link", String.format("Hi "
		        + Participant.find.byId(participant.getId()).getRealName() + ","
		        + "\n\nThe following is the participation link for you to join the project:" + project.getName() + ","
		        + "please finish the consent form and your basic information to complete the participation process."
		        + "If you change your mind not to join this project, feel free to DECLINE this invitation by the link."),
		        getParticipantViewLink(project, participant.getId(), request.host()));
		String textBody = "Hello! \n\nWe send you this email for your participation in DataFondry."
		        + "Just click the link below to be forwarded to a form where you can finish the participation process."
		        + "If you change your mind not to join this project, feel free to DECLINE this invitation by the link. \n\n"
		        + getParticipantViewLink(project, participant.getId(), request.host()) + "\n\n";

		notificationService.sendMail(participant.getEmail(), textBody, htmlBody,
		        getParticipantViewLink(project, participant.getId(), request.host()), "[ID Data Foundry] Invitation",
		        "Invitation link: " + getParticipantViewLink(project, participant.getId(), request.host()));

		return redirect(routes.ParticipantsController.view(participant.getId())).addingToSession(request, "message",
		        "Ok, we sent an invite link to participant " + participant.getName());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public String getParticipantViewLink(Project project, Long participant_id, String host) {
		return routes.ParticipationController
		        .view(tokenResolverUtil.getParticipationToken(project.getId(), participant_id)).absoluteURL(true, host);
	}

	public String getParticipantDiaryEntryLink(Project project, Long participant_id, String host) {
		Dataset ds = project.getDiaryDataset();
		if (ds != null) {
			return routes.ParticipationController
			        .recordForm(ds.getId(), tokenResolverUtil.getParticipationToken(project.getId(), participant_id))
			        .absoluteURL(true, host);
		} else {
			return "";
		}
	}

}

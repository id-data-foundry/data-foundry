package models.sr;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.telegram.telegrambots.meta.api.objects.Location;
import org.telegram.telegrambots.meta.api.objects.User;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vdurmont.emoji.EmojiParser;

import datasets.DatasetConnector;
import io.ebean.Finder;
import io.ebean.Model;
import io.ebean.annotation.DbJson;
import io.ebean.annotation.DbJsonType;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Transient;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.DiaryDS;
import models.ds.MovementDS;
import play.Logger;
import play.cache.SyncCacheApi;
import play.libs.Json;
import services.telegrambot.TelegramResponse;
import utils.DataUtils;
import utils.auth.TokenResolverUtil;
import utils.telegrambot.TelegramBotUtils;

@Entity
public class TelegramSession extends Model {

	private static final String START = "/start";
	private static final String STOP = "/stop";
	private static final String HELP = "/help";
	private static final String CANCEL = "/cancel";
	private static final String DONE = "/cancel";
	private static final String PROJECT = "/project";
	private static final String ANSWER_RESEARCHER = "researcher";
	private static final String ANSWER_PARTICIPANT = "participant";
	private static final String MESSAGE = "/message";
	private static final String MESSAGE_ALL = "/messageall";
	private static final String DIARY = "/diary";
	private static final String PHOTO = "/photo";
	private static final String LOCATION = "/location";
	private static final Logger.ALogger logger = Logger.of(TelegramSession.class);

	public static enum TelegramSessionState {
		NEW, REGISTERING, PARTICIPANT, RESEARCHER, IDLE
	}

	public static enum TelegramSessionAction {

		// idle, waiting for stuff to happen
		NONE,

		// from /start to authenticated participant or researcher
		REGISTER_AS_PARTICIPANT, REGISTER_AS_RESEARCHER,

		// from participant to study (consent form)
		SIGNIN_PROJECT,

		// from researcher
		ACTIVATE_PROJECT,

		// based on project: consented participant contributing ...
		DIARY, SURVEY, MEDIA,

		// participant: message researcher
		MESSAGE_RESEARCHER,

		// researcher: message participant(s)
		MESSAGE_PARTICIPANTS, MESSAGE_PARTICIPANT
	}

	@Id
	private Long id;
	/**
	 * what's the global state {new, registering, participant, researcher, idle}
	 */
	private String state;
	/**
	 * which is currently pursued
	 */
	private String action;
	/**
	 * time of last action
	 */
	private Date lastAction;
	/**
	 * id of currently active project
	 */
	private long activeProjectId = -1;
	/**
	 * Telegram chat id
	 */
	private long chatId = -1;
	/**
	 * Telegram username
	 */
	private String username;
	/**
	 * email address registered in DF
	 */
	private String email;
	/**
	 * provided token for authentication
	 */
	private String token;

	/**
	 * messages exchanged between the researcher and this participant (in this TelegramSession)
	 */
	@DbJson(storage = DbJsonType.BLOB)
	@ElementCollection(targetClass = String.class)
	private List<String> messages = new ArrayList<String>();

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * non-persisted reference to participant
	 */
	@Transient
	public volatile Participant participant;
	/**
	 * non-persisted reference to researcher
	 */
	@Transient
	public volatile Person researcher;

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static final Finder<Long, TelegramSession> find = new Finder<Long, TelegramSession>(TelegramSession.class);

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public TelegramSession() {
		super();
	}

	/**
	 * call this to pre-fill the researcher or participant reference, so you can directly make use of it in the
	 * processing
	 * 
	 */
	public void initialize() {
		if (researcher == null && this.getEmail() != null && is(TelegramSessionState.RESEARCHER)) {
			Optional<Person> rOpt = Person.findByEmail(this.getEmail());
			if (rOpt.isPresent()) {
				researcher = rOpt.get();
			}
		} else if (participant == null && this.getEmail() != null && is(TelegramSessionState.PARTICIPANT)) {
			Optional<Participant> rOpt = Participant.findByEmailAndProject(this.getEmail(), this.getActiveProjectId());
			if (rOpt.isPresent()) {
				participant = rOpt.get();
			}
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * return name of communication partner
	 * 
	 * @return
	 */
	public String getUserScreenName() {
		if (participant != null) {
			return participant.getName();
		} else if (researcher != null) {
			return researcher.getName();
		} else {
			return "Anonymous";
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public List<String> getMessages() {
		return messages;
	}

	public void setMessages(List<String> messages) {
		this.messages = messages;
	}

	public Person getResearcher() {
		return researcher;
	}

	public void setResearcher(Person researcher) {
		this.researcher = researcher;
	}

	public void setParticipant(Participant participant) {
		this.participant = participant;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public boolean is(TelegramSessionState tss) {
		return (getState() == null && tss == TelegramSessionState.NEW)
				|| (getState() != null && getState().toUpperCase().equals(tss.name()));
	}

	public boolean currently(TelegramSessionAction tsa) {

		if (getAction() == null) {
			return (getAction() == null && tsa == TelegramSessionAction.NONE);
		}

		// try to separate target id and TelegramSessionAction
		String[] comps = null;
		try {
			comps = getAction().split(" ", 2);
		} catch (Exception e) {
			logger.error("action could not be split properly", e);
		}

		return (getAction() != null && comps != null && comps[0] != null && comps[0].toUpperCase().equals(tsa.name()));
	}

	/**
	 * return true if this session operated in past 5 mins
	 * 
	 */
	public boolean isActive() {
		return this.getLastAction() != null
				&& this.getLastAction().after(new Date(System.currentTimeMillis() - (5 * 60 * 1000l)));
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public boolean missingEmail() {
		return this.getEmail() == null || this.getEmail().isEmpty();
	}

	public boolean missingToken() {
		return this.getToken() == null || this.getToken().isEmpty();
	}

	public void register() {
		this.setState(TelegramSessionState.REGISTERING.name());
	}

	public void startAction(TelegramSessionAction tsAction) {
		this.setAction(tsAction.name());
	}

	private void completeAction() {
		startAction(TelegramSessionAction.NONE);
	}

	public void cancelAction() {
		this.setAction(TelegramSessionAction.NONE.name());
	}

	public void stopSession() {
		try {
			this.setState(TelegramSessionState.NEW.name());
			this.setAction(TelegramSessionAction.NONE.name());
			this.setEmail(null);
			this.setToken(null);
			this.setActiveProjectId(-1);
			this.update();

			// this seems to be tricky in development?
			this.messages.clear();
			this.update();
		} catch (Exception e) {
			logger.error("stopSession Ebean problem", e);
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * add a message from a given sender to the array of message in this session
	 * 
	 * @param from
	 * @param message
	 * @return
	 */
	public String addMessage(String from, String message) {
		final String attributedMessage = "[" + from + "]: " + message;
		final SimpleDateFormat messageDateFormatter = new SimpleDateFormat("MMM d 'at' HH:mm");
		String result = messageDateFormatter.format(new Date()) + " " + attributedMessage;
		this.messages.add(result);
		this.update();
		return attributedMessage;
	}

	/**
	 * add a fileId from a given sender to the array of fileIds in this session
	 * 
	 * @param sender
	 * @param caption
	 * @param filePath
	 * @return
	 */
	public String addTelegramPhoto(String sender, String caption, String filePath) {
		final String attributedMessage = "[" + sender + "]: " + caption;
		final SimpleDateFormat messageDateFormatter = new SimpleDateFormat("MMM d 'at' HH:mm");
		String result = messageDateFormatter.format(new Date()) + " " + attributedMessage + "\n" + "image:" + filePath;
		this.messages.add(result);
		this.update();
		return filePath;
	}

	/**
	 * add a fileId from a given sender to the array of fileIds in this session
	 * 
	 * @param sender
	 * @param caption
	 * @param filePath
	 * @return
	 */
	public String addLocalPhoto(String sender, String caption, String fileUrl) {
		final String attributedMessage = "[" + sender + "]: " + caption;
		final SimpleDateFormat messageDateFormatter = new SimpleDateFormat("MMM d 'at' HH:mm");
		String result = messageDateFormatter.format(new Date()) + " " + attributedMessage + "\n" + "img:" + fileUrl;
		this.messages.add(result);
		this.update();
		return fileUrl;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether the participant is given and can access an active Diary dataset to store text
	 * 
	 * @return
	 */
	private boolean hasActiveDiaryDataset() {
		if (participant != null) {
			// needs a refresh because of secondary join
			participant.getProject().refresh();
			return participant.getProject().getDiaryDataset().getId() > -1;

		}
		return false;
	}

	/**
	 * check whether the participant is given and can access an active Media dataset to store photos
	 * 
	 * @return
	 */
	public boolean hasActiveMediaDataset() {
		if (participant != null) {
			// needs a refresh because of secondary join
			Project project = participant.getProject();
			project.refresh();
			// check for available active media dataset and that the participant has accepted the study
			return project.getMediaDataset().getId() > -1
					&& (participant.accepted() || project.getMediaDataset().isOpenParticipation());
		}
		return false;
	}

	/**
	 * retrieve the active media dataset
	 * 
	 * @return
	 */
	public Optional<Dataset> activeMediaDataset() {
		if (participant != null) {
			// needs a refresh because of secondary join
			participant.getProject().refresh();
			// check for available active media dataset which is also open for participation
			return Optional.ofNullable(participant.getProject().getMediaDataset());
		} else {
			return Optional.empty();
		}
	}

	/**
	 * check whether the participant is given and can access an active Movement dataset to store the current location
	 * 
	 * @return
	 */
	private boolean hasActiveMovementDataset() {
		if (participant != null) {
			// needs a refresh because of secondary join
			participant.getProject().refresh();
			return participant.getProject().getMovementDataset().getId() > -1;
		}
		return false;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * process a command
	 * 
	 * @param text
	 * @return
	 */
	public List<TelegramResponse> processCommand(String text, Optional<User> userOpt, TokenResolverUtil tokenResolver) {

		final List<TelegramResponse> replies = new ArrayList<>();

		// STOP
		if (text.startsWith(STOP)) {
			stopSession();
			replies.add(response(
					"Ok, see you around! :wave: You can click the START button below to start a new conversation.",
					new String[] { START }));
		}
		// START
		else if (text.startsWith(START)) {
			// check token in /start command
			if (text.contains(" ") && userOpt.isPresent()) {
				String startToken = text.split(" ")[1];
				long projectId = tokenResolver.getProjectIdFromBase32ParticipationToken(startToken);

				// check correct project id
				Project project = Project.find.byId(projectId);
				if (project != null && project.isSignupOpen()) {
					User user = userOpt.get();
					Optional<Participant> participantOpt = project.getParticipants().stream().filter(p -> {
						String legacyUserName = TelegramBotUtils.getLegacyUserName(user);
						return (legacyUserName != null && !p.getEmail().isEmpty()
								&& p.getEmail().equals(legacyUserName))
								|| p.getEmail().equals(TelegramBotUtils.getUserName(user));
					}).findAny();
					if (participantOpt.isPresent()) {
						this.participant = participantOpt.get();
						this.setEmail(participant.getEmail());
					} else {
						// if signup open, create new participant
						Participant newParticipant = new Participant(TelegramBotUtils.getUserName(user));
						newParticipant.setFirstname(user.getFirstName());
						newParticipant.setLastname(user.getLastName());
						newParticipant.setProject(project);
						newParticipant.save();

						// store participant in setup
						project.getParticipants().add(newParticipant);
						project.update();

						// complete the session setup
						this.participant = newParticipant;
						this.setEmail(TelegramBotUtils.getUserName(user));
					}
					this.setActiveProjectId(project.getId());
					this.setState(TelegramSessionState.PARTICIPANT.name());
					this.update();

					// respond that all is well
					replies.add(response("Thanks for signing up. :+1:", getParticipantOptions()));
				}
			} else if (is(TelegramSessionState.NEW) || is(TelegramSessionState.REGISTERING)) {
				register();

				// response with the next steps
				replies.add(response("Hey, it seems we have never met, are you a participant or researcher?",
						new String[] { ANSWER_PARTICIPANT, ANSWER_RESEARCHER }));
			} else {
				// do nothing, the participant is already registered
			}
		}
		// NEW session
		else if (is(TelegramSessionState.NEW)) {
			register();

			// response with the next steps
			replies.add(response("Hey, it seems we have never met, are you a participant or researcher?",
					new String[] { ANSWER_PARTICIPANT, ANSWER_RESEARCHER }));
		}
		// CANCEL
		else if (text.startsWith(CANCEL)) {
			cancelAction();
			if (is(TelegramSessionState.RESEARCHER)) {
				this.setActiveProjectId(-1);
				replies.add(response(":+1: Got it.", new String[] { PROJECT, HELP }));
			} else if (is(TelegramSessionState.PARTICIPANT)) {
				replies.add(response("Done. :+1:"));
			} else {
				if (is(TelegramSessionState.REGISTERING)) {
					this.setEmail(null);
					this.setToken(null);
				}
				replies.add(response("Nice to meet you :) \nYou can click START button to get back in connection.",
						new String[] { START }));
			}
		}
		// HELP
		else if (text.startsWith(HELP)) {
			if (is(TelegramSessionState.RESEARCHER)) {
				replies.add(response(
						"You can switch the active project by typing /project, and within a project you can use /messageAll to send a message to all participants or /message for a single participant (we will let you choose which one).",
						new String[] { PROJECT, MESSAGE, MESSAGE_ALL, HELP }));
			} else if (is(TelegramSessionState.PARTICIPANT)) {
				replies.add(replyParticipantHelp(""));
			} else {
				replies.add(response("Click START button to start conversation.", new String[] { START }));
			}
		}
		// BY SESSION: researcher
		else if (researcher != null && is(TelegramSessionState.RESEARCHER)) {
			// PROJECT
			if (text.startsWith(PROJECT)) {
				final List<Project> researchProjects = researcher.getOwnAndCollabProjects();
				if (researchProjects.isEmpty()) {
					replies.add(response("It looks that you don't have any project yet."));
				} else {
					String[] comps = text.split(" ", 2);
					// check if we are indexing already
					if (comps.length == 2 && comps[1] != null && comps[1].trim().length() > 0) {
						// parse index and select it
						try {
							// parse and convert from 1-based to 0-based index
							int index = DataUtils.parseInt(comps[1]) - 1;
							if (index >= 0 && index < researchProjects.size()) {
								Project project = researchProjects.get(index);
								this.setActiveProjectId(project.getId());
								completeAction();
								replies.add(response("Selected project #" + (index + 1) + ":" + project.getName()
										+ ". You can now /messageAll participants or /message individuals."));
							}
						} catch (Exception e) {
							// parsing error is no problem, just output the list below
						}
					}

					if (replies.isEmpty()) {
						// output the list of projects
						AtomicInteger mi = new AtomicInteger(1);
						String indexedProjectList = researchProjects.stream()
								.map(p -> mi.getAndIncrement() + " " + p.getName()).collect(Collectors.joining(", "));
						// startAction(TelegramSessionAction.ACTIVATE_PROJECT);
						replies.add(response(
								"Yes, choose one of your current projects, type '/project 1' for the first project: "
										+ indexedProjectList));
					}
				}
			}
			// MESSAGE ALL
			else if (text.startsWith(MESSAGE_ALL)) {
				if (this.getActiveProjectId() == -1) {
					replies.add(response("Almost, you need to first select a /project."));
				} else {
					if (getActiveProject().getParticipants().isEmpty()) {
						replies.add(response(
								":woman_shrug: There are no particiants in this project yet, add them first please."));
					} else {
						// message all activated
						startAction(TelegramSessionAction.MESSAGE_PARTICIPANTS);
						replies.add(response("Type your message and send it, we will take care of delivery.",
								new String[] { DONE }));
					}
				}
			}
			// MESSAGE ONE PARTICIPANT
			else if (text.startsWith(MESSAGE) && this.getActiveProjectId() > -1) {
				// get project participants
				if (this.getActiveProjectId() == -1) {
					replies.add(response("Almost, you need to first select a /project."));
				} else {
					Project project = getActiveProject();

					String[] comps = text.split(" ", 2);
					// check if we are indexing already
					if (comps.length == 2 && comps[1] != null && comps[1].trim().length() > 0
							&& this.getActiveProjectId() > -1) {
						// parse index and select it
						try {
							// parse and convert from 1-based to 0-based index
							int index = DataUtils.parseInt(comps[1]) - 1;
							if (index >= 0 && index < project.getParticipants().size()) {
								setAction(TelegramSessionAction.MESSAGE_PARTICIPANT + " " + index);
								replies.add(response("Type your message and send it, we will take care of delivery.",
										new String[] { DONE }));
							}
						} catch (Exception e) {
							// parsing error is no problem, just output the list below
						}
					}

					// nobody selected
					if (replies.isEmpty()) {
						if (project.getParticipants().isEmpty()) {
							replies.add(response(
									":woman_shrug: There are no particiants in this project yet, add them first please."));
						} else {
							// output the entire list
							AtomicInteger mi = new AtomicInteger(1);
							String participantList = project.getParticipants().stream()
									.map(p -> mi.getAndIncrement() + " " + p.getName())
									.collect(Collectors.joining(", "));
							replies.add(response("Select any participant to message, type '/message 1' for the first. "
									+ participantList));
						}
					}
				}
			} else {
				replies.add(defaultResponse());
			}
		}
		// BY SESSION: participant
		else if (participant != null && is(TelegramSessionState.PARTICIPANT)) {
			// MESSAGE RESEARCHER
			if (text.startsWith(MESSAGE)) {
				startAction(TelegramSessionAction.MESSAGE_RESEARCHER);
				replies.add(response("Ready to go, type your messages or click /cancel to end sending messages.",
						new String[] { DONE }));
			}
			// DIARY
			else if (text.startsWith(DIARY)) {
				// check diary dataset
				if (!hasActiveDiaryDataset()) {
					replies.add(replyParticipantHelp("Currently, you cannot post diary notes. "));
				} else {
					startAction(TelegramSessionAction.DIARY);
					replies.add(response("Opened the diary, ready here :memo:", new String[] { DONE }));
				}
			}
			// PHOTO
			else if (text.startsWith(PHOTO)) {
				// check media dataset
				if (!hasActiveMediaDataset()) {
					replies.add(replyParticipantHelp("Currently, you cannot upload photos. "));
				} else {
					startAction(TelegramSessionAction.MEDIA);
					// replies.add(response("Cool, go ahead and send us a picture :camera:."));
					replies.add(replyParticipantHelp("Cool, go ahead and send us a picture :camera:. "));
				}
			} else {
				replies.add(defaultResponse());
			}
		} else {
			replies.add(defaultResponse());
		}

		return replies;
	}

	/**
	 * process a text message
	 * 
	 * @param text
	 * @param datasetConnector
	 * @return
	 */
	public List<TelegramResponse> processTextMessage(final String text, Optional<User> userOpt, SyncCacheApi cache,
			DatasetConnector datasetConnector) {

		final List<TelegramResponse> replies = new ArrayList<>();

		// REGISTRATION state
		if (is(TelegramSessionState.REGISTERING)) {
			if (currently(TelegramSessionAction.NONE)) {
				String checkTitle = text.toLowerCase();
				if (checkTitle.startsWith("researcher") || checkTitle.contains("researcher")) {
					startAction(TelegramSessionAction.REGISTER_AS_RESEARCHER);
					if (missingEmail()) {
						replies.add(
								response("Ok, could you type your email address quickly?", new String[] { CANCEL }));
					} else if (missingToken()) {
						replies.add(response("Ok, could you enter your PIN?", new String[] { CANCEL }));
					}
				} else if (text.startsWith("participant") || checkTitle.contains("participant")) {
					startAction(TelegramSessionAction.REGISTER_AS_PARTICIPANT);
					if (missingEmail()) {
						replies.add(
								response("Ok, could you type your email address quickly?", new String[] { CANCEL }));
					} else if (missingToken()) {
						replies.add(response("Ok, could you enter your PIN?", new String[] { CANCEL }));
					}
				} else {
					replies.add(response("Hey, are you a participant or researcher?",
							new String[] { ANSWER_PARTICIPANT, ANSWER_RESEARCHER }));
				}
			} else {
				String trimmedText = text.trim();
				if (currently(TelegramSessionAction.REGISTER_AS_RESEARCHER) && !text.equals("researcher")) {
					// first step
					if (missingEmail()) {
						// check for valid email
						Optional<Person> person = Person.findByEmail(text);
						if (person.isPresent()) {
							Person r = person.get();
							this.setEmail(text.toLowerCase());
							replies.add(response(":smiley:" + " Hi " + r.getName()
									+ "! I knew I recognized your style. Could you quickly enter the PIN from your 'profile' page (looks like 12345)?"));
						} else {
							replies.add(response(
									":female_shrug: Could not find you, can you try again? Or /cancel this operation.",
									new String[] { CANCEL }));
						}
					}
					// second step
					else if (missingToken()) {
						// check token
						Optional<String> cachedToken = cache
								.get(TelegramBotUtils.TG_USER_CACHE_PREFIX + this.getEmail());
						if (cachedToken.isPresent() && cachedToken.get().trim().length() > 0
								&& cachedToken.get().equals(trimmedText)) {
							this.setToken(text);
							completeAction();
							this.setState(TelegramSessionState.RESEARCHER.name());
							replies.add(response(
									":+1: Worked perfectly. Now you can choose the active project by typing /project.",
									new String[] { PROJECT }));
						} else {
							// fail
							replies.add(response(":female_shrug: That PIN was not correct, can you check? Or /cancel",
									new String[] { CANCEL }));
						}
					}
				} else if (currently(TelegramSessionAction.REGISTER_AS_PARTICIPANT)
						&& !text.equalsIgnoreCase("participant")) {

					// first step (with anonymous sign-up, the first step just records the email)
					if (missingEmail()) {
						this.setEmail(text.toLowerCase());
						replies.add(response(
								":smiley: Hi, can you type the sign-up code? Something like 12345 or ABCDEFGH."));
					}
					// second step
					else if (missingToken() && userOpt.isPresent()) {
						// check token and participation in a particular study
						// the token is very important not only to verify, but also to assign the participant to the
						// right
						// study (project)

						// anonymous participant with project PIN
						if (TelegramBotUtils.isValidTelegramProjectCode(trimmedText)) {
							long[] ids = TelegramBotUtils.extractIdsFromtelegramProjectCode(trimmedText);
							Project project = Project.find.byId(ids[0]);
							if (project != null) {
								Optional<Dataset> ds = project.getStudyManagementDataset();
								if (ds.isPresent()) {
									String token = ds.get().configuration(Dataset.TELEGRAM_PROJECT_TOKEN, "");
									if (token.equals(trimmedText.toUpperCase())) {
										// open participation and ready to go
										User user = userOpt.get();

										// initialize the Telegram session
										this.setEmail(TelegramBotUtils.getUserName(user));
										this.setActiveProjectId(project.getId());
										this.setState(TelegramSessionState.PARTICIPANT.name());

										// check if we have by chance an existing user in the project
										Optional<Participant> existingUserOpt = project.getParticipants().stream()
												.filter(p -> {
													String legacyUserName = TelegramBotUtils.getLegacyUserName(user);
													return (legacyUserName != null && !p.getEmail().isEmpty()
															&& p.getEmail().equals(legacyUserName))
															|| p.getEmail().equals(TelegramBotUtils.getUserName(user));
												}).findAny();
										if (existingUserOpt.isPresent()) {
											// complete the session setup
											this.participant = existingUserOpt.get();
											this.update();
										} else {
											// if signup open, create new participant
											Participant newParticipant = new Participant(
													TelegramBotUtils.getUserName(user));
											newParticipant.setFirstname(user.getFirstName());
											newParticipant.setLastname(user.getLastName());
											newParticipant.setProject(project);
											newParticipant.save();

											// store participant in setup
											project.getParticipants().add(newParticipant);
											project.update();

											// complete the session setup
											this.participant = newParticipant;
											this.update();
										}

										// respond that all is well
										replies.add(response("Thanks for signing up. :+1:", getParticipantOptions()));
									}
								}

							}
						}
						// participant-specific numerical PIN
						else {
							List<Participant> participants = Participant.findByEmail(this.getEmail());
							for (final Participant p : participants) {
								Optional<String> cachedToken = cache
										.get(TelegramBotUtils.TG_PARTICIPANT_CACHE_PREFIX + p.getId());
								if (cachedToken.isPresent() && cachedToken.get().trim().length() > 0) {
									String[] cachedTokenComponents = cachedToken.get().split(",", 2);
									String pin = cachedTokenComponents[0];
									try {
										Long projectId = DataUtils.parseLong(cachedTokenComponents[1]);
										if (pin.equals(trimmedText) && projectId.equals(p.getProject().getId())) {
											completeAction();
											this.setToken(trimmedText);
											this.participant = p;
											this.setActiveProjectId(p.getProject().getId());
											this.setState(TelegramSessionState.PARTICIPANT.name());
											replies.add(response(
													":+1: Worked perfectly. Now you can wait for messages and respond when available."));
											break;
										}
									} catch (Exception e) {
									}
								}
							}
						}

						if (replies.isEmpty()) {
							// fail
							replies.add(response(":female_shrug: That PIN was not correct, can you check? Or /cancel"));
						}
					} else {
						// nothing here
					}
				} else {
					this.setEmail("");
					replies.add(response("No idea. Perhaps you need to /stop and /start over again?"));
				}
			}
		}
		// RESEARCHER
		else if (researcher != null && is(TelegramSessionState.RESEARCHER)) {
			// choose the active project
			if (currently(TelegramSessionAction.ACTIVATE_PROJECT)) {
				// output the list of projects
				AtomicInteger mi = new AtomicInteger(1);
				final List<Project> researchProjects = researcher.getOwnAndCollabProjects();
				String indexedProjectList = researchProjects.stream().map(p -> mi.getAndIncrement() + " " + p.getName())
						.collect(Collectors.joining(", "));
				startAction(TelegramSessionAction.ACTIVATE_PROJECT);
				replies.add(
						response("Ok, choose one of your current projects, type '/project 1' for the first project: "
								+ indexedProjectList));
			}
			// message to all participants and researchers
			else if (this.getActiveProjectId() > -1 && currently(TelegramSessionAction.MESSAGE_PARTICIPANTS)) {
				// log messages
				addMessage("researcher", text);

				// send to participants
				replies.addAll(sendToParticipants(text));
				// send to researchers
				replies.addAll(sendToResearchers(text));
			}
			// message to one participant
			else if (this.getActiveProjectId() > -1 && getAction() != null
					&& getAction().startsWith(TelegramSessionAction.MESSAGE_PARTICIPANT.name())) {
				// log messages
				addMessage("researcher", text);

				Participant recipient = getParticipant();
				if (recipient != null) {
					// send participant #index
					replies.addAll(sendToParticipant(text, recipient));
				}
			} else {
				// add nothing
			}
		}
		// PARTICIPANT
		else if (participant != null && is(TelegramSessionState.PARTICIPANT)) {
			// message to researcher
			if (currently(TelegramSessionAction.MESSAGE_RESEARCHER)) {
				// log messages
				addMessage("participant", text);

				// send message to researchers
				replies.addAll(sendToResearchers(text));
			}
			// enter diary entry
			else if (currently(TelegramSessionAction.DIARY)) {
				if (hasActiveDiaryDataset()) {
					// process text to convert all emojis
					final String textWithoutEmojis = EmojiParser.parseToAliases(text);

					// store text in diary dataset
					Dataset diary = participant.getProject().getDiaryDataset();
					DiaryDS dds = (DiaryDS) datasetConnector.getDatasetDS(diary);
					dds.addRecord(participant, new Date(), "from Telegram", textWithoutEmojis);
					replies.add(response(":handshake: Entry added, ready for another one, /cancel to stop.",
							new String[] { DONE }));
				}
			} else {
				// add nothing
			}
		} else {
			// nothing for now
			replies.add(response("And a random emoji for you: " + TelegramBotUtils.randomEmoji()
					+ "\n Perhaps /start a conversation?", new String[] { START }));
		}

		return replies;
	}

	/**
	 * process a location and store it for the participant in this session into the movement dataset location button is
	 * not a command and only available as there is a movement dataset in the project
	 * 
	 * @param location
	 * @param datasetConnector
	 * @return
	 */
	public List<TelegramResponse> processLocationMessage(final Location location, DatasetConnector datasetConnector) {
		final List<TelegramResponse> replies = new ArrayList<>();

		if (participant != null && is(TelegramSessionState.PARTICIPANT) && hasActiveMovementDataset()) {
			Dataset movement = participant.getProject().getMovementDataset();
			MovementDS mvds = (MovementDS) datasetConnector.getDatasetDS(movement);
			mvds.addMovement(participant, location.getLongitude().floatValue(), location.getLatitude().floatValue(), 0);
		}

		replies.add(response("Thanks!"));
		return replies;

	}

	/**
	 * process message that contains a photo or else
	 * 
	 * @param caption
	 * @param fileId
	 * @return
	 */
	public List<TelegramResponse> processMediaMessage(String caption, String fileId, String filePath) {

		final List<TelegramResponse> replies = new ArrayList<>();

		// researcher: send image to participant(s)
		if (is(TelegramSessionState.RESEARCHER) && researcher != null) {
			// to all participants
			if (this.getActiveProjectId() > -1 && currently(TelegramSessionAction.MESSAGE_PARTICIPANTS)) {
				// add log for researcher
				addTelegramPhoto("researcher", caption, filePath);

				// send image
				replies.addAll(sendToParticipants(caption, fileId, filePath));

				// reply to researcher after sending image
				replies.add(response("Image sent, choose another photo to send or click /cancel to end sending. ",
						new String[] { DONE }));
			}
			// to a specific participant
			else if (this.getActiveProjectId() > -1 && currently(TelegramSessionAction.MESSAGE_PARTICIPANT)) {
				// add log for researcher
				addTelegramPhoto("researcher", caption, filePath);

				// check participant before sending image
				Participant recipient = getParticipant();
				if (recipient != null) {
					// send participant #index
					replies.addAll(sendToParticipant(recipient, caption, fileId, filePath));
				}
			}
		}

		return replies;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * send a message to one participant in the same project
	 * 
	 * @param text
	 */
	private List<TelegramResponse> sendToParticipant(final String text, Participant recipient) {
		// find connected chat from researchers
		List<TelegramSession> ots = TelegramSession.find.query().where().ieq("email", recipient.getEmail())
				.eq("activeProjectId", this.getActiveProjectId()).eq("state", TelegramSessionState.PARTICIPANT.name())
				.findList();

		// stream transform
		final List<TelegramResponse> responses = new LinkedList<TelegramResponse>();
		responses.addAll(ots.stream().filter(o -> o.getChatId() != -1).map(o -> {
			final String message = o.addMessage("researcher", text);
			o.update();
			return sendTo(o.getChatId(), message, "");
		}).collect(Collectors.toList()));

		// reply to researcher
		responses.add(response(":mailbox_with_mail: Type to send one more, /cancel to stop.", new String[] { DONE }));

		return responses;
	}

	/**
	 * send a message to all participants in the same project
	 * 
	 * @param text
	 */
	private List<TelegramResponse> sendToParticipants(final String text) {
		// find connected chat from researchers
		List<TelegramSession> ots = TelegramSession.find.query().where()
				.eq("activeProjectId", this.getActiveProjectId()).eq("state", TelegramSessionState.PARTICIPANT.name())
				.findList();

		// stream transform
		final List<TelegramResponse> responses = new LinkedList<TelegramResponse>();
		responses.addAll(ots.stream().filter(o -> o.getChatId() != -1).map(o -> {
			final String message = o.addMessage("researcher", text);
			o.update();
			return sendTo(o.getChatId(), message, "");
		}).collect(Collectors.toList()));

		// reply to researcher
		responses.add(response(":mailbox_with_mail: Type to send one more, /cancel to stop.", new String[] { DONE }));

		return responses;
	}

	/**
	 * send a message to researchers in the same project
	 * 
	 * @param text
	 */
	private List<TelegramResponse> sendToResearchers(final String text) {
		// find connected chat from researchers
		List<TelegramSession> ots = TelegramSession.find.query().where()
				.eq("activeProjectId", this.getActiveProjectId()).eq("state", TelegramSessionState.RESEARCHER.name())
				.findList();

		// message all researchers
		final List<TelegramResponse> responses = new LinkedList<TelegramResponse>();
		if (is(TelegramSessionState.PARTICIPANT)) {
			responses.addAll(ots.stream().filter(o -> o.getChatId() != -1).map(o -> {
				final String message = o.addMessage(participant.getName(), text);
				o.update();
				return sendTo(o.getChatId(), message, null);
			}).collect(Collectors.toList()));
		} else if (is(TelegramSessionState.RESEARCHER)) {
			responses.addAll(ots.stream().filter(o -> o.getChatId() != -1).map(o -> {
				final String message = o.addMessage("researcher", text);
				o.update();
				return sendTo(o.getChatId(), message, "");
			}).collect(Collectors.toList()));
		}

		// check whether any researcher is active now
		final String returnMsg;
		if (ots.stream().anyMatch(r -> r.isActive())) {
			returnMsg = ":postbox: done.";
		} else {
			returnMsg = ":postbox: done, researchers might respond later.";
		}

		// reply to sender
		responses.add(response(returnMsg + " Type to send one more, /cancel to stop.", new String[] { DONE }));

		return responses;
	}

	/**
	 * researchers send images to a specific participant in the same project
	 * 
	 * @param recipient
	 * @param file
	 * @return
	 */
	private List<TelegramResponse> sendToParticipant(Participant recipient, String caption, String fileId,
			String filePath) {
		// find connected chat from researchers
		List<TelegramSession> ots = TelegramSession.find.query().where().ieq("email", recipient.getEmail())
				.eq("activeProjectId", this.getActiveProjectId()).eq("state", TelegramSessionState.PARTICIPANT.name())
				.findList();

		// stream transform
		final List<TelegramResponse> responses = new LinkedList<TelegramResponse>();
		responses.addAll(ots.stream().filter(o -> o.getChatId() != -1).map(o -> {

			// add log for participant
			o.addTelegramPhoto("researcher", caption, filePath);
			o.update();

			return sendTo(o.getChatId(), caption, fileId);
		}).collect(Collectors.toList()));

		// reply to researcher
		responses.add(response(":mailbox_with_mail: Type to send one more, /cancel to stop.", new String[] { DONE }));

		return responses;
	}

	/**
	 * researchers send images to all participants in the same project
	 * 
	 * @param file
	 * @return
	 */
	private List<TelegramResponse> sendToParticipants(String caption, String fileId, String filePath) {
		// find connected chat from researchers
		List<TelegramSession> ots = TelegramSession.find.query().where()
				.eq("activeProjectId", this.getActiveProjectId()).eq("state", TelegramSessionState.PARTICIPANT.name())
				.findList();

		// stream transform
		final List<TelegramResponse> responses = new LinkedList<TelegramResponse>();
		responses.addAll(ots.stream().filter(o -> o.getChatId() != -1).map(o -> {
			// add log for participant
			o.addTelegramPhoto("researcher", caption, filePath);
			o.update();

			return sendTo(o.getChatId(), caption, fileId);
		}).collect(Collectors.toList()));

		// reply to researcher
		responses.add(response(":mailbox_with_mail: Send more, or type /cancel to stop.", new String[] { DONE }));

		return responses;
	}

	/**
	 * provide the /help menu for the participant
	 * 
	 */
	private TelegramResponse replyParticipantHelp(String prepend) {

		String reply = is(TelegramSessionState.PARTICIPANT) ? "Now you can /message the researcher"
				: "You are participant in a project, you can /message the researcher";

		int items = countDS(), current = 0;

		if (items > 0) {
			if (hasActiveDiaryDataset()) {
				current++;
				reply += items == current ? " or " : ", ";
				reply += "write a /diary entry";
			}
			if (hasActiveMediaDataset()) {
				current++;
				reply += items == current ? " or " : ", ";
				reply += "upload a /photo for the study";
			}
			if (hasActiveMovementDataset()) {
				current++;
				reply += items == current ? " or " : ", ";
				reply += "share your location to the study by the button below";
			}
		}
		reply += ". Type /help to get these options again.";

		return response(prepend + reply, getParticipantOptions());
	}

	/**
	 * get options for the given participant
	 * 
	 * @return
	 */
	private String[] getParticipantOptions() {
		List<String> dsOptions = new ArrayList<>();

		dsOptions.add(MESSAGE);
		if (hasActiveDiaryDataset()) {
			dsOptions.add(DIARY);
		}
		if (hasActiveMediaDataset()) {
			dsOptions.add(PHOTO);
		}
		if (hasActiveMovementDataset()) {
			dsOptions.add(LOCATION);
		}

		String[] options = new String[dsOptions.size()];
		for (int i = 0; i < dsOptions.size(); i++) {
			options[i] = dsOptions.get(i);
		}

		return options;
	}

	/**
	 * return the number of options
	 * 
	 * @return
	 */
	private int countDS() {
		int dsNum = 0;

		if (hasActiveDiaryDataset()) {
			dsNum++;
		}
		if (hasActiveMediaDataset()) {
			dsNum++;
		}
		if (hasActiveMovementDataset()) {
			dsNum++;
		}

		return dsNum;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get the active project for this session
	 * 
	 * @return
	 */
	private Project getActiveProject() {
		// get project
		Project project = researcher.getOwnAndCollabProjects().stream()
				.filter(p -> p.getId().equals(this.getActiveProjectId())).findFirst().get();
		return project;
	}

	/**
	 * get the participant from the session
	 * 
	 * @return
	 */
	private Participant getParticipant() {
		// get active project
		Project project = getActiveProject();

		// find participant
		Participant recipient = null;
		String[] comps = getAction().split(" ", 2);
		if (comps.length == 2 && comps[1] != null && comps[1].length() > 0) {
			try {
				int index = DataUtils.parseInt(comps[1]);
				recipient = project.getParticipants().get(index);
			} catch (Exception e) {
			}
		}
		return recipient;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * compose a default response with text
	 * 
	 * @return
	 */
	public TelegramResponse response(String message) {
		return new TelegramResponse(this.getChatId(), message);
	}

	/**
	 * send a message to a remote telegram session with text
	 * 
	 * @return
	 */
	private TelegramResponse sendTo(long remoteChatId, String message, String fileId) {
		return new TelegramResponse(remoteChatId, message, null, fileId);
	}

	/**
	 * construct a response based on a message and a few answer options
	 * 
	 * @param message
	 * @param answerOptions
	 * @return
	 */
	private TelegramResponse response(String message, String[] answerOptions) {
		return new TelegramResponse(this.getChatId(), message, answerOptions, null);
	}

	/**
	 * construct a default response based on whether this is a researcher or participant session
	 * 
	 * @return
	 */
	public TelegramResponse defaultResponse() {
		if (is(TelegramSessionState.RESEARCHER)) {
			return response("No idea. Do you need /help?", new String[] { HELP });
		} else if (is(TelegramSessionState.PARTICIPANT)) {
			return response(
					"Got your text, but is it a diary entry or a message? Please send /diary or /message first. :pray:",
					new String[] { MESSAGE, DIARY, HELP });
		} else {
			return response("Hey, who are you? Hit /start !", new String[] { START });
		}
	}

	/**
	 * return message to participant and show the available options
	 * 
	 * @param prepend
	 * @return
	 */
	public List<TelegramResponse> getParticipantHelp(String prepend) {
		List<TelegramResponse> replies = new ArrayList<>();
		replies.add(replyParticipantHelp(prepend));
		return replies;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * reflection-based stringifier to JSON
	 * 
	 * @return
	 */
	public String toJson() {
		ObjectNode on = DataUtils.toJson(this);
		return on.toPrettyString();
	}

	@Override
	public String toString() {
		return Json.toJson(this).toString();
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public long getChatId() {
		return chatId;
	}

	public void setChatId(long chatId) {
		this.chatId = chatId;
	}

	public long getActiveProjectId() {
		return activeProjectId;
	}

	public void setActiveProjectId(long activeProjectId) {
		this.activeProjectId = activeProjectId;
	}

	public Date getLastAction() {
		return lastAction;
	}

	public void setLastAction(Date lastAction) {
		this.lastAction = lastAction;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}
}

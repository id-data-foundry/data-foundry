package services.telegrambot;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.GetFile;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Audio;
import org.telegram.telegrambots.meta.api.objects.CallbackQuery;
import org.telegram.telegrambots.meta.api.objects.Location;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.PhotoSize;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.Video;
import org.telegram.telegrambots.meta.api.objects.Voice;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import com.google.gson.JsonObject;

import datasets.DatasetConnector;
import models.Dataset;
import models.Project;
import models.ds.MediaDS;
import models.sr.TelegramSession;
import models.sr.TelegramSession.TelegramSessionState;
import play.Logger;
import play.cache.SyncCacheApi;
import services.jsexecutor.JSActor;
import services.jsexecutor.JSExecutorService;
import services.slack.Slack;
import utils.auth.TokenResolverUtil;
import utils.telegrambot.TelegramBotUtils;

public class TelegramBotClient extends TelegramLongPollingBot {

	private static final Logger.ALogger logger = Logger.of(TelegramBotClient.class);

	private final Map<String, List<String>> loggedMessages = new ConcurrentHashMap<>();

	private final String botUsername;
	private final String botToken;
	private final SyncCacheApi cache;
	private final DatasetConnector datasetConnector;
	private final TokenResolverUtil tokenResolver;
	private JSExecutorService actorService;
	private final ExecutorService EXECUTOR = Executors.newWorkStealingPool();

	public TelegramBotClient(String username, String token, SyncCacheApi cache, TokenResolverUtil tokenResolver,
	        DatasetConnector datasetConnector) {
		this.botUsername = username;
		this.botToken = token;
		this.cache = cache;
		this.datasetConnector = datasetConnector;
		this.tokenResolver = tokenResolver;
	}

	@Override
	public String getBotUsername() {
		return botUsername;
	}

	@Override
	public String getBotToken() {
		return botToken;
	}

	/**
	 * helper method to avoid circular dependencies in Guice DI
	 * 
	 * @param jsActorService
	 */
	public void setJSActorService(JSExecutorService jsActorService) {
		this.actorService = jsActorService;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void onUpdateReceived(final Update update) {
		// run Telegram updates async on EXECUTOR
		CompletableFuture.runAsync(() -> {
			try {
				// message?
				if (update.hasMessage() && (update.getMessage().hasText() || update.getMessage().hasPhoto()
				        || update.getMessage().hasVoice() || update.getMessage().hasAudio()
				        || update.getMessage().hasVideo() || update.getMessage().hasLocation())) {
					handleIncomingMessage(update.getMessage());
				}
				// message?
				else if (update.hasChannelPost()
				        && (update.getChannelPost().hasText() || update.getChannelPost().hasPhoto())) {
					handleIncomingMessage(update.getChannelPost());
				}
				// callback from all inlineKeyboard buttons
				else if (update.hasCallbackQuery()) {
					handleCallbackQuery(update);
				}
				// anything else? or no text, no photo?
				else {

					final long chatId;
					if (update.hasMessage()) {
						chatId = update.getMessage().getChatId();
					} else if (update.hasChannelPost()) {
						chatId = update.getChannelPost().getChatId();
					} else {
						chatId = -1;
					}

					if (chatId > -1) {
						// Create a SendMessage object with mandatory fields
						SendMessage message = new SendMessage();
						message.setChatId(chatId + "");
						message.setText("Sure, have a random emoji: " + TelegramBotUtils.randomEmoji());

						// Call method to send the message
						execute(message);
					}
				}
			} catch (TelegramApiException e) {
				logger.error("Telegram exception in Telegram bot", e);
			} catch (Exception e) {
				logger.error("General exception in Telegram bot", e);
			}
		}, EXECUTOR);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * message handler
	 * 
	 * @param message
	 * @throws TelegramApiException
	 */
	private void handleIncomingMessage(Message message) throws TelegramApiException {

		// // no text no processing
		// if (/* !message.isUserMessage() && */ !message.hasText() && !message.hasPhoto() && !message.hasLocation()) {
		// return;
		// }

		// get chat context
		long chatId = message.getChatId();
		String cmd = "";

		// get chat session
		Optional<TelegramSession> tgs = TelegramSession.find.query().setMaxRows(1).where().eq("chatId", chatId)
		        .findOneOrEmpty();
		final TelegramSession ts;
		if (tgs.isPresent()) {
			// get existing session
			ts = tgs.get();

			// update the username if not existing
			if (ts.getUsername() == null) {
				ts.setUsername(TelegramBotUtils.getUserName(message.getFrom()));
				ts.update();
			}
			ts.initialize();
		} else {
			// create new session
			ts = new TelegramSession();
			ts.setChatId(chatId);
			ts.setUsername(TelegramBotUtils.getUserName(message.getFrom()));
			ts.save();
		}

		List<TelegramResponse> replies = null;
		// check again for channel messages
		if (message.isChannelMessage()) {
			replies = handleChannelMessage(chatId);
		} else {
			// check text content
			if (message.hasText()) {
				// retrieve message data
				String text = message.getText();
				text = text != null ? text.trim() : text;
				cmd = text;

				if (text != null && text.length() > 0) {
					// handle command messages
					if (text.startsWith("/")) {
						replies = ts.processCommand(text.toLowerCase(), Optional.of(message.getFrom()), tokenResolver);
					}
					// check open input (= non-command text) based on state of telegram session
					else {
						replies = ts.processTextMessage(text, Optional.of(message.getFrom()), cache, datasetConnector);

						// if there is no response, try the scripting
						if (replies.isEmpty()) {
							// try updating all linked bot scripts
							if (!updateBotScripts(ts, text)) {
								// add a default reply if there is no script available to respond
								replies.add(ts.defaultResponse());
							}
						}
					}
				}
			}
			// check by message content: photo
			else if (message.hasPhoto()) {
				List<PhotoSize> photos = message.getPhoto();
				// get the highest resolution image
				PhotoSize photo = photos.get(photos.size() - 1);

				// from researchers to participants
				if (ts.is(TelegramSessionState.RESEARCHER)) {
					String filePath = null;
					if (photo.getFilePath() != null) {
						// if the file_path is already present, we are done!
						filePath = photo.getFilePath();
					} else {
						// if not, let's find it: we create a GetFile method and set the file_id from the photo
						GetFile getFileMethod = new GetFile();
						getFileMethod.setFileId(photo.getFileId());

						try {
							// execute the get file method
							org.telegram.telegrambots.meta.api.objects.File file = execute(getFileMethod);
							// now have the file_path
							filePath = file.getFilePath();
						} catch (TelegramApiException e) {
							logger.error("Error in getting file from Telegram.", e);
						}
					}

					String caption = message.getCaption() == null ? "" : message.getCaption();
					replies = ts.processMediaMessage(caption, photo.getFileId(), filePath);
				}
				// from participants to Media dataset
				else if (ts.is(TelegramSessionState.PARTICIPANT)) {
					// make sure Media dataset is ready and participant is allowed to upload images
					if (ts.hasActiveMediaDataset()) {
						String filePath = null;
						if (photo.getFilePath() != null) {
							// if the file_path is already present, we are done!
							filePath = photo.getFilePath();
						} else {
							// if not, let's find it: we create a GetFile method and set the file_id from the photo
							GetFile getFileMethod = new GetFile();
							getFileMethod.setFileId(photo.getFileId());

							try {
								// execute the get file method
								org.telegram.telegrambots.meta.api.objects.File file = execute(getFileMethod);
								// now have the file_path
								filePath = file.getFilePath();
							} catch (TelegramApiException e) {
								logger.error("Error in getting file from Telegram.", e);
							}
						}

						if (filePath != null) {
							File temp = downloadFile(filePath);

							// moving photo into media dataset
							replies = processMediaMessage(ts, TelegramMediaType.PHOTO, message.getCaption(), temp,
							        datasetConnector);
						}
					}
					// otherwise return error message and available options
					else {
						replies = ts.getParticipantHelp(
						        "We are not collecting images at the moment, please wait for the research team to come back to you.");
					}
				}
			}
			// check by message content: voice
			// process only for participant sessions
			else if (message.hasVoice() && ts.is(TelegramSessionState.PARTICIPANT)) {
				Voice voice = message.getVoice();
				Optional<File> fileOpt = getMediaFile(voice.getFileId());
				if (fileOpt.isPresent()) {
					// moving audio into media dataset
					replies = processMediaMessage(ts, TelegramMediaType.VOICE, message.getCaption(), fileOpt.get(),
					        datasetConnector);
				}
			}
			// check by message content: audio
			// process only for participant sessions
			else if (message.hasAudio() && ts.is(TelegramSessionState.PARTICIPANT)) {
				Audio audio = message.getAudio();
				Optional<File> fileOpt = getMediaFile(audio.getFileId());
				if (fileOpt.isPresent()) {
					// moving audio into media dataset
					replies = processMediaMessage(ts, TelegramMediaType.AUDIO, message.getCaption(), fileOpt.get(),
					        datasetConnector);
				}
			}
			// check by message content: video
			// process only for participant sessions
			else if (message.hasVideo() && ts.is(TelegramSessionState.PARTICIPANT)) {
				Video video = message.getVideo();
				Optional<File> fileOpt = getMediaFile(video.getFileId());
				if (fileOpt.isPresent()) {
					// moving video into media dataset
					replies = processMediaMessage(ts, TelegramMediaType.VIDEO, message.getCaption(), fileOpt.get(),
					        datasetConnector);
				}
			}
			// check by message content: location
			// process only for participant sessions
			else if (message.hasLocation() && ts.is(TelegramSessionState.PARTICIPANT)) {
				Location location = message.getLocation();
				if (location != null) {
					replies = ts.processLocationMessage(location, datasetConnector);
				}
			}
		}

		// send out reply
		if (replies != null) {
			ts.setLastAction(new Date());
			final String finalCmd = cmd;
			replies.stream().forEach(r -> {
				try {
					if (finalCmd != null && !finalCmd.isEmpty() && finalCmd.startsWith("/start")) {
						execute(r.messageWithCallbackQuery(message));
					} else {
						if (nnne(r.fileId)) {
							execute(r.photo());
						} else {
							execute(r.message(message));
						}
					}
				} catch (TelegramApiException e) {
					Slack.call("Error", "Telegram response sending problem");
					logger.error("Telegram response sending problem", e);
				}
			});
		}

		// update session in database
		ts.update();
	}

	/**
	 * process message that contain a photo or else
	 * 
	 * @param caption
	 * @param file
	 * @param datasetConnector
	 * @return
	 */
	private List<TelegramResponse> processMediaMessage(TelegramSession ts, TelegramMediaType mediaType, String caption,
	        File file, DatasetConnector datasetConnector) {

		final List<TelegramResponse> replies = new ArrayList<>();

		// ensure that caption is filled
		if (caption == null) {
			caption = "";
		}

		// check file size before accepting it as an upload
		if (file.length() > 1024L * 1024L * 30L) {
			// TODO send message that the file is too big?
			return replies;
		}

		// participant: save file to Media dataset
		Optional<Dataset> mediaDS = ts.activeMediaDataset();
		if (mediaDS.isPresent()) {
			Dataset dataset = mediaDS.get();
			if (dataset.getId() > -1) {
				MediaDS mds = (MediaDS) datasetConnector.getDatasetDS(dataset);

				// determine correct file extension for uploaded file
				final String fileExtension;
				switch (mediaType) {
				case AUDIO:
					fileExtension = "mp3";
					break;
				case VOICE:
					fileExtension = "ogg";
					break;
				case VIDEO:
					fileExtension = "mp4";
					break;
				case PHOTO:
				default:
					fileExtension = "png";
				}

				// transform file name
				String fileName = file.getName().replace(".tmp", "." + fileExtension);

				Date now = new Date();

				// ensure that filename is unique on disk
				fileName = ts.participant.getName() + "_" + now.getTime() + "_telegram." + fileExtension;

				// store, add, import
				Optional<String> storeFile = mds.storeFile(file, fileName);
				if (storeFile.isPresent()) {

					mds.addRecord(ts.participant, storeFile.get(), caption, now, "from Telegram");
					mds.importFileContents(ts.participant,
					        controllers.api.routes.MediaDSController.image(dataset.getId(), storeFile.get()).url(),
					        fileExtension, caption, now);

					if (mediaType == TelegramMediaType.PHOTO) {
						// add log for participant
						ts.addLocalPhoto("participant", caption,
						        controllers.api.routes.MediaDSController.image(dataset.getId(), storeFile.get()).url());
					}
				}
			}

			// send message to linked scripts
			boolean scriptUpdated = false;
			switch (mediaType) {
			case PHOTO:
				scriptUpdated = updateBotScripts(ts, "PHOTO_UPLOAD");
				break;
			case AUDIO:
			case VOICE:
				scriptUpdated = updateBotScripts(ts, "AUDIO_UPLOAD");
				break;
			case VIDEO:
				scriptUpdated = updateBotScripts(ts, "VIDEO_UPLOAD");
			}

			// only respond with a generic photo-received message if there is no script linked
			if (!scriptUpdated && mediaType == TelegramMediaType.PHOTO) {
				// generate response with message size as variant indicator
				String responseText = TelegramBotUtils.getResponse(TelegramBotUtils.PARTICIPANT_SENT_PHOTO, -1);
				replies.add(ts.response(responseText));
			}
		}

		return replies;
	}

	enum TelegramMediaType {
		PHOTO, VIDEO, AUDIO, VOICE
	}

	private Optional<File> getMediaFile(String fileId) {
		// if not, let's find it: we create a GetFile method and set the file_id from the photo
		GetFile getFileMethod = new GetFile();
		getFileMethod.setFileId(fileId);

		String filePath = null;
		try {
			// execute the get file method
			org.telegram.telegrambots.meta.api.objects.File file = execute(getFileMethod);
			// now have the file_path
			filePath = file.getFilePath();
		} catch (TelegramApiException e) {
			logger.error("Error in getting file from Telegram.", e);
		}

		if (filePath != null) {
			File temp;
			try {
				temp = downloadFile(filePath);
				return Optional.ofNullable(temp);
			} catch (TelegramApiException e) {
				return Optional.empty();
			}
		}

		return Optional.empty();
	}

//	private Optional<File> retrieveFile(String filePath, GetFile getFileMethod) {
//		try {
//			// execute the get file method
//			org.telegram.telegrambots.meta.api.objects.File file = execute(getFileMethod);
//			// now have the file_path
//			filePath = file.getFilePath();
//		} catch (TelegramApiException e) {
//			logger.error("Error in getting file from Telegram.", e);
//		}
//
//		if (filePath != null) {
//			File temp;
//			try {
//				temp = downloadFile(filePath);
//				return Optional.ofNullable(temp);
//			} catch (TelegramApiException e) {
//				return Optional.empty();
//			}
//		}
//
//		return Optional.empty();
//	}

	/**
	 * updates all linked bot scripts (linked to TelegramSession) with the given message; returns true if there are bot
	 * scripts, false otherwise
	 * 
	 * @param ts
	 * @param actorMessage
	 * @return
	 */
	private boolean updateBotScripts(TelegramSession ts, String actorMessage) {
		List<JSActor> actors = actorService.getSubscribedActors("telegram_" + ts.getActiveProjectId());
		if (!actors.isEmpty()) {
			actors.stream().forEach(actor -> {
				// script found --> call actor with input
				JsonObject data = new JsonObject();
				data.addProperty("message", actorMessage);
				data.addProperty("chatId", "" + ts.getChatId());
				if (ts.is(TelegramSessionState.PARTICIPANT)) {
					data.addProperty("participant", ts.is(TelegramSessionState.PARTICIPANT));
					data.addProperty("participant_id", ts.participant.getRefId());
					data.addProperty("participant_name", ts.participant.getName());
				} else if (ts.is(TelegramSessionState.RESEARCHER)) {
					data.addProperty("researcher", ts.is(TelegramSessionState.RESEARCHER));
					data.addProperty("researcher_name", ts.researcher.getName());
				}
				actor.update(data);
			});

			return true;
		}

		return false;
	}

	/**
	 * process any channel message
	 * 
	 * @param chatId
	 * @return
	 */
	private List<TelegramResponse> handleChannelMessage(Long chatId) {
		ArrayList<TelegramResponse> replies = new ArrayList<>();
		replies.add(new TelegramResponse(chatId,
		        "Hi, channels are for multiple parties, so it's not good to share potentially personal information here. "
		                + "Please don't post anything private here, also, the @" + botUsername + " won't work properly "
		                + "here. Instead, click on the bot name " + "@" + botUsername + " to start a new direct chat "
		                + "with the Data Foundry bot. See you on the other side!",
		        new String[] {}, null));
		return replies;
	}

	private void handleCallbackQuery(Update update) throws TelegramApiException {
		// no text no processing
		CallbackQuery callbackQuery = update.getCallbackQuery();
		if (callbackQuery == null) {
			return;
		}

		long chatId = callbackQuery.getMessage().getChatId();

		// get chat session
		Optional<TelegramSession> tgs = TelegramSession.find.query().setMaxRows(1).where().eq("chatId", chatId)
		        .findOneOrEmpty();
		final TelegramSession ts;
		if (tgs.isPresent()) {
			// get existing session
			ts = tgs.get();
			ts.initialize();
		} else {
			// create new session
			ts = new TelegramSession();
			ts.setChatId(chatId);
			ts.setUsername(TelegramBotUtils.getUserName(callbackQuery.getFrom()));
			ts.save();
		}

		// get chat context
		CallbackQuery callbackquery = callbackQuery;
		String data = callbackquery.getData();
		List<TelegramResponse> replies = null;

		// handle callback message
		if (data != null && data.length() > 0) {
			// handle message commands
			if (data.startsWith("/")) {
				replies = ts.processCommand(data.toLowerCase(), Optional.of(callbackQuery.getFrom()), tokenResolver);
			}
			// check open input (= non-command data) based on state of telegram session
			else {
				replies = ts.processTextMessage(data, Optional.of(callbackQuery.getFrom()), cache, datasetConnector);
			}
		}

		// send out reply
		if (replies != null) {
			ts.setLastAction(new Date());

			replies.stream().forEach(r -> {
				try {
					execute(r.message(callbackquery.getMessage()));
				} catch (TelegramApiException e) {
					logger.error("Error in responding with message.", e);
				}
			});
		}

		// update session in database
		ts.update();
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	void replyMessage(String chatId, Long activeProjectId, String text) {
		try {
			// sleep a little
			Thread.sleep(2000);

			String message = logMessageForTelegramSession(chatId, "bot", text);
			execute(new TelegramResponse(chatId, message, null, null).message());
		} catch (TelegramApiException e) {
			logger.error("Error in responding with message.", e);
		} catch (Exception e) {
			logger.error("Error in responding with message (log session).", e);
		}
	}

	/**
	 * send message to project owner by DF
	 * 
	 * @param activeProjectId
	 * @param text
	 */
	void sendMessageToProjectOwner(Long activeProjectId, String text) {
		try {
			// sleep a little
			Thread.sleep(2000);

			// find connected chat from project owner
			Project project = Project.find.byId(activeProjectId);
			if (project == null) {
				return;
			}
			project.getOwner().refresh();
			final String ownerEmail = project.getOwner().getEmail();

			TelegramSession.find.query().where().eq("activeProjectId", activeProjectId)
			        .eq("state", TelegramSessionState.RESEARCHER.name()).findEach(o -> {

				        // check ownership
				        if (!ownerEmail.equals(o.getEmail()) || o.getChatId() == -1) {
					        return;
				        }

				        // message owner
				        String message = logMessageForTelegramSession(o.getChatId() + "", "system", text);
				        try {
					        execute(new TelegramResponse(o.getChatId(), message, null, null).message());
				        } catch (TelegramApiException e) {
					        logger.error("Error in responding with message.", e);
				        }
			        });
		} catch (Exception e) {
			logger.error("Error in responding with message (DB access).", e);
		}
	}

	/**
	 * send message to all researcher of the project via / by DF
	 * 
	 * @param name
	 * @param activeProjectId
	 * @param text
	 */
	void sendMessageToResearchers(String name, Long activeProjectId, String text) {
		try {
			// sleep a little
			Thread.sleep(2000);

			// find connected chats from all researchers
			TelegramSession.find.query().where().eq("activeProjectId", activeProjectId)
			        .eq("state", TelegramSessionState.RESEARCHER.name()).findEach(o -> {
				        if (o.getChatId() == -1) {
					        return;
				        }

				        // message all researchers
				        String message = logMessageForTelegramSession(o.getChatId() + "", "script", text);
				        try {
					        execute(new TelegramResponse(o.getChatId(), message, null, null).message());
				        } catch (TelegramApiException e) {
					        logger.error("Error in responding with message.", e);
				        }
			        });
		} catch (Exception e) {
			logger.error("Error in responding with message (DB access).", e);
		}
	}

	/**
	 * send message to the specific participant via / by DF
	 * 
	 * @param activeProjectId
	 * @param recipientEmail
	 * @param text
	 */
	void sendMessageToParticipant(Long activeProjectId, String recipientEmail, String text) {

		// don't attempt to send for participants without an email address
		if (recipientEmail.isEmpty()) {
			return;
		}

		try {
			// sleep a little
			Thread.sleep(2000);

			// find connected chat from participants
			TelegramSession.find.query().where().ieq("email", recipientEmail).eq("activeProjectId", activeProjectId)
			        .eq("state", TelegramSessionState.PARTICIPANT.name()).findEach(o -> {
				        if (o.getChatId() == -1) {
					        return;
				        }

				        // message participant
				        String message = logMessageForTelegramSession(o.getChatId() + "", "researcher", text);
				        try {
					        execute(new TelegramResponse(o.getChatId(), message, null, null).message());
				        } catch (TelegramApiException e) {
					        logger.error("Error in responding with message.", e);
				        }
			        });
		} catch (Exception e) {
			logger.error("Error in responding with message (DB access).", e);
		}
	}

	void logMessagesIntoSessions() {
		loggedMessages.entrySet().stream().forEach(e -> {
			String chatId = e.getKey();

			// find chat to reply to
			Optional<TelegramSession> tsOpt = TelegramSession.find.query().setMaxRows(1).where().eq("chatId", chatId)
			        .findOneOrEmpty();
			if (!tsOpt.isPresent()) {
				return;
			}

			// update the telegram session with the new message
			TelegramSession ts = tsOpt.get();
			List<String> messages = e.getValue();
			for (String message : messages) {
				ts.getMessages().add(message);
			}
			ts.update();
		});
		loggedMessages.clear();
	}

	private String logMessageForTelegramSession(String chatId, String sender, String text) {
		final String attributedMessage = "[" + sender + "]: " + text;
		final SimpleDateFormat messageDateFormatter = new SimpleDateFormat("MMM d 'at' HH:mm");
		String result = messageDateFormatter.format(new Date()) + " " + attributedMessage;

		// log messages to a central map that will then store the messages in the right Telegram session
		// why? because from here, any write access to a TG session will cause problems
		loggedMessages.putIfAbsent(chatId, new LinkedList<String>());
		loggedMessages.get(chatId).add(result);

		return attributedMessage;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * return true if text really has content
	 * 
	 * @param text
	 * @return
	 */
	private boolean nnne(String text) {
		return text != null && text.length() > 0;
	}

}

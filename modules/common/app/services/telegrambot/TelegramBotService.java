package services.telegrambot;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.meta.generics.BotSession;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import datasets.DatasetConnector;
import play.Logger;
import play.cache.SyncCacheApi;
import services.inlets.ScheduledService;
import services.jsexecutor.JSExecutorService;
import utils.auth.TokenResolverUtil;
import utils.conf.ConfigurationUtils;

@Singleton
public class TelegramBotService implements ScheduledService {

	private static final Logger.ALogger logger = Logger.of(TelegramBotService.class);

	private BotSession botSession;
	private TelegramBotClient bot;

	private String botToken;

	@Inject
	public TelegramBotService(Config config, SyncCacheApi cache, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolver) {

		// check the configuration
		if (!config.hasPath(ConfigurationUtils.DF_TELEGRAM_BOTNAME)
		        || !config.hasPath(ConfigurationUtils.DF_TELEGRAM_BOTTOKEN)) {
			logger.warn("Telegram service was not started. ");

			// no further service operation here
			return;
		}

		// retrieve configuration
		String botUsername = config.getString(ConfigurationUtils.DF_TELEGRAM_BOTNAME);
		botToken = config.getString(ConfigurationUtils.DF_TELEGRAM_BOTTOKEN);

		try {
			final TelegramBotsApi botsApi = new TelegramBotsApi(DefaultBotSession.class);

			// Register our bot
			logger.info("Registering bot...");
			bot = new TelegramBotClient(botUsername, botToken, cache, tokenResolver, datasetConnector);
			botSession = botsApi.registerBot(bot);
			logger.info("Bot registered.");
		} catch (TelegramApiException e) {
			logger.error("Error in communicating with Telegram API.", e);
		} catch (Exception e) {
			logger.error("General error in Telegram service.", e);
		}
	}

	@Override
	public void refresh() {
		if (bot != null) {
			bot.logMessagesIntoSessions();
		}
	}

	@Override
	public void stop() {
		if (botSession != null) {
			botSession.stop();
			logger.info("Bot stopped.");
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public String getBotUsername() {
		return bot != null ? bot.getBotUsername() : "";
	}

	public String getBotToken() {
		return bot != null ? bot.getBotToken() : "";
	}

	public void sendMessageToProjectOwner(Long activeProjectId, String text, Executor executor) {
		if (bot != null) {
			CompletableFuture.runAsync(() -> {
				bot.sendMessageToProjectOwner(activeProjectId, text);
			}, executor);
		}
	}

	public void sendMessageToParticipant(Long id, String email, String message, ExecutorService executor) {
		if (bot != null) {
			CompletableFuture.runAsync(() -> {
				bot.sendMessageToParticipant(id, email, message);
			}, executor);
		}
	}

	public void sendMessageToResearchers(String datasetName, long projectId, String message, ExecutorService executor) {
		if (bot != null) {
			CompletableFuture.runAsync(() -> {
				bot.sendMessageToResearchers(datasetName, projectId, message);
			}, executor);
		}
	}

	public void replyMessage(String chatId, long projectId, String message, ExecutorService executor) {
		if (bot != null) {
			CompletableFuture.runAsync(() -> {
				bot.replyMessage(chatId, projectId, message);
			}, executor);
		}
	}

	public void setJSActorService(JSExecutorService jsExecutorService) {
		if (bot != null) {
			bot.setJSActorService(jsExecutorService);
		}
	}

}

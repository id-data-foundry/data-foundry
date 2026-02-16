package services.telegrambot;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.objects.InputFile;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.ReplyKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.KeyboardButton;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.KeyboardRow;

import com.vdurmont.emoji.EmojiParser;

/**
 * container for responses that also includes helper methods to structure the responses
 * 
 */
public class TelegramResponse {
	String message;
	String chatId;
	String[] answerOptions;
	String fileId;

	public TelegramResponse(long chatId, String message) {
		this(chatId + "", message, null, null);
	}

	public TelegramResponse(long chatId, String message, String[] answerOptions, String fileId) {
		this(chatId + "", message, answerOptions, fileId);
	}

	public TelegramResponse(String chatId, String message, String[] answerOptions, String fileId) {
		this.chatId = chatId + "";
		this.message = message = message == null ? "" : message;
		this.answerOptions = answerOptions;
		this.fileId = fileId == null ? "" : fileId;

		// parse the text for inline answer options if the given answerOptions are null
		if (this.answerOptions == null) {
			try {
				List<String> options = new ArrayList<>(10);
				Pattern p = Pattern.compile("\\[\\[([^\\]\\[]*)\\]\\]");
				Matcher m = p.matcher(message);
				int iter = 0;
				while (m.find() && iter++ < 10) {
					String key = m.group(1);
					int start = m.start(1);
					int end = m.end(1);
					if (key.contains("|")) {
						String k[] = key.split("\\|");
						options.add(k[1]);
						message = message.substring(0, start - 2) + k[0] + message.substring(end + 2);
					} else {
						options.add(key);
						message = message.substring(0, start - 2) + message.substring(end + 2);
					}

					m = p.matcher(message);
				}
				// store answer options
				this.answerOptions = options.toArray(new String[] {});
				// store new message in which buttons have been replaces
				this.message = message;
			} catch (Exception e) {
				// do nothing, parsing failed apparently
			}
		}
	}

	public SendMessage message() {
		SendMessage reply = new SendMessage();
		reply.setChatId(chatId + "");
		reply.setText(EmojiParser.parseToUnicode(message));
		// provide keyboards according to message
		if (answerOptions != null && answerOptions.length > 0) {
			reply.setReplyMarkup(getMenuKeyboard(answerOptions));
		}
		return reply;
	}

	public SendMessage message(Message origin) {
		SendMessage reply = new SendMessage();
		reply.setChatId(chatId + "");
		reply.setText(EmojiParser.parseToUnicode(message));

		// provide keyboards according to message
		if (answerOptions != null && answerOptions.length > 0) {
			if (origin.isChannelMessage()) {
				// enable inline buttons and capture responses
				reply.setReplyMarkup(getInlineKeyboard(answerOptions));
			} else if (origin.isUserMessage()) {
				reply.setReplyMarkup(getMenuKeyboard(answerOptions));
			}
		}
		return reply;
	}

	public SendMessage messageWithCallbackQuery(Message origin) {
		SendMessage reply = new SendMessage();
		reply.setChatId(chatId + "");
		reply.setText(EmojiParser.parseToUnicode(message));

		// TODO: handle origin messages
		if (answerOptions != null && answerOptions.length > 0) {
			reply.setReplyMarkup(getInlineKeyboard(answerOptions));
		}

		return reply;
	}

	public SendPhoto photo() {
		SendPhoto reply = new SendPhoto();
		reply.setChatId(chatId + "");
		reply.setPhoto(new InputFile(fileId));
		if (message != null && message.length() > 0) {
			reply.setCaption(EmojiParser.parseToUnicode(message));
		}

		return reply;
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * display buttons at bottom of screen according to answer options
	 * 
	 * @param newAnswerOptions
	 * @return
	 */
	private ReplyKeyboardMarkup getMenuKeyboard(String[] newAnswerOptions) {

		ReplyKeyboardMarkup replyKeyboardMarkup = new ReplyKeyboardMarkup();
		replyKeyboardMarkup.setResizeKeyboard(true);
		replyKeyboardMarkup.setOneTimeKeyboard(true);

		List<KeyboardRow> keyboard = new ArrayList<>();
		KeyboardRow keyboardFirstRow = null, keyboardSecondRow = null;
		for (int i = 0; i < newAnswerOptions.length; i++) {
			if (i == 0) {
				keyboardFirstRow = new KeyboardRow();
				keyboard.add(keyboardFirstRow);
			}
			if (i == 2) {
				keyboardSecondRow = new KeyboardRow();
				keyboard.add(keyboardSecondRow);
			}

			String ao = newAnswerOptions[i];
			if (i <= 1) {
				if (ao.equals("/location")) {
					KeyboardButton kbb = new KeyboardButton(ao);
					kbb.setRequestLocation(true);
					if (keyboardFirstRow != null) {
						keyboardFirstRow.add(kbb);
					}
				} else {
					if (keyboardFirstRow != null) {
						keyboardFirstRow.add(newAnswerOptions[i]);
					}
				}
			} else {
				if (ao.equals("/location")) {
					KeyboardButton kb = new KeyboardButton(ao);
					kb.setRequestLocation(true);
					if (keyboardSecondRow != null) {
						keyboardSecondRow.add(kb);
					}
				} else {
					if (keyboardSecondRow != null) {
						keyboardSecondRow.add(ao);
					}
				}
			}
		}
		replyKeyboardMarkup.setKeyboard(keyboard);

		return replyKeyboardMarkup;
	}

	/**
	 * display buttons under message according to answer options
	 *
	 * @param newAnswerOptions
	 * @return
	 */
	private InlineKeyboardMarkup getInlineKeyboard(String[] newAnswerOptions) {
		InlineKeyboardMarkup inlineKeyboardMarkup = new InlineKeyboardMarkup();
		List<List<InlineKeyboardButton>> keyboard = new ArrayList<>();
		for (int i = 0; i < newAnswerOptions.length; i++) {
			ArrayList<InlineKeyboardButton> buttonList = new ArrayList<InlineKeyboardButton>(1);
			InlineKeyboardButton inlineKeyboardButton = new InlineKeyboardButton(newAnswerOptions[i]);
			inlineKeyboardButton.setCallbackData(newAnswerOptions[i]);
			buttonList.add(inlineKeyboardButton);
			keyboard.add(buttonList);
		}
		inlineKeyboardMarkup.setKeyboard(keyboard);

		return inlineKeyboardMarkup;
	}
}
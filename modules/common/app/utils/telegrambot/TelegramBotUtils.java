package utils.telegrambot;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hashids.Hashids;
import org.telegram.telegrambots.meta.api.objects.User;

import com.vdurmont.emoji.Emoji;
import com.vdurmont.emoji.EmojiManager;

public class TelegramBotUtils {

	public static final String PARTICIPANT_SENT_PHOTO = "participant_sent_photo";
	public static final String TG_PARTICIPANT_CACHE_PREFIX = "tg_chat_participant_";
	public static final String TG_USER_CACHE_PREFIX = "tg_chat_user_";

	// uppercase-numerical alphabet
	private static final Hashids hashids = new Hashids("DataFoundryTelegramSalt", 4,
	        "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");

	private static final Map<String, ResponseSet> RESPONSES = new HashMap<>();

	static {
		// responses for "participant sent photo"
		ResponseSet participantSentPhotoResponses = new ResponseSet();
		participantSentPhotoResponses.add("Nice photo :camera_flash:");
		participantSentPhotoResponses.add("Thanks for sharing!");
		participantSentPhotoResponses.add("Thank you.");
		participantSentPhotoResponses.add("Done.");
		participantSentPhotoResponses.add("Super.");
		participantSentPhotoResponses.add(":stars:");
		participantSentPhotoResponses.add("Got it.");
		participantSentPhotoResponses.add("Thanks for this! :thumbsup:");
		participantSentPhotoResponses.add("Nice.");
		participantSentPhotoResponses.add("All good.");
		participantSentPhotoResponses.add("Thanks :grinning:");
		participantSentPhotoResponses.add("Image received :robot_face:");
		participantSentPhotoResponses.add(":blush:");
		participantSentPhotoResponses.add("Thank you :grinning:");
		participantSentPhotoResponses.add(":star:");
		participantSentPhotoResponses.add("Photo received :robot_face:");
		participantSentPhotoResponses.add("Nice shot :ok_hand:");
		participantSentPhotoResponses.add(":wink:");
		participantSentPhotoResponses.add(":slightly_smiling:");
		participantSentPhotoResponses.add(":hugging:");
		participantSentPhotoResponses.add("Yes!");
		participantSentPhotoResponses.add(":pray:");
		participantSentPhotoResponses.add(":clap:");
		RESPONSES.put(PARTICIPANT_SENT_PHOTO, participantSentPhotoResponses);
	}

	/**
	 * pick a random emoji
	 * 
	 * @return
	 */
	public static String randomEmoji() {
		int count = EmojiManager.getAll().size();
		Emoji e = EmojiManager.getAll().stream().skip((int) Math.floor(Math.random() * count - 2)).findAny().get();
		return e.getUnicode();
	}

	/**
	 * extract valid Telegram user name based on user id; we need to do this because it is possible that the user name
	 * is not set in Telegram
	 * 
	 * @param user
	 * @return
	 */
	public static String getUserName(User user) {
		return "telegram_id_" + user.getId();
	}

	/**
	 * for existing user checks we can use the official Telegram user name which might be null
	 * 
	 * @param user
	 * @return
	 */
	public static String getLegacyUserName(User user) {
		return user.getUserName();
	}

	/**
	 * pick a response for the given key from the set of responses, use the variant to control which response is picked
	 * if multiple responses are available for the given key. if variant is -1, pick randomly. an empty string will be
	 * return if there are no responses for the given key.
	 * 
	 * @param key
	 * @param variant
	 * @return
	 */
	public static String getResponse(String key, int variant) {
		if (!RESPONSES.containsKey(key)) {
			return "";
		}

		if (variant > -1) {
			return RESPONSES.get(key).get(variant);
		} else {
			return RESPONSES.get(key).get();
		}
	}

	/**
	 * generate a PIN number for authenticating with Telegram via the Participation controller view
	 * 
	 * @return
	 */
	public static String generateTelegramPersonalPIN() {
		return "" + Math.round(10000 + Math.random() * 80000);
	}

	/**
	 * generate a PIN number for authenticating with Telegram via the Project controller view (anonymous participation)
	 * 
	 * @return
	 */
	public static String generateTelegramProjectPIN(long projectId) {
		return hashids.encode(projectId, Math.round(1000 + Math.random() * 8000));
	}

	/**
	 * check if string is in Telegram Project PIN format
	 * 
	 * @param pin
	 * @return
	 */
	public static boolean isValidTelegramProjectCode(String pin) {
		try {
			long[] twoIds = hashids.decode(pin.toUpperCase());
			return twoIds.length == 2;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * extract ids from string in Telegram Project PIN format
	 * 
	 * @param pin
	 * @return
	 */
	public static long[] extractIdsFromtelegramProjectCode(String pin) {
		try {
			return hashids.decode(pin.toUpperCase());
		} catch (Exception e) {
			return new long[] { -1, -1 };
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * container for responses that have the same key and can be used interchangeably
	 * 
	 */
	static class ResponseSet {
		private final List<String> responses = new LinkedList<>();

		/**
		 * retrieve a random response
		 * 
		 * @return
		 */
		public String get() {
			if (responses.isEmpty()) {
				return "";
			}

			int index = (int) Math.round(Math.random() * (responses.size() - 1));
			return responses.get(index);
		}

		/**
		 * retrieve response at index variant
		 * 
		 * @param variant
		 * @return
		 */
		public String get(int variant) {
			if (responses.isEmpty()) {
				return "";
			}

			return responses.get(variant % responses.size());
		}

		/**
		 * add responses to the list
		 * 
		 * @param response
		 */
		public void add(String response) {
			responses.add(response);
		}
	}

}

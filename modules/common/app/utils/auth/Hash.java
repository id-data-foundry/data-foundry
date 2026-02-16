package utils.auth;

import java.nio.charset.StandardCharsets;

import com.google.common.hash.Hashing;

import play.Logger;

/**
 * Simple Hashing util to allow for hashing passwords with SHA512
 * 
 * 
 * @author mathias
 *
 */
public class Hash {

	private static final String HASH_PREFIX = "hashed";
	private static final Logger.ALogger logger = Logger.of(Hash.class);

	/**
	 * has the given password (any String is possible), an error will be logged if the given String is already hashed,
	 * which would point at a problem in the calling routine
	 * 
	 * @param password
	 * @return
	 */
	public static String hashPassword(String password) {
		if (isHashed(password)) {
			logger.error("Hashed password entered to hashing again.");
			return password;
		}

		return HASH_PREFIX + Hashing.sha512().hashString(password, StandardCharsets.UTF_8).toString();
	}

	/**
	 * check if given String is hashed
	 * 
	 * @param password
	 * @return
	 */
	public static boolean isHashed(String password) {
		return password.startsWith(HASH_PREFIX);
	}
}

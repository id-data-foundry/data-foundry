package utils.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.mindrot.jbcrypt.BCrypt;

import com.google.common.hash.Hashing;

import play.Logger;

/**
 * Hashing util to allow for hashing passwords with BCrypt (new) and verifying legacy SHA512
 * 
 * @author mathias
 *
 */
public class Hash {

	public static final String BCRYPT_PREFIX = "bcrypt$";
	public static final String HASH_PREFIX = "hashed";
	private static final Logger.ALogger logger = Logger.of(Hash.class);

	/**
	 * hashes the given password with BCrypt (work factor 12)
	 * 
	 * @param password
	 * @return
	 */
	public static String hashPassword(String password) {
		if (isHashed(password)) {
			logger.error("Hashed password entered to hashing again.");
			return password;
		}

		return BCRYPT_PREFIX + BCrypt.hashpw(password, BCrypt.gensalt(12));
	}

	/**
	 * checks candidate password against stored hash (supporting both new bcrypt$ and legacy hashed prefixes)
	 * 
	 * @param candidate
	 * @param storedHash
	 * @return
	 */
	public static boolean checkPassword(String candidate, String storedHash) {
		if (candidate == null || storedHash == null) {
			return false;
		}

		if (storedHash.startsWith(BCRYPT_PREFIX)) {
			String rawBcrypt = storedHash.substring(BCRYPT_PREFIX.length());
			try {
				return BCrypt.checkpw(candidate, rawBcrypt);
			} catch (Exception e) {
				logger.error("Error verifying BCrypt password hash", e);
				return false;
			}
		}

		if (storedHash.startsWith(HASH_PREFIX)) {
			String expected = HASH_PREFIX + Hashing.sha512().hashString(candidate, StandardCharsets.UTF_8).toString();
			return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
					storedHash.getBytes(StandardCharsets.UTF_8));
		}

		if (storedHash.startsWith("$2a$") || storedHash.startsWith("$2b$") || storedHash.startsWith("$2y$")) {
			try {
				return BCrypt.checkpw(candidate, storedHash);
			} catch (Exception e) {
				logger.error("Error verifying BCrypt password hash", e);
				return false;
			}
		}

		return false;
	}

	/**
	 * check if given String is hashed
	 * 
	 * @param password
	 * @return
	 */
	public static boolean isHashed(String password) {
		if (password == null) {
			return false;
		}
		return password.startsWith(BCRYPT_PREFIX) || password.startsWith(HASH_PREFIX) || password.startsWith("$2a$")
				|| password.startsWith("$2b$") || password.startsWith("$2y$");
	}
}

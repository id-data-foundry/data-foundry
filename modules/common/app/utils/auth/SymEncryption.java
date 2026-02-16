package utils.auth;

import java.security.Key;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import play.Logger;

public class SymEncryption {

	private static final String ALGORITHM = "AES";
	private static final SecureRandom randomNumberGenerator = new SecureRandom();

	public static final String encryptToken(final String valueEnc, final String secKey) {
		return SymEncryption.encrypt(valueEnc, secKey);
	}

	private static final String encrypt(final String valueEnc, final String secKey) {

		String encryptedVal = null;

		try {
			final Key key = generateKeyFromString(secKey);
			final Cipher c = Cipher.getInstance(ALGORITHM);
			c.init(Cipher.ENCRYPT_MODE, key);
			final byte[] encValue = c.doFinal(valueEnc.getBytes());
			encryptedVal = Base64.getEncoder().encodeToString(encValue);
		} catch (Exception ex) {
			// do nothing
		}

		return encryptedVal;
	}

	public static final String decryptToken(final String encryptedValue, final String secretKey) {
		return SymEncryption.decrypt(encryptedValue, secretKey);
	}

	private static final String decrypt(final String encryptedValue, final String secretKey) {

		String decryptedValue = null;

		try {

			final Key key = generateKeyFromString(secretKey);
			final Cipher c = Cipher.getInstance(ALGORITHM);
			c.init(Cipher.DECRYPT_MODE, key);
			final byte[] decorVal = Base64.getDecoder().decode(encryptedValue);
			final byte[] decValue = c.doFinal(decorVal);
			decryptedValue = new String(decValue);
		} catch (Exception ex) {
			// do nothing
		}

		return decryptedValue;
	}

	private static final Key generateKeyFromString(final String secKey) throws Exception {
		final byte[] keyVal = Base64.getEncoder().encode(secKey.getBytes());
		final Key key = new SecretKeySpec(Arrays.copyOf(keyVal, 16), ALGORITHM);
		return key;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * generic way to pack project id and person id into a token
	 * 
	 * @param firstId
	 * @param secondId
	 * @param key
	 * @return
	 */
	public static String getToken(Long firstId, Long secondId, String key) {
		return encryptToken(firstId + ":" + secondId + ":" + randomNumberGenerator.nextInt(), key);
	}

	/**
	 * generic way to pack project id and person id into a token
	 * 
	 * @param firstId
	 * @param secondId
	 * @param key
	 * @return
	 */
	public static String getStableToken(Long firstId, Long secondId, String key) {
		return encryptToken(firstId + ":" + secondId + ":" + 0, key);
	}

	/**
	 * generic way to pack project id and a string into a token
	 * 
	 * @param firstId
	 * @param secondStr
	 * @param key
	 * @return
	 */
	public static String getToken(Long firstId, String secondStr, String key) {
		return encryptToken(firstId + ":" + secondStr.replace(':', ';') + ":" + randomNumberGenerator.nextInt(), key);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * generic way to unpack project id from token
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	public static Long getFirstIdFromToken(String token, String key) {
		try {
			return Long.parseLong(decryptToken(token, key).split(":")[0]);
		} catch (Exception e) {
			return -1l;
		}
	}

	/**
	 * generic way to unpack person id from token
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	public static Long getSecondIdFromToken(String token, String key) {
		try {
			return Long.parseLong(decryptToken(token, key).split(":")[1]);
		} catch (Exception e) {
			return -1l;
		}
	}

	/**
	 * generic way to unpack a string from token
	 * 
	 * @param token
	 * @param key
	 * @return
	 */
	public static String getSecondStringFromToken(String token, String key) {
		try {
			return decryptToken(token, key).split(":")[1];
		} catch (Exception e) {
			return null;
		}
	}

}

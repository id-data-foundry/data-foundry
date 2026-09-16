package utils.auth;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class SymEncryption {

	public static final String V2_PREFIX = "v2:";
	private static final String ALGORITHM = "AES";
	private static final String ALGORITHM_GCM = "AES/GCM/NoPadding";
	private static final int GCM_IV_LENGTH = 12;
	private static final int GCM_TAG_LENGTH = 128;
	private static final SecureRandom randomNumberGenerator = new SecureRandom();

	public static final String encryptToken(final String valueEnc, final String secKey) {
		return SymEncryption.encrypt(valueEnc, secKey);
	}

	private static final String encrypt(final String valueEnc, final String secKey) {
		String encryptedVal = null;
		try {
			final Key key = generateV2KeyFromString(secKey);
			final byte[] iv = new byte[GCM_IV_LENGTH];
			randomNumberGenerator.nextBytes(iv);
			final Cipher c = Cipher.getInstance(ALGORITHM_GCM);
			c.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH, iv));
			final byte[] encValue = c.doFinal(valueEnc.getBytes(StandardCharsets.UTF_8));
			final byte[] combined = new byte[iv.length + encValue.length];
			System.arraycopy(iv, 0, combined, 0, iv.length);
			System.arraycopy(encValue, 0, combined, iv.length, encValue.length);
			encryptedVal = V2_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(combined);
		} catch (Exception ex) {
			// do nothing
		}

		return encryptedVal;
	}

	public static final String decryptToken(final String encryptedValue, final String secretKey) {
		return SymEncryption.decrypt(encryptedValue, secretKey);
	}

	private static final String decrypt(final String encryptedValue, final String secretKey) {
		if (encryptedValue == null || encryptedValue.isEmpty()) {
			return null;
		}

		if (encryptedValue.startsWith(V2_PREFIX)) {
			return decryptV2(encryptedValue.substring(V2_PREFIX.length()), secretKey);
		}

		return decryptLegacy(encryptedValue, secretKey);
	}

	private static final String decryptV2(final String encryptedValueWithoutPrefix, final String secretKey) {
		String decryptedValue = null;
		try {
			final Key key = generateV2KeyFromString(secretKey);
			byte[] combined;
			try {
				combined = Base64.getUrlDecoder().decode(encryptedValueWithoutPrefix);
			} catch (IllegalArgumentException e) {
				combined = Base64.getDecoder().decode(encryptedValueWithoutPrefix);
			}
			if (combined.length < GCM_IV_LENGTH + 16) {
				return null;
			}
			final byte[] iv = Arrays.copyOfRange(combined, 0, GCM_IV_LENGTH);
			final byte[] cipherText = Arrays.copyOfRange(combined, GCM_IV_LENGTH, combined.length);
			final Cipher c = Cipher.getInstance(ALGORITHM_GCM);
			c.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH, iv));
			final byte[] decValue = c.doFinal(cipherText);
			decryptedValue = new String(decValue, StandardCharsets.UTF_8);
		} catch (Exception ex) {
			// do nothing
		}
		return decryptedValue;
	}

	private static final String decryptLegacy(final String encryptedValue, final String secretKey) {
		String decryptedValue = null;
		try {
			final Key key = generateLegacyKeyFromString(secretKey);
			final Cipher c = Cipher.getInstance(ALGORITHM);
			c.init(Cipher.DECRYPT_MODE, key);
			byte[] decorVal;
			try {
				decorVal = Base64.getDecoder().decode(encryptedValue);
			} catch (IllegalArgumentException e) {
				decorVal = Base64.getUrlDecoder().decode(encryptedValue);
			}
			final byte[] decValue = c.doFinal(decorVal);
			decryptedValue = new String(decValue, StandardCharsets.UTF_8);
		} catch (Exception ex) {
			// do nothing
		}
		return decryptedValue;
	}

	private static final Key generateLegacyKeyFromString(final String secKey) throws Exception {
		final byte[] keyVal = Base64.getEncoder().encode(secKey.getBytes());
		final Key key = new SecretKeySpec(Arrays.copyOf(keyVal, 16), ALGORITHM);
		return key;
	}

	private static final Key generateV2KeyFromString(final String secKey) throws Exception {
		final MessageDigest md = MessageDigest.getInstance("SHA-256");
		final byte[] keyVal = md.digest(secKey.getBytes(StandardCharsets.UTF_8));
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
			String dec = decryptToken(token, key);
			if (dec == null) {
				return -1l;
			}
			return Long.parseLong(dec.split(":")[0]);
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
			String dec = decryptToken(token, key);
			if (dec == null) {
				return -1l;
			}
			return Long.parseLong(dec.split(":")[1]);
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
			String dec = decryptToken(token, key);
			if (dec == null) {
				return null;
			}
			return dec.split(":")[1];
		} catch (Exception e) {
			return null;
		}
	}

}

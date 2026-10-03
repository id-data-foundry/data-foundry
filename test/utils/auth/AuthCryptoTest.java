package utils.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.junit.Test;

import com.google.common.hash.Hashing;

public class AuthCryptoTest {

	private static final String TEST_KEY = "test-secret-key-12345";

	@Test
	public void testV2TokenEncryptionAndDecryption() {
		String payload = "123:456:7890";
		String encrypted = SymEncryption.encryptToken(payload, TEST_KEY);

		assertNotNull(encrypted);
		assertTrue("Token must start with v2: prefix", encrypted.startsWith(SymEncryption.V2_PREFIX));

		String decrypted = SymEncryption.decryptToken(encrypted, TEST_KEY);
		assertEquals(payload, decrypted);

		assertEquals(Long.valueOf(123), SymEncryption.getFirstIdFromToken(encrypted, TEST_KEY));
		assertEquals(Long.valueOf(456), SymEncryption.getSecondIdFromToken(encrypted, TEST_KEY));
	}

	@Test
	public void testLegacyTokenDecryptionFallback() throws Exception {
		// Manually create a legacy AES-ECB encrypted token as the old system did
		String payload = "999:888:1234";
		byte[] keyVal = Base64.getEncoder().encode(TEST_KEY.getBytes(StandardCharsets.UTF_8));
		Key key = new SecretKeySpec(Arrays.copyOf(keyVal, 16), "AES");
		Cipher c = Cipher.getInstance("AES");
		c.init(Cipher.ENCRYPT_MODE, key);
		byte[] encValue = c.doFinal(payload.getBytes(StandardCharsets.UTF_8));
		String legacyToken = Base64.getEncoder().encodeToString(encValue);

		assertFalse("Legacy token should not have v2: prefix", legacyToken.startsWith("v2:"));

		// Verify SymEncryption decrypts the legacy token without errors
		String decrypted = SymEncryption.decryptToken(legacyToken, TEST_KEY);
		assertEquals(payload, decrypted);

		assertEquals(Long.valueOf(999), SymEncryption.getFirstIdFromToken(legacyToken, TEST_KEY));
		assertEquals(Long.valueOf(888), SymEncryption.getSecondIdFromToken(legacyToken, TEST_KEY));
	}

	@Test
	public void testBcryptPasswordHashingAndVerification() {
		String password = "SecretPassword123!";
		String hash = Hash.hashPassword(password);

		assertNotNull(hash);
		assertTrue("New hash should start with bcrypt$", hash.startsWith(Hash.BCRYPT_PREFIX));
		assertTrue("Hash should be identified as hashed", Hash.isHashed(hash));

		assertTrue("Valid password must verify against bcrypt hash", Hash.checkPassword(password, hash));
		assertFalse("Wrong password must fail", Hash.checkPassword("WrongPassword", hash));
		assertFalse("Null password must fail", Hash.checkPassword(null, hash));
		assertFalse("Null hash must fail", Hash.checkPassword(password, null));
	}

	@Test
	public void testLegacySha512PasswordVerification() {
		String password = "LegacyPassword456!";
		String legacyHash = Hash.HASH_PREFIX + Hashing.sha512().hashString(password, StandardCharsets.UTF_8).toString();

		assertTrue("Legacy hash should be identified as hashed", Hash.isHashed(legacyHash));
		assertTrue("Valid password must verify against legacy SHA-512 hash", Hash.checkPassword(password, legacyHash));
		assertFalse("Wrong password must fail against legacy SHA-512 hash", Hash.checkPassword("WrongPassword", legacyHash));
	}

	@Test
	public void testStableTokenDeterminismAndDecryption() {
		String token1 = SymEncryption.getStableToken(123L, 456L, TEST_KEY);
		String token2 = SymEncryption.getStableToken(123L, 456L, TEST_KEY);

		assertNotNull(token1);
		assertEquals("Stable token must be deterministic across calls", token1, token2);
		assertTrue("Stable token must start with v2: prefix", token1.startsWith(SymEncryption.V2_PREFIX));

		assertEquals(Long.valueOf(123), SymEncryption.getFirstIdFromToken(token1, TEST_KEY));
		assertEquals(Long.valueOf(456), SymEncryption.getSecondIdFromToken(token1, TEST_KEY));
	}

	@Test
	public void testPasswordHashInjectionPrevention() {
		// Attacker attempts to downgrade password by submitting an already hashed-looking string
		String pseudoHash = Hash.HASH_PREFIX + Hashing.sha512().hashString("attacker_pwd", StandardCharsets.UTF_8).toString();
		String resultHash = Hash.hashPassword(pseudoHash);

		// Must be hashed with BCrypt, not returned verbatim
		assertNotNull(resultHash);
		assertTrue("Result hash must start with bcrypt$", resultHash.startsWith(Hash.BCRYPT_PREFIX));
		assertFalse("Result hash must not be equal to input pseudo-hash", pseudoHash.equals(resultHash));
		assertTrue("Password must verify against the newly generated bcrypt hash", Hash.checkPassword(pseudoHash, resultHash));
	}

	@Test
	public void testConstantTimeEquals() {
		// Identical strings
		assertTrue("Identical strings must return true",
				Hash.constantTimeEquals("secretToken1234567890", "secretToken1234567890"));
		assertTrue("Empty strings must return true",
				Hash.constantTimeEquals("", ""));

		// Null handling
		assertTrue("Both null must return true",
				Hash.constantTimeEquals(null, null));
		assertFalse("First null must return false",
				Hash.constantTimeEquals(null, "secretToken1234567890"));
		assertFalse("Second null must return false",
				Hash.constantTimeEquals("secretToken1234567890", null));

		// Different strings same length
		assertFalse("Different string same length must return false",
				Hash.constantTimeEquals("secretToken1234567890", "secretToken1234567891"));

		// Different strings different length
		assertFalse("Prefix match different length must return false",
				Hash.constantTimeEquals("secretToken", "secretToken1234567890"));
		assertFalse("Empty vs non-empty must return false",
				Hash.constantTimeEquals("", "secret"));
	}
}


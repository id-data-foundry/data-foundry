package utils.auth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import com.google.common.hash.Hashing;

import models.Person;
import models.sr.Participant;

public class PasswordUpgradeSecurityTest {

	@Test
	public void testHashIsLegacyHash() {
		String legacyHash = "hashed" + Hashing.sha512().hashString("myPassword123", StandardCharsets.UTF_8).toString();
		assertTrue(Hash.isLegacyHash(legacyHash));
		assertTrue(Hash.isHashed(legacyHash));

		String bcryptHash = Hash.hashPassword("myPassword123");
		assertFalse(Hash.isLegacyHash(bcryptHash));
		assertTrue(Hash.isHashed(bcryptHash));
		assertFalse(Hash.isLegacyHash(null));
		assertFalse(Hash.isLegacyHash("plaintext123"));
	}

	@Test
	public void testPersonPasswordVerification() {
		Person person = new Person();
		String rawPassword = "SecurePassword123!";
		String legacyHash = "hashed" + Hashing.sha512().hashString(rawPassword, StandardCharsets.UTF_8).toString();
		person.setPasswordHash(legacyHash);

		// Verifying with correct password succeeds
		assertTrue(person.checkPassword(rawPassword));

		// Verifying with wrong password fails
		assertFalse(person.checkPassword("WrongPassword"));
	}

	@Test
	public void testParticipantPasswordVerification() {
		Participant participant = new Participant("Test", "User");
		String rawPassword = "ParticipantPassword456!";
		String legacyHash = "hashed" + Hashing.sha512().hashString(rawPassword, StandardCharsets.UTF_8).toString();
		// Set legacy hash directly
		participant.setPasswordHash(legacyHash);

		// Verification with correct password matches
		assertTrue(participant.checkPassword(rawPassword));
		assertFalse(participant.checkPassword("WrongPassword"));

		// Plaintext fallback verification
		Participant plaintextParticipant = new Participant("Test", "Plaintext");
		plaintextParticipant.setPasswordHash("cleartextSecret");
		assertTrue(plaintextParticipant.checkPassword("cleartextSecret"));
		assertFalse(plaintextParticipant.checkPassword("wrongCleartext"));
	}
}

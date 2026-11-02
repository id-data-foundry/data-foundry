package utils.validators;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OOCSIChannelValidationTest {

	@Test
	public void testReservedOOCSIChannelsAreRejected() {
		AbstractValidator channelValidator = Validators.get("OOCSI_channel");

		assertFalse(channelValidator.validate("OOCSI_events"));
		assertFalse(channelValidator.validate("OOCSI_connections"));
		assertFalse(channelValidator.validate("OOCSI_channels"));
		assertFalse(channelValidator.validate("OOCSI_clients"));
		assertFalse(channelValidator.validate("OOCSI_metrics"));
		assertFalse(channelValidator.validate("testchannel"));
		assertFalse(channelValidator.validate("timechannel"));
		assertFalse(channelValidator.validate("echochannel"));
	}

	@Test
	public void testValidCustomChannelsAreAccepted() {
		AbstractValidator channelValidator = Validators.get("OOCSI_channel");

		assertTrue(channelValidator.validate("heartrate"));
		assertTrue(channelValidator.validate("room101"));
		assertTrue(channelValidator.validate("sensor_stream"));
		assertTrue(channelValidator.validate("study-2026-accelerometer"));
		assertTrue(channelValidator.validate(""));
		assertTrue(channelValidator.validate(null));
	}

	@Test
	public void testOutputAndBackupValidatorsMatchChannelValidator() {
		AbstractValidator outputValidator = Validators.get("OOCSI_output");
		AbstractValidator backupValidator = Validators.get("OOCSI_backup");

		assertFalse(outputValidator.validate("OOCSI_events"));
		assertFalse(backupValidator.validate("OOCSI_events"));

		assertTrue(outputValidator.validate("custom_output"));
		assertTrue(backupValidator.validate("custom_backup"));
	}
}

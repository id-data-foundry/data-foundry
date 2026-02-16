package utils.validators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Validators {

	static private final Map<String, AbstractValidator> validators = new HashMap<String, AbstractValidator>();
	static private final AbstractValidator yesMan = new AbstractValidator() {
		@Override
		public boolean validate(String value) {
			return true;
		}
	};

	static {
		validators.put("OOCSI_backup", new AbstractValidator() {
			private final List<String> BUSY_CHANNELS = new ArrayList<String>(
			        Arrays.asList("testchannel", "timechannel", "echochannel", "OOCSI_events", "OOCSI_connections",
			                "OOCSI_channels", "OOCSI_clients", "OOCSI_metrics"));

			@Override
			public boolean validate(String value) {
				return BUSY_CHANNELS.stream().noneMatch(c -> value.startsWith(c));
			}

			@Override
			public String explainNo(String key, String value) {
				return "The channel '" + value + "' is not allowed for an OOCSI_channel subscription.";
			}
		});
		validators.put("OOCSI_service", new AbstractValidator() {
			private final List<String> BUSY_CHANNELS = new ArrayList<String>(
			        Arrays.asList("testchannel", "timechannel", "echochannel", "OOCSI_events", "OOCSI_connections",
			                "OOCSI_channels", "OOCSI_clients", "OOCSI_metrics"));

			@Override
			public boolean validate(String value) {
				return BUSY_CHANNELS.stream().noneMatch(c -> value.startsWith(c));
			}

			@Override
			public String explainNo(String key, String value) {
				return "The channel '" + value + "' is not allowed for an Entity dataset service.";
			}
		});
	}

	public static final AbstractValidator get(String validationChoice) {
		return validators.containsKey(validationChoice) ? validators.get(validationChoice) : yesMan;
	}
}

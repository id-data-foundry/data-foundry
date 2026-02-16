package utils.admin;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.typesafe.config.Config;

import models.Person;
import utils.conf.ConfigurationUtils;

@Singleton
public class AdminUtils {

	public static final String SYSTEM_PROJECT = "SYSTEM_PROJECT";

	private final List<String> adminUsers;

	@Inject
	public AdminUtils(Config configuration) {
		// get admin users from config
		if (configuration.hasPath(ConfigurationUtils.DF_USERS_ADMINS)) {
			this.adminUsers = configuration.getStringList(ConfigurationUtils.DF_USERS_ADMINS);
		} else {
			this.adminUsers = Collections.emptyList();
		}
	}

	public boolean isAdmin(Optional<Person> userOpt) {
		// check admin status of user account
		Person user = userOpt.get();
		if (adminUsers.stream().anyMatch(s -> s.equals(user.getEmail()))) {
			// registered admin email matches user email
			return true;
		}

		return false;
	}

	public Optional<Person> getFirstAdminUser() {
		if (adminUsers.isEmpty()) {
			return Optional.empty();
		}

		String username = adminUsers.get(0);
		return Person.findByEmail(username);
	}
}

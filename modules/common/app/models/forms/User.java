package models.forms;

import play.data.validation.Constraints.Email;
import play.data.validation.Constraints.MinLength;
import play.data.validation.Constraints.Required;

public class User {

	@Required
	@Email
	public String email;
	@Required
	@MinLength(value = 2)
	public String firstname;
	@Required
	@MinLength(value = 2)
	public String lastname;
}

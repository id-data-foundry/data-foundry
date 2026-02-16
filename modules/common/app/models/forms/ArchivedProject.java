package models.forms;

import play.data.validation.Constraints.MinLength;
import play.data.validation.Constraints.Required;

public class ArchivedProject {

	@Required
	public long userId;
	@Required
	public String type;
	@Required
	@MinLength(value = 2)
	public String name;
	@Required
	public String year;

	public String intro;
	public String description;
	public String code;
	public String squad;

	// coach
	public String user;

}

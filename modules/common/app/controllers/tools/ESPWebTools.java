package controllers.tools;

import controllers.AbstractAsyncController;
import controllers.auth.UserAuth;
import play.mvc.Result;
import play.mvc.Security.Authenticated;

@Authenticated(UserAuth.class)
public class ESPWebTools extends AbstractAsyncController {

	public Result index() {
		// just render the tools page
		return ok(views.html.tools.espweb.index.render());
	}

}

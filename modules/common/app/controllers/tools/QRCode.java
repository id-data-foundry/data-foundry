package controllers.tools;

import controllers.auth.UserAuth;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Security.Authenticated;

@Authenticated(UserAuth.class)
public class QRCode extends Controller {

	public Result index() {
		return ok(views.html.tools.qrcode.index.render());
	}
}

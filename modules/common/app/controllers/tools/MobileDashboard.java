package controllers.tools;

import java.io.File;
import java.util.Arrays;

import javax.inject.Inject;

import controllers.AbstractAsyncController;
import models.Dataset;
import models.DatasetType;
import models.Project;
import play.mvc.Http.Request;
import play.mvc.Result;
import utils.auth.TokenResolverUtil;
import utils.tools.QRCodeUtil;

public class MobileDashboard extends AbstractAsyncController {

	@Inject
	TokenResolverUtil tokenResolverUtil;

	@Inject
	QRCodeUtil qrCodeUtil;

	public Result view(String token) {

		// retrieve id from token
		Long id = tokenResolverUtil.getDatasetIdFromToken(token);

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || ds.getDsType() != DatasetType.IOT) {
			return badRequest(views.html.tools.mobile.error.render());
		}

		// check if the token is current
		String pat = ds.getConfiguration().get(Dataset.PUBLIC_ACCESS_TOKEN);
		if (!token.equals(pat)) {
			return badRequest(views.html.tools.mobile.error.render());
		}

		// check project
		Project project = ds.getProject();
		project.refresh();

		// get projection
		String projection = ds.getConfiguration().getOrDefault(Dataset.DATA_PROJECTION, "");
		String[] columns = projection.split(",");
		if (columns.length > 7) {
			columns = Arrays.copyOfRange(columns, 7, columns.length);
		}

		return ok(views.html.tools.mobile.view.render(ds, token, columns, project.getDevices()));
	}

	public Result generateQRCode(Request request, String token) {
		// retrieve id from token
		Long id = tokenResolverUtil.getDatasetIdFromToken(token);

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || ds.getDsType() != DatasetType.IOT) {
			return badRequest();
		}

		// check if the token is current
		String pat = ds.getConfiguration().get(Dataset.PUBLIC_ACCESS_TOKEN);
		if (!token.equals(pat)) {
			return badRequest();
		}

		String pathStr = qrCodeUtil.generateCachedQRCode(routes.MobileDashboard.view(token).absoluteURL(request, true),
		        token, 300);
		return ok(new File(pathStr));
	}
}

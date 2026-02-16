package controllers.api;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Project;
import models.ds.FormDS;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.auth.TokenResolverUtil;
import utils.components.OnboardingSupport;
import utils.rendering.FormMarkdown;

public class FormDSController extends AbstractDSController {

	private static final String FORM_TEMPLATE_TEXT = "form_template_text";
	private static final String FORM_HTML_TEXT = "form_html_text";
	private static final Logger.ALogger logger = Logger.of(FormDSController.class);

	private final TokenResolverUtil tokenResolverUtil;

	@Inject
	public FormDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolverUtil, OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);

		this.tokenResolverUtil = tokenResolverUtil;
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		// find data set
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(controllers.routes.HomeController.index()).addingToSession(request, "error",
			        "We could not find this dataset.");
		}

		// check ownership and collaboratorship
		if (!ds.visibleFor(username)) {
			return redirect(controllers.routes.ProjectsController.view(ds.getProject().getId()))
			        .addingToSession(request, "error", "Dataset is not accessible.");
		}

		return ok(views.html.datasets.form.view.render(ds, username, getParticipantViewLink(ds, request.host()),
		        request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		return ok(views.html.datasets.form.add.render(csrfToken(request), p));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "Project not valid or you don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data");
		}

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.FORM, p, df.get("description"),
		        df.get("target_object"), "", df.get("license"));

		// dates
		storeDates(ds, df);
		ds.save();

		onboardingSupport.updateAfterDone(username, "new_dataset");

		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result edit(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		return ok(views.html.datasets.form.edit.render(csrfToken(request), ds));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
			        "Expecting some data");
		}

		ds.setName(htmlTagEscape(nss(df.get("dataset_name"), 64)));
		ds.setDescription(htmlTagEscape(nss(df.get("description"))));
		ds.setOpenParticipation(df.get("isPublic") == null ? false : true);
		ds.setLicense(df.get("license"));

		// dates
		storeDates(ds, df);

		// metadata
		storeMetadata(ds, df);
		ds.update();

		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result transform(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return ok("Error: Dataset not found.");
		}

		// check authorization
		if (!ds.editableBy(username)) {
			return ok("You need to be either project owner or collaborator to perform this action.");
		}

		String input = request.body().asText();

		FormMarkdown fmd = new FormMarkdown();
		String output = fmd.renderFormPreview(input);

		return ok(output);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@AddCSRFToken
	public Result recordForm(Request request, Long id, String invite_token) {

		// check invite_token
		if (invite_token == null || invite_token.length() == 0) {
			return redirect(controllers.routes.HomeController.index()).addingToSession(request, "error",
			        "Form token is not given.");
		}

		if (!tokenResolverUtil.getDatasetIdFromToken(invite_token).equals(id)) {
			logger.info("Form token is not recognized: " + tokenResolverUtil.getDatasetIdFromToken(invite_token)
			        + " (given) + " + id + " (target)");
			return redirect(controllers.routes.HomeController.index()).addingToSession(request, "error",
			        "Form token is not recognized.");
		}

		// check participant id from invite_id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
			        "Dataset is closed (adjust start and end dates to open).");
		}

		// make sure the text is available for rendering
		if (ds.getConfiguration().get(FORM_HTML_TEXT) == null) {
			// render again?
		}

		String template_text = ds.getConfiguration().get(FORM_TEMPLATE_TEXT);
		if (template_text == null) {
			// no form to render
			return ok(views.html.datasets.form.notavailable.render(ds));
		}

		// render and store
		FormMarkdown fmd = new FormMarkdown();
		String html_text = fmd.renderForm(template_text);
		String projection = fmd.getProjection();
		ds.getConfiguration().put(FORM_HTML_TEXT, html_text);
		ds.getConfiguration().put(Dataset.DATA_PROJECTION, projection);
		ds.update();

		return ok(views.html.datasets.form.record.render(csrfToken(request), ds));
	}

	@RequireCSRFCheck
	public Result record(Request request, Long id) {

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
			        "Dataset is closed (adjust start and end dates to open).");
		}

		Map<String, String[]> df = request.body().asFormUrlEncoded();
		if (df == null) {
			return badRequest("Expecting some data");
		}

		// check for resubmission
		String[] token = df.get("csrfToken");
		if (token == null || token.length == 0 || cache.get(token[0]).isPresent()) {
			return ok(views.html.datasets.form.thanks.render(ds));
		}

		// record the information in the database for this data set
		final FormDS fmsc = (FormDS) datasetConnector.getDatasetDS(ds);

		// collect items
		ObjectNode on = Json.newObject();
		df.keySet().stream()
		        .filter(key -> key.startsWith("choice_") || key.startsWith("text_") || key.startsWith("numerical_"))
		        .forEach(key -> {
			        String[] value = df.get(key);
			        if (value != null) {
				        if (value.length == 1) {
					        if (key.startsWith("text_")) {
						        String text = value[0].replace("\n", "<br>")
						                .replaceAll("[\\p{Cntrl}\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}]", "");
						        on.put(key, text);
					        } else {
						        on.put(key, value[0]);
					        }
				        } else {
					        on.put(key, String.join(",", value));
				        }
			        }
		        });

		// text
		String text = on.toString();

		// store data
		fmsc.addRecord(new Date(), text);

		// avoid resubmissions
		cache.set(token[0], true, 30000);

		// flash and redirect
		return ok(views.html.datasets.form.thanks.render(ds)).addingToSession(request, "message",
		        "Form entry recorded, thanks!");
	}

	@Authenticated(UserAuth.class)
	public Result visualize(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		if (!ds.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project is not accessible.");
		}

		String template_text = ds.getConfiguration().get(FORM_TEMPLATE_TEXT);
		if (template_text == null) {
			// no form to render
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "message",
			        "There is no form set up yet.");
		}

		// render visualization
		FormMarkdown fmd = new FormMarkdown(ds);
		// first pass through the form to collect different items
		fmd.renderForm(template_text);
		// second pass through the form to visualize the items
		String visualization = fmd.renderVisualization(template_text);

		return ok(views.html.datasets.form.visualize.render(ds, visualization));
	}

	public CompletionStage<Result> downloadExternal(Dataset ds, long limit, long start, long end) {
		return CompletableFuture.supplyAsync(() -> internalExport(ds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks)
		                .withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
		                .as("text/csv"));
	}

	public CompletionStage<Result> downloadInternal(Dataset ds, long limit, long start, long end) {
		return CompletableFuture.supplyAsync(() -> internalExport(ds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadRaw(Request request, Long id, long limit, long start, long end) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		Project project = ds.getProject();
		project.refresh();
		if (!project.visibleFor(username)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "Project is not accessible."));
		}

		if (acceptLicenseFirst(request, username, project)) {
			// redirect to accept license first
			return redirectCS(controllers.routes.ProjectsController.license(project.getId(),
			        controllers.api.routes.FormDSController.downloadRaw(id, -1L, -1l, -1l).relativeTo("/")));
		}

		return CompletableFuture.supplyAsync(() -> internalExportRaw(ds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks)
		                .withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
		                .as("text/csv"));
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * internal export projected data (if projection is available)
	 * 
	 * @param ds
	 * @param end
	 * @param start
	 * @return
	 */
	private Source<ByteString, ?> internalExport(Dataset ds, long limit, long start, long end) {
		FormDS fmsc = (FormDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> fmsc.exportProjected(sourceActor, limit, start, end));
			return sourceActor;
		});
	}

	/**
	 * internal raw export
	 * 
	 * @param ds
	 * @return
	 */
	private Source<ByteString, ?> internalExportRaw(Dataset ds, long limit, long start, long end) {
		FormDS fmsc = (FormDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> fmsc.export(sourceActor, limit, start, end));
			return sourceActor;
		});
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public String getParticipantViewLink(Dataset ds, String host) {
		return controllers.api.routes.FormDSController
		        .recordForm(ds.getId(), tokenResolverUtil.getDatasetToken(ds.getId())).absoluteURL(true, host);
	}

}

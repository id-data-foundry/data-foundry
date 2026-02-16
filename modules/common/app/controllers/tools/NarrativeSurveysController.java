package controllers.tools;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.FileUtils;

import controllers.AbstractAsyncController;
import controllers.DatasetsController;
import controllers.api.CompleteDSController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.ds.LinkedDS;
import play.Logger;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.DataUtils;
import utils.StringUtils;
import utils.auth.TokenResolverUtil;
import utils.tools.NarrativeSurveyUtils;

@Authenticated(UserAuth.class)
@Singleton
public class NarrativeSurveysController extends AbstractAsyncController {

	private final DatasetConnector datasetConnector;

	private final TokenResolverUtil tokenResolverUtil;

	private final CompleteDSController completeDSController;

	private static final Logger.ALogger logger = Logger.of(NarrativeSurveysController.class);

	@Inject
	public NarrativeSurveysController(DatasetConnector datasetConnentor, TokenResolverUtil tokenResolverUtil,
	        FormFactory formFactory, DatasetsController datasetController, CompleteDSController completeDSController) {
		this.datasetConnector = datasetConnentor;
		this.tokenResolverUtil = tokenResolverUtil;
		this.completeDSController = completeDSController;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// MANAGEMENT

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addNarrativeSurveyView(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(user)) {
			return noContent();
		}

		Optional<Dataset> smOpt = project.getStudyManagementDataset();
		if (smOpt.isEmpty()) {
			return noContent();
		}

		Dataset sm = smOpt.get();
		if (!sm.configuration(Dataset.NS_NARRATIVE_SURVEY_DATASETS, "").isEmpty()) {
			return redirect(controllers.routes.ProjectsController.viewNarrativeSurvey(id));
		}

		// create complete dataset for twee files and for web-ready files: html exports and images
		Dataset cdsWeb = datasetConnector.create("Narrative Survey Web", DatasetType.COMPLETE, project,
		        "This dataset stores web-accessible files for the narrative surveys.", "", null, null);
		cdsWeb.setCollectorType(Dataset.NARRATIVE_SURVEY);
		cdsWeb.save();

		// activate web access
		String token = tokenResolverUtil.getDatasetToken(cdsWeb.getId());
		cdsWeb.getConfiguration().put(Dataset.WEB_ACCESS_TOKEN, token);
		cdsWeb.update();

		// register the datasets in the studymanagement
		sm.getConfiguration().put(Dataset.NS_NARRATIVE_SURVEY_DATASETS, "" + cdsWeb.getId());
		sm.update();

		logger.info("Created Narrative Survey view for project " + id);

		return redirect(controllers.routes.ProjectsController.viewNarrativeSurvey(id));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addSurvey(Request request, long id) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));
		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(user)) {
			return noContent();
		}

		Optional<Dataset> smOpt = project.getStudyManagementDataset();
		if (smOpt.isEmpty()) {
			return noContent();
		}

		Dataset sm = smOpt.get();
		long cdsWebId = DataUtils.parseLong(sm.configuration(Dataset.NS_NARRATIVE_SURVEY_DATASETS, "-1L"));
		Dataset cdsWeb = Dataset.find.byId(cdsWebId);
		if (cdsWeb == null) {
			return noContent();
		}

		// create a twee template file
		if (!request.hasHeader("HX-Prompt")) {
			return noContent();
		}
		Optional<String> tweeFileNameOpt = request.header("HX-Prompt");
		if (tweeFileNameOpt.isEmpty()) {
			return noContent();
		}
		String tweeFileName = tweeFileNameOpt.get() + ".twee";

		// create an entity dataset to log into
		Dataset cdsEntity = datasetConnector.create("Narrative Survey Data for " + tweeFileName, DatasetType.ENTITY,
		        project, "This dataset stores data for the narrative survey " + tweeFileName, "", null, null);
		cdsEntity.setCollectorType(Dataset.NARRATIVE_SURVEY);
		cdsEntity.save();

		// write template file
		CompleteDS cdsWebDS = datasetConnector.getTypedDatasetDS(cdsWeb);
		Optional<String> surveyNameOpt = NarrativeSurveyUtils.createEmptySurveyFile(cdsWebDS, cdsEntity, tweeFileName,
		        new Date());
		if (surveyNameOpt.isPresent()) {
			cdsWebDS.addRecord(surveyNameOpt.get(), "", new Date());
		}

		String htmlFileName = tweeFileName.replace(".twee", ".html");
		Optional<String> htmlNameOpt = NarrativeSurveyUtils.createEmptyHTMLFile(cdsWebDS, cdsEntity, htmlFileName,
		        new Date());
		if (htmlNameOpt.isPresent()) {
			cdsWebDS.addRecord(htmlNameOpt.get(), "", new Date());
		}

		// link entity dataset to the twee files and its exports, via the configuration of the web dataset
		cdsWeb.getConfiguration().put(tweeFileName, "" + cdsEntity.getId());
		cdsWeb.update();

		logger.info("Created Narrative Survey for project " + id + ": " + tweeFileName);

		return redirect(controllers.routes.ProjectsController.viewNarrativeSurvey(id));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result removeSurvey(Request request, long id, long fileId) {
		getAuthenticatedUserOrReturn(request, redirect(LANDING));

		// remove twee template file
		completeDSController.delete(request, id, fileId);

		// remove analysis file

		// close entity dataset --> inactive

		logger.info("Removed Narrative Survey for project " + id + ": " + fileId);

		return redirect(controllers.routes.ProjectsController.viewNarrativeSurvey(id));
	}

	@Authenticated(UserAuth.class)
	public Result viewSurveyAnalysis(Request request, long id, long fileId) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		Project project = Project.find.byId(id);
		if (project == null || !project.editableBy(user)) {
			return noContent();
		}

		Optional<Dataset> dsOpt = project.getNarrativeSurveyDataset();
		if (dsOpt.isEmpty()) {
			return noContent();
		}

		Dataset cdsWeb = dsOpt.get();
		CompleteDS cdsWebDS = datasetConnector.getTypedDatasetDS(cdsWeb);
		Optional<File> surveyFile = cdsWebDS.getFile(fileId);
		if (surveyFile.isEmpty()) {
			return noContent();
		}

		String surveyName = surveyFile.get().getName();

		long edsId = DataUtils.parseLong(cdsWeb.configuration(surveyName, ""));
		Dataset eds = Dataset.find.byId(edsId);
		if (eds == null) {
			return noContent();
		}

		LinkedDS edsDS = datasetConnector.getDatasetDS(eds);
		String dataJson = edsDS.retrieveProjected(null, -1, -1, -1).toString();

		// inject the data into the analysis
		return ok(views.html.tools.twine.analysis.render(project, surveyName, dataJson));
	}

	@RequireCSRFCheck
	public Result uploadImage(Request request, long id, long dsId) {
		completeDSController.uploadFile(request, dsId);
		return redirect(controllers.routes.ProjectsController.viewNarrativeSurvey(id));
	}

	@RequireCSRFCheck
	public Result deleteImage(Request request, long id, long fileId) {
		completeDSController.delete(request, id, fileId);
		return ok();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// AUTHORING

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result loadTwee(Request request, long dsId, long fid) {
		Person user = getAuthenticatedUserOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(dsId);
		if (ds == null || !ds.editableBy(user.getEmail())) {
			return redirect(LANDING).addingToSession(request, "error", "You cannot edit this dataset with this tool.");
		}

		CompleteDS cds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> fileOpt = cds.getFile(fid);
		if (fileOpt.isEmpty()) {
			return redirect(LANDING).addingToSession(request, "error", "You cannot edit this dataset with this tool.");
		}
		File file = fileOpt.get();
		if (!file.exists()) {
			return redirect(LANDING).addingToSession(request, "error", "You cannot edit this dataset with this tool.");
		}

		// read file contents
		String contents = "";
		try {
			contents = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
		} catch (IOException e) {
			// do nothing
		}

		// escape html template quotes and also closing script tags just in case
		contents = contents.replace("`", "\\`").replace("</script>", "<\\/script>");

		String htmlFileName = file.getName().replace(".twee", ".html");
		Optional<Long> htmlFileOpt = cds.getLatestFileVersionId(htmlFileName);
		if (htmlFileOpt.isEmpty()) {
			return redirect(LANDING).addingToSession(request, "error", "You cannot edit this dataset with this tool.");
		}

		// post base URL, pre-rendered
		String urlTwee = routes.NarrativeSurveysController.saveTwee(dsId, fid).absoluteURL(request, true);
		String urlHTML = routes.NarrativeSurveysController.saveTwee(dsId, htmlFileOpt.get()).absoluteURL(request, true);
		return ok(views.html.tools.twine.view.render(contents, urlTwee, urlHTML, csrfToken(request)));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result saveTwee(Request request, long dsId, long fid) {
		Person user = getAuthenticatedUserOrReturn(request, forbidden());

		if (!request.hasBody()) {
			return badRequest();
		}

		Dataset ds = Dataset.find.byId(dsId);
		if (ds == null || !ds.editableBy(user.getEmail())) {
			return notFound();
		}

		CompleteDS cds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> file = cds.getFile(fid);
		if (file.isEmpty()) {
			return notFound();
		}
		if (!file.get().exists()) {
			return notFound();
		}

		// write body contents to file
		String contents = request.body().asText();
		// unscramble the contents if necessary
		contents = StringUtils.unscrambleTransport(contents);

		try {
			FileUtils.write(file.get(), contents, StandardCharsets.UTF_8);
		} catch (IOException e) {
			// do nothing
		}

		return ok();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}

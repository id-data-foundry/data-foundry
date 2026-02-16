package controllers.api;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.ds.MovementDS;
import models.sr.Cluster;
import models.sr.Participant;
import models.vm.TimedMedia;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Files.TemporaryFile;
import play.mvc.Http;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.slack.Slack;
import utils.components.OnboardingSupport;
import utils.validators.FileTypeUtils;

public class MovementDSController extends AbstractDSController {

	private static final Logger.ALogger logger = Logger.of(MovementDSController.class);

	@Inject
	public MovementDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
	        OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		Project project = ds.getProject();
		if (!ds.visibleFor(username)) {
			return redirect(controllers.routes.ProjectsController.view(project.getId())).addingToSession(request,
			        "error", "Project is not accessible.");
		}

		final MovementDS cpsc = (MovementDS) datasetConnector.getDatasetDS(ds);
		final List<TimedMedia> fileList = cpsc.getFiles();

		// refresh project to get the participants; enumerate participants before render, beanlist needs it.
		project.refresh();
		project.getParticipants().size();

		return ok(views.html.datasets.movement.view.render(ds, project.getParticipants(), username, fileList, request));
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

		return ok(views.html.datasets.movement.add.render(csrfToken(request), p));
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

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.MOVEMENT, p, df.get("description"),
		        df.get("target_object"), df.get("isPublic"), df.get("license"));

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

		if (!ds.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
			        "You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		return ok(views.html.datasets.movement.edit.render(csrfToken(request), ds));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result editMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		if (!ds.editableBy(username)) {
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

		LabNotesEntry.log(MovementDSController.class, LabNotesEntryType.MODIFY, "Dataset edited: " + ds.getName(),
		        ds.getProject());
		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> uploadFile(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		return CompletableFuture.supplyAsync(() -> {
			final Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
			} else if (!ds.canAppend()) {
				return redirect(HOME).addingToSession(request, "error",
				        "Dataset is closed (adjust start and end dates to open).");
			} else if (!ds.editableBy(username)) {
				return redirect(HOME).addingToSession(request, "error",
				        "You need to be either project owner or collaborator to perform this action.");
			}

			Project project = ds.getProject();
			project.refresh();
			try {
				Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
				if (body == null) {
					return redirect(HOME).addingToSession(request, "error", "Bad request");
				}

				DynamicForm df = formFactory.form().bindFromRequest(request);
				if (df == null) {
					return redirect(HOME).addingToSession(request, "error", "Expecting some data");
				}

				long pid = -1;
				Participant participant = Participant.EMPTY_PARTICIPANT;
				String participantId = df.get("participant_id");
				if (participantId != null && participantId.length() > 0) {
					pid = Long.parseLong(participantId);
					participant = Participant.find.byId(pid);
					if (participant == null || !project.hasParticipant(participant)) {
						// don't add if participant is empty or does not belong to this project
						return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
						        "error", "No valid participant Id provided.");
					}
				}

				List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
				if (!fileParts.isEmpty()) {
					final MovementDS cpds = (MovementDS) datasetConnector.getDatasetDS(ds);

					for (int i = 0; i < fileParts.size(); i++) {
						FilePart<TemporaryFile> filePart = fileParts.get(i);
						TemporaryFile file = filePart.getRef();
						String fileName = filePart.getFilename();

						// restrict file type by name
						if (!FileTypeUtils.looksLikeMovementDataFile(fileName)) {
							logger.error("Only .gpx or .xml files allowed in this dataset type.");
							continue;
						}

						// content-based validation
						if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.XML)) {
							logger.error("Only .csv files allowed in this dataset type.");
							continue;
						}

						Date now = new Date();

						// ensure that filename is unique on disk for participants
						if (participant != Participant.EMPTY_PARTICIPANT) {
							// note: compared to the CompleteDS this is needed to ensure that repeated uploads by
							// participants can be distinguished
							fileName = participant.getId() + "_" + now.getTime() + "_" + fileName;
						}

						// store file, add record
						Optional<String> storeFile = cpds.storeFile(file.path().toFile(), fileName);
						if (storeFile.isPresent()) {
							final String description = nss(df.get("description"));
							if (cpds.importFileContents(file.path().toFile(), participant)) {
								cpds.addRecord(participant, storeFile.get(), description, now, "import successful");
							} else {
								cpds.addRecord(participant, storeFile.get(), description, now, "import failed");
							}
						}
					}

					LabNotesEntry.log(MovementDSController.class, LabNotesEntryType.MODIFY,
					        "Files uploaded to dataset: " + ds.getName(), ds.getProject());
				}

				return redirect(controllers.routes.DatasetsController.view(ds.getId()));
			} catch (NullPointerException e) {
				logger.error("Error uploading file to dataset.", e);
				Slack.call("Exception", e.getLocalizedMessage());
			}

			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
			        "Invalid request, no files have been included in the request.");
		});
	}

	@Authenticated(UserAuth.class)
	public Result downloadFile(Request request, long id, long fileId, String fileName) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project project = ds.getProject();
		project.refresh();
		if (!project.visibleFor(username)) {
			return redirect(controllers.routes.ProjectsController.view(project.getId())).addingToSession(request,
			        "error", "Project is not accessible.");
		}

		if (acceptLicenseFirst(request, username, project)) {
			// redirect to accept license first
			return redirect(controllers.routes.ProjectsController.license(project.getId(),
			        routes.MovementDSController.downloadFile(id, fileId, fileName).relativeTo("/")));
		}

		// compose file path and check existence
		final MovementDS cpds = (MovementDS) datasetConnector.getDatasetDS(ds);
		Optional<File> requestedFile = cpds.getFile(fileId);
		if (!requestedFile.isPresent() || !requestedFile.get().exists()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
			        "No file found: " + fileName);
		}

		LabNotesEntry.log(MovementDSController.class, LabNotesEntryType.DOWNLOAD, "Dataset downloaded: " + ds.getName(),
		        ds.getProject());

		return ok(requestedFile.get()).as("application/x-download").withHeader("Content-disposition",
		        "attachment; filename=" + fileName);
	}

	public CompletionStage<Result> downloadExternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get participant ids (if cluster is given)
		final List<Long> participantIds = cluster.getParticipantList();
		return CompletableFuture.supplyAsync(() -> internalExport(ds, participantIds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks)
		                .withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
		                .as("text/csv"));
	}

	public CompletionStage<Result> downloadInternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get participant ids (if cluster is given)
		final List<Long> participantIds = cluster.getParticipantList();
		return CompletableFuture.supplyAsync(() -> internalExport(ds, participantIds, limit, start, end))
		        .thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * internal export structured data
	 * 
	 * @param ds
	 * @param end
	 * @param start
	 * @return
	 */
	private Source<ByteString, ?> internalExport(Dataset ds, List<Long> participantIds, long limit, long start,
	        long end) {
		MovementDS mvsc = (MovementDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> mvsc.export(sourceActor, participantIds, limit, start, end));
			return sourceActor;
		});
	}

}

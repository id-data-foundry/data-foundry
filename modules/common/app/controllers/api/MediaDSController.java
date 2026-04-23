package controllers.api;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import javax.inject.Inject;

import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import controllers.auth.DatasetApiAuth;
import controllers.auth.ParticipantAuth;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.ds.MediaDS;
import models.sr.Cluster;
import models.sr.Participant;
import models.vm.TimedMedia;
import play.Environment;
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
import play.mvc.RangeResults;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.slack.Slack;
import utils.DataUtils;
import utils.components.OnboardingSupport;
import utils.rendering.ImageUtil;
import utils.validators.FileTypeUtils;

public class MediaDSController extends AbstractDSController {

	private static final Logger.ALogger logger = Logger.of(MediaDSController.class);

	private final ImageUtil imageUtil;
	private final Environment environment;

	@Inject
	public MediaDSController(Environment environment, FormFactory formFactory, SyncCacheApi cache,
			DatasetConnector datasetConnector, ImageUtil imageUtil, OnboardingSupport onboardingSupport) {
		super(formFactory, cache, datasetConnector, onboardingSupport);
		this.environment = environment;
		this.imageUtil = imageUtil;
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project is not accessible.");
		}

		final MediaDS mds = (MediaDS) datasetConnector.getDatasetDS(ds);
		final List<TimedMedia> fileList = mds.getFiles();

		// refresh project to get the participants; enumerate participants before render, beanlist needs it.
		Project project = ds.getProject();
		project.refresh();
		project.getParticipants().size();

		return ok(views.html.datasets.media.view.render(ds, project.getParticipants(), username, fileList, request));
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

		return ok(views.html.datasets.media.add.render(csrfToken(request), p));
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

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.MEDIA, p, df.get("description"),
				df.get("target_object"), df.get("isPublic"), df.get("license"));

		// dates
		storeDates(ds, df);
		ds.save();

		onboardingSupport.updateAfterDone(username, "new_dataset");

		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result editItem(Request request, Long id, Long itemId) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		if (!ds.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You don't have permissions for this action. Need to be the owner or a collaborator of the project.");
		}

		final MediaDS mds = (MediaDS) datasetConnector.getDatasetDS(ds);
		Optional<TimedMedia> item = mds.getItem(itemId);
		if (item.isEmpty()) {
			return notFound("Item not found.");
		}

		// refresh project to get the participants; enumerate participants before render, beanlist needs it.
		Project project = ds.getProject();
		project.refresh();
		List<Participant> participants = project.getParticipants();
		participants.size();

		return ok(views.html.datasets.media.editItem.render(csrfToken(request), ds, item.get(), participants, username,
				request));
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

		return ok(views.html.datasets.media.edit.render(csrfToken(request), ds));
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

		LabNotesEntry.log(MediaDSController.class, LabNotesEntryType.MODIFY, "Dataset edited: " + ds.getName(),
				ds.getProject());
		return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "message",
				"Changes saved.");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Dataset API file download ("get item")
	 * 
	 * @param request
	 * @param id
	 * @param fileName
	 * @return
	 */
	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> getItemApi(Request request, Long id, String fileNameParam) {
		return CompletableFuture.supplyAsync(() -> {
			String fileName = fileNameParam;

			// check id
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound("Dataset not found.");
			}

			Project p = ds.getProject();
			p.refresh();

			Participant participant = null;
			String participantId = request.headers().get("participant_id").orElse("");
			if (participantId == null || participantId.isEmpty()) {
				return notFound();
			}

			Optional<Participant> participantOpt = Participant.findByRefId(participantId);
			if (participantOpt.isEmpty() || !p.hasParticipant(participantOpt.get())) {
				return notFound();
			}

			final MediaDS meds = (MediaDS) datasetConnector.getDatasetDS(ds);
			Optional<File> requestedFile = meds.getLatestFileVersionForParticipant(participant, fileName);
			if (!requestedFile.isPresent() || !requestedFile.get().exists()) {
				return notFound("No file found: " + fileName);
			}

			LabNotesEntry.log(MediaDSController.class, LabNotesEntryType.DOWNLOAD,
					"Dataset downloaded: " + ds.getName(), ds.getProject());

			// remove all non-printable, non-ASCII characters
			fileName = fileName.replaceAll("\\P{Print}", "%");
			fileName = fileName.replaceAll("\\s", "%");
			return ok(requestedFile.get()).as("application/x-download").withHeader("Content-disposition",
					"attachment; filename=" + fileName);
		});

	}

	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> addItemApi(Request request, Long id) {
		return addItem(request, id);
	}

	/**
	 * Dataset API file upload ("add item")
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> addItem(Request request, Long id) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound("Dataset not found.");
			} else if (!ds.canAppend()) {
				return forbidden("Dataset is not accessible.");
			}

			Project p = ds.getProject();
			p.refresh();

			try {
				Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
				if (body == null) {
					return badRequest("Body malformed.");
				}

				DynamicForm df = formFactory.form().bindFromRequest(request);
				if (df == null) {
					return badRequest("Form malformed.");
				}

				long pid = -1;
				Participant participant = Participant.EMPTY_PARTICIPANT;
				String participantId = df.get("participant_id");
				if (participantId != null && participantId.length() > 0) {
					pid = DataUtils.parseLong(participantId);
					participant = Participant.find.byId(pid);
					if (participant == null || !p.hasParticipant(participant)) {
						participant = Participant.EMPTY_PARTICIPANT;
					}
				}

				List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
				if (!fileParts.isEmpty()) {
					final MediaDS cpds = datasetConnector.getTypedDatasetDS(ds);

					File theFolder = cpds.getFolder();
					if (!theFolder.exists()) {
						theFolder.mkdirs();
					}

					for (int i = 0; i < fileParts.size(); i++) {
						FilePart<TemporaryFile> filePart = fileParts.get(i);
						TemporaryFile tempfile = filePart.getRef();
						String fileName = filePart.getFilename();
						String fileType = filePart.getContentType();
						String timestamp = df.get(fileName);

						// restrict file type
						if (!FileTypeUtils.looksLikeImageFile(fileName)) {
							continue;
						}

						// content-based validation
						if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.IMAGE)) {
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
						Optional<String> storeFile = cpds.storeFile(tempfile.path().toFile(), fileName);
						if (storeFile.isPresent()) {
							long ts = DataUtils.parseLong(timestamp);
							String description = nss(df.get("description"));
							cpds.addRecord(participant, storeFile.get(), description, now, "imported fully");
							cpds.importFileContents(participant,
									routes.MediaDSController.image(id, storeFile.get()).absoluteURL(request,
											environment.isProd()),
									fileType, description, ts != -1 ? new Date(ts) : now);
						}
					}

					LabNotesEntry.log(MediaDSController.class, LabNotesEntryType.DATA,
							"Files uploaded to dataset: " + ds.getName(), ds.getProject());
				}
			} catch (Exception e) {
				// do nothing
			}

			return ok("").as("application/json");
		}).exceptionally(e -> {
			logger.error("Media dataset index problem", e);
			return badRequest();
		});
	}

	@Authenticated(DatasetApiAuth.class)
	public CompletionStage<Result> updateItemApi(Request request, Long id, Long itemId) {
		return internalUpdateItem(request, id, itemId, null);
	}

	/**
	 * Dataset API file update ("update item")
	 * 
	 * @param request
	 * @param id
	 * @param itemId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public CompletionStage<Result> updateItem(Request request, Long id, Long itemId) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));
		return internalUpdateItem(request, id, itemId, username);
	}

	/**
	 * Internal file update ("update item")
	 * 
	 * @param request
	 * @param id
	 * @param itemId
	 * @param username
	 * @return
	 */
	private CompletionStage<Result> internalUpdateItem(Request request, Long id, Long itemId, String username) {
		return CompletableFuture.supplyAsync(() -> {
			Dataset ds = Dataset.find.byId(id);
			if (ds == null) {
				return notFound("Dataset not found.");
			}

			// check access
			if (username != null) {
				if (!ds.editableBy(username)) {
					return forbidden("You don't have permissions for this action.");
				}
			} else {
				if (!ds.canAppend()) {
					return forbidden("Dataset is not accessible.");
				}
			}

			final MediaDS mds = datasetConnector.getTypedDatasetDS(ds);
			Optional<TimedMedia> item = mds.getItem(itemId);
			if (item.isEmpty()) {
				return notFound("Item not found.");
			}

			Project p = ds.getProject();
			p.refresh();

			try {
				Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
				DynamicForm df = formFactory.form().bindFromRequest(request);

				String description = item.get().caption;
				if (df != null && df.get("description") != null) {
					description = nss(df.get("description"));
				}

				// Get participant from form
				Participant participant = item.get().participant;
				if (df != null && df.get("participant_id") != null) {
					long pid = DataUtils.parseLong(df.get("participant_id"));
					Participant newParticipant = Participant.find.byId(pid);
					if (newParticipant != null && p.hasParticipant(newParticipant)) {
						participant = newParticipant;
					} else if (pid == -1) {
						participant = Participant.EMPTY_PARTICIPANT;
					}
				}

				String fileName = item.get().link;
				String link = routes.MediaDSController.image(id, fileName).absoluteURL(request, environment.isProd());

				if (body != null) {
					List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
					if (!fileParts.isEmpty()) {
						// we only take the first file part for update
						FilePart<TemporaryFile> filePart = fileParts.get(0);
						TemporaryFile tempfile = filePart.getRef();
						String oldFileName = item.get().link;

						// restrict file type
						if (FileTypeUtils.looksLikeImageFile(filePart.getFilename())
								&& FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.IMAGE)) {

							// New filename logic: timestamp + original filename
							fileName = System.currentTimeMillis() + "_" + filePart.getFilename();
							link = routes.MediaDSController.image(id, fileName).absoluteURL(request,
									environment.isProd());

							// store file with NEW name
							mds.storeFile(tempfile.path().toFile(), fileName);

							// clear thumbnails and delete OLD file
							Optional<File> originalFile = mds.getFile(oldFileName);
							if (originalFile.isPresent()) {
								imageUtil.clearThumbnails(originalFile.get());
								originalFile.get().delete();
							}
						}
					}
				}

				// update record in DB
				mds.updateItemRecord(itemId, participant, fileName, link, description);

				LabNotesEntry.log(MediaDSController.class, LabNotesEntryType.DATA,
						"File updated in dataset: " + ds.getName(), ds.getProject());

			} catch (Exception e) {
				logger.error("Media dataset update problem", e);
				return badRequest();
			}

			return ok("").as("application/json");
		}).exceptionally(e -> {
			logger.error("Media dataset update problem", e);
			return badRequest();
		});
	}

	@Authenticated(UserAuth.class)
	public Result test(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		final Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound("not found");
		}

		if (!ds.editableBy(username)) {
			return forbidden("you don't have access");
		}
		return ok(views.html.datasets.media.test.render(ds, request));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	public Result uploadFile(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		final Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		} else if (!ds.canAppend()) {
			return redirect(HOME).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You need to be either project owner or collaborator to perform this action.");
		}

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
				pid = DataUtils.parseLong(participantId);
				participant = Participant.find.byId(pid);
				if (participant == null || !p.hasParticipant(participant)) {
					// don't add if participant is empty or does not belong to this project
					return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
							"error", "No valid participant Id provided.");
				}
			}

			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
			if (!fileParts.isEmpty()) {
				final MediaDS cpds = (MediaDS) datasetConnector.getDatasetDS(ds);

				File theFolder = cpds.getFolder();
				if (!theFolder.exists()) {
					theFolder.mkdirs();
				}

				for (int i = 0; i < fileParts.size(); i++) {
					FilePart<TemporaryFile> filePart = fileParts.get(i);
					TemporaryFile tempfile = filePart.getRef();
					String fileName = filePart.getFilename();
					String fileType = filePart.getContentType();
					String timestamp = df.get(fileName);

					// restrict file type
					if (!FileTypeUtils.looksLikeImageFile(fileName)) {
						continue;
					}

					// content-based validation
					if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.IMAGE)) {
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
					Optional<String> storeFile = cpds.storeFile(tempfile.path().toFile(), fileName);
					if (storeFile.isPresent()) {
						long ts = DataUtils.parseLong(timestamp);
						String description = nss(df.get("description"));
						cpds.addRecord(participant, storeFile.get(), description, now, "imported fully");
						cpds.importFileContents(participant,
								routes.MediaDSController.image(id, storeFile.get()).absoluteURL(request,
										environment.isProd()),
								fileType, description, ts != -1 ? new Date(ts) : new Date());
					}
				}

				LabNotesEntry.log(MediaDSController.class, LabNotesEntryType.DATA,
						"Files uploaded to dataset: " + ds.getName(), ds.getProject());
			}

			return redirect(controllers.routes.DatasetsController.view(ds.getId()));
		} catch (NullPointerException e) {
			logger.error("Error uploading file to dataset.", e);
			Slack.call("Exception", e.getLocalizedMessage());
		}

		return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
				"Invalid request, no files have been included in the request.");
	}

	@Authenticated(UserAuth.class)
	public Result downloadFile(Request request, long id, String fileName) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project project = ds.getProject();
		project.refresh();
		if (!project.visibleFor(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project is not accessible.");
		}

		if (acceptLicenseFirst(request, username, project)) {
			// redirect to accept license first
			return redirect(controllers.routes.ProjectsController.license(project.getId(),
					routes.MediaDSController.downloadFile(id, fileName).relativeTo("/")));
		}

		// compose file path and check existence
		final MediaDS meds = (MediaDS) datasetConnector.getDatasetDS(ds);
		Optional<File> requestedFile = meds.getFile(fileName);
		if (!requestedFile.isPresent()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"No file found: " + fileName);
		}

		LabNotesEntry.log(MediaDSController.class, LabNotesEntryType.DOWNLOAD, "Dataset downloaded: " + ds.getName(),
				ds.getProject());

		// remove all non-printable, non-ASCII characters
		fileName = fileName.replaceAll("\\P{Print}", "%");
		fileName = fileName.replaceAll("\\s", "%");
		return ok(requestedFile.get()).as("application/x-download").withHeader("Content-disposition",
				"attachment; filename=" + fileName);
	}

	public CompletionStage<Result> image(Request request, long id, String fileName) {
		return scaledImage(request, id, fileName, "");
	}

	public CompletionStage<Result> scaledImage(Request request, long id, String fileName, String scale) {

		// high-load possibility, wrap in CompletableFuture
		return CompletableFuture.supplyAsync(() -> {

			// retrieve potentially logged in user
			Optional<String> usernameOpt = getAuthenticatedUserName(request);
			// if no logged in user, go for participant route, otherwise redirect to authenticatedImage method which
			// resolves the image for an authenticated user
			if (!usernameOpt.isPresent()) {
				// retrieve invite_token from session
				String invite_token = session(request, ParticipantAuth.PARTICIPANT_ID);
				if (invite_token != null && invite_token.length() > 0) {

					// retrieve participant id
					final long participant_id = tokenResolverUtil.getParticipantIdFromParticipationToken(invite_token);
					if (participant_id < 0) {
						return badRequest();
					}

					// find dataset
					Dataset ds = Dataset.find.byId(id);
					if (ds == null) {
						return notFound();
					}

					// check dataset project against participant project
					long project_id = tokenResolverUtil.getProjectIdFromParticipationToken(invite_token);
					if (project_id != ds.getProject().getId()) {
						return notFound();
					}

					// check whether participant is part of the dataset's project
					if (ds.getProject().getParticipants().stream().noneMatch(p -> p.getId() == participant_id)) {
						// participant not part of project
						return notFound();
					}

					// compose file path and check existence
					if (participant_id > -1l) {
						final MediaDS mds = (MediaDS) datasetConnector.getDatasetDS(ds);
						long pid = mds.checkParticipantByFilename(fileName);
						if (participant_id == pid) {
							Optional<File> requestedFile = mds.getFile(fileName);
							if (requestedFile.isPresent()) {
								// response().setHeader("Content-disposition", "attachment; filename=" + fileName);
								return deliverScaledFile(request, requestedFile.get(), scale);
							} else {
								return notFound();
							}
						} else {
							return forbidden();
						}
					} else {
						return authenticatedImage(request, id, fileName, scale);
					}
				} else {
					return authenticatedImage(request, id, fileName, scale);
				}
			} else {
				return authenticatedImage(request, id, fileName, scale);
			}
		});
	}

	@Authenticated(UserAuth.class)
	private Result authenticatedImage(Request request, long id, String fileName, String scale) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		// check whether file is accessible either to registered user or in general because the project is public
		if (!ds.visibleFor(username) && !datasetConnector.checkLibrarianAccess(username)) {
			return redirect(HOME).addingToSession(request, "error", "Project is not accessible.");
		}

		// if (acceptLicenseFirst(username, project)) {
		// // redirect to accept license first
		// return redirect(controllers.routes.ProjectsController.license(project.id,
		// routes.MediaDSController.image(id, fileName).relativeTo("/")));
		// }

		// compose file path and check existence
		final MediaDS cpds = (MediaDS) datasetConnector.getDatasetDS(ds);
		Optional<File> requestedFile = cpds.getFile(fileName);
		if (requestedFile.isPresent()) {
			File originalImageFile = requestedFile.get();
			return deliverScaledFile(request, originalImageFile, scale);
		}

		return notFound();
	}

	/**
	 * handle the file delivery for scaled files
	 * 
	 * @param request
	 * @param originalImageFile
	 * @param scale
	 * @return
	 */
	private Result deliverScaledFile(Request request, File originalImageFile, String scale) {
		String acceptHeader = request.header(ACCEPT).orElseGet(() -> "");
		if (acceptHeader.contains("image/avif")) {
			return RangeResults.ofFile(request, imageUtil.deliverScaledFile(originalImageFile, scale, "avif"))
					.withHeader(CACHE_CONTROL, "public, max-age=31536000");
		} else {
			return RangeResults.ofFile(request, imageUtil.deliverScaledFile(originalImageFile, scale, "png"))
					.withHeader(CACHE_CONTROL, "public, max-age=31536000");
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public CompletionStage<Result> downloadExternal(Dataset ds, Function<String, String> linkMapper, Cluster cluster,
			long limit, long start, long end) {
		// get participant ids (if cluster is given)
		final List<Long> participantIds = cluster.getParticipantList();
		return CompletableFuture.supplyAsync(() -> internalExport(ds, linkMapper, participantIds, limit, start, end))
				.thenApplyAsync(chunks -> ok().chunked(chunks)
						.withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
						.as("text/csv"));
	}

	public CompletionStage<Result> downloadInternal(Dataset ds, Cluster cluster, long limit, long start, long end) {
		// get participant ids (if cluster is given)
		final List<Long> participantIds = cluster.getParticipantList();
		return CompletableFuture
				.supplyAsync(() -> internalExport(ds, Function.identity(), participantIds, limit, start, end))
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
	private Source<ByteString, ?> internalExport(Dataset ds, Function<String, String> linkMapper,
			List<Long> participantIds, long limit, long start, long end) {
		MediaDS meds = (MediaDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> meds.export(sourceActor, linkMapper, participantIds, limit, start, end));
			return sourceActor;
		});
	}

}

package controllers.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import io.jsonwebtoken.lang.Strings;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.ds.TimeseriesDS;
import models.vm.TimedMedia;
import play.Logger;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Files;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Http.MultipartFormData.FilePart;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import services.slack.Slack;
import utils.CSVLineParserUtil;
import utils.DataUtils;
import utils.StringUtils;
import utils.components.OnboardingSupport;
import utils.validators.FileTypeUtils;

public class CompleteDSController extends AbstractDSController {

	public static final String CACHE_FILES = "CP_DS_FILES_";

	private final ActorSystem actorSystem;
	private static final Logger.ALogger logger = Logger.of(CompleteDSController.class);

	@Inject
	public CompleteDSController(FormFactory formFactory, SyncCacheApi cache, DatasetConnector datasetConnector,
			OnboardingSupport onboardingSupport, ActorSystem actorSystem) {
		super(formFactory, cache, datasetConnector, onboardingSupport);

		this.actorSystem = actorSystem;
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result view(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "We could not find this dataset.");
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		final List<TimedMedia> fileList = cache.getOrElseUpdate(CACHE_FILES + id, () -> cpds.getFiles(), 30);

		if (!ds.visibleFor(username)) {
			return redirect(controllers.routes.ProjectsController.view(ds.getProject().getId()));
		}

		return ok(views.html.datasets.complete.view.render(ds, username, fileList, csrfToken(request), request));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result add(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"Project not valid or you don't have permissions for this action. Need to be the owner or "
							+ "a collaborator of the project.");
		}

		return ok(views.html.datasets.complete.add.render(csrfToken(request), p));
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result addMe(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Project p = Project.find.byId(id);
		if (p == null || !p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"Project not valid or you don't have permissions for this action. Need to be the owner or "
							+ "a collaborator of the project.");
		}

		DynamicForm df = formFactory.form().bindFromRequest(request);
		if (df == null) {
			return redirect(HOME).addingToSession(request, "error", "Expecting some data");
		}

		Dataset ds = datasetConnector.create(df.get("dataset_name"), DatasetType.COMPLETE, p, df.get("description"),
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

		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You don't have permissions for this action. Need to be the owner or a collaborator of the "
							+ "project.");
		}

		return ok(views.html.datasets.complete.edit.render(csrfToken(request), ds));
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
					"You don't have permissions for this action. Need to be the owner or a collaborator of the "
							+ "project.");
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

		LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.MODIFY, "Dataset edited: " + ds.getName(),
				ds.getProject());
		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result newFile(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Optional<String> filenameOpt = request.header("HX-Prompt");
		if (filenameOpt.isEmpty()) {
			return redirect(controllers.routes.DatasetsController.view(id)).addingToSession(request, "error",
					"Name of new file is not given.");
		}

		Project project = ds.getProject();
		if (!project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You need to be either project owner or collaborator to perform this action.");
		}

		// restrict file type
		String fileName = filenameOpt.get();
		if (FileTypeUtils.looksLikeExecutableFile(fileName)) {
			logger.error("Executable file upload attempt blocked");
		}

		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

		// store file, add record
		File dummyTempFile = Files.singletonTemporaryFileCreator().create("empty_file", "txt").path().toFile();
		Optional<String> storeFile = cpds.storeFile(dummyTempFile, fileName);
		if (storeFile.isPresent()) {
			cpds.addRecord(storeFile.get(), "", new Date());
		}

		// invalidate cache
		cache.remove(CACHE_FILES + id);

		return redirect(controllers.routes.DatasetsController.view(ds.getId())).withHeader("HX-Refresh", "true");
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result uploadFile(Request request, Long id) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "Dataset is not available.");
		} else if (!ds.canAppend()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Dataset is closed (adjust start and end dates to open).");
		}

		Project project = ds.getProject();
		if (!project.editableBy(username)) {
			return redirect(HOME).addingToSession(request, "error",
					"You need to be either project owner or collaborator to perform this action.");
		}

		// invalidate cache
		cache.remove(CACHE_FILES + id);

		try {
			Http.MultipartFormData<TemporaryFile> body = request.body().asMultipartFormData();
			if (body == null) {
				return redirect(HOME).addingToSession(request, "error", "Bad request");
			}

			DynamicForm df = formFactory.form().bindFromRequest(request);
			if (df == null) {
				return redirect(HOME).addingToSession(request, "error", "Expecting some data");
			}

			List<Http.MultipartFormData.FilePart<TemporaryFile>> fileParts = body.getFiles();
			if (!fileParts.isEmpty()) {
				final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);

				for (int i = 0; i < fileParts.size(); i++) {
					FilePart<TemporaryFile> filePart = fileParts.get(i);
					TemporaryFile tempfile = filePart.getRef();
					String fileName = nss(filePart.getFilename());

					// filename-based quick check
					if (FileTypeUtils.looksLikeExecutableFile(fileName)) {
						logger.error("Participant upload rejected due to executable-like filename: " + fileName);
						continue;
					}

					// content-based validation
					if (!FileTypeUtils.validateAndLog(filePart, FileTypeUtils.FileCategory.ANY)) {
						continue;
					}

					// store file, add record
					Optional<String> storeFile = cpds.storeFile(tempfile.path().toFile(), fileName);
					if (storeFile.isPresent()) {
						cpds.addRecord(storeFile.get(), nss(df.get("description")), new Date());
					}
				}

				LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.DATA,
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
	public Result delete(Request request, Long id, Long fileId) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project p = ds.getProject();
		p.refresh();
		// only the owner can delete
		if (!p.belongsTo(username)) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Only the project owner can delete files.");
		}

		// remove the file
		final CompleteDS cpds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> requestedFileOpt = cpds.getFile(fileId);
		if (requestedFileOpt.isPresent()) {
			File requestedFile = requestedFileOpt.get();
			if (requestedFile.exists()) {
				requestedFile.delete();
				cpds.deleteRecord(fileId);
				LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.DELETE,
						"Files deleted from dataset: " + ds.getName(), ds.getProject());
			}
		}

		// invalidate cache
		cache.remove(CACHE_FILES + id);

		return redirect(controllers.routes.DatasetsController.view(ds.getId()));
	}

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result importFile(Request request, long id, Long fileId, String fileName) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME);
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project p = ds.getProject();
		p.refresh();
		// only the owner can delete
		if (!p.editableBy(username)) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Only the project owner or collaborators can import files.");
		}

		// remove the file
		final CompleteDS cpds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> requestedFileOpt = cpds.getFile(fileId);
		if (requestedFileOpt.isPresent()) {
			File requestedFile = requestedFileOpt.get();
			if (requestedFile.exists()) {

				// read first line of file for CSV header and separator heuristics
				char separator = ',';
				try (FileReader fileReader = new FileReader(requestedFile);
						BufferedReader br = new BufferedReader(fileReader);) {
					String header = br.readLine();
					int commas = Strings.countOccurrencesOf(header, ",");
					int semics = Strings.countOccurrencesOf(header, ";");
					int tabs = Strings.countOccurrencesOf(header, "\t");
					int max = Math.max(Math.max(commas, semics), tabs);

					if (max == commas) {
						separator = ',';
					} else if (max == semics) {
						separator = ';';
					} else {
						separator = '\t';
					}
				} catch (IOException e) {
					// do nothing, probably abort
				}

				// read again for the column names in the header with initialized parser
				CSVParser parser = new CSVParserBuilder().withSeparator(separator).withIgnoreLeadingWhiteSpace(true)
						.build();
				String[] header = {};
				try (FileReader fileReader = new FileReader(requestedFile);
						CSVReader reader = new CSVReaderBuilder(fileReader).withCSVParser(parser).build();) {
					try {
						header = reader.readNext();
					} catch (CsvValidationException e) {
						logger.error("Error importing CSV file.", e);
					}
				} catch (IOException e) {
					logger.error("Error reading file to import it.", e);
				}

				// clean up the header
				for (int i = 0; i < header.length; i++) {
					String headerItem = header[i];
					header[i] = headerItem.trim();
				}

				// read iot datasets for choice
				Person user = Person.findByEmail(username).get();
				List<Dataset> datasets = user.projects().stream().flatMap(prj -> prj.getIoTDatasets().stream())
						.collect(Collectors.toList());

				// render choices
				return ok(views.html.datasets.complete.importFile.render(csrfToken(request), ds, fileId, fileName,
						header, datasets, request));
			}
		}

		return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
				"File was not found.");
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result importFileMe(Request request, long id, Long fileId) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound(Json.newObject().put("error", "Dataset not found."));
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project p = ds.getProject();
		p.refresh();
		// only the owner can delete
		if (!p.editableBy(username)) {
			return forbidden(
					Json.newObject().put("error", "Only the project owner or collaborators can import files."));
		}

		// check the file
		int totalLines = 0;
		final CompleteDS cpds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> requestedFileOpt = cpds.getFile(fileId);
		if (requestedFileOpt.isPresent()) {
			File requestedFile = requestedFileOpt.get();
			if (requestedFile.exists()) {

				// check the spec
				DynamicForm df = formFactory.form().bindFromRequest(request);
				if (df == null) {
					return badRequest(Json.newObject().put("error", "Expecting some data."));
				}

				String datasetId = nss(df.get("import_dataset"));
				if (datasetId.isEmpty()) {
					return badRequest(Json.newObject().put("error", "Expecting some dataset selection."));
				}

				final Long did = DataUtils.parseLong(datasetId);
				if (did == -1L) {
					return badRequest(Json.newObject().put("error", "Expecting some dataset selection."));
				}

				final Dataset destinationDS = Dataset.find.byId(did);
				if (!destinationDS.getProject().getId().equals(p.getId())) {
					return forbidden(Json.newObject().put("error", "No access rights for destination dataset."));
				}
				final TimeseriesDS tds = (TimeseriesDS) datasetConnector.getDatasetDS(destinationDS);

				// read first line of file for CSV header and separator heuristics
				char separator = ',';
				try (FileReader fileReader = new FileReader(requestedFile);
						BufferedReader br = new BufferedReader(fileReader);) {
					String header = br.readLine();
					int commas = Strings.countOccurrencesOf(header, ",");
					int semics = Strings.countOccurrencesOf(header, ";");
					int tabs = Strings.countOccurrencesOf(header, "\t");
					int max = Math.max(Math.max(commas, semics), tabs);

					if (max == commas) {
						separator = ',';
					} else if (max == semics) {
						separator = ';';
					} else {
						separator = '\t';
					}

					// now count all the rows in this file
					while (br.readLine() != null) {
						totalLines++;
					}
				} catch (IOException e) {
					// do nothing, probably abort
				}

				final String uuid = UUID.randomUUID().toString();
				final Character sep = separator;

				final CSVLineParserUtil clpu = new CSVLineParserUtil();

				// add pattern if provided
				if (!nss(df.get("timestamp_pattern")).isEmpty()) {
					clpu.setTimestampPattern(nss(df.get("timestamp_pattern")));
				}

				// check the time format with the first data row
				{
					CSVParser parser = new CSVParserBuilder().withSeparator(sep).withIgnoreLeadingWhiteSpace(true)
							.build();
					try (FileReader fileReader = new FileReader(requestedFile);
							CSVReader reader = new CSVReaderBuilder(fileReader).withSkipLines(0).withCSVParser(parser)
									.build();) {
						final String[] headerSplit = reader.readNext();
						if (headerSplit != null) {
							final List<String> headers = Arrays.stream(headerSplit).map(h -> h.trim())
									.collect(Collectors.toList());

							// process manual timestamp column
							if (!nss(df.get("timestamp_column")).isEmpty()) {
								int i = headers.indexOf(nss(df.get("timestamp_column")));
								clpu.putIndex("timestamp", i);
							}

							// header-index assignment
							for (int i = 0; i < headers.size(); i++) {
								String col = headers.get(i);
								if (col.equalsIgnoreCase("ts") || col.equalsIgnoreCase("timestamp")
										|| col.equalsIgnoreCase("date") || col.equalsIgnoreCase("datetime")) {
									clpu.putIndex("timestamp", i);
								} else if (col.equalsIgnoreCase("time")) {
									clpu.putIndex("timestamp", i);
								} else if (col.equalsIgnoreCase("activity") || col.equalsIgnoreCase("event")) {
									clpu.putIndex("activity", i);
								} else if (col.equalsIgnoreCase("id") && col.contains("articipant")) {
									clpu.putIndex("resource_id", i);
								} else if (col.equalsIgnoreCase("id") && col.contains("evice")) {
									clpu.putIndex("resource_id", i);
								} else if (col.equalsIgnoreCase("id")) {
									clpu.putIndex("resource_id", i);
								} else if (col.equalsIgnoreCase("pp1")) {
									clpu.putIndex("pp1", i);
								} else if (col.equalsIgnoreCase("pp2")) {
									clpu.putIndex("pp2", i);
								} else if (col.equalsIgnoreCase("pp3")) {
									clpu.putIndex("pp3", i);
								} else {
									clpu.putIndex(col, i);
									clpu.putDataColumn(col);
								}
							}
						}

						// check timestamps
						String[] line = reader.readNext();
						if (line != null) {
							// extract and parse timestamp
							String timestamp = clpu.extract(line, "timestamp");
							try {
								// simply check if the timestamp can be parsed
								if (clpu.parseTimestamp(timestamp, null) == null) {
									return badRequest(Json.newObject().put("error", "Timestamp could not be parsed."));
								}
							} catch (Exception e) {
								return badRequest(Json.newObject().put("error", "Timestamp could not be parsed."));
							}
						}
					} catch (Exception e) {
						logger.error("Error importing file to dataset.", e);
					}
				}

				final float totalCount = totalLines;
				this.actorSystem.dispatcher().execute(() -> {
					// read again for the column names in the header with initialized parser
					CSVParser parser = new CSVParserBuilder().withSeparator(sep).withIgnoreLeadingWhiteSpace(true)
							.build();
					try (FileReader fileReader = new FileReader(requestedFile);
							CSVReader reader = new CSVReaderBuilder(fileReader).withSkipLines(1).withCSVParser(parser)
									.build();) {
						int counter = 0;
						while (counter++ < totalCount) {
							try {
								String[] line = reader.readNext();
								if (line == null) {
									break;
								}

								// fetch the data items from the array
								String device_id = clpu.extract(line, "id");

								// parse date and time
								String timestamp = clpu.extract(line, "timestamp");
								final Date ts = clpu.parseTimestamp(timestamp, new Date());

								// parse other attributes
								String activity = clpu.extract(line, "activity");
								String pp1 = clpu.extract(line, "pp1");
								String pp2 = clpu.extract(line, "pp2");
								String pp3 = clpu.extract(line, "pp3");

								// construct JSON Object of all remaining data
								ObjectNode data = Json.newObject();
								for (String col : clpu.headerData) {
									data.put(col, clpu.extract(line, col));
								}

								// import line with the given mapping
								tds.internalAddRecord(device_id, pp1, pp2, pp3, ts, activity, data);

								// cache progress
								this.cache.set("import_file_progress_" + uuid,
										Math.round((100 * counter) / totalCount + 1), 3600);

							} catch (CsvValidationException e) {
								// do nothing, move to next line
							} catch (Exception e) {
								// do nothing, move to next line
							}
						}
					} catch (IOException e) {
						logger.error("Error importing file to dataset.", e);
					}

				});

				// update projection
				tds.updateProjection(clpu.headerData);

				return ok(Json.newObject().put("uuid", uuid));
			}
		}

		return ok(Json.newObject().put("error", "error"));
	}

	@Authenticated(UserAuth.class)
	public Result importStatus(String uuid) {
		return ok(this.cache.get("import_file_progress_" + uuid).orElse("") + "");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Authenticated(UserAuth.class)
	@AddCSRFToken
	public Result editFile(Request request, long id, long fileId) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirect(HOME).addingToSession(request, "error", "The dataset was not found.");
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project p = ds.getProject();
		p.refresh();
		// only the owner can edit
		if (!p.editableBy(username)) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"Only the project owner or collaborators can edit files.");
		}

		// acquire file
		final CompleteDS cpds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> requestedFileOpt = cpds.getFile(fileId);
		if (!requestedFileOpt.isPresent()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"File was not found.");
		}

		File requestedFile = requestedFileOpt.get();
		if (!requestedFile.exists()) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "error",
					"File was not found.");
		}

		// check file name to support extension selection
		String fileName = requestedFile.getName();

		// file type for syntax highlighting (if available)
		String comps[] = fileName.split("\\.");
		String fileType = comps[comps.length - 1];

		try {
			String fileContents = org.apache.commons.io.FileUtils.readFileToString(requestedFile,
					Charset.defaultCharset());

			if (fileType.equalsIgnoreCase("gg")) {
				// render notebook editor
				return ok(views.html.tools.starboard.editNotebook.render(csrfToken(request), ds, fileId, fileName,
						fileType, fileContents, request));
			} else if (fileType.equalsIgnoreCase("twee") || fileType.equalsIgnoreCase("tw")) {
				// redirect to Twine editor
				return redirect(controllers.tools.routes.NarrativeSurveysController.loadTwee(id, fileId));
			} else {
				final List<TimedMedia> fileList = cache.getOrElseUpdate(CACHE_FILES + id, () -> cpds.getFiles(), 30)
						.stream().filter(tl -> FileTypeUtils.lookLikeTextFile(tl.link)).collect(Collectors.toList());

				// edit json files as js
				if (fileType.equals("json")) {
					fileType = "js";
				}
				// render default text editor
				return ok(views.html.datasets.complete.editFile.render(ds, fileId, fileName, fileType, fileContents,
						fileList, csrfToken(request)));
			}
		} catch (Exception e) {
			return redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request, "message",
					"File contents could not be read.");
		}
	}

	@Authenticated(UserAuth.class)
	@RequireCSRFCheck
	public Result saveFile(Request request, long id, long fileId) {
		String username = getAuthenticatedUserNameOrReturn(request, forbidden("You are not logged in."));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return notFound("The dataset was not found.");
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project p = ds.getProject();
		p.refresh();
		if (!p.editableBy(username)) {
			return forbidden("Only the project owner or collaborators can edit files.");
		}

		String content = nss(request.body().asText());
		// unscramble the contents if necessary
		content = StringUtils.unscrambleTransport(content);

		// save the file
		final CompleteDS cpds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> requestedFileOpt = cpds.getFile(fileId);
		if (!requestedFileOpt.isPresent()) {
			return notFound("File was not found.");
		}

		File requestedFile = requestedFileOpt.get();
		if (!requestedFile.exists()) {
			return notFound("File was not found.");
		}

		try {
			// save file contents
			org.apache.commons.io.FileUtils.write(requestedFile, content, Charset.defaultCharset());
			return ok();
		} catch (Exception e) {
			return badRequest("Problem saving the file.");
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * download file directly as authorized user
	 * 
	 * @param id
	 * @param fileId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadFile(Request request, long id, long fileId) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project project = ds.getProject();
		project.refresh();
		if (!project.visibleFor(username) && !datasetConnector.checkLibrarianAccess(username)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "Project is not accessible."));
		}

		if (acceptLicenseFirst(request, username, project) && !datasetConnector.checkLibrarianAccess(username)) {
			// redirect to accept license first
			return redirectCS(controllers.routes.ProjectsController.license(project.getId(),
					routes.CompleteDSController.downloadFile(id, fileId).relativeTo("/")));
		}

		// compose file path and check existence
		final CompleteDS cpds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> requestedFileOpt = cpds.getFile(fileId);
		if (!requestedFileOpt.isPresent()) {
			return cs(() -> redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
					"error", "No file found: " + fileId));
		}

		File requestedFile = requestedFileOpt.get();
		if (!requestedFile.exists()) {
			return cs(() -> redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
					"error", "No file found: " + fileId));
		}

		LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.DOWNLOAD, "Dataset downloaded: " + ds.getName(),
				ds.getProject());

		return cs(() -> ok(requestedFile).as("application/x-download"));
	}

	/**
	 * download file directly as authorized user
	 * 
	 * @param id
	 * @param fileId
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> downloadLatestFile(Request request, long id, String fileName) {
		String username = getAuthenticatedUserNameOrReturn(request, redirect(LANDING));

		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "We could not find this dataset."));
		}

		// check whether file is accessible either to registered user or in general because the project is public
		Project project = ds.getProject();
		project.refresh();
		if (!project.visibleFor(username)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "Project is not accessible."));
		}

		if (acceptLicenseFirst(request, username, project)) {
			// redirect to accept license first
			return redirectCS(controllers.routes.ProjectsController.license(project.getId(),
					routes.CompleteDSController.downloadLatestFile(id, fileName).relativeTo("/")));
		}

		// compose file path and check existence
		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		Optional<File> requestedFileOpt = cpds.getFile(fileName);
		if (!requestedFileOpt.isPresent()) {
			return cs(() -> redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
					"error", "No file found: " + fileName));
		}

		File requestedFile = requestedFileOpt.get();
		if (!requestedFile.exists()) {
			return cs(() -> redirect(controllers.routes.DatasetsController.view(ds.getId())).addingToSession(request,
					"error", "No file found: " + fileName));
		}

		LabNotesEntry.log(CompleteDSController.class, LabNotesEntryType.DOWNLOAD, "Dataset downloaded: " + ds.getName(),
				ds.getProject());

		return cs(() -> ok(requestedFile).as("application/x-download"));
	}

	/**
	 * download file with dataset id, file name, and access_token
	 * 
	 * @param fileName
	 * @param access_token
	 * @return
	 */
	public CompletionStage<Result> downloadFilePublic(String fileName, String token) {

		// retrieve id from token
		Long id = tokenResolverUtil.getDatasetIdFromToken(token);

		// check id
		Dataset ds = Dataset.find.byId(id);
		if (ds == null) {
			return redirectCS(HOME);
		}

		// check if the token is current
		String pat = ds.getConfiguration().get(Dataset.PUBLIC_ACCESS_TOKEN);
		if (!token.equals(pat)) {
			return redirectCS(HOME);
		}

		// compose file path and check existence
		final CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		Optional<File> requestedFile = cpds.getFile(fileName);
		if (requestedFile.isPresent()) {
			return okCS(requestedFile.get());
		}

		return redirectCS(controllers.routes.DatasetsController.view(id));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get file list in CSV or download directly if there is only one file; this will deliver a CSV file with authorized
	 * links inside to download the different files in this dataset (as an authorized user)
	 * 
	 * @param id
	 * @param end
	 * @param start
	 */
	public CompletionStage<Result> downloadExternal(Function<String, String> linkMapper, Dataset ds, long limit,
			long start, long end) {

		return CompletableFuture.supplyAsync(() -> internalExport(ds, linkMapper, limit, start, end))
				.thenApplyAsync(chunks -> ok().chunked(chunks)
						.withHeader(CONTENT_DISPOSITION, "attachment; filename=" + ds.getSlug() + ".csv")
						.as("text/csv"));
	}

	/**
	 * get file list in CSV or download directly if there is only one file; this will deliver a CSV file with authorized
	 * links inside to download the different files in this dataset (as an authorized user)
	 * 
	 * @param id
	 * @param end
	 * @param start
	 */
	public CompletionStage<Result> downloadInternal(Function<String, String> linkMapper, Dataset ds, long limit,
			long start, long end) {
		return CompletableFuture.supplyAsync(() -> internalExport(ds, linkMapper, limit, start, end))
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
	private Source<ByteString, ?> internalExport(Dataset ds, Function<String, String> linkMapper, long limit,
			long start, long end) {
		CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(ds);
		return createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> cpds.export(sourceActor, linkMapper, limit, start, end));
			return sourceActor;
		});
	}

}

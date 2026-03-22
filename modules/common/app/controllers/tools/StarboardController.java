package controllers.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.AbstractAsyncController;
import controllers.api.CompleteDSController;
import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.Person;
import models.Project;
import models.ds.CompleteDS;
import models.vm.TimedMedia;
import play.cache.SyncCacheApi;
import play.data.DynamicForm;
import play.data.FormFactory;
import play.filters.csrf.AddCSRFToken;
import play.filters.csrf.RequireCSRFCheck;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.DataUtils;

public class StarboardController extends AbstractAsyncController {

	@Inject
	FormFactory formFactory;

	@Inject
	DatasetConnector datasetConnector;

	@Inject
	SyncCacheApi cache;

	private static final Pattern URL_PATTERN = Pattern.compile("https?://[^/]+/web/[^/]+/(.+)");
	private static final Pattern PYODIDE_OPEN_URL_PATTERN = Pattern.compile("pyodide\\.open_url\\(([^)]+)\\)");
	private static final Pattern PD_TO_DATETIME_PATTERN = Pattern
			.compile("pd\\.to_datetime\\((\\s*\\w+\\[(\"|')Date\\2\\]\\s*)(?![^)]*format\\s*=)[^)]*\\)");

	@AddCSRFToken
	@Authenticated(UserAuth.class)
	public Result index(Request request) {
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		List<Dataset> allNotebookDatasets = user.getUserNotebookDatasets(datasetConnector);
		List<TimedMedia> allNotebooks = allNotebookDatasets.stream().flatMap(ds -> {
			CompleteDS cds = (CompleteDS) datasetConnector.getDatasetDS(ds);
			return cds.getFiles(Optional.of("^.*[\\.]gg$")).stream().map(tm -> {
				tm.ds_id = ds.getId();
				return tm;
			});
		}).collect(Collectors.toList());

		List<TimedMedia> allCollabNotebooks = user.collaborations().stream().flatMap(p -> {
			p.refresh();
			return p.getCompleteDatasets().stream();
		}).filter(ds -> ds.isWebsite()).flatMap(ds -> {
			CompleteDS cds = (CompleteDS) datasetConnector.getDatasetDS(ds);
			return cds.getFiles(Optional.of("^.*[\\.]gg$")).stream().map(tm -> {
				tm.ds_id = ds.getId();
				return tm;
			});
		}).collect(Collectors.toList());

		String token = csrfToken(request);
		return ok(views.html.tools.starboard.index.render(allNotebookDatasets, allNotebooks, allCollabNotebooks, token,
				request));
	}

	@RequireCSRFCheck
	@Authenticated(UserAuth.class)
	public Result addNotebook(Request request) {
		getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		DynamicForm df = formFactory.form().bindFromRequest(request);
		String datasetId = df.get("dataset");
		long dsId = -1;
		try {
			dsId = DataUtils.parseLong(datasetId);
		} catch (Exception e) {
			return redirect(routes.StarboardController.index()).flashing("error", "Dataset id not valid.");
		}
		String notebookName = df.get("name");
		if (!notebookName.toLowerCase().endsWith(".gg")) {
			notebookName += ".gg";
		}

		Dataset ds = Dataset.find.byId(dsId);
		if (ds == null) {
			return redirect(routes.StarboardController.index()).flashing("error", "Dataset id not found.");
		}

		CompleteDS cds = datasetConnector.getTypedDatasetDS(ds);
		Optional<String> nbNameOpt = cds.createNotebookFile(notebookName, new Date(),
				new String[] { "# %% [javascript]", "let what = {", "	test: true", "}", "", "console.log(what)",
						"# %% [python]", "globalWhat = {}", "", "print(globalWhat)" });
		if (nbNameOpt.isPresent()) {
			cds.addRecord(nbNameOpt.get(), "", new Date());
		}

		// invalidate cache
		cache.remove(CompleteDSController.CACHE_FILES + dsId);

		return redirect(routes.StarboardController.index());
	}

	@Authenticated(UserAuth.class)
	public Result datasets(Request request) {
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(LANDING).addingToSession(request, "error", "Please log in first to use this tool."));

		List<Project> projects = user.getOwnAndCollabProjects();
		projects.stream().forEach(p -> {
			p.refresh();
		});

		return ok(views.html.tools.starboard.comps.datasets.render(projects, request));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Result exportJupyterNotebook(Request request, long datasetId, long fileId) {
		Person user = getAuthenticatedUserOrReturn(request, forbidden("You are not logged in."));

		Dataset ds = Dataset.find.byId(datasetId);
		if (ds == null || !user.canEdit(ds.getProject())) {
			return forbidden("Dataset not found or not accessible.");
		}

		CompleteDS cds = datasetConnector.getTypedDatasetDS(ds);
		Optional<File> requestedFileOpt = cds.getFile(fileId);
		if (!requestedFileOpt.isPresent()) {
			return notFound("File was not found in dataset.");
		}

		File requestedFile = requestedFileOpt.get();
		if (!requestedFile.exists()) {
			return notFound("File was not found in dataset.");
		}

		try {
			// run conversion
			String output = convertStarboardToJupyter(requestedFile);
			return ok(output);
		} catch (IOException e) {
			// conversion issue
			return notFound("Conversion failed, export unsuccessful.");
		}
	}

	/**
	 * Modified convertStarboardToJupyter method
	 * 
	 * @param inputFile
	 * @return
	 * @throws IOException
	 */
	private String convertStarboardToJupyter(File inputFile) throws IOException {
		// Read the Starboard notebook
		List<String> starboardLines = Files.readAllLines(inputFile.toPath());

		// List of blocked patterns
		List<String> BLOCKED_PATTERNS = List.of("import pyodide", "import micropip",
				"await micropip.install('seaborn==0.13.2')", "await micropip.install('scipy')");

		// construct translated data structure
		ArrayNode cells = Json.newArray();
		String cellType = null;
		ArrayNode cell = null;
		for (String l : starboardLines) {
			if (l.startsWith("# %% [")) {
				// store existing cell
				if (cell != null) {
					ObjectNode on = Json.newObject();
					// change cell type from "python" to "code", otherwise just cell type
					if ("python".equals(cellType)) {
						cellType = "code";
						on.set("execution_count", null);
						on.set("outputs", Json.newArray());
					}
					on.put("cell_type", cellType);
					on.set("metadata", Json.newObject());
					on.set("source", cell);
					cells.add(on);
				}

				// new cell
				cell = Json.newArray();
				cellType = l.replace("# %% [", "").replace("]", "");
			} else if (cell != null) {
				if (l.trim().isEmpty()) {
					cell.add("\n");
				} else {
					// check for blocked strings
					final String checkString = l;
					if (BLOCKED_PATTERNS.stream().anyMatch(p -> checkString.contains(p))) {
						// if any match is found for a blocked pattern, we jump to the next iteration of the for loop
						continue;
					}

					// check for hosted web resource
					Matcher urlMatcher = URL_PATTERN.matcher(l);
					if (urlMatcher.find()) {
						String fileName = urlMatcher.group(1);
						l = l.replaceAll(URL_PATTERN.pattern(), "./datasets/" + fileName);
					}

					// check for pyodide.open_url
					Matcher pyodideMatcher = PYODIDE_OPEN_URL_PATTERN.matcher(l);
					if (pyodideMatcher.find()) {
						String fileVar = pyodideMatcher.group(1);
						l = l.replaceAll(PYODIDE_OPEN_URL_PATTERN.pattern(), fileVar);
					}

					// Check for pd.to_datetime and add format parameter if not already present
					Matcher pdToDatetimeMatcher = PD_TO_DATETIME_PATTERN.matcher(l);
					if (pdToDatetimeMatcher.find()) {
						String dataset = pdToDatetimeMatcher.group(1);
						l = l.replaceAll(PD_TO_DATETIME_PATTERN.pattern(),
								"pd.to_datetime(" + dataset + ", format=\"%d-%m-%Y\")");
					}

					// split the line by commas and join with newline characters
					if ("python".equals(cellType) && !l.endsWith("\n")) {
						l += "\n";
					}

					cell.add(l);
				}
			}
		}

		// store existing cell if necessary
		if (cell != null) {
			ObjectNode on = Json.newObject();
			// change cell type from "python" to "code", otherwise just cell type
			if ("python".equals(cellType)) {
				cellType = "code";
				on.set("execution_count", null);
				on.set("outputs", Json.newArray());
			}
			on.put("cell_type", cellType);
			on.set("metadata", Json.newObject());
			on.set("source", cell);
			cells.add(on);
		}

		// construct rest of data structure
		String metadata = """
				{
				  "kernelspec": {
				   "display_name": "Python 3",
				   "language": "python",
				   "name": "python3"
				  },
				  "language_info": {
				   "codemirror_mode": {
				    "name": "ipython",
				    "version": 3
				   },
				   "file_extension": ".py",
				   "mimetype": "text/x-python",
				   "name": "python",
				   "nbconvert_exporter": "python",
				   "pygments_lexer": "ipython3",
				   "version": "3.8.5"
				  },
				  "toc": {
				   "base_numbering": 1,
				   "nav_menu": {
				    "height": "228px",
				    "width": "252px"
				   },
				   "number_sections": false,
				   "sideBar": true,
				   "skip_h1_title": false,
				   "title_cell": "Table of Contents",
				   "title_sidebar": "Contents",
				   "toc_cell": true,
				   "toc_position": {},
				   "toc_section_display": "block",
				   "toc_window_display": true
				  }
				}""";

		// Create a new Jupyter notebook structure
		ObjectNode jupyterNotebook = Json.newObject();
		jupyterNotebook.set("cells", cells);
		jupyterNotebook.set("metadata", Json.parse(metadata));
		jupyterNotebook.put("nbformat", 4);
		jupyterNotebook.put("nbformat_minor", 2);

		return jupyterNotebook.toPrettyString();
	}
}

package services.maintenance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import org.apache.pekko.actor.ActorSystem;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import controllers.routes;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.ds.CompleteDS;
import models.ds.ExpSamplingDS;
import models.ds.LinkedDS;
import models.ds.MediaDS;
import models.ds.MovementDS;
import models.sr.Cluster;
import models.sr.TelegramSession;
import play.Logger;
import play.cache.SyncCacheApi;
import play.libs.Files;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http.Request;
import play.twirl.api.Html;
import scala.concurrent.ExecutionContext;
import services.email.NotificationService;
import services.inlets.ScheduledService;
import services.slack.Slack;
import utils.DatasetUtils;
import utils.DateUtils;
import utils.StringUtils;
import utils.conf.ConfigurationUtils;
import utils.export.AssetsHelper;
import utils.export.MetaDataUtils;
import utils.export.ZenodoRecord;
import utils.export.ZenodoPublishingUtil;

@Singleton
public class ProjectLifecycleService implements ScheduledService {

	private static final String THIS_PROJECT_HAS_BEEN_FROZEN = "THIS PROJECT HAS BEEN FROZEN.";
	private static final String THIS_PROJECT_HAS_BEEN_DEIDENTIFIED = "THIS PROJECT HAS BEEN DEIDENTIFIED.";

	private final NotificationService notificationService;
	private final DatasetConnector datasetConnector;
	private final ZenodoPublishingUtil zenodoPublishingUtil;
	private String applicationHost = null;
	private ActorSystem actorSystem;
	private final SyncCacheApi cache;
	private ExecutionContext executionContext;

	private static final Logger.ALogger logger = Logger.of(ProjectLifecycleService.class);

	@Inject
	public ProjectLifecycleService(Config config, ActorSystem actorSystem, SyncCacheApi cache,
	        ExecutionContext executionContext, NotificationService notificationService,
	        DatasetConnector datasetConnector, ZenodoPublishingUtil zenodoService) {
		this.notificationService = notificationService;
		this.datasetConnector = datasetConnector;
		this.actorSystem = actorSystem;
		this.cache = cache;
		this.executionContext = executionContext;
		this.zenodoPublishingUtil = zenodoService;

		// get the base url from configuration
		if (config.hasPath(ConfigurationUtils.DF_BASEURL)) {
			applicationHost = config.getString(ConfigurationUtils.DF_BASEURL);
		}
	}

	@Override
	public void refresh() {

		// check if configuration is complete
		if (applicationHost == null) {
			logger.error("The lifecycle was not executed because the application host was not set.");
			return;
		}

		// go through all DF native projects that have not yet been deidentified or frozen
		int deidentifiedCount = 0, frozenCount = 0;
		logger.info("Starting project lifecycle checks for deidentification (M3) and freeze (M24)...");
		for (Project p : Project.find.query().where().eq("frozenProject", false).findList()) {
			// check project type
			if (!p.isDFNativeProject()) {
				// we don't touch non-DF native projects
				continue;
			}

			// if project end date longer than 3 months ago and not yet deidentified, deidentify it!
			if (p.isDeidentifiable() && !p.getRemarks().contains(THIS_PROJECT_HAS_BEEN_DEIDENTIFIED)) {
				logger.info("Deidentify project " + p.getName() + " (" + p.getId() + ").");
				deidentifyProject(p);
				// add lab notes entry about deidentifcation
				LabNotesEntry.log(Dataset.class, LabNotesEntryType.DELETE, "Participant information", p);
				deidentifiedCount++;
			}

			// if project end date longer than 24 months ago, freeze it!
			if (p.isOutdated()) {
				logger.info("Freeze project " + p.getName() + " (" + p.getId() + ").");
				freezeProject(Optional.empty(), p);
				// add lab notes entry about project freeze
				LabNotesEntry.log(Dataset.class, LabNotesEntryType.CREATE, "Exported project archive", p);
				LabNotesEntry.log(Dataset.class, LabNotesEntryType.DELETE, "Deleted project data (excl. archive)", p);
				frozenCount++;
			}
		}

		// check telegram sessions
		int sessionDeleteCount = 0;
		logger.info("Starting Telegram session purge for idle sessions (M16) ... ");
		List<TelegramSession> outdatedSessions = TelegramSession.find.query().where()
		        .lt("last_action", DateUtils.moveMonths(new Date(), -24)).findList();
		for (TelegramSession tgs : outdatedSessions) {
			try {
				logger.info(
				        "Delete session " + tgs.getId() + " (last active project: " + tgs.getActiveProjectId() + ")");
				tgs.delete();
			} catch (Exception e) {
				// do nothing
			}
			sessionDeleteCount++;
		}

		// log summary
		if (deidentifiedCount + frozenCount + sessionDeleteCount > 0) {
			final String message = "---> Maintenance: " + deidentifiedCount + " project(s) deidentified, " + frozenCount
			        + " project(s) frozen and " + sessionDeleteCount + " outdated Telegram session(s) cleared.";

			// log message to stdout and Slack
			logger.info(message);
			Slack.call("Project lifecycle", message);
		}
	}

	@Override
	public void stop() {
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * deidentify project by means of resetting devices, participants and wearables
	 * 
	 * @param p
	 */
	public void deidentifyProject(Project p) {
		logger.info("Resetting/deidentifing devices, participants and wearables of project (" + p.getId() + ")...");

		// deidentify participant
		p.getParticipants().stream().filter(pc -> pc.canDeidentify()).forEach(pc -> {
			pc.deidentify();
		});

		// deidentify devices
		p.getDevices().stream().filter(d -> d.canDeidentify()).forEach(d -> {
			d.deidentify();
		});

		// deidentify the wearable
		p.getWearables().stream().filter(w -> w.canDeidentify()).forEach(w -> {
			w.deidentify();
		});

		// mark project as deidentified (to be sure, truncate remarks to 960 chars)
		p.setRemarks(THIS_PROJECT_HAS_BEEN_DEIDENTIFIED + "\n\n" + StringUtils.nss(p.getRemarks(), 960));
		p.update();
	}

	/**
	 * archive project
	 * 
	 * @param p
	 */
	public void archiveProject(Project p) {
		logger.info("Archive project (" + p.getId() + ")");

		// archive the project
		if (!p.isArchivedProject()) {
			p.setArchivedProject(true);
			p.update();
		}
	}

	/**
	 * freeze the project by means of exporting the project, then adding the exported ZIP file to a new dataset in the
	 * project, which also marks the project as frozen; finally all datatables are dropped to free up space in the
	 * central database
	 * 
	 * @param request
	 * @param p
	 */
	public void freezeProject(Optional<Request> request, Project p) {

		// check if possible
		if (p.isFrozen()) {
			logger.error("Freezing frozen project (" + p.getId() + ") not possible.");
		}

		logger.info("Freezing project (" + p.getId() + ")...");
		Date start = p.getStart();
		Date end = p.getEnd();

		// export all project data
		TemporaryFile tf = exportProject(request, p);
		logger.info("  Exported project data");

		// delete all files in complete and derivative datasets & then drop all dataset tables
		logger.info("  Deleting project files and dropping project data from database");
		for (Dataset ds : p.getDatasets()) {
			LinkedDS lds = datasetConnector.getDatasetDS(ds);
			if (lds instanceof CompleteDS) {
				CompleteDS cds = (CompleteDS) lds;
				cds.deleteAllFiles();
			} else {
				// no files to delete
			}

			// clear all relevant configuration entries in dataset
			Map<String, String> configuration = ds.getConfiguration();
			String[] keysToRemove = new String[] { Dataset.PUBLIC_ACCESS_TOKEN, Dataset.WEB_ACCESS_TOKEN,
			        Dataset.ACTOR_CHANNEL, Dataset.API_TOKEN, Dataset.WEB_ACCESS_ENTRY };
			for (String key : keysToRemove) {
				configuration.remove(key);
			}

			// remove any operational functionality and linking from the dataset
			ds.setRefId("");
			ds.setApiToken("");
			ds.setOpenParticipation(false);
			ds.setTargetObject("");
			ds.update();

			// finally drop table
			lds.dropTable();

			logger.info("  - " + ds.getName() + " (" + ds.getRefId() + ")");
		}

		// where to store this file now?
		Dataset tfds = datasetConnector.create(Dataset.PROJECT_DATA_EXPORT, DatasetType.COMPLETE, p,
		        "This dataset contains the exported data from this entire project.", "", Boolean.FALSE.toString(),
		        p.getLicense());
		tfds.save();
		CompleteDS cds = datasetConnector.getTypedDatasetDS(tfds);

		// store file and add record for file in DB
		Optional<String> storeFile = cds.storeFile(tf.path().toFile(), "project_export.zip");
		if (storeFile.isPresent()) {
			cds.addRecord(storeFile.get(), "Project data export", new Date());
		}

		// ensure we have the right start and end dates
		tfds.refresh();
		tfds.setStart(start);
		tfds.setEnd(end);
		tfds.update();

		// mark project as frozen (to be sure, truncate remarks to 960 chars)
		p.setRemarks(THIS_PROJECT_HAS_BEEN_FROZEN + "\n\n" + StringUtils.nss(p.getRemarks(), 960));
		p.setArchivedProject(true);
		p.setFrozen(true);
		p.update();

		logger.info("  Storing export into new dataset: " + cds.getFolder().getAbsolutePath() + File.separator
		        + storeFile.orElse("project_export.zip"));
	}

	/**
	 * export project to zip archive (all datasets without scripts and study management)
	 * 
	 * @param request
	 * @param id
	 * @param project
	 * @return
	 */
	public TemporaryFile exportProject(Optional<Request> request, Project project) {
		return exportProject(request, project, DatasetUtils.scriptsExcluded(project.getOperationalDatasets()));
	}

	/**
	 * export project to zip archive (given datasets only)
	 * 
	 * @param request
	 * @param project
	 * @param datasetsToExport
	 * @return
	 */
	public TemporaryFile exportProject(Optional<Request> request, Project project, List<Dataset> datasetsToExport) {

		List<TemporaryFile> filesToCleanUp = new LinkedList<>();

		// create temp file
		final String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		final String zipFileName = today + "_" + project.getName().replaceAll("[\\W_]+", "_");

		logger.trace("Exporting project " + project.getId() + " to " + zipFileName);
		TemporaryFile tf = Files.singletonTemporaryFileCreator().create(zipFileName, ".zip");
		filesToCleanUp.add(tf);
		try (FileOutputStream fos = new FileOutputStream(tf.path().toFile());
		        ZipOutputStream zipOut = new ZipOutputStream(fos);) {

			// write metadata
			AssetsHelper.zipString(zipOut, zipFileName + "/metadata.json", getMetaDataNode(project).toString());

			// write license
			String projectCollaborators = project.getCollaborators().stream().map(c -> c.getCollaborator().getName())
			        .collect(Collectors.joining(", "));
			String projectTeam = (projectCollaborators.isEmpty() ? project.getOwner().getName()
			        : project.getOwner().getName() + ", " + projectCollaborators);

			// collect all licenses
			String projectLicenseKey = project.getLicense();

			// render all licenses and zip them into a file
			String licenses = """
			        Project license
			        ---------------------------------------------------------------
			        """ //
			        + views.html.tools.export.license.render(today.substring(0, 4), projectTeam, projectLicenseKey)
			                .toString() //
			        + "\n\n\n"
			        + datasetsToExport.stream().filter(ds -> !ds.getLicense().equals(projectLicenseKey))
			                .map(ds -> """
			                        License for dataset '%s' (different from project license)
			                        ---------------------------------------------------------------
			                        """.formatted(ds.getName()) + views.html.tools.export.license
			                        .render(today.substring(0, 4), projectTeam, ds.getLicense()).toString())
			                .collect(Collectors.joining("\n\n\n"));
			AssetsHelper.zipString(zipOut, zipFileName + "/LICENSE", licenses);

			// write readme
			String projectViewURL;
			if (request.isPresent()) {
				projectViewURL = controllers.routes.ProjectsController.view(project.getId()).absoluteURL(request.get(),
				        true);
			} else {
				projectViewURL = getProjectViewLink(project);
			}
			AssetsHelper.zipString(zipOut, zipFileName + "/README.md",
			        views.html.tools.export.readme.render(project, datasetsToExport, projectViewURL).toString());

			// render and store the log book
			StringBuilder logbook = new StringBuilder();
			logbook.append("timestamp;type;dataset;message\n");
			List<LabNotesEntry> labNotes = LabNotesEntry.findByProject(project.getId());
			labNotes.stream().forEach(ln -> {
				logbook.append(ln.toString() + "\n");
			});
			AssetsHelper.zipString(zipOut, zipFileName + "/logbook.csv", logbook.toString());

			// write datasets and dataset files
			Cluster cluster = new Cluster("data");
			cluster.setId(-1l);

			int datasetIndex = 0;
			for (Dataset ds : datasetsToExport) {
				int dsIndex = ++datasetIndex;

				// do not export scripts, chatbots, saved exports or study management datasets
				if (ds.isSavedExport() || ds.isScript() || ds.isChatbot() || ds.isStudyManagement()) {
					continue;
				}

				final String pathPrefix = zipFileName + "/" + dsIndex + "_" + ds.getSlug();

				// write dataset data both in JSON and CSV
				TemporaryFile tempFileJSON = Files.singletonTemporaryFileCreator().create("data", "json");
				filesToCleanUp.add(tempFileJSON);
				try (FileWriter fw = new FileWriter(tempFileJSON.path().toFile());) {
					datasetConnector.getDatasetDS(ds).exportProjectedToFile(fw, cluster, -1l, -1l, -1l);
				} catch (Exception e) {
					// do nothing
				}
				AssetsHelper.zipFile(zipOut, pathPrefix + ".json", tempFileJSON.path().toFile());

				TemporaryFile tempFileCSV = Files.singletonTemporaryFileCreator().create("data", "csv");
				filesToCleanUp.add(tempFileCSV);
				try (FileWriter fw = new FileWriter(tempFileCSV.path().toFile());) {
					datasetConnector.getDatasetDS(ds).exportToFile(fw, cluster, -1l, -1l, -1l);
				} catch (Exception e) {
					// do nothing
				}
				AssetsHelper.zipFile(zipOut, pathPrefix + ".csv", tempFileCSV.path().toFile());

				switch (ds.getDsType()) {
				case COMPLETE:
					final CompleteDS cds = datasetConnector.getTypedDatasetDS(ds);
					cds.getFiles().stream().forEach(tm -> {
						AssetsHelper.zipFile(zipOut, pathPrefix + "/" + tm.link, cds.getFile(tm.id));
					});
					break;
				case MEDIA:
					final MediaDS mds = datasetConnector.getTypedDatasetDS(ds);
					mds.getFiles().stream().forEach(tm -> {
						AssetsHelper.zipFile(zipOut, pathPrefix + "/" + tm.link, mds.getFile(tm.id));
					});
					break;
				case MOVEMENT:
					final MovementDS mvds = datasetConnector.getTypedDatasetDS(ds);
					mvds.getFiles().stream().forEach(tm -> {
						AssetsHelper.zipFile(zipOut, pathPrefix + "/" + tm.link, mvds.getFile(tm.id));
					});
					break;
				case ES:
					final ExpSamplingDS esds = datasetConnector.getTypedDatasetDS(ds);
					esds.getFiles().stream().forEach(tm -> {
						AssetsHelper.zipFile(zipOut, pathPrefix + "/" + tm.link, esds.getFile(tm.link));
					});
					break;
				default:
				}
			}
		} catch (FileNotFoundException e) {
			logger.error("Error(s) occurred in producing the zip file for project " + project.getName(), e);
		} catch (IOException e) {
			logger.error("Error(s) occurred in producing the zip file for project " + project.getName(), e);
		}

		// before returning ensure that the temporary files are deleted earlier than system exit, just after 20 minutes
		// because the download should be finished then
		actorSystem.scheduler().scheduleOnce(Duration.ofMinutes(20), () -> {
			for (TemporaryFile temporaryFile : filesToCleanUp) {
				temporaryFile.path().toFile().delete();
			}
		}, this.executionContext);

		return tf;
	}

	public void publish(Optional<Request> request, Project project, List<Dataset> datasetsToExport, String cacheToken,
	        String zenodoAccessToken) {
		actorSystem.scheduler().scheduleOnce(Duration.ofSeconds(1), () -> {
			try {
				cache.set(cacheToken, "🚀 Starting project export...");
				TemporaryFile exportZip = exportProject(request, project, datasetsToExport);

				cache.set(cacheToken, "✅ Export complete.<br>🚀 Uploading to Zenodo (this may take a while)...");
				Optional<ZenodoRecord> record = zenodoPublishingUtil.createRecord(project, zenodoAccessToken,
				        cacheToken, exportZip);

				if (record.isPresent()) {
					cache.set(cacheToken, """
					        ✅ Published to Zenodo!<br>
					        DOI: %s<br>
					        <a href='%s' class='btn' target='_blank'>View on Zenodo</a>
					        <a href='%s' class='btn' target='_blank'>Edit on Zenodo</a>
					        <p>
					        		<a href="%s" class="btn">back to project</a>
					        </p>
					        """.formatted(record.get().getDoi(), record.get().getPreviewUrl(),
					        record.get().getEditUrl(), routes.ProjectsController.view(project.getId())));

					// explicit cleanup of the export zip after successful upload
					exportZip.path().toFile().delete();
				} else {
					exportZip.path().toFile().delete();
				}

			} catch (Exception e) {
				logger.error("Error during publication to Zenodo", e);
				cache.set(cacheToken, "❌ Error during publication: " + e.getMessage());
			}
		}, executionContext);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode getMetaDataNode(Project project) {
		// create Json node list for meta-data of project and datasets
		ObjectNode on = Json.newObject();
		// project node
		on.set("project", MetaDataUtils.toJson(project));
		// dataset nodes
		ArrayNode an = on.putArray("datasets");
		project.getDatasets().stream().forEach(ds -> {
			// add dataset meta data
			an.add(MetaDataUtils.toJson(ds));
		});

		return on;
	}

	private void sendMailBeforeEndDate(Project project) {
		String projectViewLink = getProjectViewLink(project);

		// send email to participant with invite link
		Html htmlBody = views.html.emails.invite.render("Ending project notification", String.format("Hi "
		        + project.getOwner().getName() + "," + "\n\nYour project, " + project.getName()
		        + ", will end in two weeks, after that, the data in the project "
		        + "will be read-only. If you need more time to access (read / write) your data, please update the end "
		        + "date of the project and its datasets. Click to check your project directly."), projectViewLink);
		String textBody = "Hello! \n\nWe send you this email for your project in Data Foundry, " + project.getName()
		        + ", "
		        + "is goint to end in two weeks. Click the link below to be forwarded to your project page and have a "
		        + "check, feel free to do some changes you need, i.e., change the end date, or archive the project "
		        + "right now. \n\n" + projectViewLink + "\n\n";

		notificationService.sendMail(project.getOwner().getEmail(), textBody, htmlBody, projectViewLink,
		        "[ID Data Foundry] Notification", "Project page link: " + projectViewLink);
	}

	private void sendMailBeforeArchive(Project project) {
		String projectViewLink = getProjectViewLink(project);

		Html htmlBody = views.html.emails.invite.render("Archiving project notification", String.format("Hi "
		        + project.getOwner().getName() + "," + "\n\nYour project, " + project.getName()
		        + ", will be archived in two weeks, after that, the whole project "
		        + "would be archived as a zip file. If you need more time to access(read / write) your data, please "
		        + "update the end date of related datasets. You can check your project by the link."), projectViewLink);
		String textBody = "Hello! \n\nWe send you this email for your project, " + project.getName()
		        + ", is goint to be archived in two weeks. Click the link below to be forwarded to your project page "
		        + "and have a look, feel free to do some changes you need, i.e., change the end date, or archive "
		        + "the project right now. \n\n" + projectViewLink + "\n\n";

		notificationService.sendMail(project.getOwner().getEmail(), textBody, htmlBody, projectViewLink,
		        "[ID Data Foundry] Notification", "Project page link: " + projectViewLink);
	}

	private void sendMailAboutArchiving(Project project) {
		String projectViewLink = getProjectViewLink(project);

		Html htmlBody = views.html.emails.invite.render("Archived project notification", String.format("Hi "
		        + project.getOwner().getName() + "," + "\n\nYour project, " + project.getName()
		        + ", has been archived as a zip file today. If you need more "
		        + "to use your data, please download the zip file; or if you want to update something for the project "
		        + "please reopen the project and update the end date for it if necessary. You can check your project "
		        + "by the link."), projectViewLink);
		String textBody = "Hello! \n\nWe send you this email for your project in Data Foundry, " + project.getName()
		        + ", has been archived today. Click the link below to be forwarded to your project page "
		        + "and have a look. \n\n" + projectViewLink + "\n\n";

		notificationService.sendMail(project.getOwner().getEmail(), textBody, htmlBody, projectViewLink,
		        "[ID Data Foundry] Notification", "Project page link: " + projectViewLink);
	}

	private String getProjectViewLink(Project project) {
		return controllers.routes.ProjectsController.view(project.getId()).absoluteURL(true, applicationHost);
	}

}
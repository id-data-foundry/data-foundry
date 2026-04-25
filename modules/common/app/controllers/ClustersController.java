package controllers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.auth.UserAuth;
import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.ds.ClusterDS;
import models.ds.CompleteDS;
import models.ds.ExpSamplingDS;
import models.ds.MediaDS;
import models.ds.MovementDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.Logger;
import play.filters.csrf.AddCSRFToken;
import play.libs.Files;
import play.libs.Files.TemporaryFile;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import play.mvc.Security.Authenticated;
import utils.components.OnboardingSupport;
import utils.export.AssetsHelper;
import utils.export.MetaDataUtils;

/**
 * This controller contains an action to handle HTTP requests to the application's home page.
 */
public class ClustersController extends AbstractAsyncController {

	private final DatasetConnector datasetConnector;
	private final OnboardingSupport onboardingSupport;
	private static final Logger.ALogger logger = Logger.of(ClustersController.class);

	@Inject
	public ClustersController(DatasetConnector datasetConnector, OnboardingSupport onboardingSupport) {
		this.datasetConnector = datasetConnector;
		this.onboardingSupport = onboardingSupport;
	}

	@Authenticated(UserAuth.class)
	public Result view(Request request, Long id) {

		// load cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		Project project = cluster.getProject();
		project.refresh();

		// check user permissions
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// show cluster overview
		return ok(views.html.sources.cluster.view.render(project, cluster, request));
	}

	@Authenticated(UserAuth.class)
	public Result rename(Request request, Long id, String name) {

		// load cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		// check user right
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));
		Project project = cluster.getProject();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// update name
		cluster.setName(name);
		cluster.update();

		return redirect(routes.ClustersController.view(id));
	}

	@Authenticated(UserAuth.class)
	public Result add(Request request, Long id, String name) {

		// check user permissions
		Person user = getAuthenticatedUserOrReturn(request, redirect(HOME));
		Project project = Project.find.byId(id);

		// check if project exists
		if (project == null) {
			return notFound();
		}

		// check ownership
		if (!project.editableBy(user)) {
			return redirect(HOME);
		}

		// create cluster with cluster name in project
		Cluster cluster = new Cluster(name);
		cluster.create();
		project.getClusters().add(cluster);
		project.update();

		onboardingSupport.updateAfterDone(user, "new_cluster");

		return ok();
	}

	/**
	 * delete specified cluster
	 * 
	 * @param request
	 * @param id
	 * @return
	 */
	@Authenticated(UserAuth.class)
	public Result delete(Request request, Long id) {

		// check user permissions
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		// load cluster
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		// check ownership
		Project project = cluster.getProject();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// delete cluster in project (will also unlink cluster resources)
		cluster.remove();

		return redirect(routes.ProjectsController.manageResources(project.getId()));
	}

	// ----------------------------------------------------------------------------------------------------
	//
	// CLUSTER COMPONENTS
	//
	// ----------------------------------------------------------------------------------------------------

	@Authenticated(UserAuth.class)
	public Result addDevice(Request request, Long id, Long device) {

		// check user permissions
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		// check ownership
		Project project = cluster.getProject();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// check if device exists and is in project and cluster
		Device d = Device.find.byId(device);
		if (d == null || !project.hasDevice(d) || cluster.hasDevice(d)) {
			return redirect(HOME);
		}

		// add device to cluster
		cluster.add(d);
		cluster.update();

		// back to cluster page
		return redirect(routes.ClustersController.view(id));
	}

	@Authenticated(UserAuth.class)
	public Result removeDevice(Request request, Long id, Long device) {

		// check user permissions
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));

		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		// check ownership
		Project project = cluster.getProject();
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// check if device exists and is in project and cluster
		Device d = Device.find.byId(device);
		if (d == null || !project.hasDevice(d) || !cluster.hasDevice(d)) {
			return redirect(HOME);
		}

		// remove device
		cluster.remove(d);
		cluster.update();

		// back to cluster page
		return redirect(routes.ClustersController.view(id));
	}

	@Authenticated(UserAuth.class)
	public Result addParticipant(Request request, Long id, Long participant) {

		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		Project project = cluster.getProject();
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// check if device exists and is in project and cluster
		Participant p = Participant.find.byId(participant);
		if (p == null || !project.hasParticipant(p) || cluster.hasParticipant(p)) {
			return redirect(HOME);
		}

		// add participant to cluster
		cluster.add(p);
		cluster.update();

		// back to cluster page
		return redirect(routes.ClustersController.view(id));
	}

	@Authenticated(UserAuth.class)
	public Result removeParticipant(Request request, Long id, Long participant) {

		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		Project project = cluster.getProject();
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// check if participant exists and is in project and cluster
		Participant p = Participant.find.byId(participant);
		if (p == null || !project.hasParticipant(p) || !cluster.hasParticipant(p)) {
			return redirect(HOME);
		}

		// remove participant
		cluster.remove(p);
		cluster.update();

		// back to cluster page
		return redirect(routes.ClustersController.view(id));
	}

	@Authenticated(UserAuth.class)
	public Result addWearable(Request request, Long id, Long wearable) {

		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		Project project = cluster.getProject();
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// check if wearable exists and is in project and cluster
		Wearable w = Wearable.find.byId(wearable);
		if (w == null || !project.hasWearable(w) || cluster.hasWearable(w)) {
			return redirect(HOME);
		}

		// add device to cluster
		cluster.add(w);
		cluster.update();

		// back to cluster page
		return redirect(routes.ClustersController.view(id));
	}

	@Authenticated(UserAuth.class)
	public Result removeWearable(Request request, Long id, Long wearable) {

		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirect(HOME);
		}

		Project project = cluster.getProject();
		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));
		if (!project.editableBy(username)) {
			return redirect(HOME);
		}

		// check if device exists and is in project and cluster
		Wearable w = Wearable.find.byId(wearable);
		if (w == null || !project.hasWearable(w) || !cluster.hasWearable(w)) {
			return redirect(HOME);
		}

		// remove device
		cluster.remove(w);
		cluster.update();

		// back to cluster page
		return redirect(routes.ClustersController.view(id));
	}

	@Authenticated(UserAuth.class)
	public CompletionStage<Result> timeseries(Request request, Long id) {
		// check id
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirectCS(HOME);
		}

		Project project = cluster.getProject();

		String username = getAuthenticatedUserNameOrReturn(request, redirect(HOME));
		if (!project.editableBy(username)) {
			return redirectCS(HOME);
		}

		ClusterDS clds = new ClusterDS(datasetConnector);

		// serve this stream with 200 OK
		return CompletableFuture.supplyAsync(() -> createStream().mapMaterializedValue(sourceActor -> {
			CompletableFuture.runAsync(() -> clds.timeseries(sourceActor, cluster));
			return sourceActor;
		})).thenApplyAsync(chunks -> ok().chunked(chunks).as("text/csv"));
	}

	/**
	 * export the metadata of this cluster and all the datasets in it, and the data of datasets in the same project
	 * which are related to the sources, participants, devices, and wearables, will be exported to the files with
	 * datasets' names
	 * 
	 * @param id
	 */
	@Authenticated(UserAuth.class)
	public CompletionStage<Result> exportMetadata(Request request, Long id) {
		// check if user exists
		Person user = getAuthenticatedUserOrReturn(request,
				redirect(routes.HomeController.logout()).addingToSession(request, "error", "User not found."));

		// check if cluster / project exists
		Cluster cluster = Cluster.find.byId(id);
		if (cluster == null) {
			return redirectCS(HOME);
		}

		Project project = cluster.getProject();
		if (project == null) {
			return cs(() -> redirect(HOME).addingToSession(request, "error", "Project not found."));
		}

		// check permission of user
		if (!project.editableBy(user)) {
			return cs(() -> redirect(HOME).addingToSession(request, "error",
					"This action is not allowed because you are not the project owner."));
		}

		// create temp file
		final String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		final String zipFileName = today + "_" + cluster.getName().replace(" ", "_");
		TemporaryFile tf = Files.singletonTemporaryFileCreator().create(zipFileName, ".zip");
		try (FileOutputStream fos = new FileOutputStream(tf.path().toFile());
				ZipOutputStream zipOut = new ZipOutputStream(fos);) {

			// write metadata
			AssetsHelper.zipString(zipOut, zipFileName + "/metadata.json", getMetaDataNode(cluster).toString());

			// write license
			String projectTeam = project.getCollaborators().stream().map(c -> c.getCollaborator().getName())
					.collect(Collectors.joining(", "));
			projectTeam = (projectTeam.isEmpty() ? project.getOwner().getName()
					: project.getOwner().getName() + ", " + projectTeam);
			AssetsHelper.zipString(zipOut, zipFileName + "/LICENSE.txt", views.html.tools.export.license
					.render(today.substring(0, 4), projectTeam, project.getLicense()).toString());

			// write readme
			AssetsHelper.zipString(zipOut, zipFileName + "/README.md",
					views.html.tools.export.readme.render(project, project.getOperationalDatasets(),
							routes.ProjectsController.view(id).absoluteURL(request, true)).toString());

			// write datasets and dataset files
			for (Dataset ds : project.getDatasets().stream()
					.filter(ds -> ds.getDsType() == DatasetType.IOT || ds.getDsType() == DatasetType.MEDIA
							|| ds.getDsType() == DatasetType.ANNOTATION || ds.getDsType() == DatasetType.DIARY
							|| ds.getDsType() == DatasetType.ES || ds.getDsType() == DatasetType.MOVEMENT
							|| ds.getDsType() == DatasetType.FITBIT || ds.getDsType() == DatasetType.GOOGLEFIT)
					.collect(Collectors.toList())) {

				// write dataset data both in JSON and CSV
				File tempFileJSON = Files.singletonTemporaryFileCreator().create("data", "json").path().toFile();
				try (FileWriter fw = new FileWriter(tempFileJSON)) {
					datasetConnector.getDatasetDS(ds).exportProjectedToFile(fw, cluster, -1l, -1l, -1l);
				} catch (Exception e) {
					// do nothing
				}
				AssetsHelper.zipFile(zipOut, zipFileName + "/" + ds.getSlug() + "/data.json", tempFileJSON);

				File tempFileCSV = Files.singletonTemporaryFileCreator().create("data", "csv").path().toFile();
				try (FileWriter fw = new FileWriter(tempFileCSV)) {
					datasetConnector.getDatasetDS(ds).exportToFile(fw, cluster, -1l, -1l, -1l);
				} catch (Exception e) {
					// do nothing
				}
				AssetsHelper.zipFile(zipOut, zipFileName + "/" + ds.getSlug() + "/data.csv", tempFileCSV);

				switch (ds.getDsType()) {
				case COMPLETE:
					final CompleteDS cds = datasetConnector.getTypedDatasetDS(ds);
					cds.getFiles().stream().filter(tm -> cluster.hasParticipant(tm.participant)).forEach(tm -> {
						AssetsHelper.zipFile(zipOut, zipFileName + "/" + ds.getSlug() + "/" + tm.link,
								cds.getFile(tm.id));
					});
					break;
				case MEDIA:
					final MediaDS mds = datasetConnector.getTypedDatasetDS(ds);
					mds.getFiles().stream().filter(tm -> cluster.hasParticipant(tm.participant)).forEach(tm -> {
						AssetsHelper.zipFile(zipOut, zipFileName + "/" + ds.getSlug() + "/" + tm.link,
								mds.getFile(tm.id));
					});
					break;
				case MOVEMENT:
					final MovementDS mvds = datasetConnector.getTypedDatasetDS(ds);
					mvds.getFiles().stream().filter(tm -> cluster.hasParticipant(tm.participant)).forEach(tm -> {
						AssetsHelper.zipFile(zipOut, zipFileName + "/" + ds.getSlug() + "/" + tm.link,
								mvds.getFile(tm.id));
					});
					break;
				case ES:
					final ExpSamplingDS esds = datasetConnector.getTypedDatasetDS(ds);
					esds.getFiles().stream().filter(tm -> cluster.hasParticipant(tm.participant)).forEach(tm -> {
						AssetsHelper.zipFile(zipOut, zipFileName + "/" + ds.getSlug() + "/" + tm.link,
								esds.getFile(tm.link));
					});
					break;
				default:
				}
			}
		} catch (FileNotFoundException e) {
			logger.error("Error(s) occurred in producing the zip file for cluster " + cluster.getName(), e);
		} catch (IOException e) {
			logger.error("Error(s) occurred in producing the zip file for cluster " + cluster.getName(), e);
		}

		// return zip file
		return okCS(tf.path().toFile());
	}

	/**
	 * get metadata of
	 * 
	 * @param project
	 */
	private ObjectNode getMetaDataNode(Cluster cluster) {

		// create Json node list for meta-data of cluster, resources, and datasets
		// root node
		ObjectNode on = Json.newObject();
		ObjectNode md = on.putObject("meta-data");

		// cluster node
		{
			// create cluster json node
			ObjectNode clusterNode = md.putObject("Cluster");

			// add meta-data
			clusterNode.put("id", cluster.getId());
			clusterNode.put("name", cluster.getName());
			clusterNode.put("project", cluster.getProject().getName());

			// // sources nodes
			// {
			// // create sources node array
			// ObjectNode sn = clusterNode.putObject("Sources");

			// // participants nodes
			// {
			// ArrayNode pn = sn.putArray("Participants");

			// // create participant node array
			// for (Long pid : cluster.getParticipantList()) {

			// Participant participant = Participant.find.byId(pid);
			// // add meta-data
			// ObjectNode metadata = Json.newObject();
			// metadata.put("id", participant.id);
			// metadata.put("name", participant.getName());
			// metadata.put("participation-status", participant.status.name());

			// pn.add(metadata);
			// }
			// }

			// // devices nodes
			// {
			// ArrayNode dn = sn.putArray("Devices");

			// // create devices node array
			// for (Long pid : cluster.getDeviceList()) {

			// Device device = Device.find.byId(pid);
			// // add meta-data
			// ObjectNode metadata = Json.newObject();
			// metadata.put("id", device.id);
			// metadata.put("name", device.name);
			// metadata.put("categort", device.getCategory());
			// metadata.put("subtype", device.getSubtype());
			// metadata.put("location", device.getLocation());

			// dn.add(metadata);
			// }
			// }

			// // wearables nodes
			// {
			// ArrayNode wn = sn.putArray("Wearables");

			// // create wearables node array
			// for (Long pid : cluster.getWearableList()) {

			// Wearable wearable = Wearable.find.byId(pid);
			// // add meta-data
			// ObjectNode metadata = Json.newObject();
			// metadata.put("id", wearable.id);
			// metadata.put("name", wearable.name);
			// metadata.put("brand", wearable.brand);
			// metadata.put("the-last-dataset", wearable.scopes);

			// wn.add(metadata);
			// }
			// }
			// }
		}

		// dataset nodes
		{
			// create dataset node
			// ObjectNode dsNode = md.putObject("Datasets");
			ArrayNode an = md.putArray("datasets");

			// create dataset node array
			cluster.getProject().getDatasets().stream()
					.filter(ds -> ds.getDsType() == DatasetType.IOT || ds.getDsType() == DatasetType.MEDIA
							|| ds.getDsType() == DatasetType.ANNOTATION || ds.getDsType() == DatasetType.DIARY
							|| ds.getDsType() == DatasetType.ES || ds.getDsType() == DatasetType.MOVEMENT
							|| ds.getDsType() == DatasetType.FITBIT || ds.getDsType() == DatasetType.GOOGLEFIT)
					.forEach(ds -> {
						ObjectNode metadata = MetaDataUtils.toJson(ds);

						// add additional information
						metadata.put("project_id", ds.getProject().getId());
						metadata.put("code", ds.getConfiguration().get(Dataset.ACTOR_CODE));
						an.add(metadata);
					});
		}

		return on;
	}

}
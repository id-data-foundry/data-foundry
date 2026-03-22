package controllers.api.js;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import controllers.DatasetsController;
import controllers.swagger.AbstractApiController;
import datasets.DatasetConnector;
import models.Dataset;
import models.Person;
import models.Project;
import models.sr.Cluster;
import play.data.FormFactory;
import play.libs.Json;
import play.mvc.Http.Request;
import play.mvc.Result;
import utils.DataUtils;
import utils.auth.TokenResolverUtil;

public class ResearcherApiController extends AbstractApiController {

	final DatasetsController datasetsController;
	final TokenResolverUtil tokenResolverUtil;
	final DatasetConnector datasetConnector;

	@Inject
	public ResearcherApiController(DatasetsController datasetsController, FormFactory formFactory,
			TokenResolverUtil tokenResolverUtil, DatasetConnector datasetConnector) {
		super(formFactory, datasetConnector, tokenResolverUtil);

		this.datasetsController = datasetsController;
		this.tokenResolverUtil = tokenResolverUtil;
		this.datasetConnector = datasetConnector;
	}

	public Result jsAPI(Request request) {
		Person user = getAuthenticatedUserOrReturn(request, notFound("").as(TEXT_JAVASCRIPT));

		// match dataset ID in the referer, then check dataset, project and provide extras
		ObjectNode on = null;
		Pattern p = Pattern.compile("datasets/web/(\\d+)/");
		Matcher m = p.matcher(request.header(REFERER).orElse(""));
		if (m.find()) {
			String datasetId = m.group(1);
			if (datasetId != null && !datasetId.isEmpty()) {
				Dataset ds = Dataset.find.byId(DataUtils.parseLong(datasetId));
				Project project = ds.getProject();
				if (project.belongsTo(user) || project.collaboratesWith(user)) {
					on = projectToJson(project);
				}
			}
		}

		String output = views.html.elements.api.researcherJSAPI.render(user, on != null ? on.toString() : "{}", request)
				.toString().replace("</script>", "");
		return ok(output).as(TEXT_JAVASCRIPT);
	}

	public CompletionStage<Result> getDataCSV(Request request, long id, String filterId, long limit, long start,
			long end) {
		Person user = getAuthenticatedUserOrReturn(request, notFound("Please log in first."));

		// check dataset and dataset type
		Dataset dataset = Dataset.find.byId(id);
		if (dataset == null) {
			return notFoundCS("No or invalid dataset ID given.");
		}

		dataset.getProject().refresh();
		if (!dataset.getProject().belongsTo(user) && !dataset.getProject().collaboratesWith(user)) {
			return notFoundCS("No or invalid dataset ID given.");
		}

		// create filtering cluster
		Cluster cluster = new Cluster("Cluster");
		cluster.setId(-1l);
		switch (dataset.getDsType()) {
		// devices
		case IOT:
		case TIMESERIES:
			dataset.getProject().getDevices().stream()
					.filter(d -> d.getRefId().equals(filterId) || d.getId().toString().equals(filterId))
					.forEach(d -> cluster.getDevices().add(d));
			break;
		// participants
		case ANNOTATION:
		case DIARY:
		case ES:
		case MOVEMENT:
		case MEDIA:
			dataset.getProject().getParticipants().stream()
					.filter(p -> p.getRefId().equals(filterId) || p.getId().toString().equals(filterId))
					.forEach(p -> cluster.getParticipants().add(p));
			break;
		// wearables
		case FITBIT:
		case GOOGLEFIT:
			dataset.getProject().getWearables().stream()
					.filter(w -> w.getRefId().equals(filterId) || w.getId().toString().equals(filterId))
					.forEach(w -> cluster.getWearables().add(w));
			break;
		default:
			// no filtering for all other dataset types
			break;
		}

		return datasetsController.downloadInternal(request, id, dataset, cluster, limit, start, end);
	}

	public CompletionStage<Result> getDataJson(Request request, long id, String filterId, long limit, long start,
			long end) {
		Person user = getAuthenticatedUserOrReturn(request, notFound("Please log in first."));

		// check dataset and dataset type
		Dataset dataset = Dataset.find.byId(id);
		if (dataset == null) {
			return notFoundCS("No or invalid dataset ID given.");
		}

		dataset.getProject().refresh();
		if (!dataset.getProject().belongsTo(user) && !dataset.getProject().collaboratesWith(user)) {
			return notFoundCS("No or invalid dataset ID given.");
		}

		// create filtering cluster
		Cluster cluster = new Cluster("Cluster");
		cluster.setId(-1l);
		switch (dataset.getDsType()) {
		// devices
		case IOT:
		case TIMESERIES:
			dataset.getProject().getDevices().stream()
					.filter(d -> d.getRefId().equals(filterId) || d.getId().toString().equals(filterId))
					.forEach(d -> cluster.getDevices().add(d));
			break;
		// participants
		case ANNOTATION:
		case DIARY:
		case ES:
		case MOVEMENT:
		case MEDIA:
			dataset.getProject().getParticipants().stream()
					.filter(p -> p.getRefId().equals(filterId) || p.getId().toString().equals(filterId))
					.forEach(p -> cluster.getParticipants().add(p));
			break;
		// wearables
		case FITBIT:
		case GOOGLEFIT:
			dataset.getProject().getWearables().stream()
					.filter(w -> w.getRefId().equals(filterId) || w.getId().toString().equals(filterId))
					.forEach(w -> cluster.getWearables().add(w));
			break;
		default:
			// no filtering for all other dataset types
			break;
		}

		return CompletableFuture.supplyAsync(() -> {
			return datasetsController.downloadJsonCluster(request, id, cluster, limit, start, end);
		});
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private ObjectNode projectToJson(Project project) {
		ObjectNode on = Json.newObject();

		ArrayNode participants = on.putArray("participants");
		project.getParticipants().stream().forEach(p -> {
			// collect devices for each participant
			ArrayNode participantDevices = Json.newArray();
			project.getClusters().stream().filter(c -> c.hasParticipant(p))
					.forEach(c -> c.getDevices().forEach(d -> participantDevices.add(d.getId())));

			// collect wearables for each participant
			ArrayNode participantWearables = Json.newArray();
			project.getClusters().stream().filter(c -> c.hasParticipant(p))
					.forEach(c -> c.getDevices().forEach(d -> participantWearables.add(d.getId())));

			// create participant object with list of devices and wearables and add it to list
			ObjectNode participantObject = Json.newObject().put("id", p.getId()).put("participant_id", p.getRefId())
					.put("name", p.getName()).put("pp1", p.getPublicParameter1()).put("pp2", p.getPublicParameter2())
					.put("pp3", p.getPublicParameter3());
			participantObject.set("devices", participantDevices);
			participantObject.set("wearables", participantWearables);
			participants.add(participantObject);
		});

		ArrayNode devices = on.putArray("devices");
		project.getDevices().stream().forEach(d -> {
			// collect participants for each device
			ArrayNode deviceParticipants = Json.newArray();
			project.getClusters().stream().filter(c -> c.hasDevice(d))
					.forEach(c -> c.getParticipants().forEach(p -> deviceParticipants.add(p.getId())));

			// create device object with list of participants
			devices.add(Json.newObject().put("id", d.getId()).put("device_id", d.getRefId()).put("name", d.getName())
					.put("pp1", d.getPublicParameter1()).put("pp2", d.getPublicParameter2())
					.put("pp3", d.getPublicParameter3()).set("participants", deviceParticipants));
		});

		ArrayNode wearables = on.putArray("wearables");
		project.getWearables().stream().forEach(d -> {
			// collect participants for each wearable
			ArrayNode wearableParticipants = Json.newArray();
			project.getClusters().stream().filter(c -> c.hasWearable(d))
					.forEach(c -> c.getParticipants().forEach(p -> wearableParticipants.add(p.getId())));

			// create wearable object with list of participants
			wearables.add(Json.newObject().put("id", d.getId()).put("wearable_id", d.getRefId())
					.put("name", d.getName()).put("brand", d.getBrand()).put("pp1", d.getPublicParameter1())
					.put("pp2", d.getPublicParameter2()).put("pp3", d.getPublicParameter3())
					.set("participants", wearableParticipants));
		});

		ArrayNode clusters = on.putArray("clusters");
		project.getClusters().stream().forEach(c -> {
			// collect participants, devices, and wearables
			ArrayNode cparticipants = Json.newArray();
			c.getParticipants().forEach(d -> participants.add(d.getId()));
			ArrayNode cdevices = Json.newArray();
			c.getDevices().forEach(d -> devices.add(d.getId()));
			ArrayNode cwearables = Json.newArray();
			c.getDevices().forEach(d -> wearables.add(d.getId()));

			// create cluster object
			ObjectNode cluster = Json.newObject().put("name", c.getName());
			cluster.set("participants", cparticipants);
			cluster.set("devices", cdevices);
			cluster.set("wearables", cwearables);
			clusters.add(cluster);
		});

		ArrayNode datasets = on.putArray("datasets");
		project.getDatasets().stream().forEach(d -> datasets.add(Json.newObject().put("id", d.getId())
				.put("dataset_id", d.getRefId()).put("name", d.getName()).put("type", d.getDsType().name())));

		return on;
	}
}

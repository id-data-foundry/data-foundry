package datasets;

import java.io.File;
import java.util.Date;
import java.util.Optional;

import models.Dataset;
import models.DatasetType;
import models.Project;
import models.ds.CompleteDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import utils.auth.TokenResolverUtil;
import utils.rendering.FileUtil;

public class ModelTemplates {

	public static void addProjectElements(Project project, DatasetConnector datasetConnector,
			TokenResolverUtil tokenResolverUtil, String template, File templatesDir) {

		// check the free form template option first, execute it if existing
		if (FileUtil.isFolderInFolderSafe(templatesDir, template)) {
			File templateFolder = new File(templatesDir, template);
			project.setDescription("Created from template: " + template);
			project.update();

			for (File datasetFolder : templateFolder.listFiles(File::isDirectory)) {
				// Create dataset
				Dataset ds = datasetConnector.create(datasetFolder.getName(), DatasetType.COMPLETE, project,
						"Template dataset: " + datasetFolder.getName(), "", null, null);
				ds.save();
				project.getDatasets().add(ds);

				// Add files
				CompleteDS cds = datasetConnector.getTypedDatasetDS(ds);
				for (File file : datasetFolder.listFiles(f -> f.isFile())) {
					cds.storeFile(file, file.getName());
					cds.addRecord(file.getName(), "Initial file from template", new Date());
				}
			}
			return;
		}

		// otherwise use the existing templates
		switch (template) {
		case "wearable": {
			Dataset ds1 = datasetConnector.create("Fitbit wearables", DatasetType.FITBIT, project,
					"TEMPLATE: Participants' Fitbit wearables will record their data into this dataset (please replace with your dataset description)",
					"steps settings", null, null);
			ds1.save();
			Dataset ds2 = datasetConnector.create("GoogleFit wearables", DatasetType.GOOGLEFIT, project,
					"TEMPLATE: Participants' GoogleFit wearables will record their data into this dataset (please replace with your dataset description)",
					"step_count openid", null, null);
			ds2.save();
			project.getDatasets().add(ds1);
			project.getDatasets().add(ds2);

			for (int i = 0; i < 10; i++) {
				Participant p = Participant.createInstance("Participant " + (i + 1), "<fill in>", "<fill in>", project);
				p.save();

				Wearable w1 = new Wearable();
				w1.create();
				w1.setName(p.getName() + " Fitbit");
				w1.setBrand(Wearable.FITBIT);
				w1.setProject(project);
				w1.setScopes(ds1.getId() + "");
				w1.save();

				Wearable w2 = new Wearable();
				w2.create();
				w2.setName(p.getName() + " GoogleFit");
				w2.setBrand(Wearable.GOOGLEFIT);
				w2.setProject(project);
				w2.setScopes(ds2.getId() + "");
				w2.save();

				Cluster c = new Cluster(p.getName());
				c.setProject(project);
				c.save();
				c.add(p);
				c.add(w1);
				c.add(w2);
				c.refresh();

				project.getParticipants().add(p);
				project.getWearables().add(w1);
				project.getWearables().add(w2);
				project.getClusters().add(c);
			}
			project.setDescription(
					"TEMPLATE: Wearable project with one Fitbit dataset, one GoogleFit dataset, and 10 participants and their wearables. Both datasets are for steps data, if you need other scopes, please create a new one. (please replace with your project description)");
			project.update();
			break;
		}
		case "remote_study": {
			Dataset ds1 = datasetConnector.create("Web prototype", DatasetType.COMPLETE, project,
					"TEMPLATE: Web prototype files will be hosted in this dataset (please replace with your dataset description)",
					"", null, null);
			ds1.save();
			Dataset ds2 = datasetConnector.create("Diary dataset", DatasetType.DIARY, project,
					"TEMPLATE: Participants experiences with the web prototype will be recorded into this dataset (please replace with your dataset description)",
					"", null, null);
			ds2.save();
			project.getDatasets().add(ds1);
			project.getDatasets().add(ds2);

			for (int i = 0; i < 10; i++) {
				Participant p = Participant.createInstance("Participant " + (i + 1), "<fill in>", "<fill in>", project);
				p.save();

				Device d1 = new Device();
				d1.create();
				d1.setName(p.getName() + " web prototype");
				d1.setProject(project);
				d1.save();

				Cluster c = new Cluster(p.getName());
				c.setProject(project);
				c.save();
				c.add(p);
				c.add(d1);

				project.getParticipants().add(p);
				project.getDevices().add(d1);
				project.getClusters().add(c);
			}

			project.setDescription(
					"TEMPLATE: Remote study with digital prototype amd 10 participants. (please replace with your project description)");
			project.update();
			break;
		}
		case "iot_devices": {
			Dataset ds1 = datasetConnector.create("IoT device data", DatasetType.IOT, project,
					"TEMPLATE: Data from IoT devices in this project will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds1.save();
			project.getDatasets().add(ds1);

			for (int i = 0; i < 10; i++) {
				Device d1 = new Device();
				d1.create();
				d1.setName("Device " + (i + 1));
				d1.setProject(project);
				d1.save();

				project.getDevices().add(d1);
			}

			project.setDescription(
					"TEMPLATE: IoT project with 10 devices. (please replace with your project description)");
			project.update();
			break;
		}
		case "mss_datalogger": {
			Dataset ds1 = datasetConnector.create("Data logger", DatasetType.IOT, project,
					"TEMPLATE: This dataset is meant to collect data from the data logger.", "", null, null);
			ds1.setOpenParticipation(true);
			ds1.save();
			// auto-generate the API token for sending data to the dataset
			ds1.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds1.getId()));
			ds1.update();
			project.getDatasets().add(ds1);

			Dataset ds2 = datasetConnector.create("Notebooks", DatasetType.COMPLETE, project,
					"TEMPLATE: This dataset contains Starboard notebooks to work with the collected data", "", null,
					null);
			ds2.save();
			project.getDatasets().add(ds2);

			Device d1 = new Device();
			d1.create();
			d1.setName("Datalogger");
			d1.setProject(project);
			d1.save();
			project.getDevices().add(d1);

			// add file to dataset
			Date now = new Date();
			CompleteDS cds = datasetConnector.getTypedDatasetDS(ds2);
			Optional<String> nbNameOpt1 = cds.createNotebookFile("DAB100-DVS.gg", now,
					views.html.elements.project.templates.mss.DAB100_DVS.render().toString().split("\n"));
			if (nbNameOpt1.isPresent()) {
				cds.addRecord(nbNameOpt1.get(), "DAB100 Starboard notebook: Data Visualisation", now);
			}
			Optional<String> nbNameOpt2 = cds.createNotebookFile("DAB100-DCA.gg", now,
					views.html.elements.project.templates.mss.DAB100_DCA.render().toString().split("\n"));
			if (nbNameOpt2.isPresent()) {
				cds.addRecord(nbNameOpt2.get(), "DAB100 Starboard notebook: Data Cleaning and Aggregation", now);
			}
			Optional<String> nbNameOpt3 = cds.createNotebookFile("DAB100-LECTURE.gg", now,
					views.html.elements.project.templates.mss.DAB100_LECTURE.render().toString().split("\n"));
			if (nbNameOpt3.isPresent()) {
				cds.addRecord(nbNameOpt3.get(), "DAB100 Starboard notebook: Lecture Example", now);
			}

			project.setDescription("TEMPLATE: Making Sense of Sensors project");
			project.update();
			break;
		}
		case "diary_study": {
			Dataset ds1 = datasetConnector.create("Diary data", DatasetType.DIARY, project,
					"TEMPLATE: Diary entries from participants in this project will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds1.save();
			Dataset ds2 = datasetConnector.create("Media data", DatasetType.MEDIA, project,
					"TEMPLATE: Media data like photo will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds2.save();
			project.getDatasets().add(ds1);
			project.getDatasets().add(ds2);

			for (int i = 0; i < 10; i++) {
				Participant p = Participant.createInstance("Participant " + (i + 1), "<fill in>", "<fill in>", project);
				p.save();

				project.getParticipants().add(p);
			}

			project.setDescription(
					"TEMPLATE: Qualitative research project with 10 participants. (please replace with your project description)");
			project.update();
			break;
		}
		case "ded_contextual": {
			Dataset ds0 = datasetConnector.create("IoT device data", DatasetType.IOT, project,
					"TEMPLATE: Data from IoT devices in this project will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds0.save();
			Dataset ds1 = datasetConnector.create("Diary data", DatasetType.DIARY, project,
					"TEMPLATE: Diary entries from participants in this project will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds1.save();
			Dataset ds2 = datasetConnector.create("Media data", DatasetType.MEDIA, project,
					"TEMPLATE: Media data like photo will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds2.save();
			project.getDatasets().add(ds0);
			project.getDatasets().add(ds1);
			project.getDatasets().add(ds2);

			for (int i = 0; i < 10; i++) {
				Participant p = Participant.createInstance("Participant " + (i + 1), "<fill in>", "<fill in>", project);
				p.save();

				Device d1 = new Device();
				d1.create();
				d1.setName(p.getName() + " device 1");
				d1.setProject(project);
				d1.save();

				Device d2 = new Device();
				d2.create();
				d2.setName(p.getName() + " device 2");
				d2.setProject(project);
				d2.save();

				Cluster c = new Cluster(p.getName());
				c.setProject(project);
				c.save();
				c.add(p);
				c.add(d1);
				c.add(d2);

				project.getParticipants().add(p);
				project.getDevices().add(d1);
				project.getDevices().add(d2);
				project.getClusters().add(c);
			}

			project.setDescription(
					"TEMPLATE: Data-enabled Design project (contextual step) with 10 participants and 20 devices. (please replace with your project description)");
			project.update();
			break;
		}
		case "ded_informed": {
			Dataset ds0 = datasetConnector.create("IoT device data", DatasetType.IOT, project,
					"TEMPLATE: Data from IoT devices in this project will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds0.save();
			Dataset ds1 = datasetConnector.create("Diary data", DatasetType.DIARY, project,
					"TEMPLATE: Diary entries from participants in this project will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds1.save();
			Dataset ds2 = datasetConnector.create("Media data", DatasetType.MEDIA, project,
					"TEMPLATE: Media data like photo will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds2.save();
			Dataset ds3 = datasetConnector.create("Scripting", DatasetType.COMPLETE, project,
					"TEMPLATE: Script reacting to events in this project will be stored in this dataset (please replace with your dataset description)",
					"", null, null);
			ds3.setCollectorType(Dataset.ACTOR);
			ds3.save();
			project.getDatasets().add(ds0);
			project.getDatasets().add(ds1);
			project.getDatasets().add(ds2);
			project.getDatasets().add(ds3);

			for (int i = 0; i < 10; i++) {
				Participant p = Participant.createInstance("Participant " + (i + 1), "<fill in>", "<fill in>", project);
				p.save();

				Device d1 = new Device();
				d1.create();
				d1.setName(p.getName() + " device 1");
				d1.setProject(project);
				d1.save();

				Device d2 = new Device();
				d2.create();
				d2.setName(p.getName() + " device 2");
				d2.setProject(project);
				d2.save();

				Cluster c = new Cluster(p.getName());
				c.setProject(project);
				c.save();
				c.add(p);
				c.add(d1);
				c.add(d2);

				project.getParticipants().add(p);
				project.getDevices().add(d1);
				project.getDevices().add(d2);
				project.getClusters().add(c);
			}

			project.setDescription(
					"TEMPLATE: Data-enabled Design project (informed step) with 10 participants, 20 devices and scripting. (please replace with your project description)");
			project.update();
			break;
		}
		}
	}

}

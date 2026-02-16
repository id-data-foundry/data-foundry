package utils.admin;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import datasets.DatasetConnector;
import io.ebean.DB;
import io.ebean.Database;
import io.ebean.Transaction;
import models.Analytics;
import models.Collaboration;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.Subscription;
import models.ds.AnnotationDS;
import models.ds.EntityDS;
import models.ds.MovementDS;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.ParticipationStatus;
import models.sr.Wearable;
import play.libs.Json;
import utils.DateUtils;
import utils.auth.TokenResolverUtil;

public class DeveloperDataUtil {

	/**
	 * setup a clean DEV DB
	 * 
	 * @param datasetConnector
	 * @param tokenResolverUtil
	 * @param platformUtil
	 */
	public void initializeDevelopmentDB(DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil) {

		// get the start of today
		Date today = DateUtils.startOfDay(new Date());

		// past start date for some projects
		Calendar nowMinus70 = new GregorianCalendar();
		nowMinus70.setTime(today);
		nowMinus70.add(Calendar.DAY_OF_YEAR, -70);

		Calendar nowMinus14 = new GregorianCalendar();
		nowMinus14.setTime(today);
		nowMinus14.add(Calendar.DAY_OF_YEAR, -14);

		Calendar nowMinus5 = new GregorianCalendar();
		nowMinus5.setTime(today);
		nowMinus5.add(Calendar.DAY_OF_YEAR, -5);

		Calendar nowPlus2 = new GregorianCalendar();
		nowPlus2.setTime(today);
		nowPlus2.add(Calendar.DAY_OF_YEAR, 2);

		Calendar nowPlus5 = new GregorianCalendar();
		nowPlus5.setTime(today);
		nowPlus5.add(Calendar.DAY_OF_YEAR, 5);

		Calendar nowPlus7 = new GregorianCalendar();
		nowPlus7.setTime(today);
		nowPlus7.add(Calendar.DAY_OF_YEAR, 7);

		Calendar nowPlus10 = new GregorianCalendar();
		nowPlus10.setTime(today);
		nowPlus10.add(Calendar.DAY_OF_YEAR, 10);

		// create mock users centrally because they are reused and connect different mock projects
		Person bob = new Person();
		bob.setUser_id("");
		bob.setFirstname("Bob");
		bob.setLastname("Datauser");
		bob.setEmail("bob@research.org");
		bob.setWebsite("www.bob.idv");
		bob.setPassword("bob");
		bob.save();
		bob.setIdentityProperty("userIntro",
		        "Hey, I'm Bob, an Industrial Design student with many different interests and great sketching skills. I have a passion for something and something else, and also really dig vague sentences.");

		Person mary = new Person();
		mary.setUser_id("");
		mary.setFirstname("Mary R.E.");
		mary.setLastname("Search");
		mary.setEmail("mary@research.org");
		mary.setWebsite("www.mary.idv");
		mary.setPassword("mary");
		mary.save();
		mary.setIdentityProperty("userIntro",
		        "My name is Mary. I like cats and I make their food at home with a vertical farm.");

		Person john = new Person();
		john.setUser_id("");
		john.setFirstname("John");
		john.setLastname("Mayer");
		john.setEmail("john@research.org");
		john.setWebsite("www.john.idv");
		john.setPassword("john");
		john.save();
		john.setIdentityProperty("userIntro",
		        "Greetings, I'm John. I don't like to say too much here, but I'm quite social and like to eat gummy bears.");

		Person alice = new Person();
		alice.setUser_id("");
		alice.setFirstname("Alice D.");
		alice.setLastname("Student");
		alice.setEmail("alice@student.tue.nl");
		alice.setWebsite("www.alice.co.uk");
		alice.setPassword("alice");
		alice.save();
		alice.setIdentityProperty("userIntro", "It's Alice!");

		Person frank_prince = new Person();
		frank_prince.setUser_id("");
		frank_prince.setFirstname("Frank");
		frank_prince.setLastname("Prince");
		frank_prince.setEmail("frank@prince.org");
		frank_prince.setWebsite("www.frank.idv");
		frank_prince.setPassword("frank");
		frank_prince.setLastAction(nowMinus70.getTime());
		frank_prince.save();
		frank_prince.setIdentityProperty("userIntro", "I'm Frank, a regular user.");

		// add projects
		createProject1(datasetConnector, tokenResolverUtil, bob, mary, john, alice);
		createProject2(datasetConnector, tokenResolverUtil, mary, bob, alice);
		createProject3(datasetConnector, bob, alice, mary);
		createProject4(datasetConnector, tokenResolverUtil, john, bob, alice);
		createProject5(datasetConnector, tokenResolverUtil, alice, john, mary);
		createProject6(datasetConnector, tokenResolverUtil, bob);
		createProject7(datasetConnector, tokenResolverUtil, bob);
	}

	/**
	 * create the first mock project for bob as owner and other persons as collaborators or subscribers
	 * 
	 * @param tokenResolverUtil
	 * 
	 * @param bob
	 * @param mary
	 * @param john
	 * @param alice
	 */
	private void createProject1(DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil, Person bob,
	        Person mary, Person john, Person alice) {

		// get the start of today
		Date today = DateUtils.startOfDay(new Date());

		Calendar nowMinus14 = new GregorianCalendar();
		nowMinus14.setTime(today);
		nowMinus14.add(Calendar.DAY_OF_YEAR, -14);
		Calendar nowMinus5 = new GregorianCalendar();
		nowMinus5.setTime(today);
		nowMinus5.add(Calendar.DAY_OF_YEAR, -5);

		Calendar nowPlus2 = new GregorianCalendar();
		nowPlus2.setTime(today);
		nowPlus2.add(Calendar.DAY_OF_YEAR, 2);
		Calendar nowPlus5 = new GregorianCalendar();
		nowPlus5.setTime(today);
		nowPlus5.add(Calendar.DAY_OF_YEAR, 5);
		Calendar nowPlus7 = new GregorianCalendar();
		nowPlus7.setTime(today);
		nowPlus7.add(Calendar.DAY_OF_YEAR, 7);

		// ------------------------------------------------------------------------------------------------------------

		// create(String project_name, Person owner, String intro, boolean publicProject, boolean shareableProject)
		Project p1 = Project.create("Movement behavior", bob,
		        "A comprehensive longitudinal study on human movement behavior in urban environments. We track participants' daily routines, combining qualitative diary entries with quantitative sensor data from wearables (Fitbit, Google Fit) and stationary IoT devices monitoring environmental conditions (PM2.5, Light). The goal is to correlate physical activity with environmental quality and subjective well-being. By integrating diverse data sources, we aim to understand how urban design influences lifestyle choices and mobility patterns.",
		        true, false);
		p1.setKeywords("movement, running, jogging, casually schlepping");
		p1.setLicense("MIT");
		p1.setRelation("Movement behavior research institute");
		p1.setOrganization("Industrial Design department, TU/e");
		p1.setDoi("https://doi.something.org/");
		p1.save();

		// ------------------------------------------------------------------------------------------------------------

		// add datasets
		Dataset ds1_1 = datasetConnector.create("Diary data 1-1", DatasetType.DIARY, p1,
		        "Daily qualitative logs from participants describing their physical activities, locations visited, and subjective feelings of energy and fatigue. Used to contextualize the sensor data.",
		        "all participants", "", null);
		ds1_1.setStart(nowMinus14.getTime());
		ds1_1.setEnd(DateUtils.endOfDay(nowPlus2.getTime()));
		ds1_1.save();

		Dataset ds1_2 = datasetConnector.create("IOT data 1-2", DatasetType.IOT, p1,
		        "Environmental sensor readings from stationary home nodes. Captures Particulate Matter (PM2.5), ambient light levels, and a generic 'value1' sensor for calibration. Data is used to assess indoor air quality during movement intervals.",
		        "air quality data", "", null);
		ds1_2.getConfiguration().put("OOCSI_channel", "ds1_iot-data");
		ds1_2.getConfiguration().put(Dataset.DATA_PROJECTION, "value1,pm25,light");
		ds1_2.setStart(nowMinus14.getTime());
		ds1_2.setEnd(DateUtils.endOfDay(nowPlus7.getTime()));
		ds1_2.save();
		// auto-generate the API token for sending data to the dataset
		ds1_2.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds1_2.getId()));
		ds1_2.update();

		Dataset ds1_3 = datasetConnector.create("Cluster annotations 1-3", DatasetType.ANNOTATION, p1,
		        "Expert annotations of specific time clusters. Researchers mark periods of 'home life', 'cooking', or 'resting' based on the aggregated sensor streams to provide ground truth for activity recognition algorithms.",
		        "home life annotations", null, null);
		ds1_3.setStart(nowMinus14.getTime());
		ds1_3.setRelation("APItesting_rel");
		ds1_3.save();

		Dataset ds1_4 = datasetConnector.create("FITBIT dataset 1-4", DatasetType.FITBIT, p1,
		        "Synced step count and activity intensity data from Fitbit wearables. Provides a baseline for daily physical exertion and movement patterns over the study period.",
		        "activity steps", "", null);
		ds1_4.setStart(nowMinus5.getTime());
		ds1_4.setEnd(DateUtils.endOfDay(nowPlus2.getTime()));
		ds1_4.setOrganization("APItesting_org");
		ds1_4.save();

		Dataset ds1_5 = datasetConnector.create("GoogleFit dataset 1-5", DatasetType.GOOGLEFIT, p1,
		        "Aggregated health metrics from Google Fit, including heart rate variability, step counts, and sleep duration. This dataset offers a holistic view of the participants' physiological state.",
		        "activity heart_rate step_count sleep", "", null);
		ds1_5.setStart(nowMinus5.getTime());
		ds1_5.setEnd(DateUtils.endOfDay(nowPlus2.getTime()));
		ds1_5.save();

		Dataset ds1_6 = datasetConnector.create("www", DatasetType.COMPLETE, p1,
		        "Project website assets and hosting files. Includes the landing page for participant recruitment, informed consent forms, and downloadable summaries of the research findings.",
		        "all files, e.g., for testing file hosting stuff and the project website", "", null);
		ds1_6.setStart(nowMinus14.getTime());
		ds1_6.setEnd(DateUtils.endOfDay(nowPlus5.getTime()));
		ds1_6.save();

		Dataset ds1_7 = datasetConnector.create("Entity dataset 1-7", DatasetType.ENTITY, p1,
		        "Participant and Device metadata registry. Stores static profile information, device IDs, and configuration parameters (e.g., 'light' sensitivity settings) for the deployed hardware.",
		        "user profile testing", "", null);
		ds1_7.setStart(nowMinus14.getTime());
		ds1_7.setEnd(DateUtils.endOfDay(nowPlus5.getTime()));
		ds1_7.save();

		Dataset ds1_8 = datasetConnector.create("Movement dataset 1-8", DatasetType.MOVEMENT, p1,
		        "High-frequency raw accelerometer and gyroscope data. Captures precise movement vectors (x, y, z) to analyze gait and posture during specific activities like running or jogging.",
		        "user profile testing", "", null);
		ds1_8.setStart(nowMinus14.getTime());
		ds1_8.setEnd(DateUtils.endOfDay(nowPlus5.getTime()));
		ds1_8.save();

		// ------------------------------------------------------------------------------------------------------------

		Device device1 = new Device();
		device1.setName("wake-up lamp 1");
		device1.setPublicParameter1("control");
		device1.setProject(p1);
		device1.create();
		Device device2 = new Device();
		device2.setName("wake-up lamp 2");
		device2.setPublicParameter1("experiment");
		device2.setProject(p1);
		device2.create();

		// ------------------------------------------------------------------------------------------------------------

		Participant participant1 = new Participant("John", "Doe");
		participant1.setEmail("joe@df.com");
		participant1.setProject(p1);
		participant1.setStatus(ParticipationStatus.ACCEPT);
		participant1.create();

		Participant participant2 = new Participant("Joanna", "Doe");
		participant2.setEmail("joanna@df.com");
		participant2.setProject(p1);
		participant2.setStatus(ParticipationStatus.ACCEPT);
		participant2.create();

		// ------------------------------------------------------------------------------------------------------------

		Wearable wearable = new Wearable();
		wearable.setName("fitbit1");
		wearable.setBrand(Wearable.FITBIT);
		wearable.setProject(p1);
		// wearable.setdataset_id( ds1_4.id;
		wearable.setScopes(Long.toString(ds1_4.getId()));
		wearable.setExpiry(DateUtils.getMillisFromDS(ds1_4.getId())[0]);
		wearable.create();

		Wearable wearable_gfit = new Wearable();
		wearable_gfit.setName("googlefit1");
		wearable_gfit.setBrand(Wearable.GOOGLEFIT);
		wearable_gfit.setProject(p1);
		// wearable_gfit.setdataset_id( ds1_4.id;
		wearable_gfit.setScopes(Long.toString(ds1_5.getId()));
		wearable_gfit.setExpiry(DateUtils.getMillisFromDS(ds1_5.getId())[0]);
		wearable_gfit.create();

		// add cluster
		Cluster cluster = new Cluster("family one");
		cluster.setProject(p1);
		cluster.create();

		// update cluster
		cluster.getWearables().add(wearable);
		cluster.getWearables().add(wearable_gfit);
		cluster.getDevices().add(device2);
		cluster.getParticipants().add(participant1);
		// cluster.save();

		// ------------------------------------------------------------------------------------------------------------

		// add collaborator
		Collaboration c1_1 = new Collaboration(mary, p1);
		c1_1.save();
		Collaboration c1_2 = new Collaboration(john, p1);
		c1_2.save();

		// ------------------------------------------------------------------------------------------------------------

		// add subscriber
		Subscription s1_1 = new Subscription(alice, p1);
		s1_1.save();
		// update project info
		p1.getDatasets().add(ds1_1);
		p1.getDatasets().add(ds1_2);
		p1.getDatasets().add(ds1_3);
		p1.getDatasets().add(ds1_4);
		p1.getDatasets().add(ds1_5);
		p1.getDatasets().add(ds1_6);
		p1.getDatasets().add(ds1_7);
		p1.getCollaborators().add(c1_1);
		p1.getCollaborators().add(c1_2);
		p1.getSubscribers().add(s1_1);
		p1.getParticipants().add(participant1);
		p1.getParticipants().add(participant2);
		p1.getClusters().add(cluster);
		p1.getDevices().add(device1);
		p1.getDevices().add(device2);
		p1.getWearables().add(wearable);
		p1.getWearables().add(wearable_gfit);
		p1.update();

		// ------------------------------------------------------------------------------------------------------------

		Calendar cal = new GregorianCalendar();
		MovementDS mvds = (MovementDS) datasetConnector.getDatasetDS(ds1_8);
		mvds.addMovement(participant1, 0.4f, 0.7f, 1.2f);

		TimeseriesDS tsdf = (TimeseriesDS) datasetConnector.getDatasetDS(ds1_2);
		tsdf.internalAddRecord(device1, cal.getTime(), "cooking",
		        Json.newObject().put("value1", 70).put("pm25", 26).put("light", 71.6).toString());
		cal.add(Calendar.HOUR, -2);
		tsdf.internalAddRecord(device1, cal.getTime(), "cooking",
		        Json.newObject().put("value1", 80).put("pm25", 76).put("light", 31.6).toString());
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsdf.internalAddRecord(device1, cal.getTime(), "washing",
		        Json.newObject().put("value1", 72).put("pm25", 12).put("light", 50.1).toString());
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsdf.internalAddRecord(device1, cal.getTime(), "cleaning",
		        Json.newObject().put("value1", 86).put("pm25", 120).put("light", 50.7).toString());
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsdf.internalAddRecord(device2, cal.getTime(), "cooking",
		        Json.newObject().put("value1", 82).put("pm25", 40).put("light", 40.7).toString());
		tsdf.internalAddRecord(device1, cal.getTime(), "sleeping",
		        Json.newObject().put("value1", 81).put("pm25", 100).put("light", 40.6).toString());
		cal.add(Calendar.HOUR, -1);
		tsdf.internalAddRecord(device2, cal.getTime(), "sleeping",
		        Json.newObject().put("value1", 81).put("pm25", 67).put("light", 40.3).toString());
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.HOUR, -1);
		tsdf.internalAddRecord(device2, cal.getTime(), "cooking",
		        Json.newObject().put("value1", 58).put("pm25", 40).put("light", 30.6).toString());
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsdf.internalAddRecord(device1, cal.getTime(), "cleaning",
		        Json.newObject().put("value1", 67).put("pm25", 30).put("light", 40.6).toString());
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsdf.internalAddRecord(device1, cal.getTime(), "cleaning",
		        Json.newObject().put("value1", 67).put("pm25", 60).put("light", 60.6).toString());
		tsdf.internalAddRecord(device1, cal.getTime(), "washing",
		        Json.newObject().put("value1", 68).put("pm25", 80).put("light", 70.6).toString());
		cal.add(Calendar.HOUR, -1);
		tsdf.internalAddRecord(device1, cal.getTime(), "washing",
		        Json.newObject().put("value1", 82).put("pm25", 10).put("light", 40.6).toString());
		cal.add(Calendar.HOUR, -1);
		tsdf.internalAddRecord(device1, cal.getTime(), "washing",
		        Json.newObject().put("value1", 87).put("pm25", 50).put("light", 50.6).toString());
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsdf.internalAddRecord(device2, cal.getTime(), "cooking",
		        Json.newObject().put("value1", 83).put("light", 0.6).toString());
		tsdf.internalAddRecord(device1, cal.getTime(), "sleeping",
		        Json.newObject().put("value1", 40).put("pm25", 100).toString());

		// annotation
		AnnotationDS ads = (AnnotationDS) datasetConnector.getDatasetDS(ds1_3);
		ads.addRecord(cluster, cal.getTime(), "first annotation", "longer annotation text, spilling the beans.");

		// create Entity data - part 3
		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds1_7);
		for (int i = 0; i < 5; i++) {
			eds.addItem(device1.getRefId() + i, Optional.of(device1.getRefId()),
			        "{\"idnumber\": 1,\"name\":\"device1\",\"light\":5.3}");
			eds.addItem(device2.getRefId() + i, Optional.of(device2.getRefId()),
			        "{\"idnumber\": 2,\"name\":\"device2\",\"light\":4.5}");
			eds.addItem(participant1.getRefId() + i, Optional.of(participant1.getRefId()),
			        "{\"idnumber\": 3,\"name\":\"participant\",\"light\":6.4}");
			eds.addItem(participant2.getRefId() + i, Optional.of(participant2.getRefId()),
			        "{\"idnumber\": 4,\"name\":\"participante\",\"light\":7.8}");
		}
		for (int i = 0; i < 10000; i++) {
			eds.updateItem(participant1.getRefId(), Optional.of(participant1.getRefId()),
			        "{\"idnumber\": 4,\"name\":\"participante\",\"light\":" + (7.8 + i) + "}");
		}
		ds1_7.getConfiguration().put(Dataset.DATA_PROJECTION, "idnumber,name,light");
		ds1_7.update();

		// ------------------------------------------------------------------------------------------------------------

		LabNotesEntry.log(Project.class, LabNotesEntryType.CREATE, "create", p1);
		LabNotesEntry.log(Project.class, LabNotesEntryType.CONFIGURE, "configure", p1);
		LabNotesEntry.log(Project.class, LabNotesEntryType.DATA, "data", p1);
		LabNotesEntry.log(Project.class, LabNotesEntryType.DELETE, "delete", p1);
		LabNotesEntry.log(Project.class, LabNotesEntryType.DOWNLOAD, "download", p1);
	}

	/**
	 * 
	 * @param mary
	 * @param bob
	 * @param alice
	 */
	private void createProject2(DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil, Person mary,
	        Person bob, Person alice) {

		// get the start of today
		Date today = DateUtils.startOfDay(new Date());

		Calendar nowMinus14 = new GregorianCalendar();
		nowMinus14.setTime(today);
		nowMinus14.add(Calendar.DAY_OF_YEAR, -14);

		Calendar nowPlus10 = new GregorianCalendar();
		nowPlus10.setTime(today);
		nowPlus10.add(Calendar.DAY_OF_YEAR, 10);

		// ------------------------------------------------------------------------------------------------------------

		// create(String project_name, Person owner, String intro, boolean publicProject, boolean shareableProject)
		Project p2 = Project.create("Satisfaction study", mary,
		        "We study the behavior of monkeys when they get old and stop liking bananas. Posuere lorem ipsum dolor sit amet. Ut etiam sit amet nisl purus in. Gravida quis blandit turpis cursus in hac. Porta lorem mollis aliquam ut. Egestas integer eget aliquet nibh praesent tristique magna. Ullamcorper morbi tincidunt ornare massa eget egestas purus viverra accumsan. Turpis massa tincidunt dui ut ornare. Sit amet consectetur adipiscing elit pellentesque habitant morbi tristique. Risus feugiat in ante metus. Orci dapibus ultrices in iaculis nunc sed augue. Aliquam ut porttitor leo a diam sollicitudin tempor id. Justo donec enim diam vulputate ut pharetra sit amet aliquam. Lacus luctus accumsan tortor posuere ac ut consequat. Ultrices mi tempus imperdiet nulla malesuada pellentesque elit eget. Quisque egestas diam in arcu cursus euismod quis. Egestas quis ipsum suspendisse ultrices gravida dictum fusce ut placerat. Consectetur adipiscing elit ut aliquam.",
		        true, true);
		p2.setKeywords("joy, happiness, golden faceshine, more joy");
		p2.setLicense("CC BY-SA");
		p2.setRelation("Satisfaction Inc.");
		p2.setOrganization("Industrial Design department, TU/e");
		p2.setDoi("https://doi.something.org/");
		p2.save();

		// ------------------------------------------------------------------------------------------------------------

		Dataset ds2_1 = datasetConnector.create("IOT data 2-1", DatasetType.IOT, p2,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "5000 lucky items", null, null);
		ds2_1.getConfiguration().put(Dataset.DATA_PROJECTION, "value1,pm25,light,value2");
		ds2_1.setStart(nowMinus14.getTime());
		ds2_1.setRelation("APItesting_rel");
		ds2_1.setOrganization("APItesting_org");
		ds2_1.setStart(nowMinus14.getTime());
		ds2_1.setEnd(DateUtils.endOfDay(nowPlus10.getTime()));
		ds2_1.save();

		// create token for dataset + auto-generate the API token for sending data to the dataset
		ds2_1.refresh();
		ds2_1.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds2_1.getId()));
		ds2_1.getConfiguration().put(Dataset.PUBLIC_ACCESS_TOKEN, tokenResolverUtil.getDatasetToken(ds2_1.getId()));
		ds2_1.update();

		// ------------------------------------------------------------------------------------------------------------

		Device device3 = new Device();
		device3.setName("wake-up lamp 1");
		device3.setPublicParameter1("control");
		device3.setProject(p2);
		device3.create();
		Device device4 = new Device();
		device4.setName("wake-up lamp 2");
		device4.setPublicParameter1("experiment");
		device4.setProject(p2);
		device4.create();

		// ------------------------------------------------------------------------------------------------------------

		Subscription s2_1 = new Subscription(bob, p2);
		s2_1.save();
		Subscription s2_2 = new Subscription(alice, p2);
		s2_2.save();

		// ------------------------------------------------------------------------------------------------------------

		p2.getDatasets().add(ds2_1);
		p2.getDevices().add(device3);
		p2.getDevices().add(device4);
		p2.getSubscribers().add(s2_1);
		p2.getSubscribers().add(s2_2);
		p2.update();

		// ------------------------------------------------------------------------------------------------------------

		// create IoT data
		Calendar cal = new GregorianCalendar();
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds2_1);
		for (int i = 0; i < 1500; i++) {
			tsds.internalAddRecord(device3, cal.getTime(), "stop", "{\"value1\": 72, \"value2\": 11}");
			cal.add(Calendar.HOUR, -1);
			tsds.internalAddRecord(device4, cal.getTime(), "drive", "{\"value1\": 80, \"value2\": 12}");
			cal.add(Calendar.DAY_OF_YEAR, -1);
			tsds.internalAddRecord(device3, cal.getTime(), "stop", "{\"value1\": 72, \"value2\": 13}");
			cal.add(Calendar.HOUR, 1);
			cal.add(Calendar.DAY_OF_YEAR, -1);
			tsds.internalAddRecord(device3, cal.getTime(), "turn", "{\"value1\": 86, \"value2\": 14}");
			cal.add(Calendar.HOUR, -1);
			cal.add(Calendar.HOUR, (int) -Math.round(Math.random() * 15));
			cal.add(Calendar.DAY_OF_YEAR, (int) -Math.round(Math.random() * 5));
			tsds.internalAddRecord(device4, cal.getTime(), "drive", "{\"value1\": 82, \"value2\": 15}");
		}
		for (int i = 0; i < 1500; i++) {
			tsds.internalAddRecord(device3, cal.getTime(), "stop", "{\"value1\": 72}");
			cal.add(Calendar.HOUR, -1);
			tsds.internalAddRecord(device3, cal.getTime(), "drive", "{\"value1\": 80}");
			cal.add(Calendar.DAY_OF_YEAR, -1);
			tsds.internalAddRecord(device4, cal.getTime(), "stop", "{\"value1\": 72}");
			cal.add(Calendar.HOUR, 1);
			cal.add(Calendar.DAY_OF_YEAR, -1);
			tsds.internalAddRecord(device3, cal.getTime(), "turn", "{\"value1\": 86}");
			cal.add(Calendar.HOUR, -1);
			cal.add(Calendar.HOUR, (int) -Math.round(Math.random() * 15));
			cal.add(Calendar.DAY_OF_YEAR, (int) -Math.round(Math.random() * 5));
			tsds.internalAddRecord(device4, cal.getTime(), "drive", "{\"value1\": 82}");
		}

		// ------------------------------------------------------------------------------------------------------------

		LabNotesEntry.log(Project.class, LabNotesEntryType.APPROVE, "project reviewed and approved", p2);
	}

	/**
	 * 
	 * @param bob
	 * @param alice
	 * @param mary
	 */
	private void createProject3(DatasetConnector datasetConnector, Person bob, Person alice, Person mary) {

		// get the start of today
		Date today = DateUtils.startOfDay(new Date());

		Calendar nowMinus14 = new GregorianCalendar();
		nowMinus14.setTime(today);
		nowMinus14.add(Calendar.DAY_OF_YEAR, -14);

		Calendar nowPlus2 = new GregorianCalendar();
		nowPlus2.setTime(today);
		nowPlus2.add(Calendar.DAY_OF_YEAR, 2);

		Calendar nowPlus7 = new GregorianCalendar();
		nowPlus7.setTime(today);
		nowPlus7.add(Calendar.DAY_OF_YEAR, 7);

		Calendar nowPlus10 = new GregorianCalendar();
		nowPlus10.setTime(today);
		nowPlus10.add(Calendar.DAY_OF_YEAR, 10);

		// ------------------------------------------------------------------------------------------------------------

		// create(String project_name, Person owner, String intro, boolean publicProject, boolean shareableProject)
		Project p3 = Project.create("Sugar-high study", bob,
		        "We study sugar addiction amongst first year students with mild autistic disorders and bad teeth. Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Imperdiet nulla malesuada pellentesque elit eget gravida cum sociis natoque. Ac turpis egestas maecenas pharetra. Et tortor at risus viverra. Vestibulum lectus mauris ultrices eros. Porta nibh venenatis cras sed felis. Etiam sit amet nisl purus in mollis nunc. Id faucibus nisl tincidunt eget. Et sollicitudin ac orci phasellus. Mattis pellentesque id nibh tortor id aliquet lectus proin. Dolor sit amet consectetur adipiscing elit pellentesque. Elementum curabitur vitae nunc sed velit dignissim. Arcu non sodales neque sodales ut etiam sit amet nisl. Aliquam faucibus purus in massa tempor nec. Eleifend quam adipiscing vitae proin sagittis nisl. Eu lobortis elementum nibh tellus molestie nunc non blandit massa.",
		        true, false);
		p3.setKeywords("molasses, fermented kudzu, beans, ork belly");
		p3.setLicense("CC BY");
		p3.setRelation("Sugar, Fat and Milk producers of North-West Kentucky");
		p3.setOrganization("Industrial Design department, TU/e");
		p3.setDoi("https://doi.something.org/");
		p3.save();

		// ------------------------------------------------------------------------------------------------------------

		Dataset ds3_1 = datasetConnector.create("Form data", DatasetType.FORM, p3,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "find out how they feel", null, null);
		ds3_1.setStart(nowMinus14.getTime());
		ds3_1.setEnd(DateUtils.endOfDay(nowPlus10.getTime()));
		ds3_1.save();

		Dataset ds3_2 = datasetConnector.create("Sweet images", DatasetType.MEDIA, p3,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "find out how they feel", null, null);
		ds3_2.setStart(nowMinus14.getTime());
		ds3_2.setEnd(DateUtils.endOfDay(nowPlus2.getTime()));
		ds3_2.setOpenParticipation(true);
		ds3_2.save();

		Dataset ds3_3 = datasetConnector.create("Sugar diary", DatasetType.DIARY, p3,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "find out how they feel", null, null);
		ds3_3.setStart(nowMinus14.getTime());
		ds3_3.setEnd(DateUtils.endOfDay(nowPlus7.getTime()));
		ds3_3.save();

		// ------------------------------------------------------------------------------------------------------------

		Participant participant2 = new Participant("Joanna", "Doe");
		participant2.setEmail("joanna@df.com");
		participant2.setProject(p3);
		participant2.setStatus(ParticipationStatus.ACCEPT);
		participant2.create();

		// ------------------------------------------------------------------------------------------------------------

		Subscription s3_1 = new Subscription(alice, p3);
		s3_1.save();
		Subscription s3_2 = new Subscription(mary, p3);
		s3_2.save();

		// ------------------------------------------------------------------------------------------------------------

		p3.getDatasets().add(ds3_1);
		p3.getDatasets().add(ds3_2);
		p3.getDatasets().add(ds3_3);
		p3.getSubscribers().add(s3_1);
		p3.getSubscribers().add(s3_2);
		p3.getParticipants().add(participant2);
		p3.update();
	}

	/**
	 * 
	 * @param john
	 * @param bob
	 * @param alice
	 */
	private void createProject4(DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil, Person john,
	        Person bob, Person alice) {

		// get the start of today
		Date today = DateUtils.startOfDay(new Date());

		Calendar nowMinus14 = new GregorianCalendar();
		nowMinus14.setTime(today);
		nowMinus14.add(Calendar.DAY_OF_YEAR, -14);

		Calendar nowPlus7 = new GregorianCalendar();
		nowPlus7.setTime(today);
		nowPlus7.add(Calendar.DAY_OF_YEAR, 7);

		// ------------------------------------------------------------------------------------------------------------
		// create(String project_name, Person owner, String intro, boolean publicProject, boolean shareableProject)
		Project p4 = Project.create("Smart home study", john,
		        "Smart homes are not so smart actually, we study how much. Commodo elit at imperdiet dui accumsan sit. Diam quis enim lobortis scelerisque fermentum dui. Ultrices sagittis orci a scelerisque purus semper eget. Arcu ac tortor dignissim convallis aenean. Nec sagittis aliquam malesuada bibendum arcu vitae elementum. Sed sed risus pretium quam vulputate dignissim suspendisse in. Gravida dictum fusce ut placerat orci nulla pellentesque. Ullamcorper sit amet risus nullam. At elementum eu facilisis sed odio morbi quis. Mi eget mauris pharetra et ultrices neque ornare aenean euismod. Aliquam malesuada bibendum arcu vitae elementum curabitur vitae nunc sed. Odio eu feugiat pretium nibh ipsum consequat nisl. Euismod in pellentesque massa placerat duis ultricies.",
		        false, true);
		p4.setKeywords("lights, toasters, climate stuff, cuckoo clocks");
		p4.setLicense("CC BY-NC-SA");
		p4.setRelation("IoT devices in the home; DIGSIM squad");
		p4.setOrganization("Industrial Design department, TU/e");
		p4.setDoi("https://doi.something.org/");
		p4.save();

		// ------------------------------------------------------------------------------------------------------------

		Dataset ds4_1 = datasetConnector.create("Entity data 4-1", DatasetType.ENTITY, p4,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "app database for the smart home study app.", "", null);
		ds4_1.setStart(nowMinus14.getTime());
		ds4_1.setEnd(DateUtils.endOfDay(nowPlus7.getTime()));
		ds4_1.save();
		ds4_1.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds4_1.getId()));
		ds4_1.update();

		addSmartHomeData(datasetConnector, ds4_1);

		// ------------------------------------------------------------------------------------------------------------

		Dataset ds4_2 = datasetConnector.create("Smart home scripting", DatasetType.COMPLETE, p4,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "Script script script", "", null);
		ds4_2.setStart(nowMinus14.getTime());
		ds4_2.setCollectorType(Dataset.ACTOR);
		ds4_2.save();

		Dataset ds4_3 = datasetConnector.create("Smart home IoT data 4-3", DatasetType.IOT, p4,
		        "Nested IoT data for smart home study.", "nested smart home data", "", null);
		ds4_3.setStart(nowMinus14.getTime());
		ds4_3.setEnd(DateUtils.endOfDay(nowPlus7.getTime()));
		ds4_3.save();
		ds4_3.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds4_3.getId()));
		ds4_3.update();

		// ------------------------------------------------------------------------------------------------------------

		Device device1 = new Device();
		device1.setName("wake-up lamp 1");
		device1.setPublicParameter1("control");
		device1.setProject(p4);
		device1.create();

		Device device2 = new Device();
		device2.setName("wake-up lamp 2");
		device2.setPublicParameter1("experiment");
		device2.setProject(p4);
		device2.create();

		addSmartHomeIoTData(datasetConnector, ds4_3, device1);

		// ------------------------------------------------------------------------------------------------------------

		Participant participant1 = new Participant("John", "Doe");
		participant1.setEmail("joe@df.com");
		participant1.setProject(p4);
		participant1.setStatus(ParticipationStatus.ACCEPT);
		participant1.create();

		Participant participant2 = new Participant("Joanna", "Doe");
		participant2.setEmail("joanna@df.com");
		participant2.setProject(p4);
		participant2.setStatus(ParticipationStatus.ACCEPT);
		participant2.create();

		// ------------------------------------------------------------------------------------------------------------

		Collaboration c4_1 = new Collaboration(bob, p4);
		c4_1.save();
		Collaboration c4_2 = new Collaboration(alice, p4);
		c4_2.save();

		// ------------------------------------------------------------------------------------------------------------

		// create Entity data - part 1
		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds4_1);
		eds.addItem(device1.getRefId(), Optional.of(device1.getRefId()),
		        "{\"idnumber\": 1,\"name\":\"device1\",\"light\":5.3}");
		eds.addItem(device2.getRefId(), Optional.of(device2.getRefId()),
		        "{\"idnumber\": 2,\"name\":\"device2\",\"light\":4.5}");
		eds.addItem(participant1.getRefId(), Optional.of(participant1.getRefId()),
		        "{\"idnumber\": 3,\"name\":\"participant\",\"light\":6.4}");
		eds.addItem(participant2.getRefId(), Optional.of(participant2.getRefId()),
		        "{\"idnumber\": 4,\"name\":\"participante\",\"light\":7.8}");
		ds4_1.getConfiguration().put(Dataset.DATA_PROJECTION, "idnumber,name,light");
		ds4_1.update();

		// ------------------------------------------------------------------------------------------------------------

		p4.getDatasets().add(ds4_1);
		p4.getDatasets().add(ds4_2);
		p4.getDatasets().add(ds4_3);
		p4.getDevices().add(device1);
		p4.getDevices().add(device2);
		p4.getParticipants().add(participant1);
		p4.getParticipants().add(participant2);
		p4.getCollaborators().add(c4_1);
		p4.getCollaborators().add(c4_2);
		p4.update();
	}

	/**
	 * 
	 * @param alice
	 * @param john
	 * @param mary
	 */
	private void createProject5(DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil, Person alice,
	        Person john, Person mary) {
		// get the start of today
		Date today = DateUtils.startOfDay(new Date());

		Calendar nowMinus14 = new GregorianCalendar();
		nowMinus14.setTime(today);
		nowMinus14.add(Calendar.DAY_OF_YEAR, -14);

		Calendar nowPlus2 = new GregorianCalendar();
		nowPlus2.setTime(today);
		nowPlus2.add(Calendar.DAY_OF_YEAR, 2);

		// ------------------------------------------------------------------------------------------------------------
		// create(String project_name, Person owner, String intro, boolean publicProject, boolean shareableProject)
		Project p5 = Project.create("VR Secret World study", alice,
		        "VR technology allows for secret worlds inside a shoe drawer. We explore whether mice and and cheese can be found in such worlds. In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        true, false);
		p5.setKeywords("unity, VR, apple, pear, cucumber");
		p5.setLicense("CC BY-NC-ND");
		p5.setRelation("Virtual reality group");
		p5.setOrganization("Industrial Design department, TU/e");
		p5.setDoi("https://doi.something.org/");
		p5.save();

		// ------------------------------------------------------------------------------------------------------------

		Dataset ds5_1 = datasetConnector.create("IOT data 5-1", DatasetType.IOT, p5,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "people over 65 years old.", "", null);
		ds5_1.setStart(nowMinus14.getTime());
		ds5_1.setEnd(DateUtils.endOfDay(nowPlus2.getTime()));
		ds5_1.save();
		// auto-generate the API token for sending data to the dataset
		ds5_1.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds5_1.getId()));
		ds5_1.update();

		// ------------------------------------------------------------------------------------------------------------

		Collaboration c5_1 = new Collaboration(john, p5);
		c5_1.save();
		Collaboration c5_2 = new Collaboration(mary, p5);
		c5_2.save();

		// ------------------------------------------------------------------------------------------------------------

		Subscription s5_1 = new Subscription(john, p5);
		s5_1.save();

		// ------------------------------------------------------------------------------------------------------------

		p5.getDatasets().add(ds5_1);
		p5.getCollaborators().add(c5_1);
		p5.getCollaborators().add(c5_2);
		p5.getSubscribers().add(s5_1);
		p5.update();
	}

	/**
	 * 
	 * @param bob
	 */
	private void createProject6(DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil, Person bob) {
		// get the start of today
		Date today = DateUtils.startOfDay(new Date());

		Calendar nowMinus14 = new GregorianCalendar();
		nowMinus14.setTime(today);
		nowMinus14.add(Calendar.DAY_OF_YEAR, -14);

		// ------------------------------------------------------------------------------------------------------------
		// create(String project_name, Person owner, String intro, boolean publicProject, boolean shareableProject)
		Project p6 = Project.create("Scripts and crops project", bob,
		        "Scripting scripts scripts and scripts can be scripted by scripts when scripting scripts. Crops, however, crop crops and cropping is not related to crops. In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        true, false);
		p6.setKeywords("remote testing, automation, scripting, data foundry features");
		p6.setLicense("CC BY-NC-ND");
		p6.setRelation("Data foundry testing");
		p6.setOrganization("Industrial Design department, TU/e");
		p6.setDoi("https://doi.something.org/");
		p6.save();

		// ------------------------------------------------------------------------------------------------------------

		Dataset ds6_1 = datasetConnector.create("IOT data 1-2", DatasetType.IOT, p6,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "air quality data", "", null);
		ds6_1.getConfiguration().put(Dataset.DATA_PROJECTION, "value1,pm25,light");
		ds6_1.setStart(nowMinus14.getTime());
		ds6_1.save();
		// auto-generate the API token for sending data to the dataset
		ds6_1.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds6_1.getId()));
		ds6_1.update();

		Dataset ds6_2 = datasetConnector.create("Entity data 4-1", DatasetType.ENTITY, p6,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "app database for the smart home study app.", "", null);
		ds6_2.setStart(nowMinus14.getTime());
		ds6_2.save();

		Dataset ds6_3 = datasetConnector.create("Script crops", DatasetType.COMPLETE, p6,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "Script script script", "", null);
		ds6_3.setStart(nowMinus14.getTime());
		ds6_3.setCollectorType(Dataset.ACTOR);
		ds6_3.save();

		Dataset ds6_4 = datasetConnector.create("Website", DatasetType.COMPLETE, p6,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "Web web web", "", null);
		ds6_4.setStart(nowMinus14.getTime());
		ds6_4.save();

		// ------------------------------------------------------------------------------------------------------------

		Device device3 = new Device();
		device3.setName("table lamp 1");
		device3.setPublicParameter1("control");
		device3.setProject(p6);
		device3.create();

		Device device4 = new Device();
		device4.setName("table lamp 2");
		device4.setPublicParameter1("experiment");
		device4.setProject(p6);
		device4.create();

		// ------------------------------------------------------------------------------------------------------------

		Participant participant3 = new Participant("Jim", "Doe");
		participant3.setEmail("jim@df.com");
		participant3.setProject(p6);
		participant3.setStatus(ParticipationStatus.ACCEPT);
		participant3.create();

		Participant participant4 = new Participant("Jill", "Doe");
		participant4.setEmail("jill@df.com");
		participant4.setProject(p6);
		participant4.setStatus(ParticipationStatus.PENDING);
		participant4.create();

		// duplicate for checking Telegram; also, Joe is quite an active guy
		Participant participant5 = new Participant("John", "Doe");
		participant5.setEmail("joe@df.com");
		participant5.setProject(p6);
		participant5.setStatus(ParticipationStatus.ACCEPT);
		participant5.create();

		// ------------------------------------------------------------------------------------------------------------

		p6.getDatasets().add(ds6_1);
		p6.getDatasets().add(ds6_2);
		p6.getDatasets().add(ds6_3);
		p6.getDatasets().add(ds6_4);
		p6.getParticipants().add(participant3);
		p6.getParticipants().add(participant4);
		p6.getParticipants().add(participant5);
		p6.getDevices().add(device3);
		p6.getDevices().add(device4);
		p6.update();

		// ------------------------------------------------------------------------------------------------------------

		Calendar cal = new GregorianCalendar();
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds6_1);
		tsds.internalAddRecord(device3, cal.getTime(), "cooking", "{\"value1\": 80}");
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device3, cal.getTime(), "washing", "{\"value1\": 72}");
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device3, cal.getTime(), "cleaning", "{\"value1\": 86}");
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device4, cal.getTime(), "cooking", "{\"value1\": 82}");
		tsds.internalAddRecord(device4, cal.getTime(), "sleeping", "{\"value1\": 81}");
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.HOUR, -1);
		tsds.internalAddRecord(device4, cal.getTime(), "cooking", "{\"value1\": 58}");
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device3, cal.getTime(), "cleaning", "{\"value1\": 67}");
		tsds.internalAddRecord(device3, cal.getTime(), "washing", "{\"value1\": 68}");
		cal.add(Calendar.HOUR, -1);
		tsds.internalAddRecord(device3, cal.getTime(), "washing", "{\"value1\": 82}");
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device4, cal.getTime(), "cooking", "{\"value1\": 83}");
		tsds.internalAddRecord(device3, cal.getTime(), "sleeping", "{\"value1\": 40}");

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds6_2);
		eds.addItem(device3.getRefId(), Optional.of(device3.getRefId()),
		        "{\"idnumber\": 1,\"name\":\"device1\",\"light\":5.3}");
		eds.addItem(device4.getRefId(), Optional.of(device4.getRefId()),
		        "{\"idnumber\": 2,\"name\":\"device2\",\"light\":4.5}");
		eds.addItem(participant3.getRefId(), Optional.of(participant3.getRefId()),
		        "{\"idnumber\": 3,\"name\":\"participant\",\"light\":6.4}");
		eds.addItem(participant4.getRefId(), Optional.of(participant4.getRefId()),
		        "{\"idnumber\": 4,\"name\":\"participante\",\"light\":7.8}");
		ds6_2.getConfiguration().put(Dataset.DATA_PROJECTION, "idnumber,name,light");
		ds6_2.update();
	}

	/**
	 * create a mock project for testing the deidentify, archive and freeze functionality
	 * 
	 * @param bob
	 */
	private void createProject7(DatasetConnector datasetConnector, TokenResolverUtil tokenResolverUtil, Person bob) {

		// get the start of today
		Date today = DateUtils.startOfDay(new Date());

		Calendar lastYearMinus14 = new GregorianCalendar();
		lastYearMinus14.setTime(today);
		lastYearMinus14.add(Calendar.DAY_OF_YEAR, -366 - 14);

		Calendar lastYear = new GregorianCalendar();
		lastYear.setTime(today);
		lastYear.add(Calendar.DAY_OF_YEAR, -366);

		// ------------------------------------------------------------------------------------------------------------

		// create(String project_name, Person owner, String intro, boolean publicProject, boolean shareableProject)
		Project p = Project.create("Archival testing project", bob,
		        "This project is just there be cleaned, then archived, then frozen. What a life!", true, false);
		p.setKeywords("archival testing");
		p.setLicense("CC BY-NC-ND");
		p.setRelation("Data foundry testing");
		p.setOrganization("Industrial Design department, TU/e");
		p.setDoi("https://doi.something.org/");
		p.save();

		// ------------------------------------------------------------------------------------------------------------

		Dataset ds7_1 = datasetConnector.create("IOT data archivable", DatasetType.IOT, p,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "air quality data", "", null);
		ds7_1.getConfiguration().put(Dataset.DATA_PROJECTION, "value1,pm25,light");
		ds7_1.setStart(lastYearMinus14.getTime());
		ds7_1.setEnd(lastYear.getTime());
		ds7_1.save();
		// auto-generate the API token for sending data to the dataset
		ds7_1.getConfiguration().put(Dataset.API_TOKEN, tokenResolverUtil.getDatasetToken(ds7_1.getId()));
		ds7_1.update();

		Dataset ds7_2 = datasetConnector.create("Entity data archivable", DatasetType.ENTITY, p,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "app database for the smart home study app.", "", null);
		ds7_2.setStart(lastYearMinus14.getTime());
		ds7_2.setEnd(lastYear.getTime());
		ds7_2.save();

		Dataset ds7_3 = datasetConnector.create("Files archivable", DatasetType.COMPLETE, p,
		        "In nibh mauris cursus mattis molestie. Imperdiet sed euismod nisi porta lorem. Diam quam nulla porttitor massa id neque. Fermentum posuere urna nec tincidunt praesent semper feugiat. Cras tincidunt lobortis feugiat vivamus at. Ut sem viverra aliquet eget sit. Vitae tempus quam pellentesque nec nam. Ac felis donec et odio pellentesque diam volutpat commodo. Eget mauris pharetra et ultrices neque. In hendrerit gravida rutrum quisque non. Elementum eu facilisis sed odio morbi quis commodo. Lobortis elementum nibh tellus molestie.",
		        "Web web web", "", null);
		ds7_3.setStart(lastYearMinus14.getTime());
		ds7_3.setEnd(lastYear.getTime());
		ds7_3.save();

		// ------------------------------------------------------------------------------------------------------------

		Device device1 = new Device();
		device1.setName("table lamp 71");
		device1.setPublicParameter1("control");
		device1.setProject(p);
		device1.create();

		Device device2 = new Device();
		device2.setName("table lamp 72");
		device2.setPublicParameter1("experiment");
		device2.setProject(p);
		device2.create();

		// ------------------------------------------------------------------------------------------------------------

		Participant participant1 = new Participant("Jim", "Doe");
		participant1.setEmail("jim7@df.com");
		participant1.setProject(p);
		participant1.setStatus(ParticipationStatus.ACCEPT);
		participant1.create();

		Participant participan2 = new Participant("Jill", "Doe");
		participan2.setEmail("jill7@df.com");
		participan2.setProject(p);
		participan2.setStatus(ParticipationStatus.PENDING);
		participan2.create();

		// ------------------------------------------------------------------------------------------------------------

		p.getDatasets().add(ds7_1);
		p.getDatasets().add(ds7_2);
		p.getDatasets().add(ds7_3);
		p.getDevices().add(device1);
		p.getDevices().add(device2);
		p.getParticipants().add(participant1);
		p.getParticipants().add(participan2);
		p.setStart(lastYearMinus14.getTime());
		p.setEnd(lastYear.getTime());
		p.update();

		// ------------------------------------------------------------------------------------------------------------

		Calendar cal = new GregorianCalendar();
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds7_1);
		tsds.internalAddRecord(device1, cal.getTime(), "cooking", "{\"value1\": 80}");
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device1, cal.getTime(), "washing", "{\"value1\": 72}");
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device1, cal.getTime(), "cleaning", "{\"value1\": 86}");
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device2, cal.getTime(), "cooking", "{\"value1\": 82}");
		tsds.internalAddRecord(device2, cal.getTime(), "sleeping", "{\"value1\": 81}");
		cal.add(Calendar.HOUR, -1);
		cal.add(Calendar.HOUR, -1);
		tsds.internalAddRecord(device2, cal.getTime(), "cooking", "{\"value1\": 58}");
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device1, cal.getTime(), "cleaning", "{\"value1\": 67}");
		tsds.internalAddRecord(device1, cal.getTime(), "washing", "{\"value1\": 68}");
		cal.add(Calendar.HOUR, -1);
		tsds.internalAddRecord(device1, cal.getTime(), "washing", "{\"value1\": 82}");
		cal.add(Calendar.DAY_OF_YEAR, -1);
		tsds.internalAddRecord(device2, cal.getTime(), "cooking", "{\"value1\": 83}");
		tsds.internalAddRecord(device1, cal.getTime(), "sleeping", "{\"value1\": 40}");

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds7_2);
		eds.addItem(device1.getRefId(), Optional.of(device1.getRefId()),
		        "{\"idnumber\": 1,\"name\":\"device1\",\"light\":5.3}");
		eds.addItem(device2.getRefId(), Optional.of(device2.getRefId()),
		        "{\"idnumber\": 2,\"name\":\"device2\",\"light\":4.5}");
		eds.addItem(participant1.getRefId(), Optional.of(participant1.getRefId()),
		        "{\"idnumber\": 3,\"name\":\"participant\",\"light\":6.4}");
		eds.addItem(participan2.getRefId(), Optional.of(participan2.getRefId()),
		        "{\"idnumber\": 4,\"name\":\"participante\",\"light\":7.8}");
		ds7_2.getConfiguration().put(Dataset.DATA_PROJECTION, "idnumber,name,light");
		ds7_2.update();
	}

	private void addSmartHomeData(DatasetConnector datasetConnector, Dataset ds) {
		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		for (int i = 1; i <= 10; i++) {
			ObjectNode json = Json.newObject();
			json.put("temperature", 20 + i);
			json.put("humidity", 40 + i);
			json.put("luminosity", 100 * i);
			json.put("presence", i % 2);

			ObjectNode energy = Json.newObject();
			energy.put("voltage", 220);
			energy.put("current", 1.5 + (i * 0.1));
			energy.put("power", 330 + (i * 20));
			json.set("energy", energy);

			ArrayNode hourly = Json.newArray();
			for (int j = 1; j <= 6; j++) {
				hourly.add(j * i);
			}
			json.set("hourly_usage", hourly);

			ArrayNode devices = Json.newArray();
			for (int k = 1; k <= 4; k++) {
				ObjectNode dev = Json.newObject();
				dev.put("device_id", "dev-" + k);
				dev.put("status", k % 2 == 0);
				dev.put("battery_level", 80 + k);
				devices.add(dev);
			}
			json.set("connected_devices", devices);

			eds.addItem(UUID.randomUUID().toString(), Optional.empty(), json);
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private void addSmartHomeIoTData(DatasetConnector datasetConnector, Dataset ds, Device device) {
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
		Calendar cal = new GregorianCalendar();
		cal.add(Calendar.DAY_OF_YEAR, -5);

		for (int i = 0; i < 20; i++) {
			ObjectNode json = Json.newObject();

			ObjectNode roomStatus = Json.newObject();
			roomStatus.put("temperature", 20 + (i * 0.1) + (Math.random() - 0.5));
			roomStatus.put("humidity", 45 + (Math.random() * 5));
			roomStatus.put("co2", 400 + (i * 10));
			json.set("room_status", roomStatus);

			ArrayNode smartDevices = Json.newArray();
			ObjectNode light = Json.newObject();
			light.put("type", "light");
			light.put("id", "L1");
			light.put("state", i % 2 == 0 ? "on" : "off");
			light.put("brightness", i % 2 == 0 ? 80 : 0);
			smartDevices.add(light);

			ObjectNode thermo = Json.newObject();
			thermo.put("type", "thermostat");
			thermo.put("id", "T1");
			thermo.put("mode", "auto");
			thermo.put("target_temp", 21);
			smartDevices.add(thermo);
			json.set("smart_devices", smartDevices);

			ObjectNode energyMeter = Json.newObject();
			energyMeter.put("current_usage_watts", 150 + (i * 5));
			energyMeter.put("daily_total_kwh", 2.5 + (i * 0.1));
			json.set("energy_meter", energyMeter);

			cal.add(Calendar.HOUR, 1);
			tsds.internalAddRecord(device, cal.getTime(), "home/livingroom", json.toString());
		}
	}

	/**
	 * migrate all datasets that match the type
	 * 
	 * @param datasetConnector
	 */
	private void datasetMigration(DatasetConnector datasetConnector) {
//		// run migrations for all datasets
//		logger.info("Running DB migrations...");
//		AtomicInteger ai = new AtomicInteger();
//		Dataset.find.all().forEach(d -> {
//			datasetConnector.getDatasetDS(d).migrateDatasetSchema();
//			if (ai.incrementAndGet() % 200 == 0) {
//				logger.info("200 tables checked, moving on...");
//			}
//		});
//		int totalTables = ai.get();
//		logger.info((totalTables % 200) + " tables checked, of " + totalTables + " tables in total, and done.");
	}

	/**
	 * reset the entire database
	 * 
	 */
	public void resetDB() {
		// delete all data, so we can test the creation
		LabNotesEntry.find.query().delete();
		Analytics.find.query().delete();
		Project.find.query().delete();
		Person.find.query().delete();
		Dataset.find.query().delete();
		Cluster.find.query().delete();
		Device.find.query().delete();
		Participant.find.query().delete();
		Wearable.find.query().delete();

		// reset sequences
		Database server = DB.getDefault();
		try (Transaction t = server.beginTransaction();
		        Connection c = t.connection();
		        Statement st = c.createStatement()) {
			st.execute("ALTER TABLE project ALTER COLUMN id RESTART WITH 1;"
			        + "ALTER TABLE dataset ALTER COLUMN id RESTART WITH 1;"
			        + "ALTER TABLE person ALTER COLUMN id RESTART WITH 1;"
			        + "ALTER TABLE cluster ALTER COLUMN id RESTART WITH 1;"
			        + "ALTER TABLE device ALTER COLUMN id RESTART WITH 1;"
			        + "ALTER TABLE wearable ALTER COLUMN id RESTART WITH 1;");
			t.commit();
			t.end();
		} catch (Exception e) {
		}
	}

}

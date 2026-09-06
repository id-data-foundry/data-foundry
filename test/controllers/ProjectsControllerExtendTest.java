package controllers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import datasets.DatasetConnector;
import datasets.DatasetUpdateQueue;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;
import play.test.WithApplication;
import utils.DateUtils;

public class ProjectsControllerExtendTest extends WithApplication {

	private ProjectsController projectsController;
	private DatasetUpdateQueue updateQueue;
	private DatasetConnector datasetConnector;
	private SessionStore sessionStore;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.build();
	}

	@Before
	public void setUp() {
		projectsController = app.injector().instanceOf(ProjectsController.class);
		updateQueue = app.injector().instanceOf(DatasetUpdateQueue.class);
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);
		updateQueue.drain();
	}

	private Person createPerson(String email) {
		Person person = new Person();
		person.setUser_id(UUID.randomUUID().toString());
		person.setFirstname("Owner");
		person.setLastname("User");
		person.setEmail(email);
		person.save();
		return person;
	}

	@Test
	public void testExtendProjectDurationEnqueuesDatasets() {
		Person owner = createPerson("owner_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		Project project = Project.create("Extend Test Project", owner, "Intro", false, false);
		project.save();

		// Create an IoT dataset with expired dates
		Dataset iotDs = datasetConnector.create("IoT Dataset", DatasetType.IOT, project, "IoT test", "", "", "MIT");
		Date pastDate = DateUtils.moveMonths(new Date(), -2);
		iotDs.setStart(DateUtils.moveMonths(pastDate, -1));
		iotDs.setEnd(pastDate);
		iotDs.save();

		// Create an Entity dataset with expired dates
		Dataset entityDs = datasetConnector.create("Entity Dataset", DatasetType.ENTITY, project, "Entity test", "", "", "MIT");
		entityDs.setStart(DateUtils.moveMonths(pastDate, -1));
		entityDs.setEnd(pastDate);
		entityDs.save();

		project.getDatasets().add(iotDs);
		project.getDatasets().add(entityDs);
		project.update();

		// Ensure both datasets are currently inactive
		assertFalse(iotDs.isActive());
		assertFalse(entityDs.isActive());

		// Drain queue to ensure clean slate
		updateQueue.drain();
		assertTrue(updateQueue.isEmpty());

		// Prepare authenticated request for ProjectsController.extendProjectDuration
		Map<String, String> formMap = new HashMap<>();
		formMap.put("months", "2");

		Http.RequestBuilder requestBuilder = Helpers.fakeRequest("POST", "/projects/" + project.getId() + "/extend")
				.bodyForm(formMap);

		Http.Request request = requestBuilder.build();
		PlayWebContext context = new PlayWebContext(request);
		ProfileManager manager = new ProfileManager(context, sessionStore);
		CommonProfile profile = new CommonProfile();
		profile.setId(owner.getEmail());
		profile.addAttribute(Person.USER_NAME, owner.getEmail());
		profile.addAttribute(Person.USER_ID, owner.getId());
		manager.save(true, profile, false);
		request = context.supplementRequest(request);

		// Execute controller action
		Result result = projectsController.extendProjectDuration(request, project.getId());
		assertEquals(Helpers.SEE_OTHER, result.status());

		// Verify that both relevant datasets were enqueued
		Set<Long> queuedIds = updateQueue.drain();
		assertEquals(2, queuedIds.size());
		assertTrue(queuedIds.contains(iotDs.getId()));
		assertTrue(queuedIds.contains(entityDs.getId()));

		// Verify datasets in DB now have updated dates and are active
		Dataset reloadedIot = Dataset.find.byId(iotDs.getId());
		assertTrue(reloadedIot.isActive());
	}
}

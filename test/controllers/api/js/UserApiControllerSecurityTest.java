package controllers.api.js;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static play.mvc.Http.Status.FORBIDDEN;
import static play.mvc.Http.Status.OK;
import static play.test.Helpers.POST;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.play.PlayWebContext;

import datasets.DatasetConnector;
import models.Dataset;
import models.DatasetType;
import models.Person;
import models.Project;
import models.sr.Participant;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Security;
import play.test.WithApplication;
import utils.auth.TokenResolverUtil;

public class UserApiControllerSecurityTest extends WithApplication {

	private UserApiController userApiController;
	private DatasetConnector datasetConnector;
	private TokenResolverUtil tokenResolver;
	private SessionStore sessionStore;

	private Person ownerUser;
	private Person participantUser;
	private Person strangerUser;
	private Project publicProject;
	private Dataset entityDataset;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder()
				.configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true)
				.configure("df.keys.project", "test-project-token-secret-123456")
				.configure("df.keys.registration", Collections.singletonList("reg-key"))
				.build();
	}

	private Person createPerson(String prefix) {
		Person person = new Person();
		person.setUser_id(UUID.randomUUID().toString());
		person.setFirstname(prefix);
		person.setLastname("User");
		person.setEmail(prefix.toLowerCase() + "_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		person.save();
		return person;
	}

	@Before
	public void setUp() throws Exception {
		userApiController = app.injector().instanceOf(UserApiController.class);
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
		tokenResolver = app.injector().instanceOf(TokenResolverUtil.class);
		sessionStore = app.injector().instanceOf(SessionStore.class);

		ownerUser = createPerson("owner");
		participantUser = createPerson("participant");
		strangerUser = createPerson("stranger");

		// Create public project
		publicProject = Project.create("Public Security Project " + UUID.randomUUID(), ownerUser, "Public Description", false, false);
		publicProject.setPublicProject(true);
		publicProject.save();

		// Add participantUser as enrolled participant in the project
		Participant p = Participant.createInstance("Enrolled", "Participant", participantUser.getEmail(), publicProject);
		publicProject.getParticipants().add(p);
		publicProject.update();

		// Create EntityDS in project
		entityDataset = datasetConnector.create("User Profile DS", DatasetType.ENTITY, publicProject, "Entity DS", "Target", "true");
		entityDataset.save();
	}

	private Http.Request createAuthenticatedPostRequest(String uri, Person user, Map<String, String> formFields) {
		Http.RequestBuilder requestBuilder = new Http.RequestBuilder()
				.method(POST)
				.uri(uri)
				.bodyForm(formFields);
		if (user != null) {
			requestBuilder.attr(Security.USERNAME, user.getEmail());
			Http.Request request = requestBuilder.build();
			PlayWebContext context = new PlayWebContext(request);
			ProfileManager manager = new ProfileManager(context, sessionStore);
			CommonProfile profile = new CommonProfile();
			profile.setId(user.getEmail());
			profile.addAttribute(Person.USER_NAME, user.getEmail());
			profile.addAttribute(Person.USER_ID, user.getId());
			manager.save(true, profile, false);
			return context.supplementRequest(request);
		}
		return requestBuilder.build();
	}

	@Test
	public void testDF62_StrangerBlockedFromWritingProfileOnPublicProject() {
		// DF-62: An authenticated stranger must not be able to write records to a public project's entity dataset
		String strangerToken = tokenResolver.getParticipationToken(publicProject.getId(), strangerUser.getId());
		Map<String, String> form = new HashMap<>();
		form.put("key", "secret_preference");
		form.put("value", "malicious_override");

		Http.Request request = createAuthenticatedPostRequest(
				"/api/v1/" + publicProject.getId() + "/user/profile/" + strangerToken,
				strangerUser,
				form);

		Result result = userApiController.setItem(request, publicProject.getId(), strangerToken);
		assertEquals("Stranger must be forbidden from writing profile on public project", FORBIDDEN, result.status());
	}

	@Test
	public void testDF62_EnrolledParticipantAllowedToWriteProfile() {
		// Enrolled participant must be allowed to write their own profile
		String participantToken = tokenResolver.getParticipationToken(publicProject.getId(), participantUser.getId());
		Map<String, String> form = new HashMap<>();
		form.put("key", "theme");
		form.put("value", "dark");

		Http.Request request = createAuthenticatedPostRequest(
				"/api/v1/" + publicProject.getId() + "/user/profile/" + participantToken,
				participantUser,
				form);

		Result result = userApiController.setItem(request, publicProject.getId(), participantToken);
		assertEquals("Enrolled participant should be permitted to set item", OK, result.status());
	}

	@Test
	public void testDF62_ProjectOwnerAllowedToWriteProfile() {
		// Project owner must be allowed to write profile
		String ownerToken = tokenResolver.getParticipationToken(publicProject.getId(), ownerUser.getId());
		Map<String, String> form = new HashMap<>();
		form.put("key", "status");
		form.put("value", "active");

		Http.Request request = createAuthenticatedPostRequest(
				"/api/v1/" + publicProject.getId() + "/user/profile/" + ownerToken,
				ownerUser,
				form);

		Result result = userApiController.setItem(request, publicProject.getId(), ownerToken);
		assertEquals("Project owner should be permitted to set item", OK, result.status());
	}
}

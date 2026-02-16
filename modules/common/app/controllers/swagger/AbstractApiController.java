package controllers.swagger;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import controllers.AbstractAsyncController;
import controllers.auth.V2UserApiAuth;
import datasets.DatasetConnector;
import jakarta.inject.Singleton;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Person;
import models.Project;
import models.vm.TimedMedia;
import play.data.FormFactory;
import play.libs.Json;
import play.mvc.Http.Request;
import utils.auth.TokenResolverUtil;

@Singleton
public class AbstractApiController extends AbstractAsyncController {

	protected static final String TEXT_JAVASCRIPT = "text/javascript";

	protected final FormFactory formFactory;
	protected final DatasetConnector datasetConnector;
	protected final TokenResolverUtil tokenResolverUtil;

	public AbstractApiController(FormFactory formFactory, DatasetConnector datasetConnector,
	        TokenResolverUtil tokenResolverUtil) {
		this.formFactory = formFactory;
		this.datasetConnector = datasetConnector;
		this.tokenResolverUtil = tokenResolverUtil;
	}

	/**
	 * create a project for the given user for storing public datasets
	 * 
	 * @param user
	 * @param website
	 */
	protected Project createPublicProjectForUser(Person user, String website) {
		Project p = Project.create(user.getName() + " @ v2 API", user,
		        "Container project for public datasets created and shared by " + user.getName() + " from v2 API", true,
		        true);
		p.setLicense("MIT");
		p.setRelation(website);
		// TODO move to configuration
		p.setOrganization("");
		p.setPublicProject(true);
		p.save();
		LabNotesEntry.log(Project.class, LabNotesEntryType.CREATE, "Public project created", p);

		return p;
	}

	/**
	 * create a project for the given user for storing private datasets
	 * 
	 * @param user
	 * @param website
	 */
	protected Project createPrivateProjectForUser(Person user, String website) {
		Project p = Project.create(user.getName() + " @ v2 API", user,
		        "Container project for non-public datasets created and shared by " + user.getName() + " from v2 API",
		        false, false);
		p.setLicense("MIT");
		p.setRelation(website);
		// TODO move to configuration
		p.setOrganization("");
		p.setPublicProject(false);
		p.save();
		LabNotesEntry.log(Project.class, LabNotesEntryType.CREATE, "Non-public project created", p);

		return p;
	}

	/**
	 * retrieve API user behind this request
	 * 
	 * @param request
	 * @return
	 */
	protected Optional<Person> getAuthenticatedAPIUser(Request request) {
		return Optional.ofNullable(Person.find.byId(getAuthenticatedAPIUserId(request)));
	}

	/**
	 * retrieve user id of API user behind this request
	 * 
	 * @param request
	 * @return
	 */
	protected Long getAuthenticatedAPIUserId(Request request) {
		String apiToken = request.headers().get(V2UserApiAuth.X_API_TOKEN).orElse("");
		if (apiToken == null || apiToken.length() == 0) {
			return -1l;
		}

		Long userId = tokenResolverUtil.retrieveUserIdFromUserAccessToken(apiToken);
		Long tokenTimeout = tokenResolverUtil.retrieveTimeoutFromUserAccessToken(apiToken);
		if (userId == -1l || tokenTimeout < System.currentTimeMillis()) {
			return -1l;
		}

		return userId;
	}

	protected ObjectNode okJSONResponseObject() {
		ObjectNode on = Json.newObject();
		on.put("result", "ok");
		return on;
	}

	protected ObjectNode okJSONResponseObject(String message) {
		ObjectNode on = Json.newObject();
		on.put("result", message);
		return on;
	}

	protected ObjectNode errorJSONResponseObject(String message) {
		ObjectNode on = Json.newObject();
		on.put("result", "error");
		on.put("message", message);
		return on;
	}

	/**
	 * add a list of files to the data property of an object node
	 * 
	 * @param newObject
	 * @param files
	 */
	protected void addFiles(ObjectNode newObject, final List<TimedMedia> files) {
		final ArrayNode an = newObject.putArray("data");
		for (TimedMedia file : files) {
			final ObjectNode fileObject = an.addObject();
			fileObject.put("ts", file.datetime());
			fileObject.put("name", file.link);
			fileObject.put("description", file.caption);
		}
	}

}
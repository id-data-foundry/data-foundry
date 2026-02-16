package utils.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Singleton;

import models.OnboardingState;
import models.Person;
import models.Project;
import utils.DateUtils;

@Singleton
public class OnboardingSupport {

	private static final Map<String, String> MAP_PAGE_TO_STATE = new HashMap<String, String>();
	public static final List<String> STATES = new ArrayList<String>();

	static {
		STATES.add("new_project");
		STATES.add("new_dataset");
		STATES.add("new_cluster");
		STATES.add("new_participant");
		STATES.add("new_fb_wearable");
		STATES.add("new_gf_wearable");
		STATES.add("new_device");

		// when visitor is on project view page, the applicable onboarding state is
		// "new_dataset", but only if the visitor identity is in this state
		// key from controller, value for template views
		MAP_PAGE_TO_STATE.put("home", "new_project");
		MAP_PAGE_TO_STATE.put("project_view", "new_dataset");
		// MAP_PAGE_TO_STATE.put("resources_view_c", "new_cluster");
		// MAP_PAGE_TO_STATE.put("resources_view_p", "new_participant");
		// MAP_PAGE_TO_STATE.put("resources_view_fb", "new_fb_wearable");
		// MAP_PAGE_TO_STATE.put("resources_view_gf", "new_gf_wearable");
		MAP_PAGE_TO_STATE.put("iot_ds", "new_device");
		MAP_PAGE_TO_STATE.put("fitbit_ds_w", "new_fb_wearable");
		MAP_PAGE_TO_STATE.put("fitbit_ds_p", "new_participant");
		MAP_PAGE_TO_STATE.put("fitbit_ds_c", "new_cluster");
		MAP_PAGE_TO_STATE.put("googlefit_ds_w", "new_gf_wearable");
		MAP_PAGE_TO_STATE.put("googlefit_ds_p", "new_participant");
		MAP_PAGE_TO_STATE.put("googlefit_ds_c", "new_cluster");
		MAP_PAGE_TO_STATE.put("diary_ds", "new_participant");
		MAP_PAGE_TO_STATE.put("annotation_ds", "new_cluster");
	}

	/**
	 * check whether the onboarding support is generally active for the given user
	 * 
	 * @param user
	 * @return
	 */
	public boolean isActive(Person user) {
		final int mainAsInt = user.getIdentity().get("main").asInt();
		if (mainAsInt == OnboardingState.ACTIVE.ordinal()) {
			return true;
		} else if (mainAsInt == OnboardingState.NEXTDAY.ordinal()) {
			if (isActiveToday(user)) {
				switchState(user, "main", "active");
				return true;
			}
		}
		return false;
	}

	/**
	 * 1. return the page of user is moving to now by pageHandle and current state in user.identity 2. called by
	 * controllers
	 * 
	 * @param user
	 * @param pageHandle
	 * @return
	 */
	public Optional<OnboardingMessage> getStateForPage(Person user, String pageHandle) {
		final JsonNode jn = user.getIdentity();

		// onboarding support is not active
		int mainCatalog = jn.get("main").asInt();
		if (mainCatalog != OnboardingState.ACTIVE.ordinal()) {
			return Optional.empty();
		}

		// get current state for pageHandle
		String targetState = MAP_PAGE_TO_STATE.get(pageHandle);
		if (targetState == null) {
			return Optional.empty();
		}

		// check whether the current state is not finished
		int stateCatalog = jn.get("scene").get(targetState).asInt();
		if (stateCatalog == OnboardingState.FINISH.ordinal()) {
			return Optional.empty();
		}

		return Optional.of(new OnboardingMessage(targetState));
	}

	/**
	 * resolves the user and delegates the rest
	 * 
	 * @param project
	 * @param username
	 * @param targetScene
	 * @return
	 */
	public Optional<OnboardingMessage> setFlashMsg(Project project, String username, String targetScene) {
		final Optional<Person> opt;
		if ((opt = Person.findByEmail(username)).isPresent()) {
			return setFlashMsg(project, opt.get(), targetScene);
		} else {
			return Optional.empty();
		}
	}

	/**
	 * return the flash message for the given project properties
	 * 
	 * @param project
	 * @param user
	 * @param targetScene
	 * @return
	 */
	private Optional<OnboardingMessage> setFlashMsg(Project project, Person user, String targetScene) {
		// check condition and set onboarding for view page of fitbit dataset
		if (!isActive(user) || !project.belongsTo(user)) {
			return Optional.empty();
		}

		String returnMsg = "";
		switch (targetScene) {
		case "fitbit_ds":
			if (project.getParticipants().size() == 0) {
				returnMsg = "fitbit_ds_p";
			} else if (project.getWearables().size() == 0) {
				returnMsg = "fitbit_ds_w";
			} else if (project.getClusters().size() == 0) {
				returnMsg = "fitbit_ds_c";
			}
			break;
		case "googlefit_ds":
			if (project.getParticipants().size() == 0) {
				returnMsg = "googlefit_ds_p";
			} else if (project.getWearables().size() == 0) {
				returnMsg = "googlefit_ds_w";
			} else if (project.getClusters().size() == 0) {
				returnMsg = "googlefit_ds_c";
			}
			break;
		case "iot_ds":
			if (project.getDevices().size() == 0) {
				returnMsg = "iot_ds";
			}
			break;
		case "diary_ds":
			if (project.getParticipants().size() == 0) {
				returnMsg = "diary_ds";
			}
			break;
		case "annotation_ds":
			if (project.getClusters().size() == 0) {
				returnMsg = "annotation_ds";
			}
			break;
		default:
		}

		if (nnne(returnMsg)) {
			return getStateForPage(user, returnMsg);
		}
		return Optional.of(new OnboardingMessage(returnMsg));
	}

	/**
	 * 1. switch main onboarding support state or update current state of user 2. called by UserController
	 * 
	 * @param user
	 * @param catalog
	 * @param newState
	 * @return
	 */
	public boolean switchState(Person user, String catalog, String newState) {
		if (catalog.equals("main")) {
			// switch main state
			final ObjectNode oldState = user.getIdentity();

			boolean isChanged = false;
			switch (newState) {
			case "never":
				oldState.put("main", OnboardingState.NEVER.ordinal());
				isChanged = true;
				break;
			case "notToday":
				oldState.put("main", OnboardingState.NEXTDAY.ordinal());
				oldState.put("available_after", Long.toString(DateUtils.getMillisFromToday(1)));
				// user.user_id = Long.toString(DatasetUtils.getMillisFromToday(1));
				isChanged = true;
				break;
			case "active":
				oldState.put("main", OnboardingState.ACTIVE.ordinal());
				isChanged = true;
				break;
			default:
			}

			if (isChanged) {
				user.setIdentity(oldState);
				return true;
			}
		}

		return false;
	}

	/**
	 * update state of current
	 * 
	 * @param user
	 * @param catalog
	 * @param newState
	 * @return
	 */
	public boolean updateCurrent(Person user, String catalog, String newState) {
		if (MAP_PAGE_TO_STATE.values().contains(catalog)) {
			if (newState.equals("ongoing")) {
				// update current state
				user.setIdentity(user.getIdentity().put("current", catalog));
				return true;
			} else if (newState.equals("finish")) {
				user.setIdentity(user.getIdentity().put("current", ""));
				return true;
			}
		}

		return false;
	}

	/**
	 * update state of one scene
	 * 
	 * @param user
	 * @param scene
	 * @param newState
	 * @return
	 */
	public boolean updateScene(Person user, String scene, String newState) {
		final ObjectNode oldState = user.getIdentity();
		if (oldState.get("scene").has(scene)) {
			// 3 = initial
			int sceneState = 3;
			boolean isChanged = false;
			switch (newState) {
			case "ongoing":
				sceneState = OnboardingState.ONGOING.ordinal();
				isChanged = true;
				break;
			case "finish":
				sceneState = OnboardingState.FINISH.ordinal();
				isChanged = true;
				break;
			default:
			}

			if (isChanged) {
				((ObjectNode) oldState.get("scene")).put(scene, sceneState);
				user.setIdentity(oldState);
				return true;
			}
		}

		return false;
	}

	/**
	 * update scene and current states as operations for one scene are done
	 * 
	 * @param username
	 * @param scene
	 */
	public void updateAfterDone(String username, String scene) {
		final Optional<Person> opt;
		if ((opt = Person.findByEmail(username)).isPresent()) {
			updateAfterDone(opt.get(), scene);
		}
	}

	/**
	 * update scene and current states as operations for one scene are done
	 * 
	 * @param user
	 * @param scene
	 */
	public void updateAfterDone(Person user, String scene) {
		final JsonNode jn = user.getIdentity();
		if (jn.get("scene").get(scene).asInt() != OnboardingState.FINISH.ordinal()) {
			updateScene(user, scene, "finish");
		}
		if (jn.get("current").asText().equals(scene)) {
			updateCurrent(user, scene, "finish");
		}
	}

	/**
	 * return true if the scene exists in MAP_PAGE_TO_STATE as a key
	 * 
	 * @param scene
	 * @return
	 */
	public boolean hasScene(String scene) {
		return MAP_PAGE_TO_STATE.values().contains(scene);
	}

	////////////////////////////////////////////////////////////////////////

	/**
	 * check whether the user is active today
	 * 
	 * @param user
	 * @return
	 */
	private boolean isActiveToday(Person user) {
		JsonNode jn = user.getIdentity();
		if (nnne(jn.get("available_after").asText())) {
			long currentMillis = System.currentTimeMillis();
			long ts = jn.get("available_after").asLong();
			if (currentMillis > ts) {
				return true;
			}
		}

		return false;
	}

	/**
	 * valid text = Not Null and Not Empty
	 */
	private boolean nnne(String text) {
		return text != null && text.trim().length() > 0;
	}

}
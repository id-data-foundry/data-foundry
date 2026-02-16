package models.sr;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import io.ebean.Finder;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.ManyToMany;
import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import play.Logger;
import utils.DataUtils;

@Entity
public class Wearable extends DataResource {

	public static final String FITBIT = "FITBIT";
	public static final String GOOGLEFIT = "GOOGLEFIT";

	private String name;
	// "FITBIT", "GOOGLE"
	private String brand;
	// dataset.id::dataset_id1, dataset_id2,...
	// id of current dataset::next datasets
	private String scopes;
	private String configuration;

	// import data from foreign API
	// FITBIT: access_token
	// Google FIT: access_token
	private String apiToken;
	// FITBIT: refresh_token
	// Google FIT: refresh_token
	private String apiKey;
	// FITBIT: keep the time in milliseconds for fetching data next time
	// Google FIT: keep the time in milliseconds for fetching data next time
	private Long expiry;
	// FITBIT: user-id
	// Google FIT: user-id
	private String user_id;

	@ManyToMany(mappedBy = "wearables", cascade = CascadeType.ALL)
	private List<Cluster> clusters = new LinkedList<Cluster>();

	public static final Finder<Long, Wearable> find = new Finder<Long, Wearable>(Wearable.class);

	private static final Logger.ALogger logger = Logger.of(Wearable.class);

	/**
	 * create wearable = assign refId for new wearable; also logs the creation
	 * 
	 */
	public void create() {
		this.setRefId("w" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		logger.info("New wearable created with refId: " + this.getRefId());
		LabNotesEntry.log(Wearable.class, LabNotesEntryType.CREATE, "Wearable created: " + this.getName(),
		        this.getProject());
	}

	/**
	 * return true if this wearable is connected
	 * 
	 * @return
	 */
	public boolean isConnected() {
		return getApiToken() != null && getApiToken().length() > 10 && getApiKey() != null && getApiKey().length() > 10;
	}

	public boolean isFitbit() {
		return this.getBrand() == null || this.getBrand().equals(FITBIT);
	}

	public boolean isGoogleFit() {
		return this.getBrand() != null && this.getBrand().equals(GOOGLEFIT);
	}

	/**
	 * find the participant for a wearable (check all clusters of this wearable, pick the smallest cluster with a
	 * participant, otherwise pick first participant in a cluster)
	 * 
	 * @return
	 */
	public Participant getClusterParticipant() {

		// first step: find cluster with only one participant
		for (Cluster cluster : clusters) {
			if (cluster.getParticipants().isEmpty()) {
				continue;
			} else if (cluster.getParticipants().size() == 1) {
				return cluster.getParticipants().get(0);
			}
		}

		// if not found, find cluster and take first participant
		for (Cluster cluster : clusters) {
			if (!cluster.getParticipants().isEmpty()) {
				return cluster.getParticipants().get(0);
			}
		}

		return null;
	}

	/**
	 * retrieve a list of participants that are associated to this wearable because they are in the same cluster
	 * together
	 * 
	 * @return
	 */
	public List<Participant> getClusterParticipants() {
		List<Participant> participants = new LinkedList<>();
		for (Cluster cluster : clusters) {
			for (Participant participant : cluster.getParticipants()) {
				participant.refresh();
				participants.add(participant);
			}
		}

		return participants;
	}

	/**
	 * return id of the associated dataset
	 * 
	 * @return
	 */
	public Long getDatasetId() {
		return DataUtils.parseLong(scopes, -1L);
	}

	/**
	 * return target scopes (targetObject) of the wearable dataset this wearable device belongs to
	 * 
	 * @return
	 */
	public String getScopeFromDataset() {
		// check dataset
		Long id = this.getDatasetId();
		if (id == -1) {
			return "";
		}

		Dataset ds = Dataset.find.byId(id);

		// check dataset type
		if (ds == null || !ds.isWearableDS()) {
			return "";
		}

		return ds.getTargetObject().split(" ").length > 1 ? ds.getTargetObject().replace(" ", ", ")
		        : ds.getTargetObject().trim();
	}

	/**
	 * get together all required scopes / URLs for authorization and return
	 * 
	 * @return
	 */
	public String getScopeURLs() {
		// check dataset
		Long id = this.getDatasetId();
		if (id == -1) {
			return "";
		}
		Dataset ds = Dataset.find.byId(id);

		// check dataset type
		if (ds == null || !ds.isWearableDS()) {
			return "";
		}

		// check dataset scopes
		String[] scopes = ds.getTargetObject().split(" ");
		if (scopes.length <= 0)
			return "";

		String returnScopes = "";
		if (ds.getDsType() == DatasetType.GOOGLEFIT) {
			boolean hasLocation = false;

			// always add activity scope for checking the step_count scope as synchronization signal
			returnScopes += getGoogleFitScope("activity") + "+";

			for (int i = 0; i < scopes.length; i++) {
				if (scopes[i].equals("activity") || scopes[i].equals("calories") || scopes[i].equals("step_count")) {
					// do nothing, activity scope has been added in advance
				} else if (scopes[i].equals("speed") || scopes[i].equals("distance")) {
					if (!hasLocation) {
						returnScopes += getGoogleFitScope(scopes[i]) + "+";
						hasLocation = true;
					}
				} else {
					returnScopes += getGoogleFitScope(scopes[i]) + "+";
				}
			}

			// add openid scope for getting participant's Google user ID
			returnScopes += "openid";
		} else if (ds.getDsType() == DatasetType.FITBIT) {
			boolean hasActivity = false;
			boolean hasWeight = false;

			for (int i = 0; i < scopes.length; i++) {

				if (scopes[i].equals("activity") || scopes[i].equals("calories") || scopes[i].equals("steps")
				        || scopes[i].equals("distance") || scopes[i].equals("elevation")
				        || scopes[i].equals("floors")) {
					if (!hasActivity) {
						returnScopes += "activity+";
						hasActivity = true;
					}
				} else if (scopes[i].equals("weight") || scopes[i].equals("bmi") || scopes[i].equals("fat")) {
					if (!hasWeight) {
						returnScopes += "weight+";
						hasWeight = true;
					}
				} else {
					returnScopes += getFitbitScope(scopes[i]) + "+";
				}
			}

			// add settings scope for getting the exact time of the last synchronization time
			returnScopes += "settings";
		}

		return returnScopes;
	}

	/**
	 * return URL for authorization according to required GoogleFit scope
	 * 
	 * @param scope
	 * @return
	 */
	private String getGoogleFitScope(String scope) {
		switch (scope) {

		// activity scopes
		case "activity":
		case "calories":
		case "step_count":
			return "https://www.googleapis.com/auth/fitness.activity.read";

		// location scopes
		case "speed":
		case "distance":
			return "https://www.googleapis.com/auth/fitness.location.read";

		// body scope
		case "weight":
			return "https://www.googleapis.com/auth/fitness.body.read";

		// heartbeat scope
		case "heart_rate":
			return "https://www.googleapis.com/auth/fitness.heart_rate.read";

		// sleep scope
		case "sleep":
			return "https://www.googleapis.com/auth/fitness.sleep.read";

		default:
			return "";
		}
	}

	/**
	 * return required Fitbit scope for authorization
	 * 
	 * @param scope
	 * @return
	 */
	private String getFitbitScope(String scope) {
		switch (scope) {

		// activity scopes
		case "activity":
		case "calories":
		case "steps":
		case "distance":
		case "elevation":
		case "floors":
			return "activity";

		// body scope
		case "weight":
		case "bmi":
		case "fat":
			return "weight";

		// heartbeat scope
		case "heartrate":
			return "heartrate";

		// sleep scope
		case "sleep":
			return "sleep";

		default:
			return "";
		}
	}

	/**
	 * check whether this wearable is linked and needs resetting to deidentify project
	 * 
	 * @return
	 */
	public boolean canDeidentify() {
		return getExpiry() != -1L || (getApiToken() != null && !getApiToken().isEmpty())
		        || (getApiKey() != null && !getApiKey().isEmpty()) || (getUserId() != null && !getUserId().isEmpty());
	}

	/**
	 * reset the wearable as the project is archived
	 * 
	 */
	public void deidentify() {
		this.setExpiry(-1L);
		this.setApiToken("");
		this.setApiKey("");
		this.setUserId("");
		this.update();

		logger.info("Reset wearable " + this.getId() + " (" + this.getRefId() + ")");
	}

	/**
	 * unlink and reset unlink wearable
	 * 
	 */
	public void reset() {
		Project project = this.getProject();
		if (isFitbit()) {
			if (project.getFitbitDataset().getId() >= 0) {
				this.setScopes(project.getFitbitDataset().getId().toString());
				this.setExpiry(project.getFitbitDataset().start().getTime());
			} else {
				this.setScopes(project.getFitbitDatasets().get(0).getId().toString());
				this.setExpiry(project.getFitbitDatasets().get(0).start().getTime());
			}

			if (this.getBrand() == null) {
				this.setBrand(FITBIT);
			}
		} else if (isGoogleFit()) {
			if (project.getGoogleFitDataset().getId() >= 0) {
				this.setScopes(project.getGoogleFitDataset().getId().toString());
				this.setExpiry(project.getGoogleFitDataset().start().getTime());
			} else {
				this.setScopes(project.getGoogleFitDatasets().get(0).getId().toString());
				this.setExpiry(project.getGoogleFitDatasets().get(0).start().getTime());
			}
		} else {
			logger.info("Wrong wearable brand: " + this.getBrand() + " with id: " + this.getId());
			this.setScopes("-1");
			this.setExpiry(-1l);
		}

		// reset
		this.setApiToken("");
		this.setApiKey("");
		this.setUserId("");
		this.update();

		logger.info("A wearable has been unlinked:" + getBrand() + " - " + getName() + " - " + getRefId());
	}

	/**
	 * reset expiration time of target wearable as the start date of the dataset it belongs to
	 * 
	 */
	public void resetExpiry() {
		long dsId = getDatasetId();
		if (dsId > 0) {
			Dataset dataset = Dataset.find.byId(dsId);

			// if dataset is found, reset expiry of this wearable
			if (dataset != null) {
				this.setExpiry(dataset.start().getTime());
				this.update();
			}
		} else {
			logger.error("Not valid dataset for resetting this wearable:" + getRefId());
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public List<Cluster> getClusters() {
		return clusters;
	}

	public void setClusters(List<Cluster> clusters) {
		this.clusters = clusters;
	}

	public String getUserId() {
		return user_id;
	}

	public void setUserId(String user_id) {
		this.user_id = user_id;
	}

	public Long getExpiry() {
		return expiry == null ? -1 : expiry;
	}

	public void setExpiry(Long expiry) {
		this.expiry = expiry;
	}

	public String getApiKey() {
		return apiKey;
	}

	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}

	public String getApiToken() {
		return apiToken;
	}

	public void setApiToken(String apiToken) {
		this.apiToken = apiToken;
	}

	public String getConfiguration() {
		return configuration;
	}

	public void setConfiguration(String configuration) {
		this.configuration = configuration;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getName() {
		return nss(name);
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSlug() {
		return getName().replaceAll("[\\s;:!@#$%^&*.,]", "_");
	}

	public String getScopes() {
		return scopes;
	}

	public void setScopes(String scopes) {
		this.scopes = scopes;
	}

	public void showInfo() {
		logger.info(getBrand() + " wearable name:" + getName());
		logger.info(getBrand() + " access token:" + getApiToken());
		logger.info(getBrand() + " refresh token:" + getApiKey());
		logger.info(getBrand() + " expiry:" + getExpiry());
		logger.info(getBrand() + " dataset id:" + getDatasetId());
		logger.info(getBrand() + " scopes:" + getScopeFromDataset());
		logger.info(getBrand() + " user ID:" + getUserId());
	}

}

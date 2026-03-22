package models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;

import datasets.DatasetConnector;
import io.ebean.DB;
import io.ebean.Finder;
import io.ebean.Model;
import io.ebean.Transaction;
import io.ebean.annotation.DbJsonB;
import io.ebean.annotation.WhenCreated;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import play.Logger;
import play.data.format.Formats;
import play.libs.Json;
import utils.DataUtils;
import utils.DateUtils;

@Entity
public class Dataset extends Model {

	private static final Logger.ALogger logger = Logger.of(Dataset.class);

	// DS sub types when reusing a dataset type
	public static final String ACTOR = "ACTOR";
	public static final String CHATBOT = "CHATBOT";
	public static final String SAVED_EXPORT = "SAVED_EXPORT";
	public static final String STUDY_MANAGEMENT = "STUDY_MANAGEMENT";
	public static final String NARRATIVE_SURVEY = "narrative_survey";
	public static final String INFORMED_CONSENT = "informed_consent";

	// target object designations
	public static final String PARTICIPANT_DASHBOARD_PAGE = "participant_dashboard_page";
	public static final String PARTICIPANT_STUDY_PAGE = "participant_study_page";
	public static final String TELEGRAM_PROJECT_TOKEN = "telegram_project_token";

	// configuration constants
	public static final String DATA_PROJECTION = "data_projection";
	public static final String API_TOKEN = "api_token";
	public static final String PUBLIC_ACCESS_TOKEN = "public_access_token";
	public static final String WEB_ACCESS_TOKEN = "web_access_token";
	public static final String WEB_ACCESS_ENTRY = "web_access_entry";

	// OOCSI channel and script code
	public static final String ACTOR_CHANNEL = "actor_channel";
	public static final String ACTOR_CODE = "actor_code";
	public static final String OOCSI_SERVICE = "OOCSI_service";

	// chatbot
	public static final String CHATBOT_NAME = "chatbot_name";
	public static final String CHATBOT_INTRODUCTION = "chatbot_introduction";
	public static final String CHATBOT_SYSTEM_PROMPT = "chatbot_system_prompt";
	public static final String CHATBOT_ASSISTANT_PROMPT = "chatbot_assistant_prompt";
	public static final String CHATBOT_USER_PROMPT = "chatbot_user_prompt";
	public static final String CHATBOT_FILE_UPLOAD = "chatbot_file_upload";
	public static final String CHATBOT_MODEL = "chatbot_model";
	public static final String CHATBOT_PUBLIC = "chatbot_public";
	public static final String CHATBOT_TEMPERATURE = "chatbot_temperature";
	public static final String CHATBOT_RAG_MAX_HITS = "chatbot_rag_max_hits";
	public static final String CHATBOT_RAG_SCORE_THRESHOLD = "chatbot_rag_score_threshold";
	public static final String CHATBOT_STORE_CHATS = "chatbot_store_chats";
	public static final String CHATBOT_SHOW_SOURCES = "chatbot_show_sources";

	// Activity Logger
	public static final String DIRECT_EVENT_ACTIVITIES = "direct_event_activities";
	public static final String STATE_EVENT_ACTIVITIES = "state_event_activities";

	// saved exports
	public static final String SAVED_EXPORT_LIMIT = "SAVED_EXPORT_LIMIT";
	public static final String SAVED_EXPORT_SAMPLING = "SAVED_EXPORT_SAMPLING";
	public static final String SAVED_EXPORT_DATASETS = "SAVED_EXPORT_DATASETS";

	// dataset for project export during freeze
	public static final String PROJECT_DATA_EXPORT = "PROJECT_DATA_EXPORT";

	public static final String NS_NARRATIVE_SURVEY_DATASETS = "ns_narrative_survey_datasets";

	protected final DateFormat tsDateFormatter = new SimpleDateFormat("yyyy-MM-dd");

	@Id
	private Long id;

	// public dataset name
	private String name;

	// internal STRING reference id
	private String refId;

	// internal API token for data access
	private String apiToken;

	// see DatasetType enum
	private DatasetType dsType;

	// free-form collector spec, depends on dsType
	// this usually indicates a special-use dataset that should be hidden from normal view
	private String collectorType;

	// collector configuration
	@ElementCollection
	@DbJsonB
	private Map<String, String> configuration = new HashMap<String, String>();

	// whether we can accept any entry
	private boolean openParticipation;

	// full project description mentioning, e.g., context, participant demographics and method
	private String description;

	// content descriptor (used for linking in LINKED dataset)
	// keep scopes for FitbitDS and GooglefitDS
	private String targetObject;

	// additional metadata fields
	// -----------------------------------------------------------------

	// add keywords relating to the content and the place/period of origin of the data
	private String keywords;

	// digital object identifier
	private String doi;

	// refer to a related dataset, publication or journal article
	private String relation;

	// organizations involved, subsidizer (subsidy number)
	private String organization;

	// general remarks / comments
	private String remarks;

	// license
	private String license;

	@Formats.DateTime(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date start;
	@Formats.DateTime(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date end;

	@WhenCreated
	@Formats.DateTime(pattern = "yyyy-MM-dd HH:mm:ss")
	private Date creation = new Date();

	@ManyToOne
	private Project project;

	public static final Finder<Long, Dataset> find = new Finder<Long, Dataset>(Dataset.class);

	/**
	 * this is an empty dataset that is returned if no dataset could be found for a dataset request in Project.java;
	 * when the dataset is used in a request or URL it will simply not be found and trigger the default behavior for
	 * unspecified datasets
	 */
	public static final Dataset EMPTY_DATASET = new Dataset();
	static {
		EMPTY_DATASET.setId(-1l);
		EMPTY_DATASET.setRefId("");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public String getSlug() {
		return getName().replaceAll("[\\s;:!@#$%^&*.,]", "_");
	}

	public String getDescription() {
		return nss(description);
	}

	public String getKeywords() {
		return nss(keywords);
	}

	public String[] getKeywordList() {
		return getKeywords().split("[,\\w]");
	}

	public String getDoi() {
		return nss(doi);
	}

	public String getRelation() {
		return nss(relation);
	}

	public String getOrganization() {
		return nss(organization);
	}

	public String getRemarks() {
		return nss(remarks);
	}

	public String getLicense() {
		return nss(license);
	}

	public Date start() {
		if (this.getStart() == null) {
			this.setStart(DateUtils.startOfDay(new Date()));

			if (this.getId() != null) {
				this.update();
			}
		}

		return this.getStart();
	}

	public String startDate() {
		return tsDateFormatter.format(getStart());
	}

	public Date end() {
		if (this.getEnd() == null) {
			this.setEnd(DateUtils.inThreeMonths(DateUtils.endOfDay(new Date())));

			if (this.getId() != null) {
				this.update();
			}
		}

		return this.getEnd();
	}

	public String endDate() {
		return tsDateFormatter.format(getEnd());
	}

	public String creationDate() {
		return tsDateFormatter.format(creation);
	}

	/**
	 * return the progress of this dataset in percent (start > now > end)
	 * 
	 * @return
	 */
	public float getTimeProgress() {
		long duration = end().getTime() - start().getTime();
		return duration == 0 ? 100 : Math.max(1, 100 * (System.currentTimeMillis() - start().getTime())) / duration;
	}

	/**
	 * set whether this data set allows any new participant to join and submit data or only registered ones
	 * 
	 * @param whitelist
	 * @return
	 */
	public Dataset openParticipation(boolean whitelist) {
		this.setOpenParticipation(whitelist);
		return this;
	}

	public boolean belongsTo(String username) {
		getProject().refresh();
		return getProject().belongsTo(username);
	}

	public boolean collaboratesWith(String username) {
		getProject().refresh();
		return getProject().collaboratesWith(username);
	}

	public boolean subscribedBy(String username) {
		getProject().refresh();
		return getProject().subscribedBy(username);
	}

	/**
	 * check whether this data set can be read-accessed by the user given by <code>username</code>
	 * 
	 * @param username
	 * @return
	 */
	public boolean visibleFor(String username) {
		getProject().refresh();
		return getProject().visibleFor(username);
	}

	/**
	 * check whether this data set can be read-accessed by the user given by <code>user</code>
	 * 
	 * @param user
	 * @return
	 */
	public boolean visibleFor(Person user) {
		getProject().refresh();
		return getProject().visibleFor(user);
	}

	/**
	 * check whether this dataset can modified by the user given by <code>username</code>
	 * 
	 * @param username
	 * @return
	 */
	public boolean editableBy(String username) {
		getProject().refresh();
		return getProject().editableBy(username);
	}

	/**
	 * check whether this dataset can modified by the user given by <code>user</code>
	 * 
	 * @param user
	 * @return
	 */
	public boolean editableBy(Person user) {
		getProject().refresh();
		return getProject().editableBy(user);
	}

	/**
	 * check active dates for the data set and decide whether this API can be opened or requests can be let pass
	 * 
	 * @return
	 */
	public boolean isActive() {
		return isActive(new Date());
	}

	public boolean isActive(long date) {
		if (date > 0) {
			return isActive(new Date(date));
		}

		return isActive(new Date());
	}

	/**
	 * check active dates for the data set and decide whether this API can be opened or requests can be let pass - given
	 * a reference point
	 * 
	 * @param reference
	 * @return
	 */
	public boolean isActive(Date reference) {
		// special datasets are always active
		if (isStudyManagement() || isSavedExport()) {
			return true;
		}

		// other datasets are handled here
		Date terminal = this.end();
		if (this.getDsType().equals(DatasetType.FITBIT) || this.getDsType().equals(DatasetType.GOOGLEFIT)) {
			// two days after end date
			terminal = DateUtils.moveDays(DateUtils.startOfDay(this.getEnd()), 2);
		} else {
			// one day after end date
			terminal = DateUtils.dayAfter(DateUtils.startOfDay(this.getEnd()));
		}

		// return true if: start date <= reference < one / two days after end date
		return !this.start().after(reference) && terminal.after(reference);
	}

	/**
	 * check whether this dataset can be modified (append-only)
	 * 
	 * @return
	 */
	public boolean canAppend() {
		return this.getId() > -1 && isActive();
	}

	/**
	 * Open participation needs to be on and API token needs to be non-null and longer than 10 chars, then it needs to
	 * match the data set API token
	 * 
	 * @param apiToken
	 * @return
	 */
	public boolean isAuthorized(String apiToken) {
		return this.isOpenParticipation() && apiToken != null && apiToken.length() > 10
				&& apiToken.equals(this.getApiToken());
	}

	/**
	 * Open participation needs to be on or a source is given and retained, and then API token needs to be non-null and
	 * longer than 10 chars, then it needs to match the data set API token
	 * 
	 * @param sourceId
	 * @param apiToken
	 * @return
	 */
	public boolean isAuthorized(String sourceId, String apiToken) {
		return (this.openParticipation || getProject().hasDevice(sourceId)) && apiToken != null
				&& apiToken.length() > 10 && apiToken.equals(this.getApiToken());
	}

	/**
	 * return false if this dataset is FITBIT or GOOGLEFIT dataset, and it will start fetching data after / in 30 mins.
	 * data fetching task will start at 23:32:22 and 23:42:22, then it won't be available to edit start date of these
	 * two kind of datasets after 23:00:00
	 * 
	 * @return
	 */
	public boolean isEditable() {
		if ((getDsType().equals(DatasetType.FITBIT) || getDsType().equals(DatasetType.GOOGLEFIT))
				&& ((new Date().getTime() - this.getStart().getTime()) > 23 * 60 * 60 * 1000l)) {
			return false;
		}

		return true;
	}

	/**
	 * return whether this dataset is a saved export (from the datatool)
	 * 
	 * @return
	 */
	public boolean isSavedExport() {
		return this.getDsType().equals(DatasetType.COMPLETE) && this.getCollectorType() != null
				&& this.getCollectorType().equals(Dataset.SAVED_EXPORT);
	}

	/**
	 * return whether this dataset is a study setup dataset that contains study settings, informed consent forms, and
	 * other documentation
	 * 
	 * @return
	 */
	public boolean isStudyManagement() {
		return this.getDsType().equals(DatasetType.COMPLETE) && this.getCollectorType() != null
				&& this.getCollectorType().equals(Dataset.STUDY_MANAGEMENT);
	}

	/**
	 * return whether this dataset is a narrative survey dataset
	 * 
	 * @return
	 */
	public boolean isNarrativeSurvey() {
		return this.getDsType().equals(DatasetType.COMPLETE) && this.getCollectorType() != null
				&& this.getCollectorType().equals(Dataset.NARRATIVE_SURVEY);
	}

	/**
	 * return whether this dataset is (possibly) a website
	 * 
	 * @return
	 */
	public boolean isWebsite() {
		return this.getDsType().equals(DatasetType.COMPLETE) && !isScript() && !isChatbot() && !isSavedExport()
				&& !isStudyManagement();
	}

	/**
	 * return whether this dataset is (possibly) a website and web access is activated
	 * 
	 * @return
	 */
	public boolean isWebsiteLive() {
		return this.getDsType().equals(DatasetType.COMPLETE) && !isScript() && !isChatbot() && !isSavedExport()
				&& !isStudyManagement() && !configuration.getOrDefault(WEB_ACCESS_TOKEN, "").trim().isEmpty();
	}

	/**
	 * return whether this dataset is a script dataset
	 * 
	 * @return
	 */
	public boolean isScript() {
		return this.getDsType().equals(DatasetType.COMPLETE) && this.getCollectorType() != null
				&& this.getCollectorType().equals(Dataset.ACTOR);
	}

	/**
	 * return whether this dataset is a script dataset AND the script is active AND receiving data from an OOCSI channel
	 * or Telegram
	 * 
	 * @return
	 */
	public boolean isScriptLive() {
		return isScript() && this.isActive() && !configuration.getOrDefault(ACTOR_CHANNEL, "").trim().isEmpty();
	}

	/**
	 * return whether this dataset is a chatbot dataset
	 * 
	 * @return
	 */
	public boolean isChatbot() {
		return this.getDsType().equals(DatasetType.COMPLETE) && this.getCollectorType() != null
				&& this.getCollectorType().equals(Dataset.CHATBOT);
	}

	/**
	 * return whether the dataset is a wearable dataset
	 * 
	 * @return
	 */
	public boolean isWearableDS() {
		return this.getDsType().equals(DatasetType.FITBIT) || this.getDsType().equals(DatasetType.GOOGLEFIT);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public String toUIString() {
		if (isScript()) {
			return "SCRIPT";
		} else if (isChatbot()) {
			return "BOT";
		} else if (isSavedExport()) {
			return "EXPORT";
		}

		return getDsType().toUIString();
	}

	public String toIcon() {
		return isScript() ? "code" : (isChatbot() ? "chatbot" : getDsType().toIcon());
	}

	public String toColor() {
		return getDsType().toColor();
	}

	public String toColorBG() {
		return isScript() ? "yellow" : (isChatbot() ? "orange" : getDsType().toColorBG());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * return positive value for the difference of this dataset to start or end; return negtive value for finished
	 * dataset
	 * 
	 * @return
	 */
	public int getDaysToGo(Date d) {
		Date date = d == null ? new Date() : d;
		int diff = 0;

		if (isActive()) {
			// positive value
			diff = (int) (Math.ceil(((double) this.getEnd().getTime() - date.getTime()) / 86400000));
		} else {
			if (this.getStart().compareTo(date) > 0) {
				// positive value, this dataset will start
				diff = (int) (Math.ceil(((double) this.getStart().getTime() - date.getTime()) / 86400000));
			} else {
				// negative value, this dataset is finished
				diff = (int) (Math.ceil(((double) this.getEnd().getTime() - date.getTime()) / 86400000));
			}
		}

		return diff;
	}

	/**
	 * return the number items in this data during the specified week
	 * 
	 * @param monday
	 * @return
	 */
	public long getItemQuantity(Date monday) {
		if (monday == null) {
			return 0;
		}

		String dataTableName = DatasetConnector.getDatasetDSUnmanaged(this).getDataTableName();
		if (dataTableName == null) {
			return 0;
		}

		// count rows in table for given time span
		long result = 0;
		try (Transaction transaction = DB.beginTransaction();
				Connection connection = transaction.connection();
				PreparedStatement stmt = connection
						.prepareStatement("SELECT count(*) FROM " + dataTableName + " WHERE ts >= ? AND ts < ?;");) {
			stmt.setTimestamp(1, new Timestamp(DateUtils.thisMonday(monday).getTime()));
			stmt.setTimestamp(2, new Timestamp(DateUtils.toNextMonday(monday).getTime()));
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getLong(1);
			}
			transaction.commit();
		} catch (SQLException e) {
			logger.error("Error in getting item quantity.", e);
		}

		return result;
	}

	/**
	 * return percentage of the variation of data points between the week of the date and last week of the date
	 * 
	 * @param date
	 * @return
	 */
	public int countItemChanges(Date date) {
		if (date == null) {
			date = new Date();
		}

		long quantityThisWeek = getItemQuantity(date),
				quantityLastWeek = getItemQuantity(DateUtils.toPreviousWeek(date));

		// if there is no data last week, then this week is defined as 100%
		if (quantityLastWeek == 0) {
			if (quantityThisWeek == 0) {
				return 0;
			} else {
				return 100;
			}
		}

		return Math.round((quantityThisWeek - quantityLastWeek) * 100f / (float) quantityLastWeek);
	}

	/**
	 * return percentage of the variation of updates between the week of the date and last week of the date
	 * 
	 * @param date
	 * @return
	 */
	public int countUpdateChanges(Date date) {
		return LabNotesEntry.countUpdateChangesByDataset(this, date);
	}

	public int countWeekUpdates(Date date) {
		return LabNotesEntry.countWeekUpdatesByDataset(this, date);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public float datasetCompleteness() {
		List<String> items = Arrays.asList(getName(), getRefId(), getTargetObject(), getDescription(), getKeywords(),
				getDoi(), getRelation(), getOrganization(), getRemarks(), getLicense());
		return items.stream().filter(s -> nnne(s)).count() / (float) items.size();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * retrieve value from configuration, with alternative
	 * 
	 * @param key
	 * @param alternative
	 * @return
	 */
	public String configuration(String key, String alternative) {
		if (configuration.containsKey(key)) {
			return configuration.get(key);
		} else {
			return alternative;
		}
	}

	public Map<String, String> getConfiguration() {
		return configuration;
	}

	/**
	 * check whether the targetChannel slot in configuration contains channelName
	 * 
	 * @param targetChannel
	 * @param channelName
	 * @return
	 */
	public boolean hasOOCSIChannel(String targetChannel, String channelName) {
		return nnne(configuration.get(targetChannel)) && configuration.get(targetChannel).equals(channelName);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get the dataset database table columns as a list of String
	 * 
	 * @return
	 */
	public List<String> getSchema() {
		String[] schema = DatasetConnector.getDatasetSchema(this);
		List<String> schemaList = new LinkedList<String>();
		schemaList.addAll(Arrays.asList(schema));

		List<String> projection = getMetaDataProjection();
		if (!projection.isEmpty() && schemaList.get(schemaList.size() - 1).equals("data")) {
			// reconstruct a new list with projection
			schemaList.remove(schemaList.size() - 1);
			schemaList.addAll(projection);
		}
		return schemaList;
	}

	/**
	 * use projection configuration variable, extract fields, sort by number, then return as list
	 * 
	 * @return
	 */
	public List<String> getMetaDataProjection() {
		String projection = this.configuration.get(DATA_PROJECTION);
		if (projection != null) {
			List<String> projectList = Arrays.asList(projection.split(","));
			Collections.sort(projectList, new Comparator<String>() {
				public int compare(String o1, String o2) {
					return extractLong(o1).compareTo(extractLong(o2));
				}

				Long extractLong(String s) {
					try {
						String num = s.replaceAll("\\D", "");
						// return 0 if no digits found
						return num.isEmpty() ? 0l : DataUtils.parseLong(num);
					} catch (Exception e) {
						logger.error("Exception in numberComparator", e);
						return 0l;
					}
				}
			});
			return new ArrayList<String>(projectList);
		} else {
			return new ArrayList<String>();
		}

	}

	/**
	 * use projection configuration variable, extract fields, retrieve descriptions and units and return as JSON object
	 * 
	 * @return
	 */
	public ObjectNode getMetaDataProjectionDescriptionsJSON() {
		ObjectNode on = Json.newObject();
		addMetadata(on, "id", "identifier of the entry", "number");
		addMetadata(on, "ts", "timestamp", "datetime");
		addMetadata(on, "participant_id", "participant identifier", "string/categorical");
		addMetadata(on, "device_id", "device identifier", "string/categorical");
		addMetadata(on, "wearable_id", "wearable identifier", "string/categorical");
		addMetadata(on, "cluster_id", "cluster identifier", "string/categorical");
		addMetadata(on, "activity", "event or activity marker", "string/categorical");
		addMetadata(on, "pp1", "public parameter 1", "string/categorical");
		addMetadata(on, "pp2", "public parameter 2", "string/categorical");
		addMetadata(on, "pp3", "public parameter 3", "string/categorical");

		final String projection = this.configuration.getOrDefault(DATA_PROJECTION, "");
		try {
			if (!projection.isEmpty()) {
				List<String> projectList = Arrays.asList(projection.split(","));
				Collections.sort(projectList, new Comparator<String>() {
					public int compare(String o1, String o2) {
						return extractLong(o1).compareTo(extractLong(o2));
					}

					Long extractLong(String s) {
						try {
							String num = s.replaceAll("\\D", "");
							// return 0 if no digits found
							return num.isEmpty() ? 0l : DataUtils.parseLong(num);
						} catch (Exception e) {
							logger.error("Exception in numberComparator", e);
							return 0l;
						}
					}
				});
				projectList.stream().forEach(i -> {
					ObjectNode oni = on.putObject(i);
					oni.put("unit", configuration.getOrDefault("meta_unit_" + i, ""));
					oni.put("description", configuration.getOrDefault("meta_desc_" + i, ""));
				});
			}
		} catch (Exception e) {
			logger.error("Expection in getMetaDataProjection (projection: " + projection + ")", e);
		}

		return on;
	}

	/**
	 * add a meta data entry to JSON object
	 * 
	 * @param on
	 */
	private void addMetadata(ObjectNode on, String key, String description, String unit) {
		ObjectNode oni = on.putObject(key);
		oni.put("unit", unit);
		oni.put("description", description);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * valid text = Not Null and Not Empty
	 */
	private static boolean nnne(String text) {
		return text != null && text.length() > 0;
	}

	/**
	 * null-safe String
	 */
	private static String nss(String text) {
		return text != null ? text : "";
	}

	/**
	 * reflection-based stringifier to JSON
	 * 
	 * @return
	 */
	public String toJson() {
		ObjectNode on = DataUtils.toJson(this);
		on.put(ACTOR, this.configuration(ACTOR, null));
		on.put(SAVED_EXPORT, this.configuration(SAVED_EXPORT, null));
		on.put(STUDY_MANAGEMENT, this.configuration(STUDY_MANAGEMENT, null));
		on.put(INFORMED_CONSENT, this.configuration(INFORMED_CONSENT, null));
		on.put(PARTICIPANT_DASHBOARD_PAGE, this.configuration(PARTICIPANT_DASHBOARD_PAGE, null));
		on.put(PARTICIPANT_STUDY_PAGE, this.configuration(PARTICIPANT_STUDY_PAGE, null));
		on.put(DATA_PROJECTION, this.configuration(DATA_PROJECTION, null));
		on.put(API_TOKEN, this.configuration(API_TOKEN, null));
		on.put(PUBLIC_ACCESS_TOKEN, this.configuration(PUBLIC_ACCESS_TOKEN, null));
		on.put(WEB_ACCESS_TOKEN, this.configuration(WEB_ACCESS_TOKEN, null));
		on.put(ACTOR_CHANNEL, this.configuration(ACTOR_CHANNEL, null));
		on.put(ACTOR_CODE, this.configuration(ACTOR_CODE, null));
		on.put(SAVED_EXPORT_LIMIT, this.configuration(SAVED_EXPORT_LIMIT, null));
		on.put(SAVED_EXPORT_SAMPLING, this.configuration(SAVED_EXPORT_SAMPLING, null));
		on.put(SAVED_EXPORT_DATASETS, this.configuration(SAVED_EXPORT_DATASETS, null));
		on.put(DIRECT_EVENT_ACTIVITIES, this.configuration(DIRECT_EVENT_ACTIVITIES, null));
		on.put(STATE_EVENT_ACTIVITIES, this.configuration(STATE_EVENT_ACTIVITIES, null));

		return on.toPrettyString();
	}

	public Date getCreation() {
		return creation;
	}

	public void setCreation(Date creation) {
		this.creation = creation;
	}

	public void setLicense(String license) {
		this.license = license;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

	public void setOrganization(String organization) {
		this.organization = organization;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public String getTargetObject() {
		return targetObject != null ? targetObject : "";
	}

	public void setTargetObject(String targetObject) {
		this.targetObject = targetObject;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getCollectorType() {
		return collectorType;
	}

	public void setCollectorType(String collectorType) {
		this.collectorType = collectorType;
	}

	public DatasetType getDsType() {
		return dsType;
	}

	public void setDsType(DatasetType dsType) {
		this.dsType = dsType;
	}

	public String getApiToken() {
		return apiToken;
	}

	public void setApiToken(String apiToken) {
		this.apiToken = apiToken;
	}

	public String getRefId() {
		return refId;
	}

	public void setRefId(String refId) {
		this.refId = refId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public Date getEnd() {
		return end;
	}

	public void setEnd(Date end) {
		this.end = end;
	}

	public Date getStart() {
		return start;
	}

	public void setStart(Date start) {
		this.start = start;
	}

	public boolean isOpenParticipation() {
		return openParticipation;
	}

	public void setOpenParticipation(boolean openParticipation) {
		this.openParticipation = openParticipation;
	}

}

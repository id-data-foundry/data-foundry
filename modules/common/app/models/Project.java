package models;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.ebean.Finder;
import io.ebean.Model;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.data.format.Formats;
import utils.DataUtils;
import utils.DateUtils;
import utils.rendering.TimeAgo;

@Entity
public class Project extends Model {

	protected final DateFormat tsDateFormatter = new SimpleDateFormat("yyyy-MM-dd");

	@Id
	private Long id;

	// public project name
	private String name;

	// internal STRING reference id
	private String refId;

	// project teaser
	private String intro;

	// additional metadata fields
	// -----------------------------------------------------------------

	// full project description
	private String description;

	// add keywords relating to the content and the place/period of origin of the
	// data
	private String keywords;

	// digital object identifier
	private String doi;

	// refer to a related dataset, publication or journal article
	private String relation;

	// organizations involved, subsidizer (subsidy number)
	private String organization;

	// general remarks / comments
	private String remarks;

	private Date start;
	private Date end;

	@ManyToOne
	private Person owner;
	@OneToMany(mappedBy = "project", cascade = CascadeType.ALL)
	private List<Collaboration> collaborators = new LinkedList<Collaboration>();
	@OneToMany(mappedBy = "project", cascade = CascadeType.ALL)
	private List<Subscription> subscribers = new LinkedList<Subscription>();

	@Formats.DateTime(pattern = "yyyy/MM//dd HH:mm:ss")
	private Date creation = new Date();

	/**
	 * project is archived and thus locked for modifications
	 */
	private boolean archivedProject;
	/**
	 * allow this project to appear in searches and externally
	 */
	private boolean publicProject;
	/**
	 * allow for subscriptions
	 */
	private boolean shareableProject;
	/**
	 * allow for participants to sign up themselves
	 */
	private boolean signupOpen = false;
	/**
	 * is this project frozen
	 */
	private boolean frozenProject;

	private String license;

	@OneToMany(mappedBy = "project", cascade = CascadeType.ALL)
	@OrderBy("id ASC")
	private List<Dataset> datasets = new LinkedList<Dataset>();

	@OneToMany(mappedBy = "project", cascade = CascadeType.ALL)
	@OrderBy("id ASC")
	private List<Cluster> clusters = new LinkedList<Cluster>();

	@OneToMany(mappedBy = "project", cascade = CascadeType.ALL)
	@OrderBy("id ASC")
	private List<Device> devices = new LinkedList<Device>();

	@OneToMany(mappedBy = "project", cascade = CascadeType.ALL)
	@OrderBy("id ASC")
	private List<Wearable> wearables = new LinkedList<Wearable>();

	@OneToMany(mappedBy = "project", cascade = CascadeType.ALL)
	@OrderBy("id ASC")
	private List<Participant> participants = new LinkedList<Participant>();

	/**
	 * these are in the (outgoing) references for this project, i.e., other projects that are referenced by this project
	 */
	@OneToMany(mappedBy = "owningProject", cascade = CascadeType.ALL)
	@OrderBy("id ASC")
	private List<Reference> references = new LinkedList<Reference>();

	/**
	 * these are in the incoming references for this project, i.e., other projects that reference this project
	 */
	@OneToMany(mappedBy = "referencedProject", cascade = CascadeType.ALL)
	@OrderBy("id ASC")
	private List<Reference> referencedBy = new LinkedList<Reference>();

	public static final Finder<Long, Project> find = new Finder<Long, Project>(Project.class);

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static Project create(String name, Person owner, String intro, boolean publicProject,
	        boolean shareableProject) {
		Project p = new Project();
		p.setName(name);
		p.setRefId("p" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		p.setOwner(owner);
		p.setIntro(intro);
		p.setDescription("");
		p.setPublicProject(publicProject);
		p.setShareableProject(shareableProject);
		p.start();
		p.end();
		return p;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether the given user owns this project
	 * 
	 * @param user
	 * @return
	 */
	public boolean belongsTo(Person user) {
		return user != null && belongsTo(user.getEmail());
	}

	/**
	 * check whether the given user owns this project
	 * 
	 * @param user
	 * @return
	 */
	public boolean belongsTo(String username) {
		getOwner().refresh();
		return getOwner().getEmail().equalsIgnoreCase(username);
	}

	/**
	 * check whether the given user collaborates in this project
	 * 
	 * @param user
	 * @return
	 */
	public boolean collaboratesWith(Person user) {
		return user != null && collaboratesWith(user.getEmail());
	}

	/**
	 * check whether the given user collaborates in this project
	 * 
	 * @param user
	 * @return
	 */
	public boolean collaboratesWith(String username) {
		Predicate<Collaboration> p1 = c -> c.getCollaborator().getEmail().equalsIgnoreCase(username);
		return username != null && collaborators.stream().map(c -> {
			c.getCollaborator().refresh();
			return c;
		}).anyMatch(p1);
	}

	/**
	 * check whether the given user subscribes to this project
	 * 
	 * @param user
	 * @return
	 */
	public boolean subscribedBy(String username) {
		Predicate<Subscription> p1 = c -> c.getSubscriber().getEmail().equals(username);
		return username != null && subscribers.stream().map(c -> {
			c.getSubscriber().refresh();
			return c;
		}).anyMatch(p1);
	}

	/**
	 * check whether the given user subscribes to this project
	 * 
	 * @param user
	 * @return
	 */
	public boolean subscribedBy(Person user) {
		Predicate<Subscription> p1 = c -> c.getSubscriber().equals(user);
		return user != null && subscribers.stream().map(c -> {
			c.getSubscriber().refresh();
			return c;
		}).anyMatch(p1);
	}

	/**
	 * check whether the project is either public or visible because the user owns it, collaborates with it, or
	 * subscribed to it
	 * 
	 * @param user
	 * @return
	 */
	public boolean visibleFor(Optional<String> usernameOpt) {
		if (!usernameOpt.isPresent()) {
			return isPublicProject();
		}

		String username = usernameOpt.get();
		return isPublicProject() || belongsTo(username) || collaboratesWith(username) || subscribedBy(username);
	}

	/**
	 * check whether the project is either public or visible because the user owns it, collaborates with it, or
	 * subscribed to it
	 * 
	 * @param user
	 * @return
	 */
	public boolean visibleForPerson(Optional<Person> userOpt) {
		if (!userOpt.isPresent()) {
			return isPublicProject();
		}

		Person user = userOpt.get();
		return isPublicProject() || belongsTo(user) || collaboratesWith(user) || subscribedBy(user);
	}

	/**
	 * check whether the project is either public or visible because the user owns it, collaborates with it, or
	 * subscribed to it
	 * 
	 * @param username
	 * @return
	 */
	public boolean visibleFor(String username) {
		return isPublicProject() || belongsTo(username) || collaboratesWith(username) || subscribedBy(username);
	}

	/**
	 * check whether the project is either public or visible because the user owns it, collaborates with it, or
	 * subscribed to it
	 * 
	 * @param user
	 * @return
	 */
	public boolean visibleFor(Person user) {
		return isPublicProject() || belongsTo(user) || collaboratesWith(user) || subscribedBy(user);
	}

	/**
	 * check whether the project is editable by the given user
	 * 
	 * @param username
	 * @return
	 */
	public boolean editableBy(Optional<String> username) {
		return username.isPresent() && editableBy(username.get());
	}

	/**
	 * check whether the project is editable by the given user
	 * 
	 * @param user
	 * @return
	 */
	public boolean editableByPerson(Optional<Person> user) {
		return user.isPresent() && editableBy(user.get());
	}

	/**
	 * check whether the project is editable by the given user
	 * 
	 * @param username
	 * @return
	 */
	public boolean editableBy(String username) {
		return belongsTo(username) || collaboratesWith(username);
	}

	/**
	 * check whether the project is editable by the given user
	 * 
	 * @param user
	 * @return
	 */
	public boolean editableBy(Person user) {
		return belongsTo(user) || collaboratesWith(user);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether this project is a team project
	 * 
	 * @param project
	 * @return
	 */
	public boolean hasTeam() {
		return !collaborators.isEmpty();
	}

	/**
	 * return a sorted list of collaborator names
	 * 
	 * @return
	 */
	public String getTeamNames() {
		return getCollaborators().stream().map(c -> c.getCollaborator())
		        .sorted((a, b) -> a.getLastname().compareToIgnoreCase(b.getLastname())).map(p -> p.getName())
		        .collect(Collectors.joining(", "));
	}

	/**
	 * return a sorted list of team members
	 * 
	 * @param project
	 * @return
	 */
	public List<Person> getTeam() {
		return Stream.concat(Stream.of(getOwner()), getCollaborators().stream().map(c -> c.getCollaborator()))
		        .sorted((a, b) -> a.getLastname().compareToIgnoreCase(b.getLastname())).collect(Collectors.toList());
	}

	/**
	 * return a sorted list of team members, the first name will be the project owner
	 * 
	 * @param project
	 * @return
	 */
	public List<Person> getOwnerAndTeam() {
		return Stream
		        .concat(Stream.of(getOwner()),
		                collaborators.stream().map(c -> c.getCollaborator())
		                        .sorted((a, b) -> a.getLastname().compareToIgnoreCase(b.getLastname())))
		        .collect(Collectors.toList());
	}

	/**
	 * return a sorted names of team members, the first name will be the project owner
	 *
	 * @param project
	 * @return
	 */
	public String getOwnerAndTeamNames() {
		return Stream
		        .concat(Stream.of(getOwner()),
		                collaborators.stream().map(c -> c.getCollaborator())
		                        .sorted((a, b) -> a.getLastname().compareToIgnoreCase(b.getLastname())))
		        .map(p -> p.getName()).collect(Collectors.joining(", "));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * can this project be annotated, i.e., is an annotation dataset available
	 *
	 * @return
	 */
	public boolean canAnnotate() {
		return getAnnotationDataset().getId() != -1L;
	}

	public boolean hasDSType(String dsType) {
		final DatasetType dtype = DatasetType.valueOf(dsType);
		return datasets.stream().anyMatch(ds -> ds.getDsType().equals(dtype));
	}

	public boolean hasParticipantWithEmail(String email) {
		Predicate<Participant> p1 = c -> c.getEmail().equalsIgnoreCase(email);
		return participants.stream().map(c -> {
			c.refresh();
			return c;
		}).anyMatch(p1);
	}

	public boolean hasParticipant(Participant participant) {
		Predicate<Participant> p1 = c -> c.getId().equals(participant.getId());
		return participants.stream().map(c -> {
			c.refresh();
			return c;
		}).anyMatch(p1);
	}

	public boolean hasDevice(String deviceRefId) {
		Predicate<Device> p1 = c -> c.getRefId().equals(deviceRefId);
		return devices.stream().map(c -> {
			c.refresh();
			return c;
		}).anyMatch(p1);
	}

	public boolean hasDevice(Device device) {
		Predicate<Device> p1 = c -> c.getId().equals(device.getId());
		return devices.stream().map(c -> {
			c.refresh();
			return c;
		}).anyMatch(p1);
	}

	public boolean hasWearable(Wearable wearable) {
		Predicate<Wearable> p1 = c -> c.getId().equals(wearable.getId());
		return wearables.stream().map(c -> {
			c.refresh();
			return c;
		}).anyMatch(p1);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public long getConsentedParticipantCount() {
		return participants.stream().filter(p -> p.accepted()).count();
	}

	public long getConnectedWearablesCount() {
		return wearables.stream().filter(p -> p.isConnected()).count();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check active datasets in this project if not null, then this project is active
	 * 
	 * @return
	 */
	public boolean isActive() {
		return datasets.stream().anyMatch(d -> {
			if (d.getStart() == null) {
				d.refresh();
			}
			return d.isActive();
		});
	}

	public boolean isActiveOrHasRecentlyEnded() {
		return this.isActive() || (this.end() != null && this.end().after(TimeAgo.monthsAgo(3)));
	}

	/**
	 * whether this project was created more than n days ago (n = 75) which means it is "locked"; it is "unlocked"
	 * otherwise
	 * 
	 * this is needed for allowing for modifications
	 * 
	 * @return
	 */
	public boolean isLocked() {
		return (this.start() != null && this.start().before(TimeAgo.daysAgo(75)));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * whether this project is close to having ended more than 3 months ago (cut-off @ 10 weeks)
	 * 
	 * @return
	 */
	public boolean isApproachingDeidentifiable() {
		return !isDeidentifiable() && DateUtils.moveWeeks(new Date(), -10).after(end());
	}

	/**
	 * whether this project has ended more than 6 months ago
	 * 
	 * @return
	 */
	public boolean isDeidentifiable() {
		return DateUtils.moveMonths(new Date(), -3).after(end());
	}

	/**
	 * whether this project is close to having ended more than 24 months ago
	 * 
	 * @return
	 */
	public boolean isApproachingOutdated() {
		return !isOutdated() && DateUtils.moveMonths(new Date(), -22).after(end());
	}

	/**
	 * whether this project has ended more than 24 months ago
	 * 
	 * @return
	 */
	public boolean isOutdated() {
		return DateUtils.moveMonths(new Date(), -24).after(end());
	}

	/**
	 * set the frozenProject property to the given value
	 * 
	 * @param frozen
	 */
	public void setFrozen(boolean frozen) {
		this.frozenProject = frozen;
	}

	/**
	 * whether this project is frozen
	 * 
	 * this is needed for project lifecycle and clean-up
	 * 
	 * @return
	 */
	public boolean isFrozen() {
		return frozenProject;
	}

	/**
	 * retrieve whether the project has been modified since last approval
	 * 
	 * @return
	 */
	public boolean isApproved() {
		Date lastApproval = LabNotesEntry.lastApproval(this.getId());
		if (lastApproval == null) {
			return false;
		}

		return lastApproval.after(LabNotesEntry.lastModification(this.getId()));
	}

	/**
	 * retrieve whether the project was once approved (whatever modifications afterwards)
	 * 
	 * @return
	 */
	public boolean hasOnceBeenApproved() {
		return LabNotesEntry.lastApproval(this.getId()) != null;
	}

	/**
	 * check whether the given project is DF native
	 * 
	 * @return
	 */
	public boolean isDFNativeProject() {
		return getRelation() == null || (!getRelation().startsWith("$report$")
		        && !getRelation().startsWith("$pinboard$") && !getRelation().startsWith("$feedback$"));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public String getName() {
		return nnne(name) ? name : "";
	}

	public String getDescription() {
		return nnne(description) ? description : "";
	}

	public String getKeywords() {
		return nnne(keywords) ? keywords : "";
	}

	public String[] getKeywordList() {
		return Arrays.stream(getKeywords().split(",")).map(String::trim).filter(kw -> !kw.isEmpty())
		        .collect(Collectors.toUnmodifiableList()).toArray(new String[] {});
	}

	public String getDoi() {
		return nnne(doi) ? doi : "";
	}

	public String getRelation() {
		return nnne(relation) ? relation : "";
	}

	public String getOrganization() {
		return nnne(organization) ? organization : "";
	}

	public String getRemarks() {
		return nnne(remarks) ? remarks : "";
	}

	/**
	 * get license acronym; fixes also null license fields
	 * 
	 * @return
	 */
	public String getLicense() {

		// fix licenses first
		if (license == null || (!license.isEmpty() && !license.startsWith("MIT") && !license.startsWith("CC"))) {
			license = "";
			this.update();
		}

		return nnne(license) ? license : "no license";
	}

	/**
	 * get link that matches the project license; empty string if project has no license
	 * 
	 * @return
	 */
	public String getLicenseLink() {
		switch (getLicense()) {
		case "MIT":
			return "https://opensource.org/licenses/MIT";
		case "CC BY":
			return "https://creativecommons.org/licenses/by/4.0/";
		case "CC BY-SA":
			return "https://creativecommons.org/licenses/by-sa/4.0/";
		case "CC BY-ND":
			return "https://creativecommons.org/licenses/by-nd/4.0/";
		case "CC BY-NC":
			return "https://creativecommons.org/licenses/by-nc/4.0/";
		case "CC BY-NC-SA":
			return "https://creativecommons.org/licenses/by-nc-sa/4.0/";
		case "CC BY-NC-ND":
			return "https://creativecommons.org/licenses/by-nc-nd/4.0/";

		default:
			return "";
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public void reopen() {
		this.setArchivedProject(false);
		this.setEnd(DateUtils.endOfDay(new Date()));
		this.update();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * deidentify participants in project
	 * 
	 */
	public void deidentifyParticipants() {
		for (Participant participant : participants) {
			participant.deidentify();
		}
	}

	/**
	 * deidentify devices in project
	 * 
	 */
	public void deidentifyDevices() {
		for (Device device : devices) {
			device.deidentify();
		}
	}

	/**
	 * deidentify all wearables in project
	 * 
	 */
	public void deidentifyWearables() {
		for (Wearable wearable : wearables) {
			wearable.deidentify();
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get starting date of earliest starting dataset in project or the project starting date (whichever is earlier)
	 * 
	 * @return
	 */
	public Date start() {
		if (this.getStart() == null) {
			this.setStart(DateUtils.startOfDay(new Date()));

			if (this.getId() != null) {
				this.update();
			}
		}

		// find earliest project dataset start date
		Optional<Dataset> ods = datasets.stream().min((a, b) -> a.start().compareTo(b.start()));
		// if found and earlier than project start date, update project start date
		if (ods.isPresent() && ods.get().getStart().before(this.getStart())) {
			this.getStart().setTime(ods.get().getStart().getTime());
			this.update();
		}

		return this.getStart();
	}

	/**
	 * get ending date of latest ending dataset in project or the project ending date (whichever is later)
	 * 
	 * @return
	 */
	public Date end() {
		if (this.getEnd() == null) {
			this.setEnd(DateUtils.inThreeMonths(DateUtils.endOfDay(new Date())));

			if (this.getId() != null) {
				this.update();
			}
		}

		// find the latest project dataset end date
		Optional<Dataset> ods = datasets.stream().max((a, b) -> a.end().compareTo(b.end()));
		// if found and later than project end date, update project end date
		if (ods.isPresent() && ods.get().getEnd().after(this.getEnd())) {
			this.getEnd().setTime(ods.get().getEnd().getTime());
			this.update();
		}

		return this.getEnd();
	}

	public String startDate() {
		return tsDateFormatter.format(start());
	}

	public String endDate() {
		return tsDateFormatter.format(end());
	}

	public String creationDate() {
		return tsDateFormatter.format(getCreation());
	}

	/**
	 * return the progress of this dataset in percent (start > now > end)
	 * 
	 * @return
	 */
	public float getTimeProgress() {
		long startTime = start().getTime();
		long duration = end().getTime() - startTime;
		return duration == 0 ? 100 : Math.max(1, 100 * (System.currentTimeMillis() - startTime)) / duration;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public List<Reference> getReferences() {
		return references;
	}

	public void createReference(Project reference, String referenceInfo) {
		Reference r = new Reference(this, reference);
		r.referenceInformation = referenceInfo;
		r.save();
	}

	public void removeReference(Long id) {
		Optional<Reference> rOpt = getReferences().stream().filter(r -> r.getId().equals(id)).findAny();
		if (!rOpt.isPresent()) {
			return;
		}

		Reference r = rOpt.get();
		r.delete();

		// remove from project
		this.getReferences().remove(r);
		this.update();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Optional<Dataset> findDatasetById(long datasetId) {
		return datasets.stream().filter(ds -> ds.getId().equals(datasetId)).findFirst();
	}

	/**
	 * returns the dataset that holds the project's website files
	 * 
	 * @return
	 */
	public Dataset getProjectWebsiteDataset() {
		Optional<Dataset> ds = this.datasets.stream()
		        .filter(d -> d.getDsType().equals(DatasetType.COMPLETE) && d.getName().equals("www")).findFirst();
		return ds.isPresent() ? ds.get() : null;
	}

	public boolean hasProjectWebsite() {
		return getProjectWebsiteDataset() != null;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * return all COMPLETE datasets in project that are ACTORS (as specified by their collectorType)
	 * 
	 * @return
	 */
	public List<Dataset> getProjectActors() {
		return this.datasets.stream().filter(d -> d.getDsType().equals(DatasetType.COMPLETE)
		        && d.getCollectorType() != null && d.getCollectorType().equals("ACTOR")).collect(Collectors.toList());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get all datasets
	 * 
	 * @return
	 */
	public List<Dataset> getAllDatasets() {
		return this.datasets.stream().filter(d -> !d.isStudyManagement()).collect(Collectors.toList());
	}

	/**
	 * get first active fitbit dataset
	 * 
	 * @return
	 */
	public Dataset getFitbitDataset() {
		return getActiveDataset(DatasetType.FITBIT);
	}

	/**
	 * get all fitbit datasets
	 * 
	 * @return
	 */
	public List<Dataset> getFitbitDatasets() {
		return getAllDatasets(DatasetType.FITBIT);
	}

	/**
	 * get first active googlefit dataset
	 * 
	 * @return
	 */
	public Dataset getGoogleFitDataset() {
		return getActiveDataset(DatasetType.GOOGLEFIT);
	}

	/**
	 * get all googlefit datasets
	 * 
	 * @return
	 */
	public List<Dataset> getGoogleFitDatasets() {
		return getAllDatasets(DatasetType.GOOGLEFIT);
	}

	/**
	 * get first active experience sampling dataset
	 * 
	 * @return
	 */
	public Dataset getExpSamplingDataset() {
		return getActiveDataset(DatasetType.ES);
	}

	/**
	 * get all experience sampling datasets
	 * 
	 * @return
	 */
	public List<Dataset> getExpSamplingDatasets() {
		return getAllDatasets(DatasetType.ES);
	}

	/**
	 * get first active survey dataset
	 * 
	 * @return
	 */
	public Dataset getSurveyDataset() {
		return getActiveDataset(DatasetType.SURVEY);
	}

	/**
	 * get all survey datasets
	 * 
	 * @return
	 */
	public List<Dataset> getSurveyDatasets() {
		return getAllDatasets(DatasetType.SURVEY);
	}

	/**
	 * get first active annotation dataset
	 * 
	 * @return
	 */
	public Dataset getAnnotationDataset() {
		return getActiveDataset(DatasetType.ANNOTATION);
	}

	/**
	 * get all annotation datasets
	 * 
	 * @return
	 */
	public List<Dataset> getAnnotationDatasets() {
		return getAllDatasets(DatasetType.ANNOTATION);
	}

	/**
	 * get first active IoT dataset
	 * 
	 * @return
	 */
	public Dataset getIoTDataset() {
		return getActiveDataset(DatasetType.IOT);
	}

	/**
	 * get all IoT datasets
	 * 
	 * @return
	 */
	public List<Dataset> getIoTDatasets() {
		return getAllDatasets(DatasetType.IOT);
	}

	/**
	 * get first active Entity dataset
	 * 
	 * @return
	 */
	public Dataset getEntityDataset() {
		return getActiveDataset(DatasetType.ENTITY);
	}

	/**
	 * get all Entity datasets
	 * 
	 * @return
	 */
	public List<Dataset> getEntityDatasets() {
		return getAllDatasets(DatasetType.ENTITY);
	}

	/**
	 * get the first active media dataset
	 * 
	 * @return
	 */
	public Dataset getMediaDataset() {
		return getActiveDataset(DatasetType.MEDIA);
	}

	/**
	 * get all media datasets
	 * 
	 * @return
	 */
	public List<Dataset> getMediaDatasets() {
		return getAllDatasets(DatasetType.MEDIA);
	}

	/**
	 * get the first active movement dataset
	 * 
	 * @return
	 */
	public Dataset getMovementDataset() {
		return getActiveDataset(DatasetType.MOVEMENT);
	}

	/**
	 * get all movement datasets
	 * 
	 * @return
	 */
	public List<Dataset> getMovementDatasets() {
		return getAllDatasets(DatasetType.MOVEMENT);
	}

	/**
	 * get the first active complete/existing dataset
	 * 
	 * @return
	 */
	public Dataset getCompleteDataset() {
		return getActiveDataset(DatasetType.COMPLETE);
	}

	/**
	 * get all active complete/existing datasets
	 * 
	 * @return
	 */
	public List<Dataset> getCompleteDatasets() {
		return getActiveDatasets(DatasetType.COMPLETE);
	}

	/**
	 * all active complete/existing datasets that are not scripts and that are LIVE!
	 * 
	 * @return
	 */
	public List<Dataset> getWebsiteDatasets() {
		return this.datasets.stream()
		        .filter(d -> d.getDsType() == DatasetType.COMPLETE && d.isWebsiteLive() && d.isActive())
		        .collect(Collectors.toList());
	}

	/**
	 * get the first active diary dataset
	 * 
	 * @return
	 */
	public Dataset getDiaryDataset() {
		return getActiveDataset(DatasetType.DIARY);
	}

	/**
	 * get all diary datasets
	 * 
	 * @return
	 */
	public List<Dataset> getDiaryDatasets() {
		return this.datasets.stream().filter(d -> d.getDsType() == DatasetType.DIARY).collect(Collectors.toList());
	}

	/**
	 * return list of datasets that are not hidden and can be configured and used openly. this will filter out datasets
	 * like a study setup dataset which needs special treatment and is kind of hidden
	 * 
	 * @return
	 */
	public List<Dataset> getOperationalDatasets() {
		return this.datasets.stream().filter(d -> !d.isStudyManagement()).collect(Collectors.toList());
	}

	/**
	 * get study setup dataset for this project
	 * 
	 * @return
	 */
	public Optional<Dataset> getStudyManagementDataset() {
		return this.datasets.stream().filter(d -> d.getDsType() == DatasetType.COMPLETE && d.isStudyManagement())
		        .findAny();
	}

	/**
	 * get narrrative survey dataset for this project
	 * 
	 * @return
	 */
	public Optional<Dataset> getNarrativeSurveyDataset() {
		return this.datasets.stream().filter(d -> d.getDsType() == DatasetType.COMPLETE && d.isNarrativeSurvey())
		        .findAny();
	}

	/**
	 * get the one active complete dataset that is designated for the participant study
	 * 
	 * @return
	 */
	public Dataset getParticipantStudyDataset() {
		return this.datasets.stream()
		        .filter(d -> d.getDsType() == DatasetType.COMPLETE && d.isActive()
		                && Dataset.PARTICIPANT_STUDY_PAGE.equals(d.getTargetObject())
		                && !d.configuration(Dataset.WEB_ACCESS_TOKEN, "").isEmpty())
		        .findFirst().orElse(Dataset.EMPTY_DATASET);
	}

	/**
	 * get the one active complete dataset that is designated for the participant dashboard
	 * 
	 * @return
	 */
	public Dataset getParticipantDashboardDataset() {
		return this.datasets.stream()
		        .filter(d -> d.getDsType() == DatasetType.COMPLETE && d.isActive()
		                && Dataset.PARTICIPANT_DASHBOARD_PAGE.equals(d.getTargetObject())
		                && !d.configuration(Dataset.WEB_ACCESS_TOKEN, "").isEmpty())
		        .findFirst().orElse(Dataset.EMPTY_DATASET);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get first active dataset for given <code>datasetType</code>
	 * 
	 * @return
	 */
	private Dataset getActiveDataset(DatasetType datasetType) {
		return getActiveDatasets(datasetType).stream().findFirst().orElse(Dataset.EMPTY_DATASET);
	}

	/**
	 * get all active datasets for given <code>datasetType</code>
	 * 
	 * @return
	 */
	private List<Dataset> getActiveDatasets(DatasetType datasetType) {
		return this.datasets.stream().filter(d -> d.getDsType() == datasetType && d.isActive())
		        .collect(Collectors.toList());
	}

	/**
	 * get all datasets for given <code>datasetType</code>
	 * 
	 * @return
	 */
	private List<Dataset> getAllDatasets(DatasetType datasetType) {
		return this.datasets.stream().filter(d -> d.getDsType() == datasetType).collect(Collectors.toList());
	}

	public Set<DatasetType> getAllDatasetTypeSet() {
		return this.datasets.stream().map(ds -> ds.getDsType()).collect(Collectors.toSet());
	}

	public Map<String, String> getAllDatasetUIStringSet() {
		return this.datasets.stream().filter(ds -> !ds.isStudyManagement() && !ds.isSavedExport())
		        .collect(Collectors.toMap(ds -> {
			        if (ds.isScript()) {
				        return "SCRIPT";
			        } else {
				        return ds.getDsType().toUIString();
			        }
		        }, ds -> {
			        if (ds.isScript()) {
				        return "orange lighten-3";
			        } else {
				        return ds.getDsType().toColorBG();
			        }
		        }, (a, b) -> a));
	}

	/**
	 * return list of dataset with the same DatasetType as targetDS and would start after it get(n): n = 0, current
	 * dataset; n >= 1: next active datasets
	 * 
	 * @param targetDS
	 * @return
	 */
	public List<Dataset> getNextDSList(Dataset targetDS) {
		if (targetDS == null) {
			return Collections.emptyList();
		}

		return Dataset.find.query().where().eq("project", this).and().eq("dsType", targetDS.getDsType()).and()
		        .ge("start", targetDS.getStart()).orderBy("start asc").findList();
	}

	/**
	 * return list of dataset with the same DatasetType as Dataset given by ds_id and would start after it get(n): n =
	 * 0, current dataset; n >= 1: next active datasets
	 * 
	 * @param ds_id
	 * @return
	 */
	public List<Dataset> getNextDSList(long ds_id) {
		return getNextDSList(Dataset.find.byId(ds_id));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public float projectCompleteness() {
		List<String> items = Arrays.asList(getName(), getRefId(), getIntro(), getDescription(), getKeywords(), getDoi(),
		        getRelation(), getOrganization(), getRemarks(), getLicense());
		return items.stream().filter(s -> nnne(s)).count() / (float) items.size();
	}

	public int metadataCompleteness() {
		return (int) (100 * (projectCompleteness() + datasets.stream().map(ds -> (double) ds.datasetCompleteness())
		        .collect(Collectors.summingDouble(d -> d))) / (float) (1 + datasets.size()));
	}

	public long getLastUpdated() {
		Date lastUpdated = LabNotesEntry.lastUpdatedByProject(this.getId());
		if (lastUpdated != null) {
			return lastUpdated.getTime();
		} else {
			return this.getCreation().getTime();
		}
	}

	public String getLastUpdatedFormatted() {
		return TimeAgo.toDuration(System.currentTimeMillis() - getLastUpdated());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * return list of collaborations created in the week of monday
	 * 
	 * @param date
	 * @return
	 */
	public List<Collaboration> getCollaborations(Date monday) {
		return this.collaborators.stream().filter(c -> DateUtils.isInWeekOf(monday, c.getCreated()))
		        .collect(Collectors.toList());
	}

	/**
	 * return list of subscriptions created in the week of monday
	 * 
	 * @param date
	 * @return
	 */
	public List<Subscription> getSubscriptions(Date monday) {
		return this.subscribers.stream().filter(s -> DateUtils.isInWeekOf(monday, s.getCreated()))
		        .collect(Collectors.toList());
	}

	/**
	 * return list of participants created in the week of monday
	 * 
	 * @param monday
	 * @return
	 */
	public List<Participant> getParticipants(Date monday) {
		return this.participants.stream().filter(s -> DateUtils.isInWeekOf(monday, s.getCreation()))
		        .collect(Collectors.toList());
	}

	/**
	 * count the number of new devices in the week of monday
	 * 
	 * @param monday
	 * @return
	 */
	public int countNewDevices(Date monday) {
		return LabNotesEntry.countNewDevices(this, monday);
	}

	/**
	 * count the number of new wearables in the week of monday
	 * 
	 * @param monday
	 * @return
	 */
	public int countNewWearables(Date monday) {
		return LabNotesEntry.countNewWearables(this, monday);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * valid text = Not Null and Not Empty
	 */
	private static boolean nnne(String text) {
		return text != null && text.length() > 0;
	}

	/**
	 * reflection-based stringifier to JSON
	 * 
	 * @return
	 */
	public String toJson() {
		ObjectNode on = DataUtils.toJson(this);
		return on.toPrettyString();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public List<Collaboration> getCollaborators() {
		return collaborators;
	}

	public void setCollaborators(List<Collaboration> collaborators) {
		this.collaborators = collaborators;
	}

	public List<Subscription> getSubscribers() {
		return subscribers;
	}

	public void setSubscribers(List<Subscription> subscribers) {
		this.subscribers = subscribers;
	}

	public Date getCreation() {
		return creation;
	}

	public void setCreation(Date creation) {
		this.creation = creation;
	}

	public List<Dataset> getDatasets() {
		return datasets;
	}

	public void setDatasets(List<Dataset> datasets) {
		this.datasets = datasets;
	}

	public List<Cluster> getClusters() {
		return clusters;
	}

	public void setClusters(List<Cluster> clusters) {
		this.clusters = clusters;
	}

	public List<Device> getDevices() {
		return devices;
	}

	public void setDevices(List<Device> devices) {
		this.devices = devices;
	}

	public List<Wearable> getWearables() {
		return wearables;
	}

	public void setWearables(List<Wearable> wearables) {
		this.wearables = wearables;
	}

	public List<Participant> getParticipants() {
		return participants;
	}

	public void setParticipants(List<Participant> participants) {
		this.participants = participants;
	}

	public DateFormat getTsDateFormatter() {
		return tsDateFormatter;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public int hashCode() {
		return getId().hashCode();
	}

	public void setName(String name) {
		this.name = name;
	}

	public Person getOwner() {
		return owner;
	}

	public void setOwner(Person owner) {
		this.owner = owner;
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

	public String getIntro() {
		return intro;
	}

	public void setIntro(String intro) {
		this.intro = intro;
	}

	public String getRefId() {
		return refId;
	}

	public void setRefId(String refId) {
		this.refId = refId;
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

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public boolean isSignupOpen() {
		return signupOpen;
	}

	public void setSignupOpen(boolean signupOpen) {
		this.signupOpen = signupOpen;
	}

	public boolean isShareableProject() {
		return shareableProject;
	}

	public void setShareableProject(boolean shareableProject) {
		this.shareableProject = shareableProject;
	}

	public boolean isPublicProject() {
		return publicProject;
	}

	public void setPublicProject(boolean publicProject) {
		this.publicProject = publicProject;
	}

	public boolean isArchivedProject() {
		return archivedProject;
	}

	public void setArchivedProject(boolean archivedProject) {
		this.archivedProject = archivedProject;
	}
}

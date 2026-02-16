package models;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import datasets.DatasetConnector;
import io.ebean.Finder;
import io.ebean.Model;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import play.Logger;
import play.data.format.Formats;
import play.libs.Json;
import services.slack.Slack;
import utils.DataUtils;
import utils.DateUtils;
import utils.admin.AdminUtils;
import utils.auth.Hash;
import utils.auth.Roles;
import utils.components.OnboardingSupport;

@Entity
public class Person extends Model {

	@Id
	private Long id;
	private String user_id;
	private String identity;
	private String firstname;
	private String lastname;
	private String email;
	private String website;
	private String accesscode;

	@Formats.DateTime(pattern = "yyyy/MM/dd")
	public Date creation = new Date();
	@Formats.DateTime(pattern = "yyyy/MM/dd")
	public Date lastAction = new Date();

	private String passwordHash;

	@OneToMany(mappedBy = "owner")
	@OrderBy("id ASC")
	private List<Project> projects = new LinkedList<Project>();

	@OneToMany(mappedBy = "collaborator")
	@OrderBy("id ASC")
	private List<Collaboration> collaborations = new LinkedList<Collaboration>();

	@OneToMany(mappedBy = "subscriber")
	@OrderBy("id ASC")
	private List<Subscription> subscriptions = new LinkedList<Subscription>();

	// ---------------

	public static final String USER_ID = "user_id";
	public static final String USER_NAME = "username";

	public static final Finder<Long, Person> find = new Finder<Long, Person>(Person.class);

	private static final Logger.ALogger logger = Logger.of(Person.class);

	////////////////////////////////////////////////////////////////////////////////////////

	public static Person register(String user_id, String firstname, String lastname, String email, String website,
	        String password, String access_inline) {
		Person p = new Person();
		p.setUser_id(user_id);
		p.setIdentity(newIdentity().toString());
		p.setFirstname(firstname);
		p.setLastname(lastname);
		p.setEmail(email.toLowerCase());
		p.setWebsite(website);
		p.setPasswordHash(Hash.hashPassword(password));
		p.setAccesscode(access_inline);
		return p;
	}

	public static Person empty() {
		Person p = new Person();
		p.setUser_id("");
		p.setIdentity(newIdentity().toString());
		p.setFirstname("");
		p.setLastname("");
		p.setEmail("");
		p.setWebsite("");
		p.setPasswordHash("");
		p.setAccesscode("");
		return p;
	}

	/**
	 * set the person's password, which is transparently hashed for security
	 * 
	 * @param password
	 */
	public void setPassword(String password) {
		this.passwordHash = Hash.hashPassword(password);
	}

	/**
	 * returns true if the password matches
	 * 
	 * @param password
	 * @return
	 */
	public boolean checkPassword(String password) {

		// migration to hashed passwords
		if (!Hash.isHashed(this.passwordHash)) {
			setPassword(this.passwordHash);
			this.update();
		}

		return this.passwordHash.equals(Hash.hashPassword(password));
	}

	/**
	 * find person from database by email
	 * 
	 * @param email
	 * @return
	 */
	public static Optional<Person> findByEmail(String email) {
		if (email == null || email.length() == 0) {
			return Optional.<Person>empty();
		}

		return Person.find.query().setMaxRows(1).where().ieq("email", email.toLowerCase()).findOneOrEmpty();
	}

	/**
	 * find person from database by email
	 * 
	 * @param email
	 * @return
	 */
	public static int findByEmailCount(String email) {
		if (email == null || email.length() == 0) {
			return 0;
		}

		return Person.find.query().setMaxRows(1).where().ieq("email", email.toLowerCase()).findCount();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public List<Project> unManagedProjects() {
		return projects;
	}

	/**
	 * retrieve all DF native projects, so everything except for reports and pinboards
	 * 
	 * @return
	 */
	public List<Project> projects() {
		return projects.stream().filter(p -> p.isDFNativeProject() && !p.getRefId().equals(AdminUtils.SYSTEM_PROJECT))
		        .collect(Collectors.toList());
	}

	public List<Project> unManagedCollaborations() {
		return collaborations.stream().map(c -> c.getProject()).collect(Collectors.toList());
	}

	/**
	 * retrieve all DF native project collaborations, so everything except for reports and pinboards
	 * 
	 * @return
	 */
	public List<Project> collaborations() {
		return collaborations.stream().map(c -> c.getProject()).filter(p -> p.isDFNativeProject())
		        .collect(Collectors.toList());
	}

	/**
	 * get all own and collaboration projects of this user in a single list, sorted within own and collabs by project ID
	 * 
	 * @return
	 */
	public List<Project> getOwnAndCollabProjects() {
		List<Project> results = new LinkedList<>();
		results.addAll(this.projects().stream().sorted((p1, p2) -> p1.getId().compareTo(p2.getId()))
		        .collect(Collectors.toList()));
		results.addAll(this.collaborations.stream().filter(c -> c.getProject().isDFNativeProject()).map(c -> {
			c.getProject().refresh();
			return c.getProject();
		}).sorted((p1, p2) -> p1.getId().compareTo(p2.getId())).collect(Collectors.toList()));
		return results;
	}

	/**
	 * retrieve all DF native project subscriptions, so everything except for reports and pinboards
	 * 
	 * @return
	 */
	public List<Project> subscriptions() {
		return subscriptions.stream().map(s -> s.getProject()).filter(p -> p.isDFNativeProject())
		        .collect(Collectors.toList());
	}

	/**
	 * retrieve all DF native archived projects, so everything archived except for reports and pinboards
	 * 
	 * @return
	 */
	public List<Project> archivedProjects() {
		return projects().stream().filter(p -> p.isArchivedProject()).collect(Collectors.toList());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether this user is the owner of the given project
	 * 
	 * @param project
	 * @return
	 */
	public boolean owns(Project project) {
		return projects.stream().anyMatch(p -> p.getId().equals(project.getId()))
		        || project.getOwner().email.equals(this.email);
	}

	/**
	 * check whether this user is a collaborator in the given project
	 * 
	 * @param project
	 * @return
	 */
	public boolean collaboratesOn(Project project) {
		Predicate<Collaboration> p1 = c -> c.getProject().getId().equals(project.getId());
		return collaborations.stream().map(c -> {
			c.getProject().refresh();
			return c;
		}).anyMatch(p1);
	}

	/**
	 * retrieve new collaborations for this user and week
	 * 
	 * @param monday
	 * @return
	 */
	public List<Collaboration> getCollaborations(Date monday) {
		return collaborations.stream().filter(s -> DateUtils.isInWeekOf(monday, s.getCreated()))
		        .collect(Collectors.toList());
	}

	/**
	 * check whether this user is a subscriber to the given project
	 * 
	 * @param project
	 * @return
	 */
	public boolean subscribesTo(Project project) {
		Predicate<Subscription> p1 = c -> c.getProject().getId().equals(project.getId());
		return subscriptions.stream().map(c -> {
			c.getProject().refresh();
			return c;
		}).anyMatch(p1);
	}

	/**
	 * retrieve new subscriptions for this user and week
	 * 
	 * @param monday
	 * @return
	 */
	public List<Subscription> getSubscriptions(Date monday) {
		return subscriptions.stream().filter(s -> DateUtils.isInWeekOf(monday, s.getCreated()))
		        .collect(Collectors.toList());
	}

	/**
	 * check whether this user can edit the given project (not archived + owns or collaborates)
	 * 
	 * @param project
	 * @return
	 */
	public boolean canEdit(Project project) {
		return !project.isArchivedProject() && (owns(project) || collaboratesOn(project));
	}

	/**
	 * check whether this user can subscribe to the given project (not subscribing, collaborating or owning currently)
	 * 
	 * @param project
	 * @return
	 */
	public boolean canSubscribe(Project project) {
		return !subscribesTo(project) && !collaboratesOn(project) && !owns(project);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * render the full name of this person
	 * 
	 * @return
	 */
	public String getName() {
		String displayName = "";
		String first = getFirstname();
		String last = getLastname();

		if (last.contains(",")) {
			String[] parts = last.split(",", 2);
			String part1 = parts[0].trim();
			String part2 = parts[1].trim();
			displayName = first + " " + part2 + " " + part1;
		} else {
			displayName = first + " " + last;
		}
		return displayName.trim();
	}

	/**
	 * register last action on this entity (only if this action is some minutes ago)
	 * 
	 */
	public void touch() {
		if (lastAction == null || lastAction.getTime() < System.currentTimeMillis() - (2 * 60 * 1000)) {
			lastAction = new Date();
			this.update();
		}
	}

	/**
	 * list all actors of this user (from all user projects' datasets)
	 * 
	 * @return
	 */
	public List<Dataset> getUserActors() {
		return this.projects().stream().flatMap(p -> p.getDatasets().stream())
		        .filter(d -> d.getDsType().equals(DatasetType.COMPLETE) && d.getCollectorType() != null
		                && d.getCollectorType().equals(Dataset.ACTOR))
		        .collect(Collectors.toList());
	}

	/**
	 * list all _active_ actors of this user (from all user projects' datasets)
	 * 
	 * @return
	 */
	public List<Dataset> getActiveUserActors() {
		return this.projects().stream().flatMap(p -> p.getDatasets().stream())
		        .filter(d -> d.getDsType().equals(DatasetType.COMPLETE) && d.isActive() && d.getCollectorType() != null
		                && d.getCollectorType().equals(Dataset.ACTOR))
		        .collect(Collectors.toList());
	}

	/**
	 * list all _live_ actors of this user (from all user projects' datasets)
	 * 
	 * @return
	 */
	public List<Dataset> getLiveUserActors() {
		return this.projects().stream().flatMap(p -> p.getDatasets().stream())
		        .filter(d -> d.getDsType().equals(DatasetType.COMPLETE) && d.isActive() && d.getCollectorType() != null
		                && d.getCollectorType().equals(Dataset.ACTOR) && d.isScriptLive())
		        .collect(Collectors.toList());
	}

	/**
	 * list all actors in this project belonging to this user
	 * 
	 * @return
	 */
	public List<Dataset> getUserActors(Long userId) {
		return Project.find.byId(userId).getDatasets().stream().filter(d -> d.isScript()).collect(Collectors.toList());
	}

	/**
	 * list all actors from this user's collaborations
	 * 
	 * @return
	 */
	public List<Dataset> getUserCollaborationActors() {
		return this.collaborations.stream().map(c -> c.getProject()).flatMap(p -> p.getDatasets().stream())
		        .filter(d -> d.isScript()).map(d -> {
			        d.getProject().refresh();
			        return d;
		        }).collect(Collectors.toList());
	}

	/**
	 * list all notebooks from this user (from all user projects' datasets)
	 * 
	 * @return
	 */
	public List<Dataset> getUserNotebookDatasets(DatasetConnector datasetConnector) {
		return this.projects().stream().flatMap(p -> p.getCompleteDatasets().stream()).filter(ds -> ds.isWebsite())
		        .collect(Collectors.toList());
	}

	/**
	 * list all saved exports for the given projects visible to this user
	 * 
	 * @return
	 */
	public List<Dataset> getSavedExports(List<Project> filterProjects) {
		return filterProjects.stream().flatMap(p -> p.getDatasets().stream()).filter(d -> d.isSavedExport())
		        .collect(Collectors.toList());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether a user-specific role is set in the identity JSON structure
	 * 
	 * @param key
	 * @return
	 */
	public boolean isRole(String key) {
		ObjectNode identityNode = getIdentity();
		if (!identityNode.has(Roles.ROLE)) {
			return false;
		}

		JsonNode roleNode = identityNode.get(Roles.ROLE).get(key);
		return roleNode != null && roleNode.asBoolean() && roleNode.asBoolean();
	}

	/**
	 * set a user-specific role, which is stored in the identity JSON structure
	 * 
	 * @param key
	 * @param value
	 */
	public void setRole(String key, boolean value) {
		ObjectNode identityNode = getIdentity();
		if (!identityNode.has(Roles.ROLE)) {
			identityNode.putObject(Roles.ROLE);
		}

		((ObjectNode) identityNode.get(Roles.ROLE)).put(key, value);
		setIdentity(identityNode);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether a user-specific property if it exists
	 * 
	 * @param key
	 * @return
	 */
	public boolean hasIdentityProperty(String key) {
		ObjectNode identityNode = getIdentity();
		if (!identityNode.has("property")) {
			return false;
		}

		JsonNode propertyNode = identityNode.get("property").get(key);
		return propertyNode != null && !propertyNode.isNull() && !propertyNode.asText("").isEmpty();
	}

	/**
	 * retrieve a user-specific property, which is stored in the identity JSON structure
	 * 
	 * @param key
	 * @return
	 */
	public String getIdentityProperty(String key) {
		ObjectNode identityNode = getIdentity();
		if (!identityNode.has("property")) {
			return "";
		}

		JsonNode propertyNode = identityNode.get("property").get(key);
		return propertyNode != null ? propertyNode.asText() : "";
	}

	/**
	 * set a user-specific property, which is stored in the identity JSON structure
	 * 
	 * @param key
	 * @param value
	 */
	public void setIdentityProperty(String key, String value) {
		ObjectNode identityNode = getIdentity();
		if (!identityNode.has("property")) {
			identityNode.putObject("property");
		}

		((ObjectNode) identityNode.get("property")).put(key, value);
		setIdentity(identityNode);
	}

	/**
	 * extract the user identity as a parsed JSON object
	 * 
	 * @return
	 */
	public ObjectNode getIdentity() {
		// check whether older users already have an identity assigned
		if (this.identity == null) {
			// if not assign it and save
			ObjectNode newIdentity = newIdentity();
			this.identity = newIdentity.toString();
			this.update();

			return newIdentity;
		}

		try {
			ObjectNode on = (ObjectNode) Json.parse(this.identity);
			return on;
		} catch (Exception e) {
			logger.error("Error in getting identity JSON.", e);
			Slack.call("Exception - parse state ", e.getLocalizedMessage());
		}

		return newIdentity();
	}

	/**
	 * set identity
	 * 
	 * @param identity
	 */
	public void setIdentity(ObjectNode identity) {
		this.identity = identity.toString();
		this.update();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * initialize identity of new user
	 * 
	 * @return
	 */
	public static ObjectNode newIdentity() {
		final ObjectNode on = Json.newObject();
		on.put("main", OnboardingState.ACTIVE.ordinal());
		// on.putObject("topic");
		on.put("current", "");
		on.put("available_after", DateUtils.getMillisFromToday(0));

		final ObjectNode sceneNode = on.putObject("scene");
		for (String basic : OnboardingSupport.STATES) {
			sceneNode.put(basic, OnboardingState.INITIAL.ordinal());
		}

		return on;
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

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getFirstname() {
		return firstname != null ? firstname : "";
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname != null ? lastname : "";
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getInitials() {
		// Extended list of common last name prefixes to exclude (all lowercase)
		final String[] PREFIXES = { "van", "von", "van der", "van de", "van den", "de", "der", "den", "del", "della",
		        "dello", "degli", "dei", "du", "des", "d'", "da", "das", "do", "dos", "af", "av", "mac", "mc", "o'",
		        "le", "la", "el" };

		// Handle first name: take the first word
		String firstInitial = "";
		if (getFirstname() != null && !getFirstname().isEmpty()) {
			String[] firstParts = getFirstname().trim().split("\\s+");
			firstInitial = firstParts[0].substring(0, 1).toUpperCase();
		}

		// Handle last name: remove prefix, then take first component
		String lastNameTrimmed = getLastname().trim().toLowerCase();

		// Check prefixes longest first to avoid partial matches (e.g., "van der" before "van")
		java.util.Arrays.sort(PREFIXES, (a, b) -> Integer.compare(b.length(), a.length()));

		for (String prefix : PREFIXES) {
			if (lastNameTrimmed.startsWith(prefix + " ")) {
				lastNameTrimmed = lastNameTrimmed.substring(prefix.length()).trim();
				break;
			}
		}

		String[] lastParts = lastNameTrimmed.split("\\s+");
		String lastInitial = lastParts[0].substring(0, 1).toUpperCase();

		return firstInitial + lastInitial;
	}

	public String getEmail() {
		return email != null ? email : "";
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getWebsite() {
		return website;
	}

	public void setWebsite(String website) {
		this.website = website;
	}

	public String getAccesscode() {
		return accesscode;
	}

	public void setAccesscode(String accesscode) {
		this.accesscode = accesscode;
	}

	public Date getCreation() {
		return creation;
	}

	public void setCreation(Date creation) {
		this.creation = creation;
	}

	public Date getLastAction() {
		return lastAction;
	}

	public void setLastAction(Date lastAction) {
		this.lastAction = lastAction;
	}

	public String getPasswordHash() {
		return passwordHash;
	}

	public void setPasswordHash(String passwordHash) {
		this.passwordHash = passwordHash;
	}

	public List<Project> getProjects() {
		return projects;
	}

	public void setProjects(List<Project> projects) {
		this.projects = projects;
	}

	public List<Collaboration> getCollaborations() {
		return collaborations;
	}

	public void setCollaborations(List<Collaboration> collaborations) {
		this.collaborations = collaborations;
	}

	public List<Subscription> getSubscriptions() {
		return subscriptions;
	}

	public void setSubscriptions(List<Subscription> subscriptions) {
		this.subscriptions = subscriptions;
	}

	public void setIdentity(String identity) {
		this.identity = identity;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	@Override
	public int hashCode() {
		return getId().hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Person) {
			Person other = (Person) o;
			return this.getId().equals(other.getId());
		}

		return super.equals(o);
	}

}

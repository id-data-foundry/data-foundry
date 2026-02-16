package models.sr;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.vdurmont.emoji.EmojiParser;

import io.ebean.Finder;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.ManyToMany;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import play.Logger;
import play.data.format.Formats;
import play.libs.Json;
import utils.DateUtils;

@Entity
public class Participant extends DataResource {

	private static final Logger.ALogger logger = Logger.of(Participant.class);
	private final static String[] EMOJIS = { "dog", "wolf", "cat", "mouse", "hamster", "rabbit", "frog", "tiger",
	        "koala", "bear", "pig", "pig_nose", "cow", "boar", "monkey_face", "monkey", "horse", "sheep", "elephant",
	        "panda_face", "penguin", "bird", "baby_chick", "hatched_chick", "hatching_chick", "chicken", "snake",
	        "turtle", "bug", "bee", "honeybee", "ant", "beetle", "snail", "octopus", "shell", "tropical_fish", "fish",
	        "dolphin", "flipper", "whale", "whale2", "cow2", "ram", "rat", "water_buffalo", "tiger2", "rabbit2",
	        "dragon", "racehorse", "goat", "rooster", "dog2", "pig2", "mouse2", "ox", "dragon_face", "blowfish",
	        "crocodile", "camel", "dromedary_camel", "leopard", "cat2", "poodle", "feet", "paw_prints", "bouquet",
	        "cherry_blossom", "tulip", "four_leaf_clover", "rose", "sunflower", "hibiscus", "maple_leaf", "leaves",
	        "fallen_leaf", "herb", "ear_of_rice", "mushroom", "cactus", "palm_tree", "evergreen_tree", "deciduous_tree",
	        "chestnut", "seedling", "blossom", "sun_with_face", "full_moon_with_face", "new_moon_with_face" };

	private String firstname;
	private String lastname;
	private int gender;
	private String email;
	private int ageRange;
	private String career;
	private String identity;
	private ParticipationStatus status;

	@ManyToMany(mappedBy = "participants", cascade = CascadeType.ALL)
	private List<Cluster> clusters = new LinkedList<Cluster>();

	@Formats.DateTime(pattern = "yyyy/MM/dd")
	private Date creation = new Date();

	private String passwordHash;

	public static final Finder<Long, Participant> find = new Finder<Long, Participant>(Participant.class);

	public static final Participant EMPTY_PARTICIPANT = new Participant("", "");
	static {
		EMPTY_PARTICIPANT.setId(-1l);
		EMPTY_PARTICIPANT.setEmail("");
		EMPTY_PARTICIPANT.setRefId("");
		EMPTY_PARTICIPANT.setPublicParameter1("");
		EMPTY_PARTICIPANT.setPublicParameter2("");
		EMPTY_PARTICIPANT.setPublicParameter3("");
		EMPTY_PARTICIPANT.setGender(4);
		EMPTY_PARTICIPANT.setCareer("");
		EMPTY_PARTICIPANT.setAgeRange(5);
	}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Participant(String firstName, String lastName) {
		this.setFirstname(firstName);
		this.setLastname(lastName);
		this.setStatus(ParticipationStatus.PENDING);
	}

	public Participant(String email) {
		this.setEmail(email.toLowerCase());
		this.setStatus(ParticipationStatus.PENDING);
	}

	public static Participant createInstance(String firstName, String lastName, String email, Project project) {
		Participant p = new Participant(firstName, lastName);
		p.setEmail(email.toLowerCase());
		p.setGender(4);
		p.setCareer("");
		p.setAgeRange(5);
		p.setPublicParameter1("");
		p.setPublicParameter2("");
		p.setPublicParameter3("");
		p.setProject(project);
		p.create();
		return p;
	}

	public void create() {
		this.setRefId("u" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		logger.info("New participant registered with refId: " + this.getRefId());

		// log this only once
		LabNotesEntry.log(Participant.class, LabNotesEntryType.CREATE, "Participant created: " + this.getName(),
		        this.getProject());
	}

	private synchronized void checkIntegrity() {
		if (this.getRefId() == null || this.getRefId().length() == 0) {
			this.create();
			this.update();
		}
	}

	/**
	 * find participants by given refId
	 * 
	 * @param refId
	 * @return
	 */
	public static Optional<Participant> findByRefId(String refId) {
		if (refId == null || refId.isEmpty()) {
			return Optional.empty();
		}

		return Participant.find.query().where().ieq("ref_id", refId.toLowerCase()).findOneOrEmpty();
	}

	/**
	 * find participants by given email
	 * 
	 * @param email
	 * @return
	 */
	public static List<Participant> findByEmail(String email) {
		if (email == null || email.isEmpty()) {
			return Collections.emptyList();
		}

		return Participant.find.query().where().ieq("email", email.toLowerCase()).findList();
	}

	/**
	 * find participant count by given email
	 * 
	 * @param email
	 * @return
	 */
	public static int findByEmailCount(String email) {
		if (email == null || email.length() == 0) {
			return 0;
		}

		return Participant.find.query().where().ieq("email", email.toLowerCase()).findCount();
	}

	/**
	 * find participant by given email within a project
	 * 
	 * @param email
	 * @param projectId
	 * @return
	 */
	public static Optional<Participant> findByEmailAndProject(String email, Long projectId) {
		if (email == null || email.length() == 0) {
			return Optional.<Participant>empty();
		}

		return Participant.find.query().setMaxRows(1).where().ieq("email", email.toLowerCase())
		        .eq("project_id", projectId).findOneOrEmpty();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * retrieve the first single participant cluster for this participant
	 * 
	 * @return
	 */
	public Optional<Cluster> getCluster() {
		return clusters.stream().filter(cluster -> cluster.getParticipants().size() == 1
		        && cluster.getParticipants().get(0).getRefId().equals(this.getRefId())).findFirst();

	}

	/**
	 * retrieve a list of devices that are associated to this participant because they are in the same cluster together
	 * 
	 * @return
	 */
	public List<Device> getClusterDevices() {
		List<Device> devices = new LinkedList<>();
		for (Cluster cluster : clusters) {
			cluster.refresh();
			if (cluster.getParticipants().size() == 1
			        && cluster.getParticipants().get(0).getRefId().equals(this.getRefId())
			        && cluster.getDevices().size() > 0) {
				for (Device device : cluster.getDevices()) {
					device.refresh();
					devices.add(device);
				}
			}
		}

		return devices;
	}

	/**
	 * retrieve a list of wearables that are associated to this participant because they are in the same cluster
	 * together
	 * 
	 * @return
	 */
	public List<Wearable> getClusterWearables() {
		List<Wearable> wearables = new LinkedList<>();
		for (Cluster cluster : clusters) {
			cluster.refresh();
			if (cluster.getParticipants().size() == 1
			        && cluster.getParticipants().get(0).getRefId().equals(this.getRefId())
			        && cluster.getWearables().size() > 0) {
				for (Wearable wearable : cluster.getWearables()) {
					wearable.refresh();
					wearables.add(wearable);
				}
			}
		}

		return wearables;
	}

	/**
	 * returns the real name of a participant
	 * 
	 * @return
	 */
	public String getRealName() {
		if (nnne(getFirstname()) && nnne(getLastname())) {
			return (getFirstname() + " " + getLastname());
		} else {
			if (nnne(getFirstname()) || nnne(getLastname())) {
				return nnne(getFirstname()) ? getFirstname() : getLastname();
			} else {
				return getEmail();
			}
		}
	}

	public String startDate() {
		return getProject().start().after(getCreation()) ? DateUtils.tsFormat(getProject().start())
		        : DateUtils.tsFormat(getCreation());
	}

	public void setPassword(String tmpPwd) {
		passwordHash = hashMe(tmpPwd);
	}

	private static String hashMe(String password) {
		return password;
	}

	public boolean checkPassword(String password) {
		return this.passwordHash.equals(hashMe(password));
	}

	public String gender() {
		switch (this.getGender()) {
		case 1:
			return "Male";
		case 2:
			return "Female";
		case 3:
			return "Non-binary";
		case 0:
			return "Prefer not to say";
		case 4:
		default:
			return "-";
		}
	}

	public String ageRange() {
		switch (this.getAgeRange()) {
		case 1:
			return "Under 18";
		case 2:
			return "18 - 25";
		case 3:
			return "25 - 50";
		case 4:
			return "Above 50";
		case 0:
			return "Prefer not to say";
		case 5:
		default:
			return "-";
		}
	}

	public String clusterMember() {
		return this.clusters.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
	}

	public boolean pending() {
		checkIntegrity();

		return getStatus() == ParticipationStatus.PENDING;
	}

	public boolean accepted() {
		checkIntegrity();

		return getStatus() == ParticipationStatus.ACCEPT;
	}

	public boolean declined() {
		checkIntegrity();

		return getStatus() == ParticipationStatus.DECLINE;
	}

	/**
	 * check for double registration in the same project with the same email
	 * 
	 * @param participant_email
	 * @param pid
	 * @return
	 */
	public static boolean existsInProject(String participant_email, long pid) {
		return Participant.findByEmailAndProject(participant_email, pid).isPresent();
	}

	/**
	 * unlike users in the system, this returns the anonymized participant name
	 * 
	 * @return
	 */
	public String getName() {
		if (this.getProject() == null) {
			return "P-1";
		}

		int index = getProject().getParticipants().indexOf(this);
		if (index == -1) {
			index = getProject().getParticipants().size();
		}

		return "P" + (index + 1) + " " + EmojiParser.parseToUnicode(":" + EMOJIS[index % EMOJIS.length] + ":");

	}

	public String getSlug() {
		return getName().replaceAll("[\\s;:!@#$%^&*.,]", "_");
	}

	/**
	 * extract user identity as plain string
	 * 
	 * @return
	 */
	public String getIdentity() {
		// check whether older participants already have an identity object assigned
		if (this.identity == null) {
			// if not assign it and save
			this.identity = Json.newObject().toString();
			this.update();
		}

		return this.identity;
	}

	public String getCareer() {
		return nss(this.career);
	}

	/**
	 * check whether participant contains potentially identifiable information and needs resetting to deidentify project
	 * 
	 * @return
	 */
	public boolean canDeidentify() {
		return (getFirstname() != null && !getFirstname().isEmpty())
		        || (getLastname() != null && !getLastname().isEmpty()) || (getEmail() != null && !getEmail().isEmpty());
	}

	/**
	 * reset the real names as the project is archived
	 * 
	 */
	public void deidentify() {
		this.setFirstname("");
		this.setLastname("");
		this.setEmail("");
		this.update();

		logger.info("Reset participant " + this.getId() + " (" + this.getRefId() + ")");
	}

	public List<Cluster> getClusters() {
		return clusters;
	}

	public void setClusters(List<Cluster> clusters) {
		this.clusters = clusters;
	}

	public String getPasswordHash() {
		return passwordHash;
	}

	public Date getCreation() {
		return creation;
	}

	public void setCreation(Date creation) {
		this.creation = creation;
	}

	public ParticipationStatus getStatus() {
		return status;
	}

	public void setStatus(ParticipationStatus status) {
		this.status = status;
	}

	public void setIdentity(String identity) {
		this.identity = nss(identity);
	}

	public void setCareer(String career) {
		this.career = nss(career);
	}

	public int getAgeRange() {
		return ageRange;
	}

	public void setAgeRange(int ageRange) {
		this.ageRange = ageRange;
	}

	public String getEmail() {
		return nss(email);
	}

	public void setEmail(String email) {
		this.email = nss(email);
	}

	public int getGender() {
		return gender;
	}

	public void setGender(int gender) {
		this.gender = gender;
	}

	public String getLastname() {
		return nss(lastname);
	}

	public void setLastname(String lastname) {
		this.lastname = nss(lastname);
	}

	public String getFirstname() {
		return nss(firstname);
	}

	public void setFirstname(String firstname) {
		this.firstname = nss(firstname);
	}

}

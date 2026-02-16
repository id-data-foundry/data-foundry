package models.sr;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import io.ebean.Finder;
import io.ebean.Model;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import play.Logger;
import utils.DataUtils;

@Entity
public class Cluster extends Model {

	@Id
	private Long id;
	private String refId;

	private String name;

	@ManyToOne
	private Project project;

	@ManyToMany(cascade = CascadeType.ALL)
	private List<Device> devices = new LinkedList<Device>();

	@ManyToMany(cascade = CascadeType.ALL)
	private List<Participant> participants = new LinkedList<Participant>();

	@ManyToMany(cascade = CascadeType.ALL)
	private List<Wearable> wearables = new LinkedList<Wearable>();

	public static final Finder<Long, Cluster> find = new Finder<Long, Cluster>(Cluster.class);

	private static final Logger.ALogger logger = Logger.of(Cluster.class);

	public Cluster(String name) {
		this.setName(name);
	}

	public Cluster(Device device) {
		this(device.getSlug());
		this.refId = "c" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
		devices.add(device);
	}

	public Cluster(Participant participant) {
		this(participant.getSlug());
		this.refId = "c" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
		participants.add(participant);
	}

	public Cluster(Wearable wearable) {
		this(wearable.getSlug());
		this.refId = "c" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
		wearables.add(wearable);
	}

	public void create() {
		this.refId = "c" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
		logger.info("New cluster created with refId: " + this.refId);

		LabNotesEntry.log(Cluster.class, LabNotesEntryType.CREATE, "Project cluster created: " + this.name,
				this.project);
	}

	/**
	 * delete this cluster, and before deleting, system will unlink all the resources in the cluster and update the
	 * project it belongs to
	 * 
	 */
	public void remove() {

		boolean hasResources = devices.isEmpty() && participants.isEmpty() && wearables.isEmpty() ? false : true;

		// remove all links to resources
		ImmutableList.copyOf(devices).forEach(d -> remove(d));
		ImmutableList.copyOf(participants).forEach(d -> remove(d));
		ImmutableList.copyOf(wearables).forEach(d -> remove(d));

		// remove this
		project.getClusters().removeIf(c -> c.id.equals(id));
		project.update();

		// log before
		LabNotesEntry.log(Cluster.class, LabNotesEntryType.DELETE, "Cluster " + this.name + " deleted ", this.project);

		project = null;

		try {
			if (hasResources) {
				update();
			}
		} catch (Exception e) {
			logger.info("Fail to update cluster. ", e);
		}

		delete();
	}

	public void add(Participant participant) {
		participants.add(participant);
		participant.getClusters().add(this);
		update();
		participant.update();

		LabNotesEntry.log(Cluster.class, LabNotesEntryType.MODIFY,
				"Participant " + participant.getName() + " added to cluster " + this.name, this.project);
	}

	public void add(Device device) {
		devices.add(device);
		device.getClusters().add(this);
		update();
		device.update();

		LabNotesEntry.log(Cluster.class, LabNotesEntryType.MODIFY,
				"Device " + device.getName() + " added to cluster " + this.name, this.project);
	}

	public void add(Wearable wearable) {
		wearables.add(wearable);
		wearable.getClusters().add(this);
		update();
		wearable.update();

		LabNotesEntry.log(Cluster.class, LabNotesEntryType.MODIFY,
				"Wearable " + wearable.getName() + " added to cluster " + this.name, this.project);
	}

	public void remove(Participant participant) {
		participants.removeIf(c -> c.getId().equals(participant.getId()));
		participant.getClusters().removeIf(c -> c.id.equals(id));
		update();
		participant.update();

		LabNotesEntry.log(Cluster.class, LabNotesEntryType.DELETE,
				"Participant " + participant.getName() + " deleted from cluster " + this.name, this.project);
	}

	public void remove(Device device) {
		devices.removeIf(c -> c.getId().equals(device.getId()));
		device.getClusters().removeIf(c -> c.id.equals(id));
		update();
		device.update();

		LabNotesEntry.log(Cluster.class, LabNotesEntryType.DELETE,
				"Device " + device.getName() + " deleted from cluster " + this.name, this.project);
	}

	public void remove(Wearable wearable) {
		wearables.removeIf(c -> c.getId().equals(wearable.getId()));
		wearable.getClusters().removeIf(c -> c.id.equals(id));
		update();
		wearable.update();

		LabNotesEntry.log(Cluster.class, LabNotesEntryType.DELETE,
				"Wearable " + wearable.getName() + " deleted from cluster " + this.name, this.project);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * get list of ids of participants in this cluster, can be empty
	 * 
	 * @return
	 */
	public List<Long> getParticipantList() {
		return participants.stream().map(l -> l.getId()).collect(Collectors.toList());
	}

	/**
	 * get list of ids of devices in this cluster, can be empty
	 * 
	 * @return
	 */
	public List<Long> getDeviceList() {
		return devices.stream().map(l -> l.getId()).collect(Collectors.toList());
	}

	/**
	 * get list of ids of wearables in this cluster, can be empty
	 * 
	 * @return
	 */
	public List<Long> getWearableList() {
		return wearables.stream().map(l -> l.getId()).collect(Collectors.toList());
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////

	public boolean hasParticipant(Participant participant) {
		return participant == null ? false : participants.stream().anyMatch(p -> p.getId().equals(participant.getId()));
	}

	public boolean hasDevice(Device device) {
		return device == null ? false : devices.stream().anyMatch(d -> d.getId().equals(device.getId()));
	}

	public boolean hasWearable(Wearable wearable) {
		return wearable == null ? false : wearables.stream().anyMatch(w -> w.getId().equals(wearable.getId()));
	}

	/**
	 * if this cluster has only one participant and the participant is in declined status, return true
	 * 
	 * @return
	 */
	public boolean hasDeclinedParticipant() {
		if (participants.size() == 1 && participants.get(0).getStatus() == ParticipationStatus.DECLINE) {
			return true;
		}
		return false;
	}

	/**
	 * reflection-based stringifier to JSON
	 * 
	 * @return
	 */
	public String toJson() {
		return DataUtils.toJson(this).toPrettyString();
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getRefId() {
		return refId;
	}

	public void setRefId(String refId) {
		this.refId = refId;
	}

	public String getName() {
		return name != null ? name : "";
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSlug() {
		return getName().replaceAll("[\\s;:!@#$%^&*.,]", "_");
	}

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public List<Device> getDevices() {
		return devices;
	}

	public void setDevices(List<Device> devices) {
		this.devices = devices;
	}

	public List<Participant> getParticipants() {
		return participants;
	}

	public void setParticipants(List<Participant> participants) {
		this.participants = participants;
	}

	public List<Wearable> getWearables() {
		return wearables;
	}

	public void setWearables(List<Wearable> wearables) {
		this.wearables = wearables;
	}
}

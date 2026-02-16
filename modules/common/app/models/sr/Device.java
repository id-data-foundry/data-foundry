package models.sr;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import io.ebean.Finder;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.ManyToMany;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import play.Logger;

@Entity
public class Device extends DataResource {

	private String name;
	private String category;
	private String subtype;
	private String ipAddress;
	private String location;
	private String configuration;

	@ManyToMany(mappedBy = "devices", cascade = CascadeType.ALL)
	private List<Cluster> clusters = new LinkedList<Cluster>();

	public static final Finder<Long, Device> find = new Finder<Long, Device>(Device.class);

	private static final Logger.ALogger logger = Logger.of(Device.class);

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Device() {
		super();
	}

	public void create() {
		this.setRefId("d" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		logger.info("new device created with refId: " + this.getRefId());

		LabNotesEntry.log(Device.class, LabNotesEntryType.CREATE, "Device created: " + this.getName(),
		        this.getProject());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether device contains potentially identifiable information and needs resetting to deidentify project
	 * 
	 * @return
	 */
	public boolean canDeidentify() {
		return (getConfiguration() != null && !getConfiguration().isEmpty())
		        || (getIpAddress() != null && !getIpAddress().isEmpty())
		        || (getLocation() != null && !getLocation().isEmpty());
	}

	/**
	 * reset the device configuration, ip, and location properties (= deidentify)
	 * 
	 */
	public void deidentify() {
		this.setConfiguration("");
		this.setIpAddress("");
		this.setLocation("");
		this.update();

		logger.info("Reset device " + this.getId() + " (" + this.getRefId() + ")");
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public List<Cluster> getClusters() {
		return clusters;
	}

	public void setClusters(List<Cluster> clusters) {
		this.clusters = clusters;
	}

	public String getConfiguration() {
		return configuration;
	}

	public void setConfiguration(String configuration) {
		this.configuration = configuration;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public String getSubtype() {
		return subtype;
	}

	public void setSubtype(String subtype) {
		this.subtype = subtype;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
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
}

package models;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import io.ebean.Finder;
import io.ebean.Model;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import utils.DateUtils;

@Entity
public class LabNotesEntry extends Model {

	@Id
	Long id;

	public String source;
	public LabNotesEntryType logType;
	public String message;

	public Date timestamp;

	@ManyToOne
	Project project;

	@ManyToOne
	Dataset dataset;

	public static final Finder<Long, LabNotesEntry> find = new Finder<Long, LabNotesEntry>(LabNotesEntry.class);

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static final List<LabNotesEntry> findByProject(Long id) {
		return LabNotesEntry.find.query().where().eq("project_id", id).orderBy("id DESC").findList();
	}

	public static final Date lastUpdatedByProject(Long id) {
		LabNotesEntry entry = LabNotesEntry.find.query().setMaxRows(1).where().eq("project_id", id)
		        .orderBy("timestamp DESC").findOne();

		return entry != null ? entry.timestamp : null;
	}

	public static final Date lastApproval(Long id) {
		LabNotesEntry entry = LabNotesEntry.find.query().setMaxRows(1).where().eq("project_id", id)
		        .eq("log_type", LabNotesEntryType.APPROVE.ordinal()).orderBy("timestamp DESC").findOne();

		return entry != null ? entry.timestamp : null;
	}

	public static final Date lastModification(Long id) {
		LabNotesEntry entry = LabNotesEntry.find.query().setMaxRows(1).where().and().eq("project_id", id).or()
		        .eq("log_type", LabNotesEntryType.CONFIGURE.ordinal())
		        .eq("log_type", LabNotesEntryType.CREATE.ordinal()).eq("log_type", LabNotesEntryType.DELETE.ordinal())
		        .eq("log_type", LabNotesEntryType.MODIFY.ordinal()).endOr().orderBy("timestamp DESC").findOne();

		return entry != null ? entry.timestamp : null;
	}

	public static final List<LabNotesEntry> findByDataset(Long id) {
		return LabNotesEntry.find.query().where().eq("dataset_id", id).findList();
	}

	public static void log(@SuppressWarnings("rawtypes") Class clazz, LabNotesEntryType logType, String message,
	        Project project) {
		log(clazz, logType, message, project, null);
	}

	public static void log(@SuppressWarnings("rawtypes") Class clazz, LabNotesEntryType logType, String message,
	        Project project, Dataset dataset) {
		LabNotesEntry alr = new LabNotesEntry();
		alr.source = clazz.getSimpleName();
		alr.logType = logType;
		alr.message = message;
		alr.timestamp = new Date();
		alr.project = project;
		alr.dataset = dataset;

		alr.save();
	}

	public static enum LabNotesEntryType {
		INIT, CREATE, CONFIGURE, MODIFY, DATA, DELETE, DOWNLOAD, APPROVE, COMMENT;

		public String toColor() {
			switch (this) {
			case INIT:
			case CREATE:
				return "blue";
			case CONFIGURE:
			case MODIFY:
				return "purple";
			case DATA:
				return "green";
			case DELETE:
				return "red";
			case DOWNLOAD:
				return "orange";
			case APPROVE:
				return "yellow";
			case COMMENT:
			default:
				return "grey";
			}
		}

	}

	////////////////////////////////////////////////////////////////////////////////

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		// timestamp
		sb.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ").format(timestamp));
		sb.append(";");
		// item type
		sb.append(this.logType.name());
		sb.append(";");
		// dataset
		if (this.dataset != null) {
			sb.append("DS:" + this.dataset.getRefId());
		} else {
			sb.append("-");
		}
		sb.append(";");
		// message
		sb.append(this.message);

		return sb.toString();
	}

	////////////////////////////////////////////////////////////////////////////////

	/**
	 * return the number of the notes in the specific week
	 * 
	 * @param user
	 * @return
	 */
	public static int countWeekUpdates(Person user, Date monday) {

		// check user first
		if (user == null) {
			return -1;
		}

		return (int) user.projects().stream().map(p -> {
			List<LabNotesEntry> allNotes = findByProject(p.getId());
			return allNotes.stream().filter(lne -> DateUtils.isInWeekOf(monday, lne.timestamp)).count();
		}).collect(Collectors.summarizingLong(i -> i)).getSum();
	}

	public static int countNewWearables(Project project, Date date) {
		// check parameters first
		if (project == null || date == null) {
			return 0;
		}

		return findByProject(project.getId()).stream()
		        .filter(p -> DateUtils.isInWeekOf(date, p.timestamp) && p.message.startsWith("Wearable created:"))
		        .collect(Collectors.toList()).size();
	}

	public static int countNewDevices(Project project, Date date) {
		// check parameters first
		if (project == null || date == null) {
			return 0;
		}

		return findByProject(project.getId()).stream()
		        .filter(p -> DateUtils.isInWeekOf(date, p.timestamp) && p.message.startsWith("Device created:"))
		        .collect(Collectors.toList()).size();
	}

	/**
	 * return quantity of updates for the ds in the week of the date but for now, the parameter---dataset of
	 * LabNotesEntry is not logged.
	 * 
	 * @param ds
	 * @param date
	 * @return
	 */
	public static int countWeekUpdatesByDataset(Dataset ds, Date date) {
		if (ds == null) {
			return -1;
		}

		final Date ts;
		if (date != null) {
			ts = date;
		} else {
			ts = new Date();
		}

		return findByDataset(ds.getId()).stream().filter(p -> DateUtils.isInWeekOf(ts, p.timestamp))
		        .collect(Collectors.toList()).size();
	}

	/**
	 * return percentage of the variation of updates between the week of the date and last week of the date
	 * 
	 * @param ds
	 * @param date
	 * @return
	 */
	public static int countUpdateChangesByDataset(Dataset ds, Date date) {
		if (ds == null) {
			return 0;
		}

		final Date ts, tsLastWeek;
		if (date != null) {
			ts = date;
			tsLastWeek = DateUtils.toPreviousWeek(date);
		} else {
			ts = new Date();
			tsLastWeek = DateUtils.toPreviousWeek(ts);
		}

		int updatesThisWeek = countWeekUpdatesByDataset(ds, ts),
		        updatesLastWeek = countWeekUpdatesByDataset(ds, tsLastWeek);

		// new dataset, and divide 0 is not available
		if (updatesLastWeek == 0) {
			return 0;
		}

		return (updatesThisWeek - updatesLastWeek) * 100 / updatesLastWeek;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public LabNotesEntryType getLogType() {
		return logType;
	}

	public void setLogType(LabNotesEntryType logType) {
		this.logType = logType;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public Dataset getDataset() {
		return dataset;
	}

	public void setDataset(Dataset dataset) {
		this.dataset = dataset;
	}
}

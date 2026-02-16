package models;

import java.util.Date;

import io.ebean.DB;
import io.ebean.Finder;
import io.ebean.Model;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;

@Entity
public class Analytics extends Model {

	@Id
	Long id;

	// timestamp
	Date timestamp;

	// metrics
	public int projectViews;
	public int projectUpdates;
	public int projectDatasetDownloads;
	public int projectDatasetUpdates;
	public int projectWebsiteViews;
	public int projectParticipantInteractions;
	public int projectScriptInvocations;

	// link to project
	@ManyToOne
	public Project project;

	public static final Finder<Long, Analytics> find = new Finder<Long, Analytics>(Analytics.class);

	public Analytics(long id, Date timestamp) {
		this.project = DB.reference(Project.class, id);
		this.timestamp = timestamp;
		this.save();
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public int getProjectViews() {
		return projectViews;
	}

	public void setProjectViews(int projectViews) {
		this.projectViews = projectViews;
	}

	public int getProjectUpdates() {
		return projectUpdates;
	}

	public void setProjectUpdates(int projectUpdates) {
		this.projectUpdates = projectUpdates;
	}

	public int getProjectDatasetDownloads() {
		return projectDatasetDownloads;
	}

	public void setProjectDatasetDownloads(int projectDatasetDownloads) {
		this.projectDatasetDownloads = projectDatasetDownloads;
	}

	public int getProjectDatasetUpdates() {
		return projectDatasetUpdates;
	}

	public void setProjectDatasetUpdates(int projectDatasetUpdates) {
		this.projectDatasetUpdates = projectDatasetUpdates;
	}

	public int getProjectWebsiteViews() {
		return projectWebsiteViews;
	}

	public void setProjectWebsiteViews(int projectWebsiteViews) {
		this.projectWebsiteViews = projectWebsiteViews;
	}

	public int getProjectParticipantInteractions() {
		return projectParticipantInteractions;
	}

	public void setProjectParticipantInteractions(int projectParticipantInteractions) {
		this.projectParticipantInteractions = projectParticipantInteractions;
	}

	public int getProjectScriptInvocations() {
		return projectScriptInvocations;
	}

	public void setProjectScriptInvocations(int projectScriptInvocations) {
		this.projectScriptInvocations = projectScriptInvocations;
	}

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}
}

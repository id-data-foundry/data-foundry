package models;

import java.util.Date;

import io.ebean.Finder;
import io.ebean.Model;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;

@Entity
public class Collaboration extends Model {
	@Id
	Long id;
	@ManyToOne
	Person collaborator;
	@ManyToOne
	Project project;

	CollaborationStatus status;

	Date created = new Date();

	public static final Finder<Long, Collaboration> find = new Finder<Long, Collaboration>(Collaboration.class);

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Collaboration(Person c, Project p) {
		this.collaborator = c;
		this.project = p;
		this.status = CollaborationStatus.INITIATED;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Person getCollaborator() {
		return collaborator;
	}

	public void setCollaborator(Person collaborator) {
		this.collaborator = collaborator;
	}

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public CollaborationStatus getStatus() {
		return status;
	}

	public void setStatus(CollaborationStatus status) {
		this.status = status;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

}

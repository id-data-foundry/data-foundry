package models;

import java.util.Date;

import io.ebean.Finder;
import io.ebean.Model;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;

@Entity
public class Reference extends Model {

	@Id
	Long id;
	@ManyToOne
	Project owningProject;
	@ManyToOne
	Project referencedProject;

	String referenceInformation;

	Date created = new Date();

	public static final Finder<Long, Reference> find = new Finder<Long, Reference>(Reference.class);

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Reference(Project owner, Project reference) {
		this.owningProject = owner;
		this.referencedProject = reference;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Project getOwner() {
		return owningProject;
	}

	public void setOwner(Project project) {
		this.owningProject = project;
	}

	public Project getReference() {
		return referencedProject;
	}

	public void setReference(Project reference) {
		this.referencedProject = reference;
	}

	public String getReferenceInformation() {
		return referenceInformation;
	}

	public void setReferenceInformation(String referenceInformation) {
		this.referenceInformation = referenceInformation;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

}

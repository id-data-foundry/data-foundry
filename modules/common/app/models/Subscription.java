package models;

import java.util.Date;

import io.ebean.Finder;
import io.ebean.Model;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;

@Entity
public class Subscription extends Model {
	@Id
	Long id;

	@ManyToOne
	Person subscriber;
	@ManyToOne
	Project project;

	Date created = new Date();

	public static final Finder<Long, Subscription> find = new Finder<Long, Subscription>(Subscription.class);

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Subscription(Person s, Project p) {
		this.setSubscriber(s);
		this.project = p;
	}

	public boolean isSubscribed(Person s, Project p) {
		if (this.getSubscriber().getEmail().equals(s.getEmail()) && this.project.getId().equals(p.getId())) {
			return true;
		} else {
			return false;
		}
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public Person getSubscriber() {
		return subscriber;
	}

	public void setSubscriber(Person subscriber) {
		this.subscriber = subscriber;
	}
}

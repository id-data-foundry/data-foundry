package models.sr;

import io.ebean.Model;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MappedSuperclass;
import models.Project;
import utils.DataUtils;

@MappedSuperclass
abstract public class DataResource extends Model {

	@Id
	private Long id;
	private String refId;
	private String publicParameter1;
	private String publicParameter2;
	private String publicParameter3;

	@ManyToOne
	private Project project;

	/**
	 * check not null AND not empty
	 * 
	 * @param text
	 * @return
	 */
	protected boolean nnne(String text) {
		return text != null && text.trim().length() > 0;
	}

	/**
	 * null safe string
	 */
	protected final String nss(String s) {
		return s != null ? s : "";
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public String getPublicParameter3() {
		return publicParameter3;
	}

	public void setPublicParameter3(String publicParameter3) {
		this.publicParameter3 = publicParameter3;
	}

	public String getPublicParameter2() {
		return publicParameter2;
	}

	public void setPublicParameter2(String publicParameter2) {
		this.publicParameter2 = publicParameter2;
	}

	public String getPublicParameter1() {
		return publicParameter1;
	}

	public void setPublicParameter1(String publicParameter1) {
		this.publicParameter1 = publicParameter1;
	}

	public String getRefId() {
		return refId;
	}

	public void setRefId(String refId) {
		this.refId = refId;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**
	 * reflection-based stringifier to JSON
	 * 
	 * @return
	 */
	public String toJson() {
		return DataUtils.toJson(this).toPrettyString();
	}
}

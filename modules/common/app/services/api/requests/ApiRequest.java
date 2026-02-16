package services.api.requests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import play.libs.Json;

abstract public class ApiRequest<T> {

	private String type;
	private String username;
	private long projectId;
	private ObjectNode params;
	private boolean isValid = true;
	private boolean isStarted = false;

	public ApiRequest(String type, String username, long projectId, String params) {
		this.setType(type);
		this.setUsername(username);
		this.setProjectId(projectId);
		try {
			JsonNode jsonNode = Json.parse(params);
			if (!jsonNode.isObject()) {
				this.isValid = false;
				this.setParams(Json.newObject());
			} else {
				this.setParams((ObjectNode) jsonNode);
			}
		} catch (Exception e) {
			this.isValid = false;
			this.setParams(Json.newObject());
		}
	}

	public ApiRequest(String type, String username, long projectId, ObjectNode params) {
		this.setType(type);
		this.setUsername(username);
		this.setProjectId(projectId);
		this.setParams(params);
	}

	/**
	 * check whether the API request is valid and should be processed
	 * 
	 * @return
	 */
	public boolean isValid() {
		return isValid;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public long getProjectId() {
		return projectId;
	}

	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	public ObjectNode getParams() {
		return params;
	}

	public void setParams(ObjectNode params) {
		this.params = params;
	}

	public boolean isStarted() {
		return isStarted;
	}

	public void setStarted(boolean isStarted) {
		this.isStarted = isStarted;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public abstract boolean isCompleted();

	public abstract T getResult();

	protected abstract void cancel();
}

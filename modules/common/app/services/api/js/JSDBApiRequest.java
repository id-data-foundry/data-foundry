package services.api.js;

import java.util.Optional;

import datasets.DatasetConnector;
import models.Dataset;
import models.Project;
import services.api.requests.ApiRequest;

/**
 * JS DB Api request: to isolate the database access from JSActor execution
 *
 */
abstract public class JSDBApiRequest extends ApiRequest<String> {

	private Optional<String> result = null;

	public JSDBApiRequest(long projectId) {
		super("", "", projectId, "");
	}

	abstract public String run(Project project, DatasetConnector datasetConnector);

	protected Optional<Dataset> getProjectDataset(Project project, String datasetId) {
		return project.getDatasets().stream()
		        .filter(ds -> ds.getId().toString().equals(datasetId) || ds.getRefId().equals(datasetId)).findAny();
	}

	/**
	 * request results, returns error message in case of timeout
	 * 
	 * @return
	 */
	@Override
	public String getResult() {
		return isCompleted() && result.isPresent() ? result.get() : "[]";
	}

	@Override
	public boolean isCompleted() {
		return result != null;
	}

	@Override
	protected void cancel() {
		setResult(Optional.empty());
	}

	public void setResult(Optional<String> result) {
		this.result = result;
	}
}
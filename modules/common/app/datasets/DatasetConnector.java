package datasets;

import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.typesafe.config.Config;

import models.Dataset;
import models.DatasetType;
import models.LabNotesEntry;
import models.LabNotesEntry.LabNotesEntryType;
import models.Project;
import models.ds.AnnotationDS;
import models.ds.CompleteDS;
import models.ds.DiaryDS;
import models.ds.EntityDS;
import models.ds.ExpSamplingDS;
import models.ds.FitbitDS;
import models.ds.FormDS;
import models.ds.GoogleFitDS;
import models.ds.LinkedDS;
import models.ds.MediaDS;
import models.ds.MovementDS;
import models.ds.SurveyDS;
import models.ds.TimeseriesDS;
import models.sr.Cluster;
import play.libs.Json;
import services.outlets.OOCSIStreamOutService;
import utils.conf.ConfigurationUtils;

@Singleton
public class DatasetConnector {

	private final Config config;
	private final OOCSIStreamOutService oocsiService;

	@Inject
	public DatasetConnector(Config config, OOCSIStreamOutService oocsiService) {
		this.config = config;
		this.oocsiService = oocsiService;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Dataset create(String name, DatasetType type, Project project, String description, String targetObj,
			String isOpenParticipation) {
		return create(name, type, project, description, targetObj, isOpenParticipation, null);
	}

	public Dataset create(String name, DatasetType type, Project project, String description, String targetObj,
			String isOpenParticipation, String license) {
		final Dataset ds = new Dataset();
		ds.setName(name);
		ds.setDsType(type);
		ds.setRefId("ds" + UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		ds.setApiToken(UUID.randomUUID().toString());
		ds.setProject(project);
		ds.setDescription(description);
		ds.setTargetObject(targetObj != null ? targetObj : "");
		ds.setOpenParticipation(isOpenParticipation != null ? true : false);
		ds.setLicense(license);
		ds.start();
		ds.end();

		// create an instance of the linked data set
		createInstance(ds);

		LabNotesEntry.log(Dataset.class, LabNotesEntryType.CREATE, "Dataset created: " + ds.getName(), ds.getProject());

		return ds;
	}

	//////////////////////////////////////////

	public LinkedDS getDatasetDS(Long dsId) {
		Dataset ds = Dataset.find.byId(dsId);
		return ds != null ? getDatasetDS(ds) : null;
	}

	public LinkedDS getDatasetDS(String dsrefId) {
		Dataset ds = Dataset.find.query().where().eq("refId", dsrefId).findOne();
		return ds != null ? getDatasetDS(ds) : null;
	}

	public LinkedDS getDatasetDS(Dataset ds) {
		LinkedDS datasetDS = getDatasetDS(ds, config);
		datasetDS.setStreamoutService(oocsiService);
		return datasetDS;
	}

	@SuppressWarnings("unchecked")
	public <T extends LinkedDS> T getTypedDatasetDS(Dataset ds) {
		LinkedDS datasetDS = getDatasetDS(ds, config);
		datasetDS.setStreamoutService(oocsiService);
		return (T) datasetDS;
	}

	public static LinkedDS getDatasetDSUnmanaged(Dataset ds) {
		return getDatasetDS(ds, null);
	}

	public static LinkedDS getDatasetDS(Dataset ds, Config config) {
		LinkedDS datasetConnector;
		// prevent NPE on empty dsType
		final DatasetType dsType = ds.getDsType() != null ? ds.getDsType() : DatasetType.LINKED;
		switch (dsType) {
		case IOT:
		case TIMESERIES:
			datasetConnector = new TimeseriesDS(ds);
			break;
		case ENTITY:
			datasetConnector = new EntityDS(ds);
			break;
		case ANNOTATION:
			datasetConnector = new AnnotationDS(ds);
			break;
		case DIARY:
			datasetConnector = new DiaryDS(ds);
			break;
		case FORM:
			datasetConnector = new FormDS(ds);
			break;
		case COMPLETE:
			datasetConnector = new CompleteDS(ds, config);
			break;
		case MOVEMENT:
			datasetConnector = new MovementDS(ds, config);
			break;
		case MEDIA:
			datasetConnector = new MediaDS(ds, config);
			break;
		case ES:
			datasetConnector = new ExpSamplingDS(ds, config);
			break;
		case FITBIT:
			datasetConnector = new FitbitDS(ds);
			break;
		case GOOGLEFIT:
			datasetConnector = new GoogleFitDS(ds);
			break;
		case SURVEY:
			datasetConnector = new SurveyDS(ds);
			break;
		case LINKED:
		default:
			datasetConnector = new LinkedDS(ds) {

				@Override
				public void createInstance() {
					// do nothing
				}

				@Override
				public String[] getSchema() {
					return new String[] {};
				}

				@Override
				public ArrayNode retrieveProjected(Cluster cluster, long limit, long start, long end) {
					return Json.newArray();
				}

				@Override
				public void lastUpdatedSource(Map<Long, Long> sourceUpdates) {
					// do nothing
				}

				@Override
				public void export(SourceQueueWithComplete<ByteString> queue, Cluster cluster, long limit, long start,
						long end) {
					// do nothing
				}
			};
		}

		return datasetConnector;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check whether the configuration mentions the given username as librarians
	 * 
	 * @param username
	 * @return
	 */
	public boolean checkLibrarianAccess(String username) {
		return config.getStringList(ConfigurationUtils.DF_USERS_LIBRARIANS).contains(username);
	}

	/**
	 * check whether the configuration mentions the given username as moderator
	 * 
	 * @param username
	 * @return
	 */
	public boolean checkModerationAccess(String username) {
		return config.getStringList(ConfigurationUtils.DF_USERS_MODERATORS).contains(username);
	}

	/**
	 * get the schema of the dataset (type-specific)
	 * 
	 * @param ds
	 * @return
	 */
	public static String[] getDatasetSchema(Dataset ds) {
		return getDatasetDSUnmanaged(ds).getSchema();
	}

	/**
	 * create instance of dataset DS for the given dataset
	 * 
	 * @param ds
	 */
	public void createInstance(Dataset ds) {
		LinkedDS lds = getDatasetDS(ds);
		lds.createInstance();
	}
}

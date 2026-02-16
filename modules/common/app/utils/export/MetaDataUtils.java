package utils.export;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.databind.node.ObjectNode;

import models.Dataset;
import models.LabNotesEntry;
import models.Person;
import models.Project;
import play.libs.Json;

public class MetaDataUtils {

	private static final DateFormat tsDateFormatter = new SimpleDateFormat("yyyy-MM-dd");

	/**
	 * export project meta data to JSON
	 * 
	 * @param project
	 * @return
	 */
	public static ObjectNode toJson(Project project) {
		ObjectNode metadata = Json.newObject();

		metadata.put("id", project.getId());
		metadata.put("name", project.getName());
		metadata.put("intro", project.getIntro());
		metadata.put("description", project.getDescription());
		metadata.put("keywords", project.getKeywords());
		metadata.put("doi", project.getDoi());
		metadata.put("relation", project.getRelation());
		metadata.put("organization", project.getOrganization());
		metadata.put("license", project.getLicense());
		metadata.put("remarks", project.getRemarks());
		metadata.put("start-date", project.startDate());
		metadata.put("end-date", project.endDate());
		metadata.put("creation-date", project.creationDate());

		return metadata;
	}

	/**
	 * export dataset meta data to JSON
	 * 
	 * @param ds
	 * @return
	 */
	public static ObjectNode toJson(Dataset ds) {
		ObjectNode metadata = Json.newObject();

		metadata.put("id", ds.getId());
		// internal reference ID
		metadata.put("reference", ds.getRefId());
		metadata.put("name", ds.getName());
		metadata.put("dataset-type", ds.getDsType().name());
		if (!ds.getDescription().isEmpty()) {
			metadata.put("description", ds.getDescription());
		}
		if (!ds.getLicense().isEmpty()) {
			metadata.put("license", ds.getLicense());
		}
		if (!ds.getKeywords().isEmpty()) {
			metadata.put("keywords", ds.getKeywords());
		}
		if (!ds.getDoi().isEmpty()) {
			metadata.put("doi", ds.getDoi());
		}
		if (!ds.getRelation().isEmpty()) {
			metadata.put("relation", ds.getRelation());
		}
		if (!ds.getOrganization().isEmpty()) {
			metadata.put("organization", ds.getOrganization());
		}
		if (!ds.getRemarks().isEmpty()) {
			metadata.put("remarks", ds.getRemarks());
		}
		metadata.put("start-date", ds.startDate());
		metadata.put("end-date", ds.endDate());
		metadata.put("creation-date", ds.creationDate());

		return metadata;
	}

	/**
	 * export person (meta) data to JSON
	 * 
	 * @param person
	 * @return
	 */
	public static ObjectNode toJson(Person person) {
		ObjectNode metadata = Json.newObject();

		metadata.put("id", person.getId());
		metadata.put("email", person.getEmail());
		metadata.put("first-name", person.getFirstname());
		metadata.put("last-name", person.getLastname());

		return metadata;
	}

	public static ObjectNode toJson(LabNotesEntry lne) {
		ObjectNode metadata = Json.newObject();

		metadata.put("type", lne.getLogType().name());
		metadata.put("message", lne.getMessage());
		metadata.put("source", lne.getSource());
		metadata.put("timestamp", tsDateFormatter.format(lne.getTimestamp().getTime()));

		return metadata;
	}

}

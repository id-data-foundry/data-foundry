package utils.export;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.inject.Inject;

import org.apache.pekko.util.ByteString;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import models.Collaboration;
import models.CollaborationStatus;
import models.Person;
import models.Project;
import play.Logger;
import play.cache.SyncCacheApi;
import play.libs.Files.TemporaryFile;
import play.libs.ws.InMemoryBodyWritable;
import play.libs.ws.WSClient;
import play.libs.ws.WSRequest;
import play.libs.ws.WSResponse;
import utils.DataUtils;
import utils.conf.ConfigurationUtils;
import utils.conf.Configurator;

public class ZenodoPublishingUtil {

	private static final Logger.ALogger logger = Logger.of(ZenodoPublishingUtil.class);
	private final WSClient ws;
	private final ObjectMapper mapper;
	private final SyncCacheApi cache;
//	private final String accessToken;

	private final String dfBaseUrl;
	private final String dfCommunitySlug = "data-foundry";
	private final String zenodoBaseURL = "https://zenodo.org/api/";

	Map<String, String> licenseMap = Map.of("MIT", "mit", "CC BY", "cc-by-4.0", "CC BY-SA", "cc-by-sa-4.0", "CC BY-ND",
			"cc-by-nd-4.0", "CC BY-NC", "cc-by-nc-4.0", "CC BY-NC-SA", "cc-by-nc-sa-4.0", "CC BY-NC-ND",
			"cc-by-nc-nd-4.0");

	@Inject
	public ZenodoPublishingUtil(Configurator configurator, WSClient ws, SyncCacheApi cache) {
		// retrieve the base url for this instance
		this.dfBaseUrl = configurator.getString(ConfigurationUtils.DF_BASEURL);

		this.ws = ws;
		this.cache = cache;
		this.mapper = new ObjectMapper();
	}

	private WSRequest getRequest(String endpoint, String accessToken) {
		WSRequest request = ws.url(zenodoBaseURL + endpoint);

		request.addHeader("Accept", "application/json");
		if (accessToken != null && !accessToken.isEmpty()) {
			request.addHeader("Authorization", "Bearer " + accessToken);
		}

		return request;
	}

	private JsonNode handleResponse(WSResponse response) {
		int status = response.getStatus();
		if (status >= 200 && status < 300) {
			try {
				String body = response.getBody();
				if (body == null || body.isEmpty()) {
					return mapper.createObjectNode();
				}
				return mapper.readTree(body);
			} catch (Exception e) {
				throw new RuntimeException("Failed to parse JSON", e);
			}
		} else {
			throw new RuntimeException(
					"HTTP error: " + response.getUri() + " --> " + status + " - " + response.getBody());
		}
	}

	private JsonNode get(String endpoint, String accessToken) {
		WSRequest request = getRequest(endpoint, accessToken);
		return request.get().thenApply(this::handleResponse).toCompletableFuture().join();
	}

	private JsonNode post(String endpoint, String accessToken, JsonNode data) {
		JsonNode body = data != null ? data : mapper.createObjectNode();
		WSRequest request = getRequest(endpoint, accessToken);
		// WSClient automatically sets Content-Type: application/json when posting JsonNode
		return request.post(body).thenApply(this::handleResponse).toCompletableFuture().join();
	}

	private JsonNode put(String endpoint, String accessToken, JsonNode data) {
		JsonNode body = data != null ? data : mapper.createObjectNode();
		WSRequest request = getRequest(endpoint, accessToken);
		return request.put(body).thenApply(this::handleResponse).toCompletableFuture().join();
	}

	public ObjectNode serializePerson(Person person, String role, String affiliation) {
		ObjectNode node = mapper.createObjectNode();
		ObjectNode personOrOrg = node.putObject("person_or_org");
		personOrOrg.put("type", "personal");
		personOrOrg.put("given_name", person.getFirstname());
		personOrOrg.put("family_name", person.getLastname());
		String orcid = person.getIdentityProperty("orcid");
		if (orcid != "") {
			ObjectNode identifier = personOrOrg.putArray("identifiers").addObject();
			identifier.put("scheme", "orcid");
			identifier.put("identifier", orcid);
		}
		if (role != "") {
			node.putObject("role").put("id", role);
		}
		if (affiliation != null && affiliation != "") {
			// TODO: Use "id" instead of "name", if a known affiliation.
			node.putArray("affiliations").addObject().put("name", affiliation);
		}
		return node;
	}

	public Optional<ZenodoRecord> createRecord(Project project, String accessToken, String cacheToken,
			TemporaryFile exportFile) {
		try {
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

			ObjectNode record = mapper.createObjectNode();

			// ACCESS
			// https://inveniordm.docs.cern.ch/reference/metadata/#access

			// REMARK: "files" can be set as "restricted", if necessary.
			// REMARK: Embargo options can be set with "embargo".
			ObjectNode access = record.putObject("access");
			access.put("record", "public");
			access.put("files", "public");

			// METADATA
			// https://inveniordm.docs.cern.ch/reference/metadata/#metadata

			// REMARK: Metadata is updated later to enable setting default preview.
			ObjectNode metadata = record.putObject("metadata");

			// FILES

			ObjectNode files = record.putObject("files");
			files.put("enabled", true);

			// Create record
			if (cacheToken != null) {
				cache.set(cacheToken, "🚀 Creating Zenodo record...");
			}
			JsonNode result = post("records", accessToken, record);

			// Get record id
			int id = result.get("id").asInt();

			// DATASET FILES

			try (ZipFile zipFile = new ZipFile(exportFile.path().toFile())) {
				Enumeration<? extends ZipEntry> entries = zipFile.entries();

				while (entries.hasMoreElements()) {
					ZipEntry entry = entries.nextElement();
					if (entry.isDirectory()) {
						continue;
					}

					String name = entry.getName();
					int idx = name.indexOf("/");
					if (idx >= 0) {
						name = name.substring(idx + 1);
					}

					if (cacheToken != null) {
						cache.set(cacheToken, "🚀 Uploading file " + name + "...");
					}

					InputStream is = zipFile.getInputStream(entry);

					ArrayNode keys = mapper.createArrayNode();
					keys.addObject().put("key", name);

					post("records/" + id + "/draft/files", accessToken, keys);

					WSRequest request = getRequest("records/" + id + "/draft/files/" + name + "/content", accessToken);
					request.addHeader("Content-Type", "application/octet-stream");

					request.put(new InMemoryBodyWritable(ByteString.fromArray(is.readAllBytes()),
							"application/octet-stream")).thenApply(this::handleResponse).toCompletableFuture().join();
					post("records/" + id + "/draft/files/" + name + "/commit", accessToken, null);
				}
			}

			// Set file to be previewed by default
			files.put("default_preview", "README.md");

			// Set resource type as dataset
			metadata.set("resource_type", mapper.createObjectNode().put("id", "dataset"));

			// Set title
			metadata.put("title", project.getName());

			// Set publication date
			// REMARK: Publication date is set as today
			metadata.put("publication_date", dateFormatter.format(new Date()));

			// Add owner and collaborators as creators
			// REMARK: List of creator roles is available at /api/vocabularies/creatorsroles
			ArrayNode creators = metadata.putArray("creators");
			creators.add(serializePerson(project.getOwner(), "producer", project.getOrganization()));

			for (Collaboration collaboration : project.getCollaborators()) {
				if (collaboration.getStatus() != CollaborationStatus.ACCEPTED) {
					continue;
				}
				creators.add(
						serializePerson(collaboration.getCollaborator(), "projectmember", project.getOrganization()));
			}

			// Set description
			metadata.put("description", project.getDescription());

			// Add intro and remarks as an additional descriptions, if exists
			// REMARK: English is assumed to be the default language.
			// REMARK: Language code is ISO-639-3.
			ArrayNode descriptions = mapper.createArrayNode();

			if (project.getRemarks() != "") {
				ObjectNode node = descriptions.addObject();
				node.put("description", project.getRemarks());
				ObjectNode type = node.putObject("type");
				type.put("id", "notes");
				type.putObject("title").put("en", "Remarks");
				node.putObject("lang").put("id", "eng");
			}

			// REMARK: getIntro() does not use nnne.
			String intro = project.getIntro();
			if (intro != null && intro != "") {
				ObjectNode node = descriptions.addObject();
				node.put("description", intro);
				ObjectNode type = node.putObject("type");
				type.put("id", "abstract");
				type.putObject("title").put("en", "Introduction");
				node.putObject("lang").put("id", "eng");
			}

			if (descriptions.size() > 0) {
				metadata.set("additional_descriptions", descriptions);
			}

			// Set license
			// REMARK: List of licenses is available at /api/vocabularies/licenses
			// REMARK: Alternatively, different dataset licences can be specified.
			String license = licenseMap.get(project.getLicense());
			if (license != null) {
				metadata.putArray("rights").addObject().put("id", license);
			}

			// Set keywords as subjects, if exist
			ArrayNode subjects = mapper.createArrayNode();

			for (String keyword : project.getKeywordList()) {
				subjects.addObject().put("subject", keyword);
			}

			if (subjects.size() > 0) {
				metadata.set("subjects", subjects);
			}

			// Set language
			// REMARK: English is assumed to be the default language.
			// REMARK: Language code is ISO-639-3.
			metadata.putArray("languages").addObject().put("id", "eng");

			// Set collection period as dates
			// REMARK: List of date types is available at /api/vocabularies/datetypes
			// REMARK: "description" can be used for free text information.
			ObjectNode date = metadata.putArray("dates").addObject();
			date.put("date", dateFormatter.format(project.start()) + "/" + dateFormatter.format(project.end()));
			date.putObject("type").put("id", "collected");

			// Set publisher
			// REMARK: Can be set as "Data Foundry".
			metadata.put("publisher", "Zenodo");

			// Set alternate identifiers
			// TODO: URL address should be replaced with the URL address of the project.
			ObjectNode identifier = metadata.putArray("identifiers").addObject();
			identifier.put("identifier", this.dfBaseUrl + "/project/" + project.getId());
			identifier.put("scheme", "url");

			// Set related identifiers
			// REMARK: List of relation types is available at
			// /api/vocabularies/relationtypes
			// REMARK: List of resource types is available at
			// /api/vocabularies/resourcetypes
			// TODO: Add Data Foundry reference DOI.
			// TODO: "identifier" should be set as the base URL of the platform.
			ArrayNode relatedIdentifiers = metadata.putArray("related_identifiers");
			ObjectNode relatedIdentifier = relatedIdentifiers.addObject();
			relatedIdentifier.put("identifier", "https://data-foundry.net");
			relatedIdentifier.put("scheme", "url");
			relatedIdentifier.putObject("relation_type").put("id", "iscompiledby");
			relatedIdentifier.putObject("resource_type").put("id", "software");

			// Set relation as reference, if exists
			if (project.getRelation() != "") {
				metadata.putArray("references").addObject().put("reference", project.getRelation());
			}

			// Update metadata
			if (cacheToken != null) {
				cache.set(cacheToken, "🚀 Updating Zenodo metadata...");
			}
			put("records/" + id + "/draft", accessToken, record);

			// DOI
			// https://inveniordm.docs.cern.ch/reference/rest_api_drafts_records/#reserve-a-doi-for-a-draft-record

			if (cacheToken != null) {
				cache.set(cacheToken, "🚀 Reserving DOI...");
			}
			JsonNode doiResult = post("records/" + id + "/draft/pids/doi", accessToken, null);

			// REMARK: According to the API documentation, DOI should be under "pids".
			// However, current Zenodo deployment does not return "pids". It uses "doi"
			// instead.
			String doi = doiResult.get("doi").asText();
			if (doi == null) {
				doi = doiResult.get("pids").get("doi").get("identifier").asText();
			}

			// COMMUNITY
			// REMARK: Setting a community changes "Publish" to "Submit for Review"

			// Get Data Foundry community metadata
			// TODO: Exception handling for invalid slug.
			JsonNode community = get("communities/" + dfCommunitySlug, accessToken);

			// Create review request
			ObjectNode review = mapper.createObjectNode();
			review.put("type", "community-submission");
			review.putObject("receiver").put("community", community.get("id").asText());

			// Send review request
			if (cacheToken != null) {
				cache.set(cacheToken, "🚀 Submitting to community review...");
			}
			put("records/" + id + "/draft/review", accessToken, review);

			// Return record information
			ZenodoRecord zenodoRecord = new ZenodoRecord(id, doi, result.get("links").get("self_html").asText(),
					result.get("links").get("preview_html").asText());
			return Optional.of(zenodoRecord);

		} catch (Exception e) {
			logger.error("Error publishing to Zenodo", e);
			if (cacheToken != null) {
				cache.set(cacheToken, "❌ Error during Zenodo publication: " + getUserFriendlyErrorMessage(e));
			}
			return Optional.empty();
		}
	}

	private String getUserFriendlyErrorMessage(Exception e) {
		String message = e.getMessage();
		if (message != null && message.startsWith("HTTP error: ")) {
			try {
				// Parse status code
				String[] parts = message.split(" - ");
				int status = DataUtils.parseInt(parts[0].replace("HTTP error: ", ""), 500);

				if (status == 401 || status == 403) {
					return "Authentication failed. Please check your Zenodo access token.";
				} else if (status >= 500) {
					return "Zenodo server error. Please try again later.";
				} else {
					return "Zenodo rejected the request (Status: " + status + "). Please check your project metadata.";
				}
			} catch (Exception parseException) {
				// Fallback if parsing fails
				return "Communication with Zenodo failed.";
			}
		} else if (e instanceof java.io.IOException) {
			return "Failed to process project files for export.";
		}

		return "An unexpected error occurred.";
	}

}

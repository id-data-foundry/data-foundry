package services.jsexecutor;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.graalvm.polyglot.HostAccess;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.gson.JsonObject;

import play.libs.Json;
import services.api.ApiServiceConstants;

public class JSActorProxy {

	private final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
	private StringBuffer sb = new StringBuffer();
	private JSActor actor;
	private final long ttl;

	@HostAccess.Export
	public JsonObject data;
	@HostAccess.Export
	public ArrayNode participants;
	@HostAccess.Export
	public ArrayNode devices;
	@HostAccess.Export
	public ArrayNode wearables;
	@HostAccess.Export
	public ArrayNode clusters;
	@HostAccess.Export
	public ArrayNode datasets;

	public JSActorProxy(JSActor actor) {
		this.actor = actor;
		this.ttl = System.currentTimeMillis() + ApiServiceConstants.API_REQUEST_JS_TIMEOUT_MS;
	}

	// event data ----------------------------------------------------------------

	@HostAccess.Export
	public JSDatasetProxy getDatasetProxy(Object id) {
		return actor.getDatasetProxy(id.toString(), ttl);
	}

	// entity dataset information

	@HostAccess.Export
	public String getEntityResources() {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return "[]";
		}

		try {
			return actor.getEntityResources();
		} catch (Exception e) {
			error("No entity dataset available.");
			return "[]";
		}
	}

	@HostAccess.Export
	public String getEntityResourcesMatching(String resourceId) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return "[]";
		}

		try {
			return actor.getEntityResourcesMatching(resourceId);
		} catch (Exception e) {
			error("No entity dataset available.");
			return "[]";
		}
	}

	@HostAccess.Export
	public String getEntityResource(String resourceId) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return "[]";
		}

		try {
			return actor.getEntityResource(resourceId);
		} catch (Exception e) {
			error("No entity dataset available.");
			return "[]";
		}
	}

	@HostAccess.Export
	public void updateEntityResource(String resourceId, String data) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return;
		}

		try {
			actor.updateEntityResource(resourceId, data);
		} catch (Exception e) {
			error("No entity dataset available.");
		}
	}

	@HostAccess.Export
	public void addEntityResource(String resourceId, String data) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return;
		}

		try {
			actor.addEntityResource(resourceId, data);
		} catch (Exception e) {
			error("No entity dataset available.");
		}
	}

	@HostAccess.Export
	public void deleteEntityResource(String resourceId) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return;
		}

		try {
			actor.deleteEntityResource(resourceId);
		} catch (Exception e) {
			error("No entity dataset available.");
		}
	}

	// OOCSI message

	@HostAccess.Export
	public void sendOOCSIMessage(String channel, String data) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return;
		}

		actor.sendOOCSIMessage(channel, data);
	}

	// Telegram messages

	@HostAccess.Export
	public void sendTelegramMessageToResearchers(String message) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return;
		}

		actor.sendTelegramMessageToResearchers(message);
	}

	@HostAccess.Export
	public void replyTelegramMessage(String chatId, String message) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return;
		}

		actor.replyTelegramMessage(chatId, message);
	}

	@HostAccess.Export
	public void sendTelegramMessageToParticipant(String participantId, String message) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return;
		}

		actor.sendTelegramMessageToParticipant(participantId, message);
	}

	// API dispatch

	@HostAccess.Export
	public String apiDispatch(String api, String data) {
		// check request validity
		if (ttl < System.currentTimeMillis()) {
			return Json.newObject().toString();
		}

		return actor.apiDispatch(api, data);
	}

	// optional: diary dataset update function ?

	// logging -------------------------------------------------------------------

	@HostAccess.Export
	public void print(Object message) {
		print(message.toString());
	}

	@HostAccess.Export
	public void print(String message) {
		if (message != null && !message.isEmpty()) {
			String timestamp = "<span class='timestamp'>" + sdf.format(new Date()) + "</span>";
			sb.append("<p class='info-msg'>" + timestamp + message.replace("\n", "<br>") + "</p>");
		}
	}

	@HostAccess.Export
	public void error(String message) {
		if (message != null && !message.isEmpty()) {
			String timestamp = "<span class='timestamp'>" + sdf.format(new Date()) + "</span>";
			sb.append("<p class='error-msg'>" + timestamp + message.replace("\n", "<br>") + "</p>");
		}
	}

	public String getMessages() {
		return sb.toString();
	}
}

package controllers.api2;

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Inject;

import controllers.AbstractAsyncController;
import models.Dataset;
import play.libs.Json;
import play.mvc.Http.MimeTypes;
import play.mvc.Http.Request;
import play.mvc.Result;
import services.api.ThrottlingService;
import services.jsexecutor.JSActor;
import services.jsexecutor.JSExecutorService;

public class ActorWebhookController extends AbstractAsyncController {

	private final JSExecutorService jsExecService;
	private final ThrottlingService throttlingService;

	@Inject
	public ActorWebhookController(JSExecutorService jsExecService, ThrottlingService throttlingService) {
		this.jsExecService = jsExecService;
		this.throttlingService = throttlingService;
	}

	public Result webhook(Request request, long id, String token) {
		Dataset ds = Dataset.find.byId(id);
		if (ds == null || !ds.isScript()) {
			return notFound("script not found");
		}

		if (token == null || !token.equals(ds.getApiToken())) {
			return forbidden("not authorized");
		}

		if (!ds.isActive()) {
			return badRequest("script is inactive");
		}

		// throttling
		if (!throttlingService.tryConsume(ds.getApiToken())) {
			return status(429, "too many requests, please slow down");
		}

		// parse body using Play JSON
		JsonNode json = request.body().asJson();
		if (json == null) {
			Map<String, String[]> formData = request.body().asFormUrlEncoded();
			if (formData != null) {
				ObjectNode node = Json.newObject();
				formData.forEach((k, v) -> {
					if (v != null && v.length > 0) {
						node.put(k, v[0]);
					}
				});
				json = node;
			} else if (request.queryString() != null && !request.queryString().isEmpty()) {
				ObjectNode node = Json.newObject();
				request.queryString().forEach((k, v) -> {
					if (v != null && v.length > 0) {
						node.put(k, v[0]);
					}
				});
				json = node;
			} else {
				json = Json.newObject();
			}
		}

		// check actor
		JSActor actor = jsExecService.getActor(ds.getId());
		if (actor == null) {
			// actor is not available, create new one
			actor = jsExecService.addActor(ds);
		}

		// convert to gson object for JSActor
		Optional<String> responseOpt = Optional.empty();
		try {
			JsonElement element = new JsonParser().parse(Json.stringify(json));
			JsonObject data;
			if (element.isJsonObject()) {
				data = element.getAsJsonObject();
			} else {
				data = new JsonObject();
				data.add("payload", element);
			}
			responseOpt = actor.updateAndGetResponse(data);
		} catch (Exception e) {
			return badRequest("invalid payload structure");
		}

		if (responseOpt.isPresent()) {
			try {
				return ok(Json.parse(responseOpt.get())).as(MimeTypes.JSON);
			} catch (Exception e) {
				return ok(responseOpt.get()).as(MimeTypes.JSON);
			}
		} else {
			return ok(Json.newObject().put("status", "triggered"));
		}
	}
}

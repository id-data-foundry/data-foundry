package utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.inject.Singleton;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.typesafe.config.Config;

import controllers.AbstractAsyncController.ResponseException;
import play.Environment;
import play.Logger;
import play.api.OptionalSourceMapper;
import play.api.UsefulException;
import play.api.routing.Router;
import play.http.DefaultHttpErrorHandler;
import play.mvc.Http.RequestHeader;
import play.mvc.Result;
import play.mvc.Results;

@Singleton
public class ErrorHandler extends DefaultHttpErrorHandler {

	private static final Logger.ALogger logger = Logger.of(ErrorHandler.class);

	private Environment environment;

	@Inject
	public ErrorHandler(Config config, Environment environment, OptionalSourceMapper sourceMapper,
	        Provider<Router> routes) {
		super(config, environment, sourceMapper, routes);

		this.environment = environment;
	}

	@Override
	protected CompletionStage<Result> onDevServerError(RequestHeader request, UsefulException exception) {
		logger.error("DevServerError", exception);
		return super.onDevServerError(request, exception);
	}

	@Override
	protected CompletionStage<Result> onProdServerError(RequestHeader request, UsefulException exception) {
		logger.error("ProdServerError", exception);
		if (environment.isDev()) {
			return super.onProdServerError(request, exception);
		} else {
			return CompletableFuture.completedFuture(Results.redirect(controllers.routes.HomeController.index()));
		}
	}

	@Override
	public CompletionStage<Result> onClientError(RequestHeader request, int statusCode, String message) {
		return super.onClientError(request, statusCode, message);
	}

	@Override
	protected CompletionStage<Result> onOtherClientError(RequestHeader request, int statusCode, String message) {
		logger.warn("OtherClientError: " + message + " --> " + request.toString());
		return super.onOtherClientError(request, statusCode, message);
	}

	@Override
	protected CompletionStage<Result> onForbidden(RequestHeader request, String message) {
		logger.warn("Forbidden: " + message + " --> " + request.toString());
		if (environment.isDev()) {
			return super.onForbidden(request, message);
		} else {
			return CompletableFuture.completedFuture(Results.redirect(controllers.routes.HomeController.index()));
		}
	}

	@Override
	protected CompletionStage<Result> onNotFound(RequestHeader request, String message) {
		logger.warn("Not found: " + message + " --> " + request.toString());
		if (environment.isDev()) {
			return super.onNotFound(request, message);
		} else {
			// check if it's an asset, API, or static file request, then plain notFound
			if (request.path().startsWith("/assets/") || request.path().startsWith("/api/") || isStaticResource(request.path())) {
				return CompletableFuture.completedFuture(Results.notFound());
			}

			return CompletableFuture.completedFuture(Results.redirect(controllers.routes.HomeController.index()));
		}
	}

	private boolean isStaticResource(String path) {
		int dotIndex = path.lastIndexOf('.');
		if (dotIndex != -1 && dotIndex < path.length() - 1) {
			String ext = path.substring(dotIndex + 1).toLowerCase();
			return ext.matches("ico|png|jpg|jpeg|gif|svg|webp|json|css|js|map|txt|xml|woff|woff2|ttf|eot");
		}
		return false;
	}

	@Override
	protected CompletionStage<Result> onBadRequest(RequestHeader request, String message) {
		logger.warn("Bad request: " + message + " --> " + request.toString());
		if (environment.isDev()) {
			return super.onBadRequest(request, message);
		} else {
			return CompletableFuture.completedFuture(Results.redirect(controllers.routes.HomeController.index()));
		}
	}

	@Override
	public CompletionStage<Result> onServerError(RequestHeader request, Throwable exception) {

		// handle redirect exceptions
		if (exception instanceof ResponseException) {
			return CompletableFuture.completedFuture(((ResponseException) exception).getResponse());
		}

		logger.error("Server error", exception);
		if (environment.isDev()) {
			return super.onServerError(request, exception);
		} else {
			return CompletableFuture.completedFuture(Results.redirect(controllers.routes.HomeController.index()));
		}
	}
}
package controllers.auth;

import java.util.Optional;

import javax.inject.Inject;

import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.play.PlayWebContext;

import controllers.AbstractAsyncController;
import models.Person;
import play.mvc.Controller;
import play.mvc.Http.Request;
import play.mvc.Http.RequestHeader;
import play.mvc.Result;

/**
 * Main user authentication for any researcher using the DF
 * 
 * @author mathias
 *
 */
public class UserAuth extends play.mvc.Security.Authenticator {

	@Inject
	private SessionStore sessionStore;

	private static String normalizeHost(String host) {
		if (host == null) {
			return null;
		}
		String h = host.trim().toLowerCase();
		if (h.endsWith(":80")) {
			return h.substring(0, h.length() - 3);
		} else if (h.endsWith(":443")) {
			return h.substring(0, h.length() - 4);
		}
		return h;
	}

	/**
	 * Validates that the request origin and fetch metadata are safe from cross-site
	 * subresource exploitation (e.g. from sandboxed or opaque origins).
	 *
	 * @param request
	 * @return true if the request context is valid, false if untrusted/cross-site subresource
	 */
	public static boolean isValidRequestContext(RequestHeader request) {
		if (request == null) {
			return false;
		}

		// 1. Reject requests originating from an opaque origin (e.g. sandboxed document) or mismatched Origin host
		Optional<String> origin = request.header("Origin");
		if (origin.isPresent()) {
			String originVal = origin.get().trim();
			if ("null".equalsIgnoreCase(originVal)) {
				return false;
			}
			String reqHost = request.host();
			if (reqHost != null && !reqHost.isEmpty()) {
				try {
					java.net.URI uri = java.net.URI.create(originVal);
					String originAuthority = uri.getRawAuthority();
					String normalizedOrigin = normalizeHost(originAuthority);
					String normalizedReqHost = normalizeHost(reqHost);
					if (normalizedOrigin == null || !normalizedOrigin.equalsIgnoreCase(normalizedReqHost)) {
						return false;
					}
				} catch (IllegalArgumentException e) {
					return false;
				}
			}
		}

		String path = request.path() != null ? request.path() : "";
		boolean isGetOrHead = "GET".equalsIgnoreCase(request.method()) || "HEAD".equalsIgnoreCase(request.method());
		boolean isRenewalRoute = isGetOrHead && path.startsWith("/datasets/web/") && !path.endsWith("/entrypoint");

		Optional<String> secFetchMode = request.header("Sec-Fetch-Mode");
		Optional<String> secFetchDest = request.header("Sec-Fetch-Dest");
		boolean isAllowedDest = secFetchDest.isEmpty() || "document".equalsIgnoreCase(secFetchDest.get())
				|| (isRenewalRoute && ("iframe".equalsIgnoreCase(secFetchDest.get())
						|| "frame".equalsIgnoreCase(secFetchDest.get())));

		// 2. Reject cross-site or same-site subresource fetches and state-changing GET navigations
		Optional<String> secFetchSite = request.header("Sec-Fetch-Site");
		if (secFetchSite.isPresent() && ("cross-site".equalsIgnoreCase(secFetchSite.get())
				|| "same-site".equalsIgnoreCase(secFetchSite.get()))) {
			if (!isGetOrHead || secFetchMode.isEmpty() || !"navigate".equalsIgnoreCase(secFetchMode.get())) {
				return false;
			}
			if (!isAllowedDest || isStateChangingGetPath(path)) {
				return false;
			}
		}

		// 3. check referrer for CSRF (defense-in-depth for legacy clients), note "referer" is correct!
		// Allow top-level GET navigations to non-mutating pages and token renewal gateway
		boolean isSafeTopLevelGetNavigation = isGetOrHead && secFetchMode.isPresent()
				&& "navigate".equalsIgnoreCase(secFetchMode.get()) && isAllowedDest && !isStateChangingGetPath(path);

		Optional<String> ref = request.header(Controller.REFERER);
		if (ref.isPresent() && !isSafeTopLevelGetNavigation && !isRenewalRoute) {
			if (ref.get().contains("/web/") || ref.get().contains("/datasets/web/")) {
				// do not authenticate if the referrer is a hosted web page
				// why? because this could be used for an attack via JavaScript
				return false;
			}
		}

		return true;
	}

	private static boolean isStateChangingGetPath(String path) {
		if (path == null || path.isEmpty()) {
			return false;
		}
		String lower = path.toLowerCase();
		return lower.contains("/delete") || lower.contains("/reset") || lower.contains("/configure")
				|| lower.contains("/add") || lower.contains("/remove") || lower.contains("/rename")
				|| lower.contains("/duplicate") || lower.contains("/generate") || lower.contains("/reveal")
				|| lower.contains("/entrypoint") || lower.contains("/archive") || lower.contains("/reopen")
				|| lower.contains("/publish") || lower.contains("/requestreview") || lower.contains("/opensignup")
				|| lower.contains("/leave") || lower.contains("/transfer");
	}

	@Override
	public Optional<String> getUsername(Request request) {
		if (!isValidRequestContext(request)) {
			return Optional.empty();
		}

		final PlayWebContext context = new PlayWebContext(request);
		final ProfileManager profileManager = new ProfileManager(context, sessionStore);
		Optional<UserProfile> proOpt = profileManager.getProfile();
		if (!proOpt.isPresent()) {
			return Optional.empty();
		}

		UserProfile userProfile = proOpt.get();

		// ensure that the username is present in profile
		// this is needed for AzureAD2 logins
		if (!userProfile.containsAttribute(Person.USER_NAME)) {
			if (userProfile instanceof CommonProfile) {
				CommonProfile cp = (CommonProfile) userProfile;
				cp.addAttribute(Person.USER_NAME, cp.getEmail().toLowerCase());
			}
		}

		// retrieve username and go
		String username = userProfile.getAttribute(Person.USER_NAME).toString();
		return Optional.ofNullable(username);
	}

	@Override
	public Result onUnauthorized(Request request) {

		// no redirect on htmx requests
		if (request.header("HX-Request").isPresent()) {
			return noContent();
		}

		// POST requests that are unauthorized should receive a proper 4xx response
		if (request.method().equalsIgnoreCase("POST")) {
			return forbidden();
		}

		// in case we have a deep link, let's make it a redirect to be followed after login
		String requestPath = request.path();
		if (requestPath.replaceAll("[^/]", "").length() > 1) {
			if (!requestPath.contains("download") && !requestPath.contains("/components/")) {
				return redirect(controllers.routes.HomeController.login("", ""))
				        .addingToSession(request, "error",
				                "Please log in first, we will take you to your destination then.")
				        .addingToSession(request, AbstractAsyncController.REDIRECT_URL, requestPath);
			}

			return redirect(controllers.routes.HomeController.login("", "")).addingToSession(request, "error",
			        "Please log in first, we will take you to your destination then.");
		} else {
			return redirect("/").addingToSession(request, "error", "Not authorized, sorry.");
		}
	}
}

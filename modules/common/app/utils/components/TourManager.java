package utils.components;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import jakarta.inject.Inject;
import play.cache.SyncCacheApi;

public class TourManager {

	private static final String TOUR_CACHE_POSTFIX = "_tour";

	private final SyncCacheApi cache;
	private final Map<String, Tour> registeredTours;

	@Inject
	public TourManager(SyncCacheApi cache) {
		this.cache = cache;
		this.registeredTours = new HashMap<>();

		// add tours
		Tour newTour = new Tour();
		newTour.name = "New project tour";
		newTour.code = views.html.elements.tours.newProjectTour.render().toString();
		newTour.landingPage = "/projects";
		registeredTours.put(newTour.slug(), newTour);

		Tour welcomeTour = new Tour();
		welcomeTour.name = "Welcome to Data Foundry";
		welcomeTour.code = views.html.elements.tours.welcomeTour.render().toString();
		welcomeTour.landingPage = "/projects";
		registeredTours.put(welcomeTour.slug(), welcomeTour);

		Tour HostingTour = new Tour();
		HostingTour.name = "Hosting tour";
		HostingTour.code = views.html.elements.tours.HostingTour.render().toString();
		HostingTour.landingPage = "/projects";
		registeredTours.put(HostingTour.slug(), HostingTour);

		Tour StarboardTour = new Tour();
		StarboardTour.name = "Starboard tour";
		StarboardTour.code = views.html.elements.tours.StarboardTour.render().toString();
		StarboardTour.landingPage = "/projects";
		registeredTours.put(StarboardTour.slug(), StarboardTour);

		Tour StarboardTourTemplate = new Tour();
		StarboardTourTemplate.name = "Starboard template tour";
		StarboardTourTemplate.code = views.html.elements.tours.StarboardTourTemplate.render().toString();
		StarboardTourTemplate.landingPage = "/projects";
		registeredTours.put(StarboardTourTemplate.slug(), StarboardTourTemplate);

		Tour AiTour = new Tour();
		AiTour.name = "AI tour";
		AiTour.code = views.html.elements.tours.AiTour.render().toString();
		AiTour.landingPage = "/projects";
		registeredTours.put(AiTour.slug(), AiTour);

		Tour tutorialDatalogger = new Tour();
		tutorialDatalogger.name = "Datalogger Tour";
		tutorialDatalogger.code = views.html.elements.tours.tutorialDatalogger.render().toString();
		tutorialDatalogger.landingPage = "/projects";
		registeredTours.put(tutorialDatalogger.slug(), tutorialDatalogger);

		Tour msos = new Tour();
		msos.name = "Making Sense of Sensors";
		msos.code = views.html.elements.tours.makingSenseofSensorsTour.render().toString();
		msos.landingPage = "/projects";
		registeredTours.put(msos.slug(), msos);
	}

	/**
	 * start a new tour for the given <code>username</code> and <code>tourName</code>
	 * 
	 * @param username
	 * @param tourName
	 * @return
	 */
	public Optional<Tour> prepare(String username, String tourName) {
		Optional<Tour> tourOpt = get(tourName);
		if (tourOpt.isEmpty()) {
			return Optional.empty();
		}

		// set tour name in session
		cache.set(username + TOUR_CACHE_POSTFIX, tourName);

		return tourOpt;
	}

	/**
	 * retrieve a running tour for the given <code>username</code>
	 * 
	 * @param username
	 * @return
	 */
	public Optional<Tour> retrieve(String username) {
		String tourName = (String) cache.get(username + TOUR_CACHE_POSTFIX).orElse("");
		return get(tourName);
	}

	/**
	 * remove a currently active tour
	 * 
	 * @param username
	 */
	public void discard(String username) {
		cache.remove(username + TOUR_CACHE_POSTFIX);
	}

	Optional<Tour> get(String tourName) {
		return Optional.ofNullable(registeredTours.get(tourName));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	static public class Tour {
		public String name;
		public String code;
		public String landingPage;

		public String slug() {
			return name.toLowerCase().replaceAll("\s", "-");
		}
	}

}

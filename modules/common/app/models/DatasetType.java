package models;

public enum DatasetType {

	// timeseries, device_id mandatory, activity, public parameters, data payload
	// suitable for IoT, web tracking, app tracking etc.
	TIMESERIES, IOT,

	// device-specific, bio-signals at scale,
	// needs to be pulled in from external service
	FITBIT,

	// based on complete dataset, movement data: x, y, z, t
	// file storage and structured data
	MOVEMENT,

	// form data set, scriptable with MarkDown
	// for simple forms
	FORM,

	// audio, video, picture data set
	// can be annotated, refers to context (+ observation)
	MEDIA,

	// participant-provided diary, simple format, either pure text or markdown?
	// context through participant
	DIARY,

	// researcher-provided annotation of a context, otherwise similar to diary
	ANNOTATION,

	// predefined data set made of existing components, only file storage, not importing
	COMPLETE,

	// predefined data set in a remote location, no local storage
	LINKED,

	// experience sampling dataset
	// uses PIEL survey tool format
	ES,

	// full survey data set, links to BOF survey installation on-premise
	// extensive coverage of scientific survey/questionnaire needs
	SURVEY,

	// device-specific, bio-signals at scale,
	// needs to be pulled in from external service
	GOOGLEFIT,

	// entity base data set, allows to add, retrieve, update and delete individual items
	// great for implementing a small web app with registration, login, and data storage
	ENTITY;

	///////////////////////////////////////////////////////////////////////////////////////

	/**
	 * display string for dataset types
	 * 
	 * @return
	 */
	public String toUIString() {
		if (this.equals(COMPLETE)) {
			return "EXISTING";
		} else {
			return super.toString();
		}
	}

	public String toColor() {
		switch (this) {
		case IOT:
		case TIMESERIES:
			return "blue";
		case ENTITY:
			return "pink";
		case FITBIT:
		case GOOGLEFIT:
		case MOVEMENT:
			return "red";
		case DIARY:
		case ES:
			return "teal";
		case FORM:
			return "lime darken-2";
		case SURVEY:
			return "green";
		case ANNOTATION:
			return "cyan";
		case COMPLETE:
			return "deep-purple";
		case MEDIA:
			return "indigo";
		case LINKED:
			return "amber";
		default:
			return "grey";
		}
	}

	public String toColorBG() {
		return toColor() + " lighten-3";
	}

	public String toIcon() {
		switch (this) {
		case IOT:
		case TIMESERIES:
			return "show_chart";
		case FITBIT:
			return "directions_run";
		case GOOGLEFIT:
			return "directions_run";
		case MOVEMENT:
			return "location_on";
		case DIARY:
			return "format_quote";
		case FORM:
			return "comment";
		case ES:
			return "question_answer";
		case SURVEY:
			return "feedback";
		case ANNOTATION:
			return "insert_comment";
		case COMPLETE:
			return "folder";
		case MEDIA:
			return "collections";
		case LINKED:
			return "input";
		default:
			return "insert_chart";
		}
	}

}

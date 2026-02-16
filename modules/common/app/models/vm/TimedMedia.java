package models.vm;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import com.fasterxml.jackson.databind.node.ObjectNode;

import models.sr.Participant;
import play.libs.Json;

public class TimedMedia extends TimedObject {

	public final static String COVER = "cover";
	public final static String MOVIE = "movie";
	public final static String IMAGE = "image";
	public final static String SNAP = "snap";

	public Long id;
	public String link;

	// values: cover, movie, image
	public String mediaType;
	public String caption;
	public Participant participant;
	public Long ds_id;

	public TimedMedia(long id, Date timestamp, String link, String mediaType, String description,
	        Participant participant) {
		this(id, timestamp, link, mediaType, description, participant, -1L);
	}

	public TimedMedia(long id, Date timestamp, String link, String mediaType, String description,
	        Participant participant, Long datasetId) {
		this.id = id;
		this.timestamp = timestamp;
		this.link = link;
		this.caption = nss(description).trim();
		this.mediaType = mediaType;
		this.participant = participant;
		this.ds_id = datasetId;
	}

	/**
	 * return whether given time d matches this object's timestamp with a tolerance of 1 sec
	 * 
	 * @param d
	 * @return
	 */
	public boolean matchTime(Date d) {
		return Math.abs(this.timestamp.getTime() - d.getTime()) <= 1000;
	}

	/**
	 * check whether this object is a cover image
	 * 
	 * @return
	 */
	public boolean isCover() {
		return this.mediaType.equals(COVER);
	}

	/**
	 * check whether this object is a movie
	 * 
	 * @return
	 */
	public boolean isMovie() {
		return this.mediaType.equals(MOVIE);
	}

	/**
	 * check whether this object is a regular image
	 * 
	 * @return
	 */
	public boolean isRegularImage() {
		return this.mediaType.equals(IMAGE);
	}

	/**
	 * check whether this object is a snap
	 * 
	 * @return
	 */
	public boolean isSnap() {
		return this.mediaType.equals(SNAP);
	}

	/**
	 * format the timestamp as JSON object
	 * 
	 * @return
	 */
	public String toJsonDate() {
		Calendar cal = new GregorianCalendar();
		cal.setTimeInMillis(timestamp.getTime());

		ObjectNode on = Json.newObject();
		on.put("minute", cal.get(Calendar.MINUTE));
		on.put("hour", cal.get(Calendar.HOUR));
		on.put("day", cal.get(Calendar.DAY_OF_MONTH));
		on.put("month", cal.get(Calendar.MONTH) + 1);
		on.put("year", cal.get(Calendar.YEAR));

		return on.toString();
	}

	public Long getId() {
		return id;
	}

}

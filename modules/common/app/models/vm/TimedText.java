package models.vm;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimedText extends TimedObject {

	public long participant_id;
	public String title;
	public String text;

	protected final DateFormat tsExportFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	public TimedText(long id, Date timestamp, String title, String text) {
		this.participant_id = id;
		this.timestamp = timestamp;
		this.title = title;
		this.text = text;
	}

}

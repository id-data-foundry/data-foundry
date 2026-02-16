package models.vm;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimedObject {

	public Date timestamp;

	protected final DateFormat prettyTSFormatter = new SimpleDateFormat("MMM dd' at 'HH:mm");
	protected final DateFormat tsFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	protected final DateFormat tsDateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	protected final DateFormat tsTimeFormatter = new SimpleDateFormat("HH:mm:ss");

	public final String nss(String str) {
		return str != null ? str : "";
	}

	/**
	 * return the file timestamp in a pretty-printed form
	 * 
	 * @return
	 */
	public final String pretty() {
		return prettyTSFormatter.format(timestamp);
	}

	/**
	 * return the file timestamp formatted
	 * 
	 * @return
	 */
	public final String datetime() {
		return tsFormatter.format(timestamp);
	}

	/**
	 * return the time component of the file timestamp
	 * 
	 * @return
	 */
	public final String time() {
		return tsTimeFormatter.format(timestamp);
	}

	/**
	 * return the date component of the file timestamp
	 * 
	 * @return
	 */
	public final String date() {
		return tsDateFormatter.format(timestamp);
	}

}

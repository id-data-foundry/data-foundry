package utils.rendering;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Pretty print the time duration given
 * 
 * 
 * from: https://stackoverflow.com/questions/3859288/how-to-calculate-time-ago-in-java
 * 
 * @author Riccardo Casatta
 *
 */
public class TimeAgo {
	private static final List<Long> times = Arrays.asList(TimeUnit.DAYS.toMillis(365), TimeUnit.DAYS.toMillis(30),
	        TimeUnit.DAYS.toMillis(7), TimeUnit.DAYS.toMillis(1), TimeUnit.HOURS.toMillis(1),
	        TimeUnit.MINUTES.toMillis(1), TimeUnit.SECONDS.toMillis(1));
	private static final List<String> timesString = Arrays.asList("year", "month", "week", "day", "hour", "minute",
	        "second");

	public static String toDuration(long duration) {
		StringBuffer res = new StringBuffer();
		for (int i = 0; i < TimeAgo.times.size(); i++) {
			Long current = TimeAgo.times.get(i);
			long temp = duration / current;
			if (temp > 0) {
				res.append(temp).append(" ").append(TimeAgo.timesString.get(i)).append(temp != 1 ? "s" : "")
				        .append(" ago");
				break;
			}
		}
		if ("".equals(res.toString()))
			return "just now";
		else
			return res.toString();
	}

	/**
	 * return the string representation of how long ago the given date is now
	 * 
	 * @param dateSomeTimeAgo
	 * @return
	 */
	public static String timeAgo(Date dateSomeTimeAgo) {
		return toDuration(System.currentTimeMillis() - dateSomeTimeAgo.getTime());
	}

	/**
	 * return the current date rolled-back by n days
	 * 
	 * @param days
	 * @return
	 */
	public static Date daysAgo(int days) {
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTimeInMillis(System.currentTimeMillis());
		gc.add(Calendar.DAY_OF_YEAR, -days);
		return gc.getTime();
	}

	/**
	 * return the current date rolled-back by n months
	 * 
	 * @param months
	 * @return
	 */
	public static Date monthsAgo(int months) {
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTimeInMillis(System.currentTimeMillis());
		gc.add(Calendar.MONTH, -months);
		return gc.getTime();
	}
}

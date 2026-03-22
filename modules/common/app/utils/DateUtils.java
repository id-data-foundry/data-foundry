package utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import models.Dataset;
import play.Logger;

public class DateUtils {

	private static final Logger.ALogger logger = Logger.of(DateUtils.class);

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final SimpleDateFormat tsDateFormatter = new SimpleDateFormat("yyyy-MM-dd");

	/**
	 * parse a String specified date to a Date object
	 * 
	 * @param date yyyy-MM-dd / MMM d, yyyy
	 * @return
	 */
	public static Date parseDate(String date) {
		if (date == null || date.trim().length() == 0) {
			return new Date();
		}

		try {
			final SimpleDateFormat sdf;
			if (date.length() <= 10) {
				sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			} else {
				sdf = new SimpleDateFormat("MMM d, yyyy HH:mm:ss");
			}

			return date.trim().isEmpty() ? new Date() : sdf.parse(date + " 00:00:00");
		} catch (ParseException e) {
			logger.info("parse date error: " + e.getMessage());
			return new Date();
		}
	}

	/**
	 * parse a String specified timestamp to a Date object
	 * 
	 * @param date unix epoch timestamp
	 * @return
	 */
	public static Date parseTimestamp(String timestamp) {
		if (timestamp == null || timestamp.trim().length() == 0) {
			return new Date();
		}

		long ts = DataUtils.parseLong(timestamp);
		return ts > -1L ? new Date(ts) : new Date();
	}

	/**
	 * return a String as the specified format for the Date object
	 * 
	 * @param date
	 * @return
	 */
	public static String parseDate(Date date) {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return date == null ? "" : sdf.format(startOfDay(date));
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static String pretty(long timestamp) {
		return timestamp > 0 ? pretty(new Date(timestamp)) : "";
	}

	public static String pretty(Date date) {
		return sdf.format(date);
	}

	public static String tsFormat(Date date) {
		return tsDateFormatter.format(date);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * return Monday midnight of the week given by d
	 * 
	 * @param d
	 * @return
	 */
	public static Date thisMonday(Date d) {
		Calendar c = new GregorianCalendar();
		// set Monday as the first day of the week
		c.setFirstDayOfWeek(2);
		c.setTime(d);

		while (c.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY) {
			c.add(Calendar.DAY_OF_WEEK, -1);
		}

		return c.getTime();
	}

	/**
	 * convert a date to the next week's Monday
	 * 
	 * @param d
	 * @return
	 */
	public static Date toNextMonday(Date d) {
		Calendar c = new GregorianCalendar();
		// set Monday as the first day of the week
		c.setFirstDayOfWeek(2);
		c.setTime(d);

		if (c.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) {
			do {
				c.add(Calendar.DAY_OF_WEEK, 1);
			} while (c.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY);
		} else {
			while (c.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY) {
				c.add(Calendar.DAY_OF_WEEK, 1);
			}
		}

		return c.getTime();
	}

	/**
	 * convert a date to the previous week's Monday
	 * 
	 * @param d
	 * @return
	 */
	public static Date toPreviousMonday(Date d) {
		Calendar c = new GregorianCalendar();
		// set Monday as the first day of the week
		c.setFirstDayOfWeek(2);
		c.setTime(thisMonday(d));

		do {
			c.add(Calendar.DAY_OF_WEEK, -1);
		} while (c.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY);

		return c.getTime();
	}

	/**
	 * return a calendar object for the given day with time set to mignight
	 * 
	 * @param d
	 * @return
	 */
	public static Date startOfDay(Date d) {
		Calendar c = new GregorianCalendar();
		c.setTime(d);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}

	/**
	 * return a calendar object for the given day with time set to mignight
	 * 
	 * @param d
	 * @return
	 */
	public static Date endOfDay(Date d) {
		Calendar c = new GregorianCalendar();
		c.setTime(d);
		c.set(Calendar.HOUR_OF_DAY, 23);
		c.set(Calendar.MINUTE, 59);
		c.set(Calendar.SECOND, 59);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}

	/**
	 * return the date 24 hours after given date
	 * 
	 * @param d
	 * @return
	 */
	public static Date dayAfter(Date d) {
		return moveDays(d, 1);
	}

	/**
	 * return the date 24 hours before given date
	 * 
	 * @param d
	 * @return
	 */
	public static Date dayBefore(Date d) {
		return moveDays(d, -1);
	}

	/**
	 * return the date as requested some days after / before the date
	 * 
	 * @param d
	 * @param days
	 * @return
	 */
	public static Date moveDays(Date d, int days) {
		Calendar c = new GregorianCalendar();
		c.setTime(d);
		c.add(Calendar.DAY_OF_YEAR, days);
		return c.getTime();
	}

	/**
	 * return the date 7 days after date
	 * 
	 * @param d
	 * @return
	 */
	public static Date toNextWeek(Date d) {
		return moveWeeks(d, 1);
	}

	/**
	 * return the date 7 days before date
	 * 
	 * @param d
	 * @return
	 */
	public static Date toPreviousWeek(Date d) {
		return moveWeeks(d, -1);
	}

	/**
	 * return the date as requested some weeks after / before the date
	 * 
	 * @param d
	 * @param weeks
	 * @return
	 */
	public static Date moveWeeks(Date d, int weeks) {
		Calendar c = new GregorianCalendar();
		c.setTime(d);
		c.add(Calendar.WEEK_OF_YEAR, weeks);
		return c.getTime();
	}

	/**
	 * return the date in 3 months from given date
	 * 
	 * @param d
	 * @return
	 */
	public static Date inThreeMonths(Date d) {
		return moveMonths(d, 3);
	}

	/**
	 * return the date as requested some months after / before the date
	 * 
	 * @param d
	 * @param months
	 * @return
	 */
	public static Date moveMonths(Date d, int months) {
		Calendar c = new GregorianCalendar();
		c.setTime(d);
		c.add(Calendar.MONTH, months);
		return c.getTime();
	}

	/**
	 * returns whether the specified <code>date</code> is in the week of <code>weekDate</code>
	 * 
	 * @param weekDate
	 * @param date
	 * @return
	 */
	public static boolean isInWeekOf(Date weekDate, Date date) {
		return thisMonday(startOfDay(weekDate)).before(date) && toNextMonday(endOfDay(weekDate)).after(date);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check start/end dates while adding a new dataset
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public static Date[] getDates(String startDate, String endDate) {
		// fix null pointers
		startDate = startDate != null ? startDate : "";
		endDate = endDate != null ? endDate : "";

		if (endDate.equals("")) {
			startDate = startDate.equals("") ? LocalDate.now().toString() : startDate;
			endDate = LocalDate.parse(startDate).plusMonths(3l).toString();
		} else {
			if (startDate.equals("")) {
				if (LocalDate.parse(endDate).isBefore(LocalDate.now())) {
					startDate = endDate;
				} else {
					startDate = LocalDate.now().toString();
				}
			}
		}

		Date parsedStartDate = null;
		Date parsedEndDate = null;
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			parsedStartDate = format.parse(startDate + " 00:00:00");
			parsedEndDate = format.parse(endDate + " 23:59:59");
		} catch (ParseException pe) {
			// do nothing: if parsing does not, we insert the current date
			// if parsing is failed, we get null as result
		}

		return new Date[] { parsedStartDate == null ? new Date() : parsedStartDate,
				parsedEndDate == null ? new Date() : parsedEndDate };
	}

	/**
	 * check start/end dates while editing a dataset
	 * 
	 * @param startDate
	 * @param endDate
	 * @param ds
	 * @return
	 */
	public static Date[] getDates(String startDate, String endDate, Dataset ds) {
		// fix null pointers
		startDate = startDate != null ? startDate : "";
		endDate = endDate != null ? endDate : "";

		if (endDate.equals("")) {
			if (!startDate.equals("") && startDate.equals(ds.getStart().toString())) {
				String[] dateSlice = startDate.split(" ");
				startDate = dateSlice[5] + "-" + DateUtils.getMonthNum(dateSlice[1]) + "-" + dateSlice[2];
			} else {
				startDate = startDate.equals("") ? LocalDate.now().toString() : startDate;
			}
			endDate = LocalDate.parse(startDate).plusMonths(3l).toString();
		} else {
			if (startDate.equals("")) {
				if (ds.getEnd() != null && endDate.equals(ds.getEnd().toString())) {
					String[] dateSlice = endDate.split(" ");
					if (LocalDate.parse(dateSlice[5] + "-" + DateUtils.getMonthNum(dateSlice[1]) + "-" + dateSlice[2])
							.isBefore(LocalDate.now())) {
						startDate = dateSlice[5] + "-" + DateUtils.getMonthNum(dateSlice[1]) + "-" + dateSlice[2];
					} else {
						startDate = LocalDate.now().toString();
					}
				} else {
					if (LocalDate.parse(endDate).isBefore(LocalDate.now())) {
						startDate = endDate;
					} else {
						startDate = LocalDate.now().toString();
					}
				}
			}
		}

		Date parsedStartDate = null;
		Date parsedEndDate = null;
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			if (ds.getStart() != null && startDate.equals(ds.getStart().toString())) {
				parsedStartDate = ds.getStart();
			} else {
				parsedStartDate = format.parse(startDate + " 00:00:00");
			}

			if (ds.getEnd() != null && endDate.equals(ds.getEnd().toString())) {
				parsedEndDate = ds.getEnd();
			} else {
				parsedEndDate = format.parse(endDate + " 23:59:59");
			}
		} catch (ParseException pe) {
			// do nothing: if parsing does not, we insert the current date
			// if parsing is failed, we get null as result
		}

		return new Date[] { parsedStartDate == null ? new Date() : parsedStartDate,
				parsedEndDate == null ? new Date() : parsedEndDate };
	}

	/**
	 * parse single date, return Date object or null if failed
	 * 
	 * @param date
	 * @param time
	 * @return
	 */
	public static Date getDate(String date, String time) {
		Date parseDate = null;
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			parseDate = format.parse(date + " " + time);
		} catch (ParseException pe) {
			// do nothing: if parsing does not, we insert the current date
			// if parsing is failed, we get null as result
		}

		return parseDate;
	}

	/**
	 * returns number as int corresponding to input month name in String
	 * 
	 * @param mon
	 * @return
	 */
	public static String getMonthNum(String mon) {
		String monNum = "";

		switch (mon) {
		case "Jan":
			monNum = "01";
			break;
		case "Feb":
			monNum = "02";
			break;
		case "Mar":
			monNum = "03";
			break;
		case "Apr":
			monNum = "04";
			break;
		case "May":
			monNum = "05";
			break;
		case "Jun":
			monNum = "06";
			break;
		case "Jul":
			monNum = "07";
			break;
		case "Aug":
			monNum = "08";
			break;
		case "Sep":
			monNum = "09";
			break;
		case "Oct":
			monNum = "10";
			break;
		case "Nov":
			monNum = "11";
			break;
		case "Dec":
			monNum = "12";
			break;
		default:
			monNum = "00";
		}
		return monNum;
	}

	/**
	 * get the target time as millisecond format
	 * 
	 * @param targetDate yyyy-MM-dd
	 * @param targetTime HH:mm:ss
	 * @return
	 */
	public static Long getMillis(String targetDate, String targetTime) {
		if (!DatasetUtils.nnne(targetDate) || !DatasetUtils.nnne(targetTime)) {
			return 0l;
		}

		String targetString = targetDate + " " + targetTime;
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		LocalDateTime targetLDT = null;

		try {
			targetLDT = LocalDateTime.parse(targetString, dtf);
		} catch (DateTimeParseException e) {
			logger.error("could not parse " + targetString, e);
		}

		if (targetLDT != null) {
			return targetLDT.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		} else {
			return 0l;
		}
	}

	/**
	 * parse targetTime string, which is expected in yyyy-MM-ddTHH:mm:ss.SSS format, to milliseconds
	 * 
	 * @param targetTime
	 * @return
	 */
	public static Long getMillis(String targetTime) {
		if (!DatasetUtils.nnne(targetTime) || targetTime.length() < 19) {
			return 0l;
		}

		// the date can be parsed only in yyyy-MM-ddTHH:mm:ss format
		targetTime = targetTime.substring(0, 19);
		Long millis = 0l;
		try {
			millis = LocalDateTime.parse(targetTime).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		} catch (DateTimeParseException e) {
			logger.error("could not parse " + targetTime, e);
		}

		return millis;
	}

	/**
	 * return millis of a date which is diff days by today ex. if today is 2019-10-10, getMillisFromToday(-3l) will be
	 * the milliseconds of 2019-10-07
	 * 
	 * @param diff
	 * @return
	 */
	public static long getMillisFromToday(long diff) {
		return getMillis(LocalDate.now().plusDays(diff).toString(), "00:00:00");
	}

	/**
	 * return a long array with millis of start / end date of dataset, {start date in long, end date in long}
	 * 
	 * @param ds
	 * @return
	 */
	public static long[] getMillisFromDS(Dataset ds) {
		if (ds != null && ds.getId() > -1l) {
			String[] dateSlice = ds.getStart().toString().split(" ");
			String startDate = dateSlice[5] + "-" + getMonthNum(dateSlice[1]) + "-" + dateSlice[2];
			dateSlice = ds.getEnd().toString().split(" ");
			String endDate = dateSlice[5] + "-" + getMonthNum(dateSlice[1]) + "-" + dateSlice[2];
			return new long[] { getMillis(startDate, "00:00:00"), getMillis(endDate, "23:59:59") };
		} else {
			return new long[] { -1l };
		}
	}

	public static long[] getMillisFromDS(long id) {
		return getMillisFromDS(Dataset.find.byId(id));
	}

	/**
	 * return the date of long with format as yyyy-MM-dd in String
	 * 
	 * @param milliSec
	 * @return
	 */
	public static String getDateFromMillis(long milliSec) {
		DateFormat simple = new SimpleDateFormat("yyyy-MM-dd");
		Date result = new Date(milliSec);

		return simple.format(result);
	}

}
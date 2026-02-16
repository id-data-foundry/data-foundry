package services.inlets;

abstract public interface ScheduledService {
	abstract public void refresh();

	abstract public void stop();

	/**
	 * null safe string
	 */
	public default String nss(String s) {
		return s != null ? s : "";
	}

	/**
	 * valid text = Not Null and Not Empty
	 */
	public default boolean nnne(String text) {
		return text != null && text.length() > 0;
	}

	/**
	 * return the dataDate with exactly zero second of the time
	 * 
	 * @param dataDate
	 * @return
	 */
	public default long setByMinute(long dataDate) {
		return dataDate % 60000 != 0l ? dataDate - dataDate % 60000 : dataDate;
	}
}

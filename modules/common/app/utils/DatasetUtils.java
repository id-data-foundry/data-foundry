package utils;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import models.Dataset;
import models.sr.Cluster;
import models.sr.Device;
import models.sr.Participant;
import models.sr.Wearable;
import play.Logger;

public class DatasetUtils {

	private static final Logger.ALogger logger = Logger.of(DatasetUtils.class);

	/**
	 * filter out datasets that are scripts
	 * 
	 * @param datasets
	 * @return
	 */
	public static List<Dataset> scriptsExcluded(List<Dataset> datasets) {
		return datasets.stream().filter(ds -> !ds.isScript()).collect(Collectors.toList());
	}

	/**
	 * filter out datasets that are scripts or chatbots
	 * 
	 * @param datasets
	 * @return
	 */
	public static List<Dataset> scriptsAndChatbotsExcluded(List<Dataset> datasets) {
		return datasets.stream().filter(ds -> !ds.isScript() && !ds.isChatbot()).collect(Collectors.toList());
	}

	/**
	 * filter out datasets that are not scripts, return scripts only
	 * 
	 * @param datasets
	 * @return
	 */
	public static List<Dataset> scriptsOnly(List<Dataset> datasets) {
		return datasets.stream().filter(ds -> ds.isScript()).collect(Collectors.toList());
	}

	/**
	 * filter out datasets that are not scripts or chatbots, return scripts and chatbots only
	 * 
	 * @param datasets
	 * @return
	 */
	public static List<Dataset> scriptsAndChatbotsOnly(List<Dataset> datasets) {
		return datasets.stream().filter(ds -> ds.isScript() || ds.isChatbot()).collect(Collectors.toList());
	}

	/**
	 * filter out datasets that are not chatbots, return chatbots only
	 * 
	 * @param datasets
	 * @return
	 */
	public static List<Dataset> chatbotsOnly(List<Dataset> datasets) {
		return datasets.stream().filter(ds -> ds.isChatbot()).collect(Collectors.toList());
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * if expiry is between the end and the start dates of the dataset, then return true; today is the date of yesterday
	 * here
	 * 
	 * @param expiry
	 * @param dsStart
	 * @param dsEnd
	 * @return
	 */
	public static boolean isStillActive(long expiry, long dsStart, long dsEnd) {
		return expiry <= dsEnd && expiry >= dsStart;
	}

	public static int isStatusChanged(Date dsStart, Date dsEnd, Date newStart, Date newEnd, long target) {
		boolean currentStatus = isStillActive(target, dsStart.getTime(), dsEnd.getTime()),
				newStatus = isStillActive(target, newStart.getTime(), newEnd.getTime());

		// if status of dataset is changed
		if (currentStatus && !newStatus) {
			// active to inactive

			if (target < newStart.getTime()) {
				// start date of dataset is delayed
				return -1;
			} else if (target > newEnd.getTime()) {
				// dataset ends earlier than expect
				return -2;
			}
		} else if (!currentStatus && newStatus) {
			// inactive to active

			if (target > dsEnd.getTime()) {
				// end date of dataset is extended
				return 1;
			} else if (target < dsStart.getTime()) {
				// next dataset starts earlier than expect
				return 2;
			}
		}
		// return 3 for being still active, otherwise return -3 for inactive
		return currentStatus ? 3 : -3;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * check the status of the dataset for the new start / end dates, wearables linked to the dataset would be updated
	 * if necessary; this function is only used by Fitbit and GoogleFit datasets now
	 * 
	 * @param dsStart
	 * @param dsEnd
	 * @param dates
	 * @param dsId
	 */
	public static void updateWearables(Date[] oldDates, Date[] dates, Dataset ds) {
		long today = new Date().getTime();
		int status = isStatusChanged(oldDates[0], oldDates[1], dates[0], dates[1], today);

		if (status != -3) {
			// status = 1 / 2 / -1 / -2 / 3
			updateActiveWearable(status, oldDates[0], oldDates[1], dates, ds.getId());
		} else {
			// List<Wearable> wList = Wearable.find.query().where().eq("scopes", Long.toString(ds.id)).findList();
			List<Wearable> wList = ds.getProject().getWearables().stream()
					.filter(w -> w.getScopes().equals(Long.toString(ds.getId()))).collect(Collectors.toList());

			if (today < dates[0].getTime()) {
				// new start date is in the future

				for (Wearable w : wList) {
					w.setExpiry(dates[0].getTime());
					w.update();
				}
			} else {
				// check delayed wearables in the dataset

				for (Wearable w : wList) {
					int wStatus = isStatusChanged(oldDates[0], oldDates[1], dates[0], dates[1], w.getExpiry());
					// if wStatus is 3, then nothing changed
					if (wStatus != 3) {
						updateDelayedWearable(wStatus, w, oldDates, dates, ds.getId());
					}
				}
			}
		}
	}

	/**
	 * handle wearables, which are linked to this dataset and their expiry is up to date
	 * 
	 * @param status
	 * @param dsStart
	 * @param dsEnd
	 * @param dates
	 * @param dsId
	 */
	private static void updateActiveWearable(int status, Date dsStart, Date dsEnd, Date[] dates, long dsId) {
		List<Wearable> wList = null;

		switch (status) {
		//// positive: dataset is changed from inactive to active
		case 1:
			// current end date < today <= new end date
			// wearables, which are waiting for starting of next dataset, should be manually linked to
			// original(previous) dataset again by researcher

			if (isStillActive(dates[0].getTime(), dsStart.getTime(), dsEnd.getTime())) {
				// some dates are overlapping
				wList = Wearable.find.query().where().eq("scopes", Long.toString(dsId)).and().or()
						.gt("expiry", dsEnd.getTime()).lt("expiry", dates[0].getTime()).endOr().findList();

				for (Wearable w : wList) {
					// new start date is between the original start date and end date
					if (w.getExpiry() < dates[0].getTime()) {
						// fetching data from new start date
						w.setExpiry(dates[0].getTime());
					} else if (w.getExpiry() > dsEnd.getTime()) {
						// fetchign data from dsEnd to dates[1]
						w.setExpiry(dsEnd.getTime() + 1000l);
					}

					w.update();
				}
			} else {
				// today >= new start date > current end date
				wList = Wearable.find.query().where().eq("scopes", Long.toString(dsId)).findList();

				for (Wearable w : wList) {
					w.setExpiry(dates[0].getTime());
					w.update();
				}
			}
			break;
		case 2:
			// current start date > today >= new start date
			// expiry of wearables linked to this dataset would be modified as new start date
			wList = Wearable.find.query().where().eq("scopes", Long.toString(dsId)).findList();

			for (Wearable w : wList) {
				w.setExpiry(dates[0].getTime());
				w.update();
			}
			break;
		case 3:
			// dataset is still active with different start / end dates, mainly we deal with the issue for new start
			// date.
			// if new start date is after current start date, update the wearables which have an earlier expiry than the
			// new start date. On the other hand, if new start date is before current start date, this would cause data
			// duplication, which could be solved by resetting the dataset or forbidding this operation;
			// TODO: solve data duplication issue -- if new start date is before current start date.
			wList = Wearable.find.query().where().eq("scopes", Long.toString(dsId)).and()
					.lt("expiry", dates[0].getTime()).findList();

			for (Wearable w : wList) {
				w.setExpiry(dates[0].getTime());
				w.update();
			}
			break;

		//// negtive: dataset is changed from active to inactive
		case -1:
			// today < new start date
			// dataset is standby now and will keep waiting until the new start date, expiry of wearables linked to this
			// dataset would be set as the new start date

			wList = Wearable.find.query().where().eq("scopes", Long.toString(dsId)).and()
					.lt("expiry", dates[0].getTime()).findList();

			for (Wearable w : wList) {
				w.setExpiry(dates[0].getTime());
				w.update();
			}
			break;
		case -2:
			// today > new end date
			// dataset is out of date now, wearables, which has higher expiry than new end date, of this dataset would
			// be linked to next dataset if it exists
			// TODO: solve data duplication issue -- if new start date is before current start date.

			Dataset ds = Dataset.find.byId(dsId);
			if (ds == null) {
				break;
			}

			List<Dataset> dsList = ds.getProject().getNextDSList(ds);
			wList = Wearable.find.query().where().eq("scopes", Long.toString(dsId)).findList();
			long current = DateUtils.getMillisFromToday(0);

			for (Wearable w : wList) {
				if (w.getExpiry() > dates[1].getTime()) {
					if (dsList.size() >= 2) {
						// has next dataset
						w.setScopes(Long.toString(dsList.get(1).getId()));
						w.setExpiry(Dataset.find.byId(dsList.get(1).getId()).getStart().getTime());
						w.update();
					} else {
						// set the expiry as today as no new dataset
						w.setExpiry(current);
						w.update();
					}
				} else if (w.getExpiry() < dates[0].getTime()) {
					w.setExpiry(dates[0].getTime());
					w.update();
				}
			}
			break;

		//// status is not changed, do nothing
		default:
		}
	}

	/**
	 * this utility method fills the empty slots of a cluster (participants, devices, wearables) so that it can be used
	 * to filter in a DB query
	 * 
	 * @param cluster
	 */
	public static Cluster fillClusterSlots(Cluster cluster) {

		if (cluster.getParticipants().isEmpty()) {
			Participant dummy = new Participant("dummy@example.org");
			dummy.setId(Long.MAX_VALUE);
			cluster.getParticipants().add(dummy);
		}

		if (cluster.getDevices().isEmpty()) {
			Device dummy = new Device();
			dummy.setId(Long.MAX_VALUE);
			cluster.getDevices().add(dummy);
		}

		if (cluster.getWearables().isEmpty()) {
			Wearable dummy = new Wearable();
			dummy.setId(Long.MAX_VALUE);
			cluster.getWearables().add(dummy);
		}

		return cluster;
	}

	/**
	 * dealing with wearables, which are linked to this inactive dataset and not synced normally
	 * 
	 * @param status
	 * @param w
	 * @param oldDates
	 * @param dates
	 * @param dsId
	 */
	private static void updateDelayedWearable(int status, Wearable w, Date[] oldDates, Date[] dates, long dsId) {
		logger.info("status of update delayed wearable:" + status);
		switch (status) {

		//// negtive: wearable is changed from active to inactive status for case -1 & case -2, case -3 for
		//// wearable stays idled
		case -1:
			// w.expiry < new start date
			// wearable is idled now and will keep waiting until new start date of the same dataset, expiry of
			// wearables linked to this dataset should be set as the new start date
			w.setExpiry(dates[0].getTime());
			w.update();
			break;
		case -2:
			// w.expiry > new end date
			// expiry of this wearable is out of date now, wearables of this wearable should be linked to next
			// dataset if it exists; otherwise, expiry of this wearable would be set as today
			List<Dataset> dsList = w.getProject().getNextDSList(dsId);
			if (dsList.size() >= 2) {
				w.setScopes(Long.toString(dsList.get(1).getId()));
				w.setExpiry(Dataset.find.byId(dsList.get(1).getId()).getStart().getTime());
				w.update();
			} else {
				w.setExpiry(DateUtils.getMillisFromToday(0));
				w.update();
			}

			break;
		case -3:
			// w.expiry is not between start / end dates of both current dates and new dates of the dataset
			if (w.getExpiry() < dates[0].getTime()) {
				// dataset will start in the future
				w.setExpiry(dates[0].getTime());
				w.update();
			} else {
				// dataset is out of date
				w.setExpiry(DateUtils.getMillisFromToday(0));
				w.update();
			}
			break;
		//// positive: dataset is changed from inactive to active for case 1 & case 2, case 3 is for dataset
		//// is still active after being modified
		case 1:
			// w.expiry < new end date
			// the dataset is inactive, the data would still be obtained for the dataset
			if (dates[0].getTime() < oldDates[1].getTime()) {
				w.setExpiry(oldDates[1].getTime());
				w.update();
			} else {
				w.setExpiry(dates[0].getTime());
				w.update();
			}
			break;
		case 2:
			// w.expiry < original start date
			// wearable is waiting for the dataset, and the dataset starts earlier
			w.setExpiry(dates[0].getTime());
			w.update();
			break;
		case 3:
			// status not changed, do nothing
		default:

		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static boolean nnne(String txt) {
		return txt == null || txt.trim().length() == 0 ? false : true;
	}
}

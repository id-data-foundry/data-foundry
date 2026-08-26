package datasets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Singleton;

import models.Dataset;
import models.DatasetType;

@Singleton
public class DatasetUpdateQueue {

	private final Set<Long> queue = ConcurrentHashMap.newKeySet();

	/**
	 * Enqueue a dataset ID for background processing
	 * 
	 * @param datasetId
	 */
	public void enqueue(Long datasetId) {
		if (datasetId != null && datasetId > 0) {
			queue.add(datasetId);
		}
	}

	/**
	 * Enqueue a dataset if it is relevant for background services
	 * 
	 * @param ds
	 */
	public void enqueue(Dataset ds) {
		if (ds != null && isRelevant(ds)) {
			enqueue(ds.getId());
		}
	}

	/**
	 * Enqueue multiple dataset IDs for background processing
	 * 
	 * @param datasetIds
	 */
	public void enqueueAll(Collection<Long> datasetIds) {
		if (datasetIds != null) {
			for (Long id : datasetIds) {
				enqueue(id);
			}
		}
	}

	/**
	 * Drain all queued dataset IDs and clear the queue atomically
	 * 
	 * @return set of changed dataset IDs
	 */
	public Set<Long> drain() {
		if (queue.isEmpty()) {
			return Collections.emptySet();
		}
		Set<Long> drained = new HashSet<>(queue);
		queue.removeAll(drained);
		return drained;
	}

	/**
	 * Check if there are any pending dataset updates in the queue
	 * 
	 * @return true if queue is empty
	 */
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	/**
	 * Check whether a dataset is relevant for background services (OOCSI and Actor scripting)
	 * 
	 * @param ds
	 * @return true if dataset is IOT, ENTITY, or an ACTOR script
	 */
	public static boolean isRelevant(Dataset ds) {
		if (ds == null || ds.getId() == null || ds.getId() <= 0) {
			return false;
		}
		return ds.getDsType() == DatasetType.IOT || ds.getDsType() == DatasetType.ENTITY || ds.isScript();
	}
}

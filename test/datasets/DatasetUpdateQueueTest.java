package datasets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import models.Dataset;
import models.DatasetType;

public class DatasetUpdateQueueTest {

	private DatasetUpdateQueue queue;

	@Before
	public void setUp() {
		queue = new DatasetUpdateQueue();
		queue.drain(); // ensure clean state
	}

	@Test
	public void testEnqueueAndDrain() {
		assertTrue(queue.isEmpty());
		assertEquals(0, queue.drain().size());

		queue.enqueue(10L);
		queue.enqueue(20L);
		assertFalse(queue.isEmpty());

		Set<Long> drained = queue.drain();
		assertEquals(2, drained.size());
		assertTrue(drained.contains(10L));
		assertTrue(drained.contains(20L));
		assertTrue(queue.isEmpty());
	}

	@Test
	public void testDeduplication() {
		queue.enqueue(10L);
		queue.enqueue(10L);
		queue.enqueue(10L);

		Set<Long> drained = queue.drain();
		assertEquals(1, drained.size());
		assertTrue(drained.contains(10L));
		assertTrue(queue.isEmpty());
	}

	@Test
	public void testEnqueueAll() {
		queue.enqueueAll(Arrays.asList(1L, 2L, 3L, 2L, 1L));

		Set<Long> drained = queue.drain();
		assertEquals(3, drained.size());
		assertTrue(drained.contains(1L));
		assertTrue(drained.contains(2L));
		assertTrue(drained.contains(3L));
	}

	@Test
	public void testIgnoreInvalidIds() {
		queue.enqueue((Long) null);
		queue.enqueue((Dataset) null);
		queue.enqueue(0L);
		queue.enqueue(-1L);

		assertTrue(queue.isEmpty());
		assertEquals(0, queue.drain().size());
	}

	@Test
	public void testIsRelevant() {
		Dataset iot = new Dataset();
		iot.setId(1L);
		iot.setDsType(DatasetType.IOT);
		assertTrue(DatasetUpdateQueue.isRelevant(iot));

		Dataset entity = new Dataset();
		entity.setId(2L);
		entity.setDsType(DatasetType.ENTITY);
		assertTrue(DatasetUpdateQueue.isRelevant(entity));

		Dataset script = new Dataset();
		script.setId(3L);
		script.setDsType(DatasetType.COMPLETE);
		script.setCollectorType(Dataset.ACTOR);
		assertTrue(DatasetUpdateQueue.isRelevant(script));

		Dataset media = new Dataset();
		media.setId(4L);
		media.setDsType(DatasetType.MEDIA);
		assertFalse(DatasetUpdateQueue.isRelevant(media));

		Dataset survey = new Dataset();
		survey.setId(5L);
		survey.setDsType(DatasetType.SURVEY);
		assertFalse(DatasetUpdateQueue.isRelevant(survey));

		Dataset nonActorComplete = new Dataset();
		nonActorComplete.setId(6L);
		nonActorComplete.setDsType(DatasetType.COMPLETE);
		assertFalse(DatasetUpdateQueue.isRelevant(nonActorComplete));

		assertFalse(DatasetUpdateQueue.isRelevant(null));
	}

	@Test
	public void testEnqueueDataset() {
		Dataset iot = new Dataset();
		iot.setId(1L);
		iot.setDsType(DatasetType.IOT);
		queue.enqueue(iot);

		Dataset media = new Dataset();
		media.setId(2L);
		media.setDsType(DatasetType.MEDIA);
		queue.enqueue(media);

		Set<Long> drained = queue.drain();
		assertEquals(1, drained.size());
		assertTrue(drained.contains(1L));
	}
}

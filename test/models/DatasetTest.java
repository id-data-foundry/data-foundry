package models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.pekko.Done;
import org.apache.pekko.stream.QueueOfferResult;
import org.apache.pekko.stream.javadsl.SourceQueueWithComplete;
import org.apache.pekko.util.ByteString;

import org.junit.Before;
import org.junit.Test;

import datasets.DatasetConnector;
import io.ebean.DB;
import play.Application;
import play.inject.guice.GuiceApplicationBuilder;
import play.test.WithApplication;
import models.ds.CompleteDS;
import models.ds.EntityDS;
import models.ds.MediaDS;
import models.ds.TimeseriesDS;

public class DatasetTest extends WithApplication {

	private DatasetConnector datasetConnector;

	@Override
	protected Application provideApplication() {
		return new GuiceApplicationBuilder().configure("db.default.driver", "org.h2.Driver")
				.configure("db.default.url", "jdbc:h2:mem:play;DB_CLOSE_DELAY=-1")
				.configure("play.evolutions.db.default.autoApply", true).build();
	}

	@Before
	public void setUp() {
		datasetConnector = app.injector().instanceOf(DatasetConnector.class);
	}

	private Person createPerson(String prefix) {
		Person person = new Person();
		person.setUser_id(UUID.randomUUID().toString());
		person.setFirstname(prefix);
		person.setLastname("User");
		person.setEmail(prefix.toLowerCase() + "_" + UUID.randomUUID().toString().substring(0, 8) + "@example.com");
		person.save();
		return person;
	}

	@Test
	public void testGetItemCount() {
		Person user = createPerson("Count");
		Project project = Project.create("Test Project", user, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Test DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();

		assertEquals(0, ds.getItemCount());

		// Add some records
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity1", play.libs.Json.newObject());
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity2", play.libs.Json.newObject());

		// flush queues
		TimeseriesDS.flushQueue();

		assertEquals(2, ds.getItemCount());
	}

	@Test
	public void testDatasetPermissions() {
		Person owner = createPerson("Owner");
		Person collaborator = createPerson("Collab");
		Person subscriber = createPerson("Sub");
		Person other = createPerson("Other");

		Project project = Project.create("Private Project", owner, "Intro", false, false);
		project.save();

		Collaboration collab = new Collaboration(collaborator, project);
		collab.save();
		project.getCollaborators().add(collab);

		Subscription sub = new Subscription(subscriber, project);
		sub.save();
		project.getSubscribers().add(sub);
		project.update();

		Dataset ds = datasetConnector.create("Perm DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();

		// belongsTo tests
		assertTrue(ds.belongsTo(owner.getEmail()));
		assertFalse(ds.belongsTo(collaborator.getEmail()));
		assertFalse(ds.belongsTo(other.getEmail()));
		assertFalse(ds.belongsTo(null));

		// collaboratesWith tests
		assertTrue(ds.collaboratesWith(collaborator.getEmail()));
		assertFalse(ds.collaboratesWith(other.getEmail()));
		assertFalse(ds.collaboratesWith(null));

		// subscribedBy tests
		assertTrue(ds.subscribedBy(subscriber.getEmail()));
		assertFalse(ds.subscribedBy(other.getEmail()));
		assertFalse(ds.subscribedBy(null));

		// visibleFor tests (String username)
		assertTrue(ds.visibleFor(owner.getEmail()));
		assertTrue(ds.visibleFor(collaborator.getEmail()));
		assertTrue(ds.visibleFor(subscriber.getEmail()));
		assertFalse(ds.visibleFor(other.getEmail()));
		assertFalse(ds.visibleFor((String) null));

		// visibleFor tests (Person user)
		assertTrue(ds.visibleFor(owner));
		assertTrue(ds.visibleFor(collaborator));
		assertTrue(ds.visibleFor(subscriber));
		assertFalse(ds.visibleFor(other));
		assertFalse(ds.visibleFor((Person) null));

		// editableBy tests (String username)
		assertTrue(ds.editableBy(owner.getEmail()));
		assertTrue(ds.editableBy(collaborator.getEmail()));
		assertFalse(ds.editableBy(subscriber.getEmail()));
		assertFalse(ds.editableBy(other.getEmail()));
		assertFalse(ds.editableBy((String) null));

		// editableBy tests (Person user)
		assertTrue(ds.editableBy(owner));
		assertTrue(ds.editableBy(collaborator));
		assertFalse(ds.editableBy(subscriber));
		assertFalse(ds.editableBy(other));
		assertFalse(ds.editableBy((Person) null));

		// Null project safety
		Dataset detachedDs = new Dataset();
		assertFalse(detachedDs.belongsTo(owner.getEmail()));
		assertFalse(detachedDs.collaboratesWith(collaborator.getEmail()));
		assertFalse(detachedDs.subscribedBy(subscriber.getEmail()));
		assertFalse(detachedDs.visibleFor(owner.getEmail()));
		assertFalse(detachedDs.visibleFor(owner));
		assertFalse(detachedDs.editableBy(owner.getEmail()));
		assertFalse(detachedDs.editableBy(owner));
	}

	@Test
	public void testProjectIsActiveWithNullDatasetStart() {
		Person owner = createPerson("ActiveOwner");
		Project project = Project.create("Active Project", owner, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Active DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.setStart(null); // start is explicitly null
		ds.setEnd(null);
		ds.save();

		project.getDatasets().add(ds);
		project.update();

		// Should not throw or crash, and ds.isActive() is true when start & end are null
		assertTrue(project.isActive());
	}

	@Test
	public void testTimeseriesQueueTracking() {
		Person owner = createPerson("TsQueueOwner");
		Project project = Project.create("TsQueue Project", owner, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("TsQueue DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();

		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);

		// ensure clean start
		TimeseriesDS.flushQueue();
		assertEquals(0, TimeseriesDS.getQueueSize());

		// add one record
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity1", play.libs.Json.newObject());
		assertEquals(1, TimeseriesDS.getQueueSize());

		// add another record
		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", new Date(), "activity2", play.libs.Json.newObject());
		assertEquals(2, TimeseriesDS.getQueueSize());

		// flush and check size resets to 0
		TimeseriesDS.flushQueue();
		assertEquals(0, TimeseriesDS.getQueueSize());
	}

	@Test
	public void testEntityDSProjectionUpdateGuard() {
		Person owner = createPerson("EntityOwner");
		Project project = Project.create("Entity Project", owner, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Entity DS", DatasetType.ENTITY, project, "Desc", "Target", "true");
		ds.save();

		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(ds);

		// Initially, no projection
		assertFalse(ds.getConfiguration().containsKey(Dataset.DATA_PROJECTION));

		// First record: introduces fieldA and fieldB
		eds.addItem("res1", Optional.empty(), play.libs.Json.newObject().put("fieldA", 1).put("fieldB", "val1"));
		assertTrue(ds.getConfiguration().containsKey(Dataset.DATA_PROJECTION));
		String proj1 = ds.getConfiguration().get(Dataset.DATA_PROJECTION);
		assertTrue(proj1.contains("fieldA"));
		assertTrue(proj1.contains("fieldB"));

		// Second record with identical fields: should not change projection or throw
		eds.addItem("res2", Optional.empty(), play.libs.Json.newObject().put("fieldA", 2).put("fieldB", "val2"));
		assertEquals(proj1, ds.getConfiguration().get(Dataset.DATA_PROJECTION));

		// Third record introducing a new field: fieldC
		eds.addItem("res3", Optional.empty(), play.libs.Json.newObject().put("fieldA", 3).put("fieldC", true));
		String proj2 = ds.getConfiguration().get(Dataset.DATA_PROJECTION);
		assertTrue(proj2.contains("fieldA"));
		assertTrue(proj2.contains("fieldB"));
		assertTrue(proj2.contains("fieldC"));
	}

	@Test
	public void testDynamicDatasetIndexesAndMigration() throws Exception {
		Person owner = createPerson("IndexOwner");
		Project project = Project.create("Index Project", owner, "Intro", false, false);
		project.save();

		// 1. TimeseriesDS (IOT)
		Dataset tsDataset = datasetConnector.create("Idx TS", DatasetType.IOT, project, "Desc", "Target", "true");
		tsDataset.save();
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(tsDataset);
		assertTableHasIndex(tsds.getDataTableName(), "ts_idx");
		assertTableHasIndex(tsds.getDataTableName(), "dev_ts_idx");
		// Verify migration is idempotent
		tsds.migrateDatasetSchema();

		// 2. CompleteDS
		Dataset cpDataset = datasetConnector.create("Idx CP", DatasetType.COMPLETE, project, "Desc", "Target", "true");
		cpDataset.save();
		CompleteDS cpds = (CompleteDS) datasetConnector.getDatasetDS(cpDataset);
		assertTableHasIndex(cpds.getDataTableName(), "fn_idx");
		assertTableHasIndex(cpds.getDataTableName(), "ts_idx");
		cpds.migrateDatasetSchema();

		// 3. EntityDS
		Dataset enDataset = datasetConnector.create("Idx EN", DatasetType.ENTITY, project, "Desc", "Target", "true");
		enDataset.save();
		EntityDS eds = (EntityDS) datasetConnector.getDatasetDS(enDataset);
		assertTableHasIndex(eds.getDataTableName(), "resource_id");
		assertTableHasIndex(eds.getDataTableName(), "ts_idx");
		eds.migrateDatasetSchema();

		// 4. MediaDS
		Dataset mdDataset = datasetConnector.create("Idx MD", DatasetType.MEDIA, project, "Desc", "Target", "true");
		mdDataset.save();
		MediaDS mds = (MediaDS) datasetConnector.getDatasetDS(mdDataset);
		assertTableHasIndex(mds.getDataTableName(), "ts_idx");
		assertTableHasIndex(mds.getDataTableName(), "pid_ts_idx");
		mds.migrateDatasetSchema();
	}

	@Test
	public void testItemCountAndQuantityCaching() {
		Person user = createPerson("Caching");
		Project project = Project.create("Cache Test Project", user, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Cache Test DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);
		String tableName = tsds.getDataTableName();

		// Initially count is 0, this will populate the cache
		assertEquals(0, ds.getItemCount());
		assertTrue(DatasetConnector.getCache().get("table_count_" + tableName).isPresent());
		assertEquals(0L, (long) (Long) DatasetConnector.getCache().get("table_count_" + tableName).get());

		// Quantity for this week
		Date now = new Date();
		assertEquals(0, ds.getItemQuantity(now));
		long mondayTime = utils.DateUtils.startOfDay(utils.DateUtils.thisMonday(now)).getTime();
		assertTrue(DatasetConnector.getCache().get("table_qty_" + tableName + "_" + mondayTime).isPresent());

		// Invalidate cache explicitly
		ds.invalidateCache();
		assertFalse(DatasetConnector.getCache().get("table_count_" + tableName).isPresent());
		assertFalse(DatasetConnector.getCache().get("table_qty_" + tableName + "_" + mondayTime).isPresent());

		// Add records and flushQueue - should also invalidate cache and update count
		ds.getItemCount(); // re-populate cache with 0
		assertTrue(DatasetConnector.getCache().get("table_count_" + tableName).isPresent());

		tsds.internalAddRecord("device1", "pp1", "pp2", "pp3", now, "activity1", play.libs.Json.newObject());
		TimeseriesDS.flushQueue();

		// After flushQueue, cache for table_count should have been cleared
		assertFalse(DatasetConnector.getCache().get("table_count_" + tableName).isPresent());

		// Subsequent getItemCount fetches 1 and caches it
		assertEquals(1, ds.getItemCount());
		assertTrue(DatasetConnector.getCache().get("table_count_" + tableName).isPresent());
		assertEquals(1L, (long) (Long) DatasetConnector.getCache().get("table_count_" + tableName).get());
	}

	@Test
	public void testTimeseriesExportChunkBatching() {
		Person user = createPerson("ExportBatch");
		Project project = Project.create("Export Batch Project", user, "Intro", false, false);
		project.save();

		Dataset ds = datasetConnector.create("Export Batch DS", DatasetType.IOT, project, "Desc", "Target", "true");
		ds.save();
		TimeseriesDS tsds = (TimeseriesDS) datasetConnector.getDatasetDS(ds);

		// Add 600 records (spanning > 2 chunks of 256)
		Date now = new Date();
		for (int i = 0; i < 600; i++) {
			tsds.internalAddRecord("device1", "p" + i, "q" + i, "r" + i, new Date(now.getTime() + i * 1000L),
					"activity_" + i, play.libs.Json.newObject());
		}
		TimeseriesDS.flushQueue();
		assertEquals(600, ds.getItemCount());

		// 1. Raw export test
		CapturingQueue queue = new CapturingQueue();
		tsds.export(queue, Collections.emptyList(), -1, -1, -1);

		// Expected chunks: 1 header chunk + ceil(600 / 256) = 3 data chunks -> total 4 chunks
		assertEquals(4, queue.chunks.size());
		assertTrue(queue.chunks.get(0).utf8String().startsWith("id,device_id,ts"));

		// Count total data lines across all chunks
		StringBuilder allExported = new StringBuilder();
		for (ByteString chunk : queue.chunks) {
			allExported.append(chunk.utf8String());
		}
		String[] lines = allExported.toString().split("\n");
		// 1 header line + 600 data lines = 601 lines
		assertEquals(601, lines.length);

		// 2. Projected export test
		ds.getConfiguration().put(Dataset.DATA_PROJECTION, "field1,field2");
		ds.save();

		CapturingQueue projQueue = new CapturingQueue();
		tsds.exportProjected(projQueue, Collections.emptyList(), -1, -1, -1);
		assertEquals(4, projQueue.chunks.size());
		assertTrue(projQueue.chunks.get(0).utf8String().startsWith("id,device_id,ts,activity,pp1,pp2,pp3,field1,field2"));

		StringBuilder allProjExported = new StringBuilder();
		for (ByteString chunk : projQueue.chunks) {
			allProjExported.append(chunk.utf8String());
		}
		String[] projLines = allProjExported.toString().split("\n");
		assertEquals(601, projLines.length);
	}

	private static class CapturingQueue implements SourceQueueWithComplete<ByteString> {
		final List<ByteString> chunks = new ArrayList<>();
		boolean completed = false;

		@Override
		public CompletionStage<QueueOfferResult> offer(ByteString elem) {
			chunks.add(elem);
			return CompletableFuture.completedFuture(QueueOfferResult.enqueued());
		}

		@Override
		public void complete() {
			completed = true;
		}

		@Override
		public void fail(Throwable ex) {
		}

		@Override
		public CompletionStage<Done> watchCompletion() {
			return CompletableFuture.completedFuture(Done.done());
		}
	}

	private void assertTableHasIndex(String tableName, String expectedSubstring) throws Exception {
		try (Connection connection = DB.getDefault().dataSource().getConnection()) {
			DatabaseMetaData meta = connection.getMetaData();
			try (ResultSet rs = meta.getIndexInfo(null, null, tableName.toUpperCase(), false, false)) {
				List<String> indexes = new ArrayList<>();
				while (rs.next()) {
					String indexName = rs.getString("INDEX_NAME");
					if (indexName != null) {
						indexes.add(indexName.toLowerCase());
					}
				}
				assertTrue("Table " + tableName + " should contain index with '" + expectedSubstring
						+ "', but had: " + indexes,
						indexes.stream().anyMatch(i -> i.contains(expectedSubstring.toLowerCase())));
			}
		}
	}
}

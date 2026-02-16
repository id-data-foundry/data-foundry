package services.search;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.pekko.actor.ActorSystem;

import models.Person;
import models.Project;
import play.Logger;
import scala.concurrent.ExecutionContext;
import services.inlets.ScheduledService;

@Singleton
public class SearchService implements ScheduledService {

	protected final ActorSystem actorSystem;
	protected final ExecutionContext executionContext;

	protected Analyzer analyzer;
	protected Directory index;
	protected AtomicInteger pings = new AtomicInteger(1);

	protected long lastIndexed = 0L;

	private static final Logger.ALogger logger = Logger.of(SearchService.class);

	@Inject
	public SearchService(ActorSystem actorSystem, ExecutionContext ec) {
		this.actorSystem = actorSystem;
		this.executionContext = ec;
	}

	/**
	 * trigger indexing of projects from scheduled action
	 * 
	 */
	@Override
	public void refresh() {
		internalIndexProjects();
	}

	/**
	 * increment a counter to signal that an indexing run is needed eventually
	 * 
	 */
	public void ping() {
		pings.incrementAndGet();
	}

	/**
	 * trigger delayed indexing of project from controller, e.g., to index a recently added or updated project
	 * 
	 */
	public void indexProjects() {
		actorSystem.scheduler().scheduleOnce(Duration.ofSeconds(2), () -> {
			internalIndexProjects();
		}, executionContext);
	}

	/**
	 * internal runner for the project indexing; will only index if the last indexing is more than half a minute ago
	 * 
	 */
	public void internalIndexProjects() {

		// only index if the last triggered indexing was more than 60 seconds ago
		if (lastIndexed + (30 * 1000) >= System.currentTimeMillis()) {
			return;
		}

		lastIndexed = System.currentTimeMillis();

		Analyzer sa = new EnglishAnalyzer();
		Directory newIndex = new ByteBuffersDirectory();

		IndexWriterConfig config = new IndexWriterConfig(sa);
		try (final IndexWriter w = new IndexWriter(newIndex, config);) {
			final AtomicInteger ai = new AtomicInteger();
			Project.find.all().forEach(project -> {

				Document doc = new Document();
				doc.add(new StoredField("id", project.getId()));

				// filter out projects that are private or archived
				if (project.isPublicProject() && !project.isArchivedProject() && !project.isFrozen()) {
					doc.add(new TextField("title", nss(project.getName()), Field.Store.NO));
					doc.add(new TextField("abstract", nss(project.getIntro()), Field.Store.NO));
					doc.add(new TextField("description", nss(project.getDescription()), Field.Store.NO));
					doc.add(new TextField("keywords", nss(project.getKeywords()).replace(";", " "), Field.Store.NO));
					doc.add(new TextField("organization", nss(project.getOrganization()), Field.Store.NO));
					doc.add(new TextField("remarks", nss(project.getRemarks()), Field.Store.NO));

					// index the owner and owner profile
					Person owner = project.getOwner();
					owner.refresh();
					doc.add(new TextField("owner", nss(owner.getName().replace(".", " ")), Field.Store.NO));
				}

				try {
					w.addDocument(doc);
					ai.incrementAndGet();
				} catch (IOException e) {
					logger.error("Project " + project.getId() + " could not be indexed for full-text search.", e);
				}
			});
			logger.info("Indexed " + ai.get() + " projects for full-text search in "
			        + (System.currentTimeMillis() - lastIndexed) + "ms.");
		} catch (IOException e) {
			logger.error("Search index refresh problem", e);
		}

		// replace analyzer and directory
		this.analyzer = sa;
		this.index = newIndex;
	}

	/**
	 * search for query in pre-built index and return up to 10 items
	 * 
	 * @param queryString
	 * @return
	 */
	public List<Project> search(String queryString) {
		return search(queryString, 10);
	}

	/**
	 * search for query in pre-built index and return up to <code>numHits</code> items
	 * 
	 * @param queryString
	 * @param numHits
	 * @return
	 */
	public List<Project> search(String queryString, int numHits) {

		// abort search if the index is incomplete
		if (analyzer == null || index == null) {
			return Collections.emptyList();
		}

		// only search for non-empty queries
		if (queryString.isEmpty()) {
			return Collections.emptyList();
		}

		// sanitize queryString
		queryString = QueryParserBase.escape(queryString);

		// allow for implicit OR connection in query; add wildcards for individual search terms
		if (queryString.contains(" ")) {
			queryString = Arrays.stream(queryString.split(" ")).map(s -> s.trim() + "*")
			        .collect(Collectors.joining(" OR "));
		}

		Query query = null;
		try {
			String[] fields = { "title", "abstract", "description", "keywords", "organization", "remarks", "owner", };
			query = new MultiFieldQueryParser(fields, analyzer).parse(queryString);
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			return Collections.emptyList();
		}

		return performSearch(query, numHits);
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * internal count is performed here on given query
	 * 
	 * @param query
	 * @param numHits
	 * @return
	 */
	protected int performCount(Query query, int numHits) {
		try (IndexReader reader = DirectoryReader.open(index);) {
			IndexSearcher searcher = new IndexSearcher(reader);
			TopScoreDocCollector collector = TopScoreDocCollector.create(numHits, numHits * 3);
			searcher.search(query, collector);
			ScoreDoc[] hits = collector.topDocs().scoreDocs;
			return hits.length;
		} catch (IOException | NullPointerException e) {
			// do nothing, this will always work because it's in memory
		}

		return 0;
	}

	/**
	 * internal search is performed here on given query; returns list of projects
	 * 
	 * @param query
	 * @param numHits
	 * @return
	 */
	protected List<Project> performSearch(Query query, int numHits) {
		List<Long> projectIds = performIDSearch(query, numHits);
		return Project.find.query().where().in("id", projectIds).findList();
	}

	/**
	 * internal search is performed here on given query; returns just IDs
	 * 
	 * @param query
	 * @param numHits
	 * @return
	 */
	protected List<Long> performIDSearch(Query query, int numHits) {
		List<Long> projectIds = new LinkedList<>();
		try (IndexReader reader = DirectoryReader.open(index);) {
			IndexSearcher searcher = new IndexSearcher(reader);
			TopScoreDocCollector collector = TopScoreDocCollector.create(numHits, numHits * 3);
			searcher.search(query, collector);
			ScoreDoc[] hits = collector.topDocs().scoreDocs;
			Arrays.stream(hits).limit(numHits).map(h -> h.doc).forEach(docId -> {
				try {
					Document d = searcher.doc(docId);
					projectIds.add(d.getField("id").numericValue().longValue());
				} catch (IOException e) {
				}
			});
		} catch (IOException | NullPointerException e) {
			// do nothing, this will always work because it's in memory
		}
		return projectIds;
	}

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * search for project similar to the given project
	 * 
	 * @param project
	 * @param numHits
	 * @return
	 */
	public List<Project> moreLikeThis(Project project, int numHits) {
		return performMoreLikeThisSearch(project.getId(), numHits);
	}

	/**
	 * internal search is performed here
	 * 
	 * @param query
	 * @param numHits
	 * @return
	 */
	protected List<Project> performMoreLikeThisSearch(long id, int numHits) {
		List<Long> projectIds = new LinkedList<>();
		try (IndexReader reader = DirectoryReader.open(index);) {
			IndexSearcher searcher = new IndexSearcher(reader);
			TopScoreDocCollector collector = TopScoreDocCollector.create(numHits, numHits * 3);

			// find similar documents
			MoreLikeThis mlt = new MoreLikeThis(reader);
			mlt.setAnalyzer(analyzer);
			mlt.setFieldNames(null);

			Query query = mlt.like((int) (id - 1));
			searcher.search(query, collector);
			ScoreDoc[] hits = collector.topDocs().scoreDocs;
			for (int i = 0; i < hits.length; ++i) {
				int docId = hits[i].doc;
				Document d = searcher.doc(docId);
				long similarProjectId = d.getField("id").numericValue().longValue();

				// only add if different from given project id
				if (id != similarProjectId) {
					projectIds.add(similarProjectId);
				}
			}
		} catch (Exception e) {
			// do nothing, this will always work because it's in memory
		}

		return Project.find.query().where().in("id", projectIds).findList();
	}

	@Override
	public void stop() {
		// do nothing
	}
}

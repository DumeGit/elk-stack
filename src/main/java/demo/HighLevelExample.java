package demo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.elasticsearch.client.RestClient;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public class HighLevelExample {

  private static final String INDEX = "events";
  private static final String USER = "elastic";
  private static final String PASS = "changeme";
  private static final Logger LOG = LogManager.getLogger(HighLevelExample.class);

  // How often to generate events (milliseconds)
  private static final int DELAY_MS = 5000;

  /**
   * Sets up Log4j context data that appears in every log message.
   * This matches the pattern in log4j2.xml
   */
  private static void setupLogContext() {
    ThreadContext.put("message_id", generateMessageId());
    ThreadContext.put("uuid", UUID.randomUUID().toString().replace("-", ""));
    ThreadContext.put("app_name", "event-service");
    ThreadContext.put("app_version", "1.0.0-SNAPSHOT");

    try {
      ThreadContext.put("hostname", InetAddress.getLocalHost().getHostName());
    } catch (Exception e) {
      ThreadContext.put("hostname", "unknown");
    }

    String pidAtHost = ManagementFactory.getRuntimeMXBean().getName();
    ThreadContext.put("process_id", pidAtHost.split("@")[0]);
  }

  /**
   * Generates a simple message ID with timestamp
   */
  private static String generateMessageId() {
    return String.format("MSG-%d", System.currentTimeMillis());
  }

  public static void main(String[] args) throws Exception {
    setupLogContext();
    LOG.info("Event service starting up");

    ElasticsearchClient es = createElasticsearchClient();
    LOG.info("Connected to Elasticsearch");

    // Create index if it doesn't exist
    createIndexIfNeeded(es);

    // Sample events to index
    List<ElkEvent> sampleEvents = createSampleEvents();

    // Main loop - index events and run queries
    int iteration = 0;
    while (true) {
      iteration++;

      // Reset log context for each iteration
      setupLogContext();
      LOG.info("Starting iteration {}", iteration);

      try {
        // Index the sample events
        indexEvents(es, sampleEvents);
        LOG.info("Indexed {} events successfully", sampleEvents.size());

        runSampleQueries(es);

        // Delete specific events
        deleteEventsByTitle(es, "Ansible Automation Workshop");

      } catch (Exception e) {
        LOG.error("Error during iteration {}: {}", iteration, e.getMessage(), e);
      }

      LOG.info("Iteration {} complete, sleeping for {} seconds",
          iteration, DELAY_MS / 1000);
      Thread.sleep(DELAY_MS);
      ThreadContext.clearAll();
    }
  }

  /**
   * Creates the Elasticsearch client with authentication
   */
  private static ElasticsearchClient createElasticsearchClient() {
    BasicCredentialsProvider credProvider = new BasicCredentialsProvider();
    credProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(USER, PASS));

    RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200, "http"))
        .setHttpClientConfigCallback(httpClient ->
            httpClient.setDefaultCredentialsProvider(credProvider))
        .build();

    ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    ElasticsearchTransport transport =
        new RestClientTransport(restClient, new JacksonJsonpMapper(mapper));

    return new ElasticsearchClient(transport);
  }

  /**
   * Creates the events index with proper field mappings
   */
  private static void createIndexIfNeeded(ElasticsearchClient es) throws Exception {
    boolean exists = es.indices().exists(e -> e.index(INDEX)).value();

    if (!exists) {
      LOG.info("Creating index '{}'", INDEX);
      es.indices().create(c -> c
          .index(INDEX)
          .mappings(m -> m
              .properties("title", p -> p.text(t -> t))
              .properties("eventType", p -> p.keyword(k -> k))
              .properties("datetime", p -> p.date(d -> d))
              .properties("place", p -> p.text(t -> t))
              .properties("description", p -> p.text(t -> t))
              .properties("subTopics", p -> p.keyword(k -> k))
          )
      );
      LOG.info("Index '{}' created successfully", INDEX);
    } else {
      LOG.debug("Index '{}' already exists", INDEX);
    }
  }

  /**
   * Creates sample events for testing
   */
  private static List<ElkEvent> createSampleEvents() {
    return List.of(
        createEvent("Building Scalable Micro-services with Spring Boot",
            EventType.WORKSHOP, "2024-07-05T13:00:00Z"),
        createEvent("Distributed Systems 101",
                EventType.TECH_TALK, "2024-06-12T17:30:00Z"),
        createEvent("Ansible Automation Workshop",
                EventType.WORKSHOP, "2024-05-30T09:00:00Z"),
        createEvent("Observability for Kubernetes",
                EventType.TECH_TALK, "2024-05-18T15:00:00Z"),
        createEvent("Data Engineering Bootcamp",
                EventType.WORKSHOP, "2024-08-20T08:30:00Z")
    );
  }

  /**
   * Helper to create a single event
   */
  private static ElkEvent createEvent(String title, EventType type, String datetime) {
      return new ElkEvent(UUID.randomUUID().toString(), title, type, Instant.parse(datetime), "TBA", "Sample event for demo", List.of("technology", "learning"));
  }

  /**
   * Bulk indexes events into Elasticsearch
   */
  private static void indexEvents(ElasticsearchClient es, List<ElkEvent> events)
      throws Exception {
    BulkRequest.Builder bulk = new BulkRequest.Builder();

    for (ElkEvent event : events) {
      String id = UUID.randomUUID().toString();
      bulk.operations(op -> op
          .index(idx -> idx
              .index(INDEX)
              .id(id)
              .document(event)
          )
      );
    }

    es.bulk(bulk.build());
  }

  /**
   * Runs sample queries and logs results
   */
  private static void runSampleQueries(ElasticsearchClient es) throws Exception {
    LOG.info("Running sample queries");

    // Query 1: Get all events
    runQuery(es, "All events",
        q -> q.matchAll(m -> m));

    // Query 2: Get workshops only
    runQuery(es, "Workshops only",
        q -> q.term(t -> t
            .field("eventType")
            .value(v -> v.stringValue("workshop"))
        ));

    // Query 3: Find specific event by title
    runQuery(es, "Specific event: Distributed Systems 101",
        q -> q.term(t -> t
            .field("title.keyword")
            .value(v -> v.stringValue("Distributed Systems 101"))
        ));

    // Query 4: Complex query with date range
    runQuery(es, "Data Engineering events after June 2024",
        q -> q.bool(b -> b
            .must(m -> m.term(t -> t
                .field("title.keyword")
                .value(v -> v.stringValue("Data Engineering Bootcamp"))
            ))
            .must(m -> m.range(r -> r
                .date(d -> d
                    .field("datetime")
                    .gt(JsonData.of("2024-06-01").toString())
                )
            ))
        ));
  }

  /**
   * Executes a query and logs results
   */
  private static void runQuery(ElasticsearchClient es, String description,
      Function<Query.Builder, ObjectBuilder<Query>> queryBuilder) throws Exception {

    SearchResponse<ElkEvent> response = es.search(s -> s
            .index(INDEX)
            .query(queryBuilder),
        ElkEvent.class
    );

    int hitCount = response.hits().hits().size();
    LOG.info("Query '{}' returned {} results", description, hitCount);

    // Log first few results
    response.hits().hits().stream()
        .limit(3)
        .forEach(hit -> {
          ElkEvent event = hit.source();
          LOG.debug("  - {} ({})", event.title(), event.eventType());
        });
  }

  /**
   * Deletes all events with a specific title
   */
  private static void deleteEventsByTitle(ElasticsearchClient es, String title)
      throws Exception {
    LOG.info("Attempting to delete events with title: {}", title);

    // Find events to delete
    SearchResponse<ElkEvent> searchResult = es.search(s -> s
            .index(INDEX)
            .query(q -> q.term(t -> t
                .field("title.keyword")
                .value(v -> v.stringValue(title))
            )),
        ElkEvent.class
    );

    List<Hit<ElkEvent>> hits = searchResult.hits().hits();

    if (hits.isEmpty()) {
      LOG.info("No events found with title: {}", title);
      return;
    }

    // Build bulk delete request
    BulkRequest.Builder bulkDelete = new BulkRequest.Builder();
    for (Hit<ElkEvent> hit : hits) {
      bulkDelete.operations(op -> op
          .delete(d -> d
              .index(INDEX)
              .id(hit.id())
          )
      );
    }

    es.bulk(bulkDelete.build());
    LOG.info("Deleted {} events with title: {}", hits.size(), title);
  }
}
package demo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

@Service
public class EventService {

    private static final String INDEX = "events";
    private final ElasticsearchClient es;

    public EventService() throws IOException {
        BasicCredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "changeme"));
        RestClient rest = RestClient.builder(new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(h -> h.setDefaultCredentialsProvider(cp))
                .build();

        ObjectMapper om = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);   // ISO-8601

        es = new ElasticsearchClient(
                new co.elastic.clients.transport.rest_client.RestClientTransport(
                        rest, new JacksonJsonpMapper(om)));

        if (!es.indices().exists(e -> e.index(INDEX)).value()) {
            es.indices().create(c -> c.index(INDEX)
                    .mappings(m -> m
                            .properties("title",      p -> p.text(t -> t))
                            .properties("eventType",  p -> p.keyword(k -> k))
                            .properties("datetime",   p -> p.date(d -> d))
                            .properties("place",      p -> p.text(t -> t))
                            .properties("description",p -> p.text(t -> t))
                            .properties("subTopics",  p -> p.keyword(k -> k))));
        }
    }


    public String store(ElkEvent ev) throws IOException {
        String id = ev.id() == null ? UUID.randomUUID().toString() : ev.id();
        es.index(i -> i.index(INDEX).id(id).document(ev));
        return id;
    }

    public ElkEvent get(String id) throws IOException {
        GetResponse<ElkEvent> rsp = es.get(g -> g.index(INDEX).id(id), ElkEvent.class);
        return rsp.found() ? rsp.source() : null;
    }

    public void update(String id, ElkEvent ev) throws IOException {
        es.update(u -> u.index(INDEX).id(id).doc(ev), ElkEvent.class);
    }

    public void delete(String id) throws IOException {
        es.delete(d -> d.index(INDEX).id(id));
    }


    public List<ElkEvent> all() throws IOException {
        return search(q -> q.matchAll(m -> m));
    }

    public List<ElkEvent> workshops() throws IOException {
        return search(q -> q.term(t -> t.field("eventType").value(v -> v.stringValue("WORKSHOP"))));
    }

    public List<ElkEvent> byTitle(String title) throws IOException {
        return search(q -> q.match(m -> m
                .field("title")
                .query(title)));
    }

    public List<ElkEvent> afterDateWithTitle(String iso, String title) throws IOException {
        return search(q -> q.bool(b -> b
                .must(m -> m.match(t -> t.field("title").query(title)))
                .must(m -> m.range(r -> r.date(d -> d.field("datetime").gt(JsonData.of(iso).toString()))))));
    }

    private List<ElkEvent> search(Function<Query.Builder, ObjectBuilder<Query>> queryBuilder) throws IOException {
        SearchResponse<ElkEvent> response = es.search(s -> s
                        .index(INDEX)
                        .query(queryBuilder),
                ElkEvent.class
        );
        return response.hits().hits().stream().map(Hit::source).toList();
    }

    public void bulkInit() throws IOException {
        BulkRequest.Builder b = new BulkRequest.Builder();

        b.operations(op -> op.index(idx -> idx.index(INDEX).id("1").document(
                new ElkEvent("1",
                        "Building Scalable Micro-services with Spring Boot",
                        EventType.WORKSHOP,
                        Instant.parse("2024-07-05T13:00:00Z"),
                        "Tech-Hub – Room A, Berlin",
                        "Hands-on Spring Boot workshop",
                        List.of("DDD","API Gateway","Observability","CI/CD")))));

        b.operations(op -> op.index(idx -> idx.index(INDEX).id("2").document(
                new ElkEvent("2",
                        "Distributed Systems 101",
                        EventType.TECH_TALK,
                        Instant.parse("2024-06-12T17:30:00Z"),
                        "Auditorium 2, Dublin",
                        "Intro to consistency models, CAP, etc.",
                        List.of("CAP theorem","Gossip","Consensus")))));

        b.operations(op -> op.index(idx -> idx.index(INDEX).id("3").document(
                new ElkEvent("3",
                        "Ansible Automation Workshop",
                        EventType.WORKSHOP,
                        Instant.parse("2024-05-30T09:00:00Z"),
                        "Lab 1, London",
                        "Hands-on with Ansible playbooks",
                        List.of("YAML","Idempotence","Role reuse")))));

        b.operations(op -> op.index(idx -> idx.index(INDEX).id("4").document(
                new ElkEvent("4",
                        "Observability for Kubernetes",
                        EventType.TECH_TALK,
                        Instant.parse("2024-05-18T15:00:00Z"),
                        "Hall C, Paris",
                        "Logging, metrics, traces",
                        List.of("Prometheus","OpenTelemetry","Jaeger")))));

        b.operations(op -> op.index(idx -> idx.index(INDEX).id("5").document(
                new ElkEvent("5",
                        "Data Engineering Bootcamp",
                        EventType.WORKSHOP,
                        Instant.parse("2024-08-20T08:30:00Z"),
                        "Campus West, Zurich",
                        "From raw data to pipelines",
                        List.of("Airflow","Spark","DeltaLake")))));

        es.bulk(b.build());
    }

    public void createIndex() throws IOException {
        if (es.indices().exists(e -> e.index(INDEX)).value()) {
            es.indices().delete(d -> d.index(INDEX));
        }
        es.indices().create(c -> c.index(INDEX)
                .mappings(m -> m
                        .properties("title",      p -> p.text(t -> t))
                        .properties("eventType",  p -> p.keyword(k -> k))
                        .properties("datetime",   p -> p.date(d -> d))
                        .properties("place",      p -> p.text(t -> t))
                        .properties("description",p -> p.text(t -> t))
                        .properties("subTopics",  p -> p.keyword(k -> k))));
    }

    public GetIndexResponse getIndex() throws IOException {
        return es.indices().get(i -> i.index(INDEX));
    }

}
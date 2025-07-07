package demo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

@RestController
public class EventController {

    private static final Logger LOG = LogManager.getLogger(EventController.class);
    private final EventService svc;

    public EventController(EventService svc) {
        this.svc = svc;
    }

    private static void mdc() {
        ThreadContext.clearAll();
        ThreadContext.put("message_id", "MSG-" + System.currentTimeMillis());
        ThreadContext.put("uuid", UUID.randomUUID().toString().replace("-", ""));
        ThreadContext.put("app_name", "event-service");
        ThreadContext.put("app_version", "1.0.0-SNAPSHOT");
        try {
            ThreadContext.put("hostname", InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
            ThreadContext.put("hostname", "unknown");
        }
        ThreadContext.put("process_id",
                ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
    }


    @PostMapping("/bulk-init")
    public ResponseEntity<Void> bulkInit() throws Exception {
        mdc();
        svc.bulkInit();
        LOG.info("Bulk initialised 5 events");
        return ResponseEntity.ok().build();
    }

    @PostMapping("/store")
    public ResponseEntity<String> store(@RequestBody ElkEvent ev) throws Exception {
        mdc();
        String id = svc.store(ev);
        LOG.info("Stored event {}", id);
        return ResponseEntity.ok(id);
    }

    @GetMapping("/get/{id}")
    public ResponseEntity<ElkEvent> get(@PathVariable String id) throws Exception {
        mdc();
        ElkEvent ev = svc.get(id);
        LOG.info("Get event {}", id);
        return ev == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(ev);
    }

    @PostMapping("/update/{id}")
    public ResponseEntity<Void> update(@PathVariable String id,
                                       @RequestBody ElkEvent body) throws Exception {
        mdc();
        svc.update(id, body);
        LOG.info("Updated event {}", id);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<Void> delete(@PathVariable String id) throws Exception {
        mdc();
        svc.delete(id);
        LOG.info("Deleted event {}", id);
        return ResponseEntity.ok().build();
    }


    @GetMapping("/query/all")
    public List<ElkEvent> all() throws Exception {
        mdc();
        return svc.all();
    }

    @GetMapping("/query/workshops")
    public List<ElkEvent> workshops() throws Exception {
        mdc();
        return svc.workshops();
    }

    @GetMapping("/query/title/{title}")
    public List<ElkEvent> byTitle(@PathVariable String title) throws Exception {
        mdc();
        return svc.byTitle(title);
    }

    @GetMapping("/query/after/{date}/{title}")
    public List<ElkEvent> after(@PathVariable String title, @PathVariable String date) throws Exception {
        mdc();
        return svc.afterDateWithTitle(date, title);
    }

    @PostMapping("/create-index")
    public ResponseEntity<Void> createIndex() throws Exception {
        mdc();
        svc.createIndex();
        LOG.info("Index '{}' created / reset", "events");
        return ResponseEntity.ok().build();
    }

    @GetMapping("/index")
    public String indexInfo() throws Exception {
        mdc();
        LOG.info("Get index {}", "events");
        return svc.getIndex().indices().get("events").toString();
    }
}
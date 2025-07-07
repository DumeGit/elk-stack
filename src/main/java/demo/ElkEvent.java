package demo;

import java.time.Instant;
import java.util.List;

public record ElkEvent(
        String            id,
        String            title,
        EventType         eventType,
        Instant           datetime,
        String            place,
        String            description,
        List<String>      subTopics
) { }

enum EventType { WORKSHOP, TECH_TALK }
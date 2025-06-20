package demo;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAlias;

public class ElkEvent {
  public String           title;
  @JsonAlias("event_type")
  public String           eventType;
  public Instant          datetime;
  public String           place;
  public String           description;
  @JsonAlias("sub_topics")
  public List<String>     subTopics;
}
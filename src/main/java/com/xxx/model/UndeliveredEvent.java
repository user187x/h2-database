package com.xxx.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import com.google.gson.JsonObject;

public class UndeliveredEvent {

  private String id = UUID.randomUUID().toString();
  private String nodeId;
  private String eventId;
  private Timestamp created = new Timestamp(new Date().getTime());

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public String getEventId() {
    return eventId;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }

  public Timestamp getCreated() {
    return created;
  }

  public void setCreated(Timestamp created) {
    this.created = created;
  }
  
  public void setCreated() {
    this.created = new Timestamp(new Date().getTime());
  }

  public JsonObject toJson() {
    
    JsonObject json = new JsonObject();
    
    json.addProperty("id", id);
    json.addProperty("nodeId", nodeId);
    json.addProperty("eventId", eventId);
    json.addProperty("created", created.toString());
    
    return json;
  }
}

package com.xxx.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import com.google.gson.JsonObject;

public class Node {

  private String id = UUID.randomUUID().toString();
  private boolean healthy = true;
  private Timestamp lastSeen = new Timestamp(new Date().getTime());
  private Timestamp created = new Timestamp(new Date().getTime());

  public Node() {}
  
  public Node(String id) {
    this.id = id;
  }
  
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public boolean isHealthy() {
    return healthy;
  }

  public void setHealthy(boolean healthy) {
    this.healthy = healthy;
  }

  public Timestamp getLastSeen() {
    return lastSeen;
  }

  public void setLastSeen(Timestamp lastSeen) {
    this.lastSeen = lastSeen;
  }

  public Timestamp getCreated() {
    return created;
  }

  public void setCreated(Timestamp created) {
    this.created = created;
  }

  public JsonObject toJson() {
    
    JsonObject json = new JsonObject();
    
    json.addProperty("id", id);
    json.addProperty("healthy", healthy);
    json.addProperty("lastSeen", lastSeen.toString());
    json.addProperty("created", created.toString());
    
    return json;
  }
}

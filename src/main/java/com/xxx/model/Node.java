package com.xxx.model;

import java.sql.Timestamp;
import java.util.UUID;

public class Node {

  private String id = UUID.randomUUID().toString();
  private boolean healthy;
  private Timestamp lastSeen;
  private Timestamp created;

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
}

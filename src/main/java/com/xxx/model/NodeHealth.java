package com.xxx.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class NodeHealth {

  private String id = UUID.randomUUID().toString();
  private String nodeId;
  private boolean healthy = true;
  private int attemptCount = 0;
  private Timestamp lastSeen = null;
  private Timestamp lastAttempt = null;
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

  public boolean isHealthy() {
    return healthy;
  }

  public void setHealthy(boolean healthy) {
    this.healthy = healthy;
  }

  public int getAttemptCount() {
    return attemptCount;
  }
  
  public void incrementAttemptCount() {
    this.attemptCount++;
  }
  
  public void resetAttemptCount() {
    this.attemptCount = 0;
  }

  public void setAttemptCount(int attemptCount) {
    this.attemptCount = attemptCount;
  }

  public Timestamp getLastSeen() {
    return lastSeen;
  }
  
  public void setLastSeen() {
    this.lastSeen = new Timestamp(new Date().getTime());
  }

  public Timestamp getLastAttempt() {
    return lastAttempt;
  }
  
  public void setLastAttempt() {
    this.lastAttempt = new Timestamp(new Date().getTime());
  }

  
  public Timestamp getCreated() {
    return created;
  }

  public void setCreated(Timestamp created) {
    this.created = created;
  }
  
  public void incrementAttempts() {
    this.attemptCount++;
  }
}

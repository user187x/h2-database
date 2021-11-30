package com.xxx.model;

import java.sql.Timestamp;
import java.util.UUID;

public class Subscription {

  private String id = UUID.randomUUID().toString();
  private String topic;
  private Timestamp created;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public Timestamp getCreated() {
    return created;
  }

  public void setCreated(Timestamp created) {
    this.created = created;
  }
}

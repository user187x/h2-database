package com.xxx.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class Subscription {

  private String id = UUID.randomUUID().toString();
  private String topic;
  private Timestamp created = new Timestamp(new Date().getTime());

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

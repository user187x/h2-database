package com.xxx.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import com.google.gson.JsonObject;

public class Event {

  private String id = UUID.randomUUID().toString();
  private String subscriptionId;
  private String message;
  private Timestamp created = new Timestamp(new Date().getTime());

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getSubscriptionId() {
    return subscriptionId;
  }

  public void setSubscriptionId(String subscriptionId) {
    this.subscriptionId = subscriptionId;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Timestamp getCreated() {
    return created;
  }

  public void setCreated(Timestamp created) {
    this.created = created;
  }

  @Override
  public String toString() {
    
    StringBuilder builder = new StringBuilder();
    
    builder.append("Event [id=")
    .append(id)
    .append(", subscriptionId=")
    .append(subscriptionId)
    .append(", message=")
    .append(message)
    .append(", created=")
    .append(created)
    .append("]");
    
    return builder.toString();
  }
  
  public JsonObject toJson() {
    
    JsonObject json = new JsonObject();
    
    json.addProperty("id", id);
    json.addProperty("subscriptionId", subscriptionId);
    json.addProperty("message", message);
    json.addProperty("created", created.toString());
    
    return json;
  }
}

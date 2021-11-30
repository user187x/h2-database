package com.xxx.model;

import java.util.UUID;

public class Event {

  private String id = UUID.randomUUID().toString();
  private String message;

  public String getId() {
    return id;
  }

  public String getMessage() {
    return message;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setMessage(String message) {
    this.message = message;
  }
  
  @Override
  public String toString() {
    
    StringBuilder builder = new StringBuilder();
    
    builder
    .append("Event [id=")
    .append(id)
    .append(", message=")
    .append(message)
    .append("]");
    
    return builder.toString();
  }
}

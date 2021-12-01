package com.xxx.service;

public enum Tables {

  EVENTS("Events"),
  NODES("Nodes"),
  NODE_SUBSCRIPTIONS("NodeSubscriptions"),
  SUBSCRIPTIONS("Subscriptions"),
  UNDELIVERED_EVENTS("UndeliveredEvents");
  
  private String actual;
  
  Tables(String actual) {
    this.actual = actual;
  }
  
  public String getActual() {
    return actual;
  }
}

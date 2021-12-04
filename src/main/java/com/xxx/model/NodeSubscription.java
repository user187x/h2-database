package com.xxx.model;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class NodeSubscription {

  private String id = UUID.randomUUID().toString();
  private String subscriptionId;
  private String nodeId;
  private Timestamp created = new Timestamp(new Date().getTime());

  public NodeSubscription() {}
  
  public NodeSubscription(String nodeId, String subscriptionId) {
    this.nodeId = nodeId;
    this.subscriptionId = subscriptionId;
  }
  
  public NodeSubscription(Node node, Subscription subscription) {
    this.nodeId = node.getId();
    this.subscriptionId = subscription.getId();
  }
  
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

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public Timestamp getCreated() {
    return created;
  }

  public void setCreated(Timestamp created) {
    this.created = created;
  }
}

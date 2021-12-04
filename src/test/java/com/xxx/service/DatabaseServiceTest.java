package com.xxx.service;

import java.sql.SQLException;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.xxx.model.Event;
import com.xxx.model.Node;
import com.xxx.model.NodeSubscription;
import com.xxx.model.Subscription;
import com.xxx.model.UndeliveredEvent;

public class DatabaseServiceTest {

  private DatabaseService databaseService;
  
  private String nodeId = "12345";
  private String message = "Hello World";
  private String topic = "beatles";
  
  @Before
  public void setUp() {
    
    databaseService = new DatabaseService();
    
    Subscription subscription = new Subscription();
    subscription.setTopic(topic);
    databaseService.saveSubscription(subscription);
    
    Event event = new Event();
    event.setMessage(message);
    event.setSubscriptionId(subscription.getId());
    databaseService.saveEvent(event);
    
    Node node = new Node();
    node.setId(nodeId);
    node.setName("xxx");
    databaseService.saveNode(node);
    
    NodeSubscription nodeSubscription = new NodeSubscription();
    nodeSubscription.setNodeId(node.getId());
    nodeSubscription.setSubscriptionId(subscription.getId());
    databaseService.saveNodeSubscription(nodeSubscription);
    
    UndeliveredEvent undeliveredEvent = new UndeliveredEvent();
    undeliveredEvent.setEventId(event.getId());
    undeliveredEvent.setNodeId(node.getId());
    databaseService.saveUndeliveredEvent(undeliveredEvent);
  }
  
  @After
  public void cleanUp() throws SQLException {
    
    //DEBUG GUI
    //Server.startWebServer(databaseService.getConnection());
  }
  
  @Test
  public void testEndToEnd() {
    
    Node node = databaseService.getNodes().iterator().next();
    Assert.assertEquals(nodeId, node.getId());
    
    List<Event> undeliveredEvents = databaseService.getUndeliveredEvents(node);
    Event event = undeliveredEvents.iterator().next();
    Assert.assertEquals(message, event.getMessage());
    
    List<Subscription> subscriptions = databaseService.getSubscriptions(node);
    Assert.assertEquals(1, subscriptions.size());
    
    Subscription subscription = subscriptions.iterator().next();
    Assert.assertEquals(topic, subscription.getTopic());
    
    Subscription newSubscription = new Subscription();
    newSubscription.setTopic("dogs");
    
    databaseService.addSubscription(node, newSubscription);
    subscriptions = databaseService.getSubscriptions(node);
    Assert.assertEquals(2, subscriptions.size());
  }
}

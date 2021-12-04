package com.xxx.service;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
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
  private String message = "Mic check";
  private String topic = "john-lennon";
  private String channel = "bbc";
  
  private String subscriptionId = "beatles";
  
  @Before
  public void setUp() {
    
    databaseService = new DatabaseService();
    databaseService.initialize();
    
    Subscription subscription = new Subscription();
    subscription.setId(subscriptionId);
    subscription.setTopic(topic);
    subscription.setChannel(channel);
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
  public void entToEndTest() {
    
    Node node = databaseService.getNodes().iterator().next();
    Assert.assertEquals(nodeId, node.getId());
    
    List<Event> undeliveredEvents = databaseService.getUndeliveredEvents(node);
    Event event = undeliveredEvents.iterator().next();
    Assert.assertEquals(message, event.getMessage());
    
    List<Subscription> subscriptions = databaseService.getSubscriptions(node);
    Assert.assertEquals(1, subscriptions.size());
    
    Subscription subscription = subscriptions.iterator().next();
    Assert.assertEquals(topic, subscription.getTopic());
    
    Subscription dogSubscription = new Subscription();
    dogSubscription.setTopic("dogs");
    dogSubscription.setChannel("animalPlanet");
    
    databaseService.addSubscription(node, dogSubscription);
    subscriptions = databaseService.getSubscriptions(node);
    Assert.assertEquals(2, subscriptions.size());
  }
  
  @Test
  public void eventExpirationTest() {
    
    Timestamp fourDaysAgo = Timestamp.from(Instant.now().minus(Duration.ofDays(4)));
    
    IntStream.range(0, 5).forEach(i -> {
      
      Event event = new Event();
      event.setMessage("Hello World [" + i + "]");
      event.setSubscriptionId(subscriptionId);
      event.setCreated(fourDaysAgo);
      
      databaseService.saveEvent(event);
    });
    
    Node node = databaseService.getNodeById(nodeId).get();
    
    System.out.println("Node subscriptions for " + node.getName());
    List<Subscription> subscriptions = databaseService.getSubscriptions(node);
    
    subscriptions.stream().forEach(s -> System.out.println(s.getTopic()));
    
    Subscription subscription = databaseService.getSubscriptionById(subscriptionId).get();
    System.out.println("Events for subscription " + subscription.getTopic());
    
    List<Event> events = databaseService.getSubscriptionEvents(subscriptionId);
    events.stream().forEach(e -> System.out.println(e.getMessage()));
    Assert.assertEquals(6, events.size());
    
    List<Event> expiredEvents = databaseService.getExpiredEvents();
    Assert.assertEquals(5, expiredEvents.size());
  }
}

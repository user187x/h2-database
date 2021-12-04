package com.xxx.controller;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import com.xxx.model.Event;
import com.xxx.model.Node;
import com.xxx.model.NodeSubscription;
import com.xxx.model.Subscription;
import com.xxx.model.UndeliveredEvent;
import com.xxx.service.DatabaseService;

@RestController
public class TestController {
  
  @Autowired
  private DatabaseService databaseService;

  @RequestMapping(value = "/test", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> generateTestData() {

    //Create a subscription
    Subscription subscription = new Subscription();
    subscription.setId("beatles");
    subscription.setTopic("johnLennon");
    subscription.setChannel("bbc");
    
    databaseService.saveSubscription(subscription);
    
    //Create 50 events for the subscription
    IntStream.range(0, 50).forEach(i -> {
      
      Event event = new Event();
      event.setMessage(UUID.randomUUID().toString());
      event.setSubscriptionId(subscription.getId());
      
      databaseService.saveEvent(event);
    });
    
    //Create 100 nodes
    IntStream.range(0, 100).forEach(i -> {
      
      Node node = new Node();
      node.setId(Integer.toString(i));
      node.setName("node-"+i);
      
      //Save the node
      databaseService.saveNode(node);
      
      //Subscribe all of the nodes
      NodeSubscription nodeSubscription = new NodeSubscription();
      nodeSubscription.setNodeId(node.getId());
      nodeSubscription.setSubscriptionId(subscription.getId());
      
      //Save the node-subscription
      databaseService.saveNodeSubscription(nodeSubscription);
      
    });
    
    
    //Make a few undelivered events
    List<Event> beBeUndeliveredEvents = databaseService.getSubscriptionEvents(subscription.getId()).stream()
    .skip(40)
    .collect(Collectors.toList());
    
    //Obtain a list of all nodes to insert an undelivered entry
    List<Node> nodes = databaseService.getNodes();
    
    beBeUndeliveredEvents.forEach(event -> {
    
      //pick a random node from the result set
      int randomNum = ThreadLocalRandom.current().nextInt(0, nodes.size() + 1);
      Node node = nodes.get(randomNum);
      
      //create an associated undelivered event
      UndeliveredEvent undeliveredEvent = new UndeliveredEvent();
      undeliveredEvent.setEventId(event.getId());
      undeliveredEvent.setNodeId(node.getId());
      
      //Save the undelivered event
      databaseService.saveUndeliveredEvent(undeliveredEvent);
    
    });
    
    //Grab some events from the subscription
    List<Event> toBeExpired = databaseService.getSubscriptionEvents(subscription.getId()).stream()
    .skip(10)
    .collect(Collectors.toList());
    
    // Make a few expired events
    toBeExpired.forEach(event -> {
      
      //re-adjust the created time so it can be considered expired
      event.setCreated(Timestamp.from(Instant.now().minus(Duration.ofDays(4))));
      
      databaseService.updateEvent(event.getId(), event);
    });
    
    return ResponseEntity.status(HttpStatus.OK).body("test data inserted");
  }
}

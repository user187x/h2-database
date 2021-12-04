package com.xxx.controller;

import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.xxx.management.EventManager;
import com.xxx.model.Event;
import com.xxx.model.Node;
import com.xxx.model.NodeSubscription;
import com.xxx.model.Subscription;
import com.xxx.service.DatabaseService;

@RestController
public class WebController {
  
  @Autowired
  private DatabaseService databaseService;
  
  @Autowired
  private EventManager eventManager;
  
  @RequestMapping(value = "/createNode/{id}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createNode(@PathVariable String id) {
    
    Node node = new Node(id);
    
    if(databaseService.nodeExists(node))
      return ResponseEntity.status(HttpStatus.OK).body(node.toString());
    
    boolean success = databaseService.saveNode(node);
    
    if(success)
      return ResponseEntity.status(HttpStatus.OK).body(node.toString());
    else
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
  }
  
  @RequestMapping(value = "/listNodes", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> listNodes() {
    
    List<Node> nodes = databaseService.getNodes();
    
    JsonArray payload = new JsonArray();
    
    for(Node node : nodes)
      payload.add(node.toJson());

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(payload));
  }
  
  @RequestMapping(value = "/deleteNode/{id}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> deleteNode(@PathVariable String id) {
    
    boolean success = databaseService.deleteNode(new Node(id));
    
    if(success)
      return ResponseEntity.status(HttpStatus.OK).build();
    else
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
  }
  
  @RequestMapping(value = "/addSubscription/{nodeId}/{channel}/{topic}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> addSubscription(@PathVariable String nodeId, @PathVariable String channel, @PathVariable String topic) {
    
    Subscription subscription = new Subscription();
    subscription.setTopic(topic);
    subscription.setChannel(channel);
    
    boolean exists = databaseService.subscriptionExists(subscription);
    
    if(!exists)
      databaseService.saveSubscription(subscription);
    
    Node node = new Node(nodeId);
    
    exists = databaseService.nodeExists(node);

    if(!exists)
      databaseService.saveNode(node);
    
    NodeSubscription nodeSubscription = new NodeSubscription(node, subscription);
    
    exists = databaseService.nodeSubscriptionExists(node.getId(), subscription.getId());
    
    if(!exists)
      databaseService.saveNodeSubscription(nodeSubscription);
    
    return ResponseEntity.status(HttpStatus.OK).build();
  }
  
  @RequestMapping(value = "/createEvent/{channel}/{topic}/", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createEvent(@PathVariable String channel, @PathVariable String topic, @RequestBody String body) {
    
    JsonObject payload = JsonParser.parseString(body).getAsJsonObject();
    String message = payload.get("message").getAsString();
    
    Subscription subscription = new Subscription();
    subscription.setChannel(channel);
    subscription.setTopic(topic);
    
    boolean exists = databaseService.subscriptionExists(channel, topic);
    
    if(!exists)
      databaseService.saveSubscription(subscription);
    
    Event event = new Event();
    event.setSubscriptionId(subscription.getId());
    event.setMessage(message);
    
    boolean eventSaveSuccess = databaseService.saveEvent(event);
    
    if(eventSaveSuccess) {
      
      eventManager.broadCastEvent(event);
      return ResponseEntity.status(HttpStatus.OK).body(payload.toString());
    }
    
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build(); 
  }
  
  @RequestMapping(value = "/getEvents/{nodeId}", method = RequestMethod.GET)
  public ResponseEntity<String> getUndeliverdEvents(@PathVariable String nodeId) {
    
    Node node = new Node(nodeId);
    
    boolean exists = databaseService.nodeExists(node);
    
    if(!exists)
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Node doesn't exists");
    
    List<Event> events = databaseService.getUndeliveredEvents(node);

    JsonArray payload = new JsonArray();
    
    for(Event event : events)
      payload.add(event.toJson());

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(payload));
  }
  
  @RequestMapping(value = "/getSubscriptions/{nodeId}", method = RequestMethod.GET)
  public ResponseEntity<String> getSubscriptions(@PathVariable String nodeId) {
    
    Node node = new Node(nodeId);
    
    if(!databaseService.nodeExists(node))
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Node doesn't exist");
    
    List<NodeSubscription> nodeSubscriptions = databaseService.getNodeSubscriptions(node);
    
    JsonArray payload = new JsonArray();
    
    for(NodeSubscription nodeSubscription : nodeSubscriptions) {
      
      Optional<Subscription> optional = databaseService.getSubscriptionById(nodeSubscription.getId());
      
      if(optional.isPresent()) {
        
        Subscription subscription = optional.get();
        
        JsonObject json = new JsonObject();
        json.addProperty("subscriptionId", subscription.getId());
        json.addProperty("channel", subscription.getChannel());
        json.addProperty("topic", subscription.getTopic());
        json.addProperty("subscribed-since", nodeSubscription.getCreated().toString());
        
        payload.add(json);
      }
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(payload));
  }
}
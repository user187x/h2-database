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
import com.xxx.model.UndeliveredEvent;
import com.xxx.service.DatabaseService;

@RestController
public class WebController {
  
  @Autowired
  private DatabaseService databaseService;
  
  @Autowired
  private EventManager eventManager;
  
  @RequestMapping(value = "/createNode/{name}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createNode(@PathVariable String name) {
    
    Node node = new Node();
    node.setName(name);
    
    if(databaseService.nodeExists(node))
      return ResponseEntity.status(HttpStatus.OK).body(node.toString());
    
    boolean success = databaseService.saveNode(node);
    
    if(success)
      return ResponseEntity.status(HttpStatus.OK).body(node.toJson().toString());
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
  
  @RequestMapping(value = "/listSubscriptions", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> listSubscriptions() {
    
    List<Subscription> subscriptions = databaseService.getSubscriptions();
    
    JsonArray payload = new JsonArray();
    
    for(Subscription subscription : subscriptions)
      payload.add(subscription.toJson());

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(payload));
  }
  
  @RequestMapping(value = {"/listNodeSubscriptions", "/listNodeSubscriptions/{nodeId}"}, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> listNodeSubscriptions(@PathVariable Optional<String> nodeId) {
    
    List<NodeSubscription> nodeSubscriptions;
    
    if(nodeId.isEmpty())
      nodeSubscriptions = databaseService.getNodeSubscriptions();
    else {
      
      boolean nodeExists = databaseService.nodeExists(nodeId.get());
      
      if(!nodeExists) {
        
        JsonObject response = new JsonObject();
        response.addProperty("success", false);
        response.addProperty("message", "Node doesn't exist");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response.toString());
      }
      
      nodeSubscriptions = databaseService.getNodeSubscriptions(nodeId.get());
    }
    
    JsonArray payload = new JsonArray();
    
    for(NodeSubscription nodeSubscription : nodeSubscriptions) {
      
      JsonObject json = nodeSubscription.toJson();
      
      Node node = databaseService.getNodeById(nodeSubscription.getNodeId()).get();
      Subscription subscription = databaseService.getSubscriptionById(nodeSubscription.getSubscriptionId()).get();
      
      json.addProperty("node-name", node.getName());
      json.addProperty("subscription-topic", subscription.getTopic());
      json.addProperty("subscription-channel", subscription.getChannel());
      
      payload.add(json);
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(payload));
  }
  
  @RequestMapping(value = "/listUndeliveredEvents", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> listUndeliveredEvents() {
    
    List<UndeliveredEvent> undeliveredEvents = databaseService.getUndeliveredEvents();
    
    JsonArray payload = new JsonArray();
    
    for(UndeliveredEvent undeliveredEvent : undeliveredEvents)
      payload.add(undeliveredEvent.toJson());

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
  
  @RequestMapping(value = "/addSubscription/{nodeId}/{subcriptionId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> addSubscription(@PathVariable String nodeId, @PathVariable String subcriptionId) {
    
    boolean exists = databaseService.subscriptionExists(subcriptionId);
    
    if(!exists) {
      
      JsonObject response = new JsonObject();
      response.addProperty("success", false);
      response.addProperty("message", "Subscription doesnt exist");
      
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response.toString());
    }
    
    exists = databaseService.nodeExists(nodeId);

    if(!exists) {
      
      JsonObject response = new JsonObject();
      response.addProperty("success", false);
      response.addProperty("message", "Node doesnt exist");
      
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response.toString());
    }
    
    Node node = databaseService.getNodeById(nodeId).get();
    Subscription subscription = databaseService.getSubscriptionById(subcriptionId).get();
    
    NodeSubscription nodeSubscription = new NodeSubscription(node, subscription);
    
    exists = databaseService.nodeSubscriptionExists(node.getId(), subscription.getId());
    
    if(!exists) {
      
      boolean success = databaseService.saveNodeSubscription(nodeSubscription);
      
      if(success) {
        
        JsonObject response = new JsonObject();
        response.addProperty("success", true);
        response.addProperty("message", "Node doesnt exist");
        
        return ResponseEntity.status(HttpStatus.OK).build();
      }
      else {
        
        JsonObject response = new JsonObject();
        response.addProperty("success", false);
        response.addProperty("message", "failure saving Node-Subscription");
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response.toString());
      }
    }
    
    JsonObject response = new JsonObject();
    response.addProperty("success", true);
    response.addProperty("message", "Node-Subscription already exists");
    
    return ResponseEntity.status(HttpStatus.OK).build();
  }
  
  @RequestMapping(value = "/createEvent/{channel}/{topic}", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createEvent(@PathVariable String channel, @PathVariable String topic, @RequestBody String body) {
    
    String message;
    
    try {
      
      JsonObject payload = JsonParser.parseString(body).getAsJsonObject();
      message = payload.get("message").getAsString();
    }
    catch(Exception e) {
      
      JsonObject response = new JsonObject();
      response.addProperty("success", false);
      response.addProperty("message", "failure parsing 'message'");
      
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response.toString());
    }
    
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
      
      eventManager.asyncBroadastEvent(event);
      
      JsonObject response = new JsonObject();
      response.addProperty("success", true);
      response.addProperty("message", message);
      
      return ResponseEntity.status(HttpStatus.OK).body(response.toString());
    }
    
    JsonObject response = new JsonObject();
    response.addProperty("success", false);
    response.addProperty("message", "failure saving message");
    
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response.toString()); 
  }
  
  @RequestMapping(value = "/createSubscription/{channel}/{topic}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> createSubscription(@PathVariable String channel, @PathVariable String topic) {
    
    Optional<Subscription> oSubscription = databaseService.getSubscription(topic, channel);
    
    if(oSubscription.isPresent()) {
      
      JsonObject response = new JsonObject();
      response.addProperty("success", true);
      response.addProperty("subscriptionId", oSubscription.get().getId());
      response.addProperty("message", "Subscription already exists");
      
      return ResponseEntity.status(HttpStatus.OK).body(response.toString());
    }
    
    Subscription subscription = new Subscription();
    subscription.setChannel(channel);
    subscription.setTopic(topic);
    
    boolean success = databaseService.saveSubscription(subscription);
    
    if(success) {
      
      JsonObject response = new JsonObject();
      response.addProperty("success", true);
      response.addProperty("subscriptionId", subscription.getId());
      response.addProperty("message", "Subscription saved");
      
      return ResponseEntity.status(HttpStatus.OK).body(response.toString());
    }
    else {
    
      JsonObject response = new JsonObject();
      response.addProperty("success", false);
      response.addProperty("message", "failure saving subscription");
      
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response.toString());
    }
  }
  
  @RequestMapping(value = "/getNode/{nodeId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getNode(@PathVariable String nodeId) {
    
    Optional<Node> node = databaseService.getNodeById(nodeId);
    
    if(node.isEmpty())
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Node doesn't exists");

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(node.get().toJson()));
  }
  
  @RequestMapping(value = "/getSubscription/{subscriptionId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getSubscription(@PathVariable String subscriptionId) {
    
    Optional<Subscription> subscription = databaseService.getSubscriptionById(subscriptionId);
    
    if(subscription.isEmpty())
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Subscription doesn't exists");

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(subscription.get().toJson()));
  }
  
  @RequestMapping(value = "/getUndeliveredEvents/{nodeId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getEvents(@PathVariable String nodeId) {
    
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
  
  @RequestMapping(value = "/getSubscriptions/{nodeId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getSubscriptions(@PathVariable String nodeId) {
    
    Optional<Node> node = databaseService.getNodeById(nodeId);
    
    if(node.isEmpty())
      return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new JsonArray().toString());
    
    List<NodeSubscription> nodeSubscriptions = databaseService.getNodeSubscriptions(node.get().getId());
    
    JsonArray payload = new JsonArray();
    
    for(NodeSubscription nodeSubscription : nodeSubscriptions) {
      
      Optional<NodeSubscription> optional = databaseService.getNodeSubscription(nodeSubscription.getId());
      
      if(optional.isPresent()) {
        
        Optional<Subscription> oSubscription = databaseService.getSubscriptionById(optional.get().getSubscriptionId());
        
        if(oSubscription.isPresent()) {
        
          Subscription subscription = oSubscription.get();
          
          JsonObject json = new JsonObject();
          json.addProperty("subscriptionId", subscription.getId());
          json.addProperty("channel", subscription.getChannel());
          json.addProperty("topic", subscription.getTopic());
          json.addProperty("subscribed-since", nodeSubscription.getCreated().toString());
          
          payload.add(json);
        }
      }
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(payload));
  }
}

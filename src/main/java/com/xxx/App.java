package com.xxx;

import java.sql.SQLException;
import org.h2.tools.Server;
import com.xxx.model.Event;
import com.xxx.model.Node;
import com.xxx.model.NodeSubscription;
import com.xxx.model.Subscription;
import com.xxx.model.UndeliveredEvent;
import com.xxx.service.DatabaseService;

public class App {

  public static void main(String[] args) throws SQLException {
    
    DatabaseService databaseService = new DatabaseService();
    
    Subscription subscription = new Subscription();
    subscription.setId("aaaaa");
    subscription.setTopic("the-goods");
    databaseService.saveSubscription(subscription);
    
    Event event = new Event();
    event.setMessage("Hello World");
    event.setSubscriptionId(subscription.getId());
    databaseService.saveEvent(event);
    
    Node node = new Node();
    databaseService.saveNode(node);
    
    NodeSubscription nodeSubscription = new NodeSubscription();
    nodeSubscription.setNodeId(node.getId());
    nodeSubscription.setSubscriptionId(subscription.getId());
    databaseService.saveNodeSubscription(nodeSubscription);
    
    UndeliveredEvent undeliveredEvent = new UndeliveredEvent();
    undeliveredEvent.setEventId(event.getId());
    undeliveredEvent.setNodeId(node.getId());
    databaseService.saveUndeliveredEvent(undeliveredEvent);
    
    Server.startWebServer(databaseService.getConnection());
  }
}

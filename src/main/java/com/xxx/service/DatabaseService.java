package com.xxx.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import com.xxx.model.Event;
import com.xxx.model.Node;
import com.xxx.model.NodeSubscription;
import com.xxx.model.Subscription;
import com.xxx.model.UndeliveredEvent;
import com.xxx.util.SQLUtil;
import com.xxx.util.SQLUtil.Tables;

public class DatabaseService {

  private static final Logger logger = Logger.getLogger(DatabaseService.class.getSimpleName());
  
  private Connection connection;

  public DatabaseService() {}
  
  public boolean initialize() {
 
    try {
      
      connection = DriverManager.getConnection("jdbc:h2:mem:");
      SQLUtil.ensureTables(connection);
      
      return true;
    }
    catch(Exception e) {
     
      logger.severe("Failure setting up database connection " + e.getMessage());
      return false;
    }

  }
  
  public Connection getConnection() {
    return connection;
  }
  
  public boolean nodeExists(Node node) {
    
    Optional<List<Node>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODES.name()))
    .where(DSL.field("ID").eq(node.getId()))
    .fetch()
    .into(Node.class));
    
    if(list.isPresent()) {
      
      List<Node> nodeList = list.get();
      
      if(nodeList.isEmpty())
        return false;
      else
        return true;
    }
    
    return false;
  }
  
  public boolean subscriptionExists(String channel, String topic) {
    
    Optional<List<Subscription>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .where(DSL.field("TOPIC").eq(topic).and(DSL.field("CHANNEL").eq(channel)))
    .fetch()
    .into(Subscription.class));
    
    if(list.isPresent()) {
      
      List<Subscription> subscriptionList = list.get();
      
      if(subscriptionList.isEmpty())
        return false;
      else
        return true;
    }
    
    return false;
  }
  
  public boolean subscriptionExists(Subscription subscription) {
    
    Optional<List<Subscription>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(subscription.getId()))
    .fetch()
    .into(Subscription.class));
    
    if(list.isPresent()) {
      
      List<Subscription> subscriptionList = list.get();
      
      if(subscriptionList.isEmpty())
        return false;
      else
        return true;
    }
    
    return false;
  }
  
  public boolean nodeSubscriptionExists(NodeSubscription nodeSubscription) {
    
    Optional<List<Subscription>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(nodeSubscription.getId()))
    .fetch()
    .into(Subscription.class));
    
    if(list.isPresent()) {
      
      List<Subscription> nodeList = list.get();
      
      if(nodeList.isEmpty())
        return false;
      else
        return true;
    }
    
    return false;
  }
  
  public boolean nodeSubscriptionExists(String nodeId, String subscriptionId) {
    
    Optional<List<Subscription>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .where(DSL.field("NODE_ID").eq(nodeId).and(DSL.field("SUBSCRIPTION_ID").eq(subscriptionId)))
    .fetch()
    .into(Subscription.class));
    
    if(list.isPresent()) {
      
      List<Subscription> nodeList = list.get();
      
      if(nodeList.isEmpty())
        return false;
      else
        return true;
    }
    
    return false;
  }
  
  public boolean nodeSubscriptionExists(Node node, Subscription subscription) {
    
    Optional<List<NodeSubscription>> nodeSubscription = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .where(DSL.field("NODE_ID").eq(node.getId())
        .and(DSL.field("SUBSCRIPTION_ID").eq(subscription.getId())))
    .fetch()
    .into(NodeSubscription.class));
    
    if(nodeSubscription.isPresent()) {
      
      List<NodeSubscription> nodeSubscriptions = nodeSubscription.get();
      
      if(nodeSubscriptions.isEmpty())
        return false;
      else
        return true;
    }
      
    return false;
  }
  
  public boolean saveNode(Node node) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.NODES.name()), DSL.field("ID"), DSL.field("NAME"), DSL.field("LAST_SEEN"), DSL.field("CREATED"))
    .values(node.getId(), node.getName(), node.getLastSeen(), node.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean updateNode(Node node) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .update(DSL.table(Tables.NODES.name()))
    .set(DSL.field("NAME", SQLDataType.VARCHAR), node.getName())
    .set(DSL.field("LAST_SEEN", SQLDataType.TIMESTAMP), DSL.currentTimestamp())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean deleteNode(Node node) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .delete(DSL.table(Tables.NODES.name()))
    .where(DSL.field("ID").eq(node.getId()))
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean saveEvent(Event event) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.EVENTS.name()), DSL.field("ID"), DSL.field("SUBSCRIPTION_ID"), DSL.field("MESSAGE"), DSL.field("CREATED"))
    .values(event.getId(), event.getSubscriptionId(), event.getMessage(), event.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean saveSubscription(Subscription subscription) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.SUBSCRIPTIONS.name()), 
        DSL.field("ID"),
        DSL.field("TOPIC"), 
        DSL.field("CHANNEL"),
        DSL.field("CREATED"))
    .values(
        subscription.getId(), 
        subscription.getTopic(),
        subscription.getChannel(),
        subscription.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean deleteSubscription(Subscription subscription) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .delete(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(subscription.getId()))
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean saveNodeSubscription(NodeSubscription nodeSubscription) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()), DSL.field("ID"), DSL.field("SUBSCRIPTION_ID"), DSL.field("NODE_ID"), DSL.field("CREATED"))
    .values(nodeSubscription.getId(), nodeSubscription.getSubscriptionId(), nodeSubscription.getNodeId(), nodeSubscription.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean deleteNodeSubscription(NodeSubscription nodeSubscription) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .delete(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(nodeSubscription.getId()))
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean saveUndeliveredEvent(UndeliveredEvent undeliveredEvent) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.UNDELIVERED_EVENTS.name()), DSL.field("ID"), DSL.field("EVENT_ID"), DSL.field("NODE_ID"), DSL.field("CREATED"))
    .values(undeliveredEvent.getId(), undeliveredEvent.getEventId(), undeliveredEvent.getNodeId(), undeliveredEvent.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public Optional<Node> getNodeById(String id) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODES.name()))
    .where(DSL.field("ID").eq(id))
    .fetchOne()
    .into(Node.class));
  }
  
  public List<Node> getNodes() {
    
    return DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODES.name()))
    .fetch()
    .into(Node.class);
  }
  
  public Optional<Event> getEventById(String id) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.EVENTS.name()))
    .where(DSL.field("ID").eq(id))
    .fetchOne()
    .into(Event.class));
  }
  
  public List<Event> getEvents() {
    
    List<Event> events = DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.EVENTS.name()))
    .fetch()
    .into(Event.class);
    
    return events;
  }
  
  public Optional<Subscription> getSubscriptionById(String id) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(id))
    .fetchOne()
    .into(Subscription.class));
  }
  
  public List<Subscription> getSubscriptions() {
    
    List<Subscription> subscriptions = DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .fetch()
    .into(Subscription.class);
    
    return subscriptions;
  }
  
  public List<UndeliveredEvent> getUndeliveredEvents() {
    
    List<UndeliveredEvent> undeliveredEvents = DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.UNDELIVERED_EVENTS.name()))
    .fetch()
    .into(UndeliveredEvent.class);
    
    return undeliveredEvents;
  }
  
  public List<Event> getUndeliveredEvents(Node node) {
    
    List<Event> undeliveredEvents = DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.UNDELIVERED_EVENTS.name())).join(Tables.EVENTS.name())
      .on(DSL.field("UNDELIVERED_EVENTS.EVENT_ID").eq(DSL.field("EVENTS.ID")))
    .where(DSL.field("NODE_ID").eq(node.getId()))
    .fetch()
    .into(Event.class);
    
    return undeliveredEvents;
  }
  
  public List<Subscription> getSubscriptions(Node node) {
    
    List<Subscription> subscriptions = DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name())).join(Tables.SUBSCRIPTIONS.name())
      .on(DSL.field("NODE_SUBSCRIPTIONS.SUBSCRIPTION_ID").eq(DSL.field("SUBSCRIPTIONS.ID")))
    .where(DSL.field("NODE_SUBSCRIPTIONS.NODE_ID").eq(node.getId()))
    .fetch()
    .into(Subscription.class);
    
    return subscriptions;
  }
  
  public List<Node> getSubscribedNodes(Subscription subscription) {
    
    List<Node> subscriptions = DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name())).join(Tables.NODES.name())
      .on(DSL.field("NODE_SUBSCRIPTIONS.NODE_ID").eq(DSL.field("NODES.ID")))
    .where(DSL.field("NODE_SUBSCRIPTIONS.SUBSCRIPTION_ID").eq(subscription.getId()))
    .fetch()
    .into(Node.class);
    
    return subscriptions;
  }
  
  public boolean addSubscription(Node node, Subscription subscription) {
    
    if(nodeSubscriptionExists(node, subscription))
      return true;
    
    NodeSubscription nodeSubscription = new NodeSubscription();
    nodeSubscription.setNodeId(node.getId());
    nodeSubscription.setSubscriptionId(subscription.getId());
    
    boolean exists = subscriptionExists(subscription);
    
    if(!exists)
      saveSubscription(subscription);
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()), DSL.field("ID"), DSL.field("NODE_ID"), DSL.field("SUBSCRIPTION_ID"), DSL.field("CREATED"))
    .values(nodeSubscription.getId(), nodeSubscription.getNodeId(), nodeSubscription.getSubscriptionId(), nodeSubscription.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean removeNodeSubscription(Node node, Subscription subscription) {
    
    Optional<NodeSubscription> nodeSubscription = getNodeSubscription(node.getId(), subscription.getId());    
          
    if(nodeSubscription.isEmpty())
      return true;
    
    return deleteNodeSubscription(nodeSubscription.get());
  }
  
  public Optional<NodeSubscription> getNodeSubscription(String nodeSubscriptionId) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(nodeSubscriptionId))
    .fetchOne()
    .into(NodeSubscription.class));
  }
  
  public Optional<NodeSubscription> getNodeSubscription(String nodeId, String subscriptionId) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .where(DSL.field("NODE_ID").eq(nodeId).and(DSL.field("SUBSCRIPTION_ID").eq(subscriptionId)))
    .fetchOne()
    .into(NodeSubscription.class));
  }
  
  public List<NodeSubscription> getNodeSubscriptions() {
    
    List<NodeSubscription> nodeSubscriptions = DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .fetch()
    .into(NodeSubscription.class);
    
    return nodeSubscriptions;
  }
  
  public List<NodeSubscription> getNodeSubscriptions(Node node) {
    
    List<NodeSubscription> nodeSubscriptions = DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .where(DSL.field("NODE_ID").eq(node.getId()))
    .fetch()
    .into(NodeSubscription.class);
    
    return nodeSubscriptions;
  }
}
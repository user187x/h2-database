package com.xxx.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Optional;
import java.util.logging.Logger;
import org.jooq.impl.DSL;
import com.xxx.model.Event;
import com.xxx.model.Node;
import com.xxx.model.NodeSubscription;
import com.xxx.model.Subscription;
import com.xxx.model.UndeliveredEvent;

public class DatabaseService {

  private static final Logger logger = Logger.getLogger(DatabaseService.class.getSimpleName());
  
  private Connection connection;

  public DatabaseService() {

    try {

      connection = DriverManager.getConnection("jdbc:h2:mem:");
      
      TableCreator.createNodeTable(connection);
      TableCreator.createSubscriptionTable(connection);
      TableCreator.createNodeSubscriptionTable(connection);
      TableCreator.createEventTable(connection);
      TableCreator.createUndeliveredEventsTable(connection);
    } 
    catch (Exception e) {

      logger.severe("Failure setting up database connection " + e.getMessage());
    }
  }
  
  public Connection getConnection() {
    return connection;
  }
  
  public boolean nodeExists(Node node) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table("NODES"))
    .where(DSL.field("ID").eq(node.getId()))
    .fetch()
    .into(Node.class)).isPresent();
  }
  
  public boolean subscriptionExists(Subscription subscription) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table("SUBSCRIPTIONS"))
    .where(DSL.field("ID").eq(subscription.getId()))
    .fetch()
    .into(Node.class)).isPresent();
  }
  
  public boolean nodeSubscriptionExists(NodeSubscription nodeSubscription) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table("NODE_SUBSCRIPTIONS"))
    .where(DSL.field("ID").eq(nodeSubscription.getId()))
    .fetch()
    .into(Node.class)).isPresent();
  }
  
  public boolean saveNode(Node node) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table("NODES"), DSL.field("ID"), DSL.field("HEALTHY"), DSL.field("LAST_SEEN"), DSL.field("CREATED"))
    .values(node.getId(), node.isHealthy(), node.getLastSeen(), node.getCreated())
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
    .insertInto(DSL.table(Tables.SUBSCRIPTIONS.name()), DSL.field("ID"), DSL.field("TOPIC"), DSL.field("CREATED"))
    .values(subscription.getId(), subscription.getTopic(), subscription.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean saveNodeSubscription(NodeSubscription nodeSubscription) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table("NODE_SUBSCRIPTIONS"), DSL.field("ID"), DSL.field("SUBSCRIPTION_ID"), DSL.field("NODE_ID"), DSL.field("CREATED"))
    .values(nodeSubscription.getId(), nodeSubscription.getSubscriptionId(), nodeSubscription.getNodeId(), nodeSubscription.getCreated())
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
  
  public Optional<Event> getEventById(String id) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.EVENTS.name()))
    .where(DSL.field("ID").eq(id))
    .fetchOne()
    .into(Event.class));
  }
  
  public Optional<Subscription> getSubscriptionById(String id) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(id))
    .fetchOne()
    .into(Subscription.class));
  }
  
  public Optional<NodeSubscription> getNodeSubscriptionById(String id) {
    
    return Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(id))
    .fetchOne()
    .into(NodeSubscription.class));
  }
}

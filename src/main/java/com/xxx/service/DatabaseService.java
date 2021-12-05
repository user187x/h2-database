package com.xxx.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.h2.tools.Server;
import org.jooq.Condition;
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
  
  private boolean dbGuiActive = false;
  private Server server = null;
  
  private Connection connection;
  private boolean inMemory = true;
  
  private String databasePath = "jdbc:h2:file:";
  
  private String user;
  private String password;
  private Integer guiPort = 8888;
  
  public DatabaseService() {}

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setDatabasePath(String path) {
    
    if(StringUtils.isNoneBlank(path)) {
      
      this.databasePath = databasePath.concat(path);
      this.inMemory = false;
    }
  }

  public void setDatabaseGuiPort(int port) {
    this.guiPort = port;
  }

  public Integer getDatbaseGuiPort() {
    return guiPort;
  }
  
  public Server getDbServer() {
    return server;
  }
  
  public void startGUIServer() {
    
   try {
     
     server = Server.createWebServer("-web", "-webAllowOthers", "-webPort", guiPort.toString());
     server.start();
     
     this.dbGuiActive = true;
     
     logger.info("Database GUI has been activated");
   }
   catch(Exception e) {
    
     logger.warning("Failure starting H2 GUI -> " + e.getMessage());
   }
  }
  
  public void stopGUIServer() {
    
    if(server !=null && server.isRunning(true)) {
      
      server.stop();
      this.dbGuiActive = false;
      
      logger.info("Database GUI has been deactivated");
    }
  }
  
  public boolean isGuiActive() {
    return dbGuiActive;
  }
  
  public String getDatabasePath() {
    return databasePath;
  }

  public boolean initialize() {
 
    try {
      
      System.getProperties().setProperty("org.jooq.no-logo", "true");
      System.getProperties().setProperty("org.jooq.no-tips", "true");
 
      if(!inMemory) {
        
        if(StringUtils.isNoneBlank(password)) { 
          this.databasePath = databasePath.concat(";" + "USER=" + getUser() + ";" + "PASSWORD=" + getPassword());
          
          connection = DriverManager.getConnection(databasePath);
        }
      }
      else {
        
        connection = DriverManager.getConnection(databasePath);
      }
      
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
    
    Condition condition = null;
    
    if(StringUtils.isBlank(node.getName()))
      condition = DSL.field("ID").eq(node.getId());
    else
      condition = DSL.field("NAME").eq(node.getName());
    
    Optional<List<Node>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODES.name()))
    .where(condition)
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
  
  public boolean nodeExists(String id) {
    
    Optional<List<Node>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODES.name()))
    .where(DSL.field("ID").eq(id))
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
  
  public boolean subscriptionExists(String subcriptionId) {
    
    Optional<List<Subscription>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .where(DSL.field("ID").eq(subcriptionId))
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
  
  public boolean eventExists(Event event) {
    
    Optional<List<Event>> list = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.EVENTS.name()))
    .where(DSL.field("ID").eq(event.getId()))
    .fetch()
    .into(Event.class));
    
    if(list.isPresent()) {
      
      List<Event> subscriptionList = list.get();
      
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
    .where(DSL.field("ID").eq(nodeSubscription.getId())
    .or(DSL.field("NODE_ID").eq(nodeSubscription.getNodeId()).and(DSL.field("SUBSCRIPTION_ID").eq(nodeSubscription.getSubscriptionId()))))
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
    
    if(nodeExists(node))
      return true;
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.NODES.name()), 
        DSL.field("ID"), 
        DSL.field("NAME"), 
        DSL.field("LAST_SEEN"), 
        DSL.field("CREATED"))
    .values(
        node.getId(), 
        node.getName(), 
        node.getLastSeen(), 
        node.getCreated())
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
    
    if(eventExists(event))
      return true;
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.EVENTS.name()), 
        DSL.field("ID"), 
        DSL.field("SUBSCRIPTION_ID"), 
        DSL.field("MESSAGE"), 
        DSL.field("CREATED"))
    .values(
        event.getId(), 
        event.getSubscriptionId(), 
        event.getMessage(), 
        event.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
  
  public boolean saveSubscription(Subscription subscription) {
    
    if(subscriptionExists(subscription))
      return true;
    
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
    
    if(nodeSubscriptionExists(nodeSubscription))
      return true;
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()), 
        DSL.field("ID"), 
        DSL.field("SUBSCRIPTION_ID"), 
        DSL.field("NODE_ID"), 
        DSL.field("CREATED"))
    .values(
        nodeSubscription.getId(), 
        nodeSubscription.getSubscriptionId(), 
        nodeSubscription.getNodeId(), 
        nodeSubscription.getCreated())
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
    
    if(undeliveredEventExists(undeliveredEvent))
      return true;
    
    Optional<UndeliveredEvent> ude = getUndeliveredEvent(undeliveredEvent.getNodeId(), undeliveredEvent.getEventId());
    
    if(ude.isPresent())
      return true;
    
    Optional<Event> event = getEventById(undeliveredEvent.getEventId());
    
    if(event.isEmpty()) {
      
      logger.warning("Attempt to save undelivered event failed -> Subscription doesn't exists");
      return false;
    }
    
    Optional<Node> node = getNodeById(undeliveredEvent.getNodeId());
    
    if(node.isEmpty()) {
      
      logger.warning("Attempt to save undelivered event failed -> Node doesn't exists");
      return false;
    }
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .insertInto(DSL.table(Tables.UNDELIVERED_EVENTS.name()), 
        DSL.field("ID"), 
        DSL.field("EVENT_ID"), 
        DSL.field("NODE_ID"), 
        DSL.field("CREATED"))
    .values(
        undeliveredEvent.getId(), 
        undeliveredEvent.getEventId(), 
        undeliveredEvent.getNodeId(), 
        undeliveredEvent.getCreated())
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }

  private boolean undeliveredEventExists(UndeliveredEvent undeliveredEvent) {
    
    Optional<List<UndeliveredEvent>> nodeSubscription = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.UNDELIVERED_EVENTS.name()))
    .where(DSL.field("ID").eq(undeliveredEvent.getId()).or(DSL.field("NODE_ID").eq(undeliveredEvent.getNodeId())
        .and(DSL.field("EVENT_ID").eq(undeliveredEvent.getEventId()))))
    .fetch()
    .into(UndeliveredEvent.class));
    
    if(nodeSubscription.isPresent()) {
      
      if(nodeSubscription.get().isEmpty())
        return false;
      else
        return true;
    }
      
    return false;
  }

  public Optional<Node> getNodeById(String id) {
    
    try {
    
      return Optional.ofNullable(DSL.using(getConnection())
      .select()
      .from(DSL.table(Tables.NODES.name()))
      .where(DSL.field("ID").eq(id))
      .fetchOne()
      .into(Node.class));
    }
    catch(Exception e) {
      
      return Optional.empty();
    }
  }
  
  public List<Node> getNodes() {
    
    return DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.NODES.name()))
    .fetch()
    .into(Node.class);
  }
  
  public Optional<Event> getEventById(String id) {
    
    try {
    
      return Optional.ofNullable(DSL.using(getConnection())
      .select()
      .from(DSL.table(Tables.EVENTS.name()))
      .where(DSL.field("ID").eq(id))
      .fetchOne()
      .into(Event.class));
    }
    catch(Exception e) {
      
      return Optional.empty();
    }
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
    
    try {
    
      return Optional.ofNullable(DSL.using(getConnection())
      .select()
      .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
      .where(DSL.field("ID").eq(id))
      .fetchOne()
      .into(Subscription.class));
    }
    catch(Exception e) {
      
      return Optional.empty();
    }
  }
  
  public List<Subscription> getSubscriptions() {
    
    try {
    
    return DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .fetch()
    .into(Subscription.class);
    
    }
    catch(Exception e){
      
      return null;
    }
  }
  
  public List<UndeliveredEvent> getUndeliveredEvents() {
    
    try {
    
      return DSL.using(getConnection())
      .select()
      .from(DSL.table(Tables.UNDELIVERED_EVENTS.name()))
      .fetch()
      .into(UndeliveredEvent.class);
    }
    catch(Exception e) {
      
      return null;
    }
  }
  
  public Optional<UndeliveredEvent> getUndeliveredEvent(Node node, Event event) {
    
    Optional<UndeliveredEvent> undeliveredEvent = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.UNDELIVERED_EVENTS.name()))
    .where(DSL.field("NODE_ID").eq(node.getId()).and(DSL.field("EVENT_ID").eq(event.getId())))
    .fetchOne()
    .into(UndeliveredEvent.class));
    
    return undeliveredEvent;
  }
  
  public Optional<UndeliveredEvent> getUndeliveredEvent(String nodeId, String eventId) {
    
    Optional<List<UndeliveredEvent>> optional = Optional.ofNullable(DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.UNDELIVERED_EVENTS.name()))
    .where(DSL.field("NODE_ID").eq(nodeId).and(DSL.field("EVENT_ID").eq(eventId)))
    .fetch()
    .into(UndeliveredEvent.class));
    
    if(optional.isPresent() && !optional.get().isEmpty())
      return Optional.of(optional.get().iterator().next());
    else
      return Optional.empty();
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
    .insertInto(DSL.table(Tables.NODE_SUBSCRIPTIONS.name()), 
        DSL.field("ID"),
        DSL.field("NODE_ID"), 
        DSL.field("SUBSCRIPTION_ID"), 
        DSL.field("CREATED"))
    .values(
        nodeSubscription.getId(), 
        nodeSubscription.getNodeId(), 
        nodeSubscription.getSubscriptionId(), 
        nodeSubscription.getCreated())
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

  public List<Event> getSubscriptionEvents(String subscriptionId) {
    
    return DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.SUBSCRIPTIONS.name()))
    .join(Tables.EVENTS.name())  
    .on(DSL.field("EVENTS.SUBSCRIPTION_ID").eq(DSL.field("SUBSCRIPTIONS.ID")))
    .where(DSL.field("EVENTS.SUBSCRIPTION_ID").eq(subscriptionId))
    .fetch()
    .into(Event.class);
  }
  
  public List<Event> getExpiredEvents() {
    
    Date oneDay = Date.from(Instant.now().minus(Duration.ofDays(1)));
    
    return DSL.using(getConnection())
    .select()
    .from(DSL.table(Tables.EVENTS.name()))
    .where(DSL.field("CREATED").le(oneDay))
    .fetch()
    .into(Event.class);
  }

  public boolean updateEvent(String eventId, Event event) {
    
    boolean success = Optional.ofNullable(DSL.using(getConnection())
    .update(DSL.table(Tables.EVENTS.name()))
    .set(DSL.field("CREATED", SQLDataType.TIMESTAMP), event.getCreated())
    .set(DSL.field("MESSAGE", SQLDataType.VARCHAR), event.getMessage())
    .set(DSL.field("SUBSCRIPTION_ID", SQLDataType.VARCHAR), event.getSubscriptionId())
    .where(DSL.field("ID").eq(eventId))
    .execute())
    .map(count -> count == 1)
    .get();
    
    return success;
  }
}

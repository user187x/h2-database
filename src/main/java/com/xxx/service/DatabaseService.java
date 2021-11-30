package com.xxx.service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.jooq.impl.DSL;
import com.xxx.model.Event;
import com.xxx.model.Node;
import com.xxx.model.NodeSubscription;
import com.xxx.model.Subscription;

public class DatabaseService {

  private static final Logger logger = Logger.getLogger(DatabaseService.class.getSimpleName());
  
  private Connection connection;

  public DatabaseService() {

    try {

      connection = DriverManager.getConnection("jdbc:h2:mem:");
      ensureTables(connection);
    } 
    catch (Exception e) {

      logger.severe("Failure setting up database connection " + e.getMessage());
    }
  }
  
  private boolean ensureTables(Connection connection) {

    try (Statement statement = connection.createStatement()) {

      InputStream inputStream = DatabaseService.class.getResourceAsStream("/sql/Events.sql");
      String sql = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

      String formatted = sql.replace("\n", "").replace("\r", "");

      return statement.execute(formatted);
    } 
    catch (Exception e) {

      logger.warning("Failure creating tables " + e.getMessage());
      return false;
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
    .insertInto(DSL.table("EVENTS"), DSL.field("ID"), DSL.field("SUBSCRIPTION_ID"), DSL.field("MESSAGE"), DSL.field("CREATED"))
    .values(event.getId(), event.getSubscriptionId(), event.getMessage(), event.getCreated())
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
  
  public boolean insert(Event event) {
    
    boolean exists = exists(event);
    
    if(exists)
      return true;
    
    try {
      
      Statement statement = connection.createStatement();
      
      StringBuilder builder = new StringBuilder();
      
      builder.append("INSERT INTO EVENTS VALUES (");
      builder.append("'" + event.getId() + "'");
      builder.append(",");
      builder.append("'" + event.getMessage() + "'");
      builder.append(")");
      
      String sql = builder.toString();
      statement.execute(sql);
      
      return true;
    }
    catch(Exception e) {
     
      logger.warning("Failure persisting entry");
      return false;
    }
  }

  public boolean exists(Event event) {
    return getEventById(event.getId()).isPresent();
  }
  
  public Optional<Event> getEventById(String eventId) {
   
    try {
      
      StringBuilder builder = new StringBuilder();
      
      builder.append("SELECT * FROM EVENTS");
      builder.append(StringUtils.SPACE);
      builder.append("WHERE");
      builder.append(StringUtils.SPACE);
      builder.append("ID=?");
      
      String sql = builder.toString();
      
      PreparedStatement statement = connection.prepareStatement(sql);
      statement.setString(1, eventId);
      
      ResultSet resultSet = statement.executeQuery();
      
      while(resultSet.next()) {
        
        String id = resultSet.getString("ID");
        String message = resultSet.getString("MESSAGE");
        
        Event event = new Event();
        event.setId(id);
        event.setMessage(message);
        
        return Optional.of(event);
      }
      
      return Optional.empty();
    }
    catch(Exception e) {
     
      logger.warning("Failure persisting entry");
      return Optional.empty();
    }
  }
}

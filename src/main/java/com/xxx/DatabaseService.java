package com.xxx;

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
import com.xxx.model.Event;

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

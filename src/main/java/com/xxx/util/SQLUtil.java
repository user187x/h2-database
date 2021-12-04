package com.xxx.util;

import java.sql.Connection;
import java.util.Optional;
import java.util.logging.Logger;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

public class SQLUtil {
  
  private static final Logger logger = Logger.getLogger(SQLUtil.class.getSimpleName());
  
  public static enum Tables {EVENTS, NODES, NODE_SUBSCRIPTIONS, SUBSCRIPTIONS, UNDELIVERED_EVENTS}
  
  public static boolean ensureTables(Connection connection) {
    
    try {

      SQLUtil.createNodeTable(connection);
      SQLUtil.createSubscriptionTable(connection);
      SQLUtil.createNodeSubscriptionTable(connection);
      SQLUtil.createEventTable(connection);
      SQLUtil.createUndeliveredEventsTable(connection);
    } 
    catch (Exception e) {

      logger.warning("Failure creating tables " + e.getMessage());
      return false;
    }
    
    return true;
  }
  
  public static boolean createEventTable(Connection connection) {
    
    boolean success = Optional.ofNullable(DSL.using(connection)
    .createTableIfNotExists(Tables.EVENTS.name())
    .column("ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("SUBSCRIPTION_ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("MESSAGE", SQLDataType.VARCHAR(2048).nullable(false))
    .column("CREATED", SQLDataType.TIMESTAMP.nullable(false))
    .constraints(
      DSL.primaryKey("ID"),
      DSL.unique("ID"),
      DSL.foreignKey("SUBSCRIPTION_ID").references(Tables.SUBSCRIPTIONS.name())
    )
    .execute())
    .map(count -> count == 0)
    .get();
    
    return success;
  }
  
  public static boolean createUndeliveredEventsTable(Connection connection) {
    
    boolean success = Optional.ofNullable(DSL.using(connection)
    .createTableIfNotExists(Tables.UNDELIVERED_EVENTS.name())
    .column("ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("NODE_ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("EVENT_ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("CREATED", SQLDataType.TIMESTAMP.nullable(false))
    .constraints(
      DSL.primaryKey("ID"),
      DSL.unique("ID"),
      DSL.foreignKey("NODE_ID").references(Tables.NODES.name()),
      DSL.foreignKey("EVENT_ID").references(Tables.EVENTS.name())
    )
    .execute())
    .map(count -> count == 0)
    .get();
    
    return success;
  }
  
  public static boolean createNodeTable(Connection connection) {
    
    boolean success = Optional.ofNullable(DSL.using(connection)
    .createTableIfNotExists(Tables.NODES.name())
    .column("ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("NAME", SQLDataType.VARCHAR(256).nullable(false))
    .column("LAST_SEEN", SQLDataType.TIMESTAMP.nullable(false))
    .column("CREATED", SQLDataType.TIMESTAMP.nullable(false))
    .constraints(
      DSL.primaryKey("ID")
    )
    .execute())
    .map(count -> count == 0)
    .get();
    
    return success;
  }
  
  public static boolean createNodeSubscriptionTable(Connection connection) {
    
    boolean success = Optional.ofNullable(DSL.using(connection)
    .createTableIfNotExists(Tables.NODE_SUBSCRIPTIONS.name())
    .column("ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("SUBSCRIPTION_ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("NODE_ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("CREATED", SQLDataType.TIMESTAMP.nullable(false))
    .constraints(
      DSL.primaryKey("ID"),
      DSL.unique("ID"),
      DSL.foreignKey("SUBSCRIPTION_ID").references(Tables.SUBSCRIPTIONS.name()),
      DSL.foreignKey("NODE_ID").references(Tables.NODES.name())
    )
    .execute())
    .map(count -> count == 0)
    .get();
    
    return success;
  }
  
  public static boolean createSubscriptionTable(Connection connection) {
    
    boolean success = Optional.ofNullable(DSL.using(connection)
    .createTableIfNotExists(Tables.SUBSCRIPTIONS.name())
    .column("ID", SQLDataType.VARCHAR(256).nullable(false))
    .column("TOPIC", SQLDataType.VARCHAR(256).nullable(false))
    .column("CHANNEL", SQLDataType.VARCHAR(256).nullable(false))
    .column("CREATED", SQLDataType.TIMESTAMP.nullable(false))
    .constraints(
      DSL.primaryKey("ID"),
      DSL.unique("ID")
    )
    .execute())
    .map(count -> count == 0)
    .get();
    
    return success;
  }
}

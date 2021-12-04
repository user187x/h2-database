package com.xxx.config;

import java.sql.SQLException;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import org.h2.tools.Server;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.xxx.service.DatabaseService;

@Configuration
public class Initializer {

  private static final Logger logger = Logger.getLogger(Initializer.class.getSimpleName());
  
  @Autowired
  private ApplicationContext context;
  
  @Bean
  public DatabaseService databaseService() {
    
    DatabaseService databaseService = new DatabaseService();
    
    databaseService.setDatabasePath("C:/Users/xxx/Desktop/h2-database");
    databaseService.setUser("xxx");
    databaseService.setPassword("xxx");
    
    if(!databaseService.initialize()) {
      
      logger.severe("Failure starting up database -> Terminating service");
      
      SpringApplication.exit(context, () -> 1);
      System.exit(1);
    }
    
    return databaseService;
  }
  
  @PostConstruct
  public void startH2GUI() {
    
    try {
      
      Integer webPort = 8888;
      
      Server server = Server.createWebServer("-web", "-webAllowOthers", "-webPort", webPort.toString());
      server.start();
      
      logger.info("Started H2 Database UI at http://localhost:" + webPort);
    } 
    catch (SQLException e) {
      
      logger.warning("Failure starting H2 GUI -> " + e.getMessage());
    }
  }
}
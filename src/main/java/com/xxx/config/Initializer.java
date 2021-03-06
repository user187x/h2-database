package com.xxx.config;

import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
  
  @Value("${database.in-memory}")
  private boolean inMemory;
  
  @Value("${database.path}")
  private String dbpath;
  
  @Value("${database.gui-port}")
  private int guiPort;
  
  @Value("${database.user}")
  private String user;
  
  @Value("${database.password}")
  private String password;
  
  @Bean
  public DatabaseService databaseService() {
    
    DatabaseService databaseService = new DatabaseService();
    
    databaseService.setDatabasePath(dbpath);
    databaseService.setInMemory(inMemory);
    databaseService.setUser(user);
    databaseService.setDatabaseGuiPort(guiPort);
    databaseService.setPassword(password);
    
    if(!databaseService.initialize()) {
      
      logger.severe("Failure starting up database -> Terminating service");
      
      SpringApplication.exit(context, () -> 1);
      System.exit(1);
    }
    
    return databaseService;
  }
}
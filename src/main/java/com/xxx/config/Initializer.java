package com.xxx.config;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.xxx.service.DatabaseService;

@Configuration
public class Initializer {

  private static final Logger logger = Logger.getLogger(Initializer.class.getSimpleName());
  
  @Autowired
  private ApplicationContext context;
  
  @Bean("startTime")
  public Date getStartTime() {
    
    return new Date(context.getStartupDate());
  }
  
  @Bean
  public DatabaseService databaseService() {
    
    DatabaseService databaseService = new DatabaseService();
    
    boolean success = databaseService.initialize();
    
    if(!success) {
      
      logger.severe("Failure starting database");
      SpringApplication.exit(context, () -> 1);
    }
    
    return databaseService;
  }
  
  @Bean
  public DataSource datasource() {
      return DataSourceBuilder.create()
        .driverClassName("org.h2.Driver")
        .url("jdbc:h2:mem")
        .username("admin")
        .password("pass")
        .build(); 
  }
  
  @PostConstruct
  public void startDatabaseGUI() {
    
    ExecutorService excutorService = Executors.newSingleThreadExecutor();
    
    Runnable task = () -> {
      
      try {
        
        //Server.startWebServer(databaseService().getConnection());
      }
      catch(Exception e) {
        
        logger.warning("Failure starting Database GUI");
      }
    };
    
    excutorService.execute(task);
  }
}
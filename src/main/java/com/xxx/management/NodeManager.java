package com.xxx.management;

import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.xxx.service.DatabaseService;

@Component
@EnableScheduling
public class NodeManager {

  private static final Logger logger = Logger.getLogger(NodeManager.class.getSimpleName());
  
  @Autowired
  private DatabaseService databaseService;
  
  public void setDatabaseService(DatabaseService databaseService) {
    this.databaseService = databaseService;
  }
  
  @Scheduled(fixedDelay = 30000)
  public void checkUnhealthyNodes() {
  
    
  }
}

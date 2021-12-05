package com.xxx.management;

import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.xxx.model.Event;
import com.xxx.model.Node;
import com.xxx.model.Subscription;
import com.xxx.model.UndeliveredEvent;
import com.xxx.service.DatabaseService;

@Component
@EnableScheduling
public class EventManager {

  private static final Logger logger = Logger.getLogger(EventManager.class.getSimpleName());
  
  @Autowired
  private DatabaseService databaseService;
  
  public void setDatabaseService(DatabaseService databaseService) {
    this.databaseService = databaseService;
  }
  
  public void broadCastEvent(Event event) {

    Optional<Subscription> optional = databaseService.getSubscriptionById(event.getSubscriptionId());
    
    if(optional.isPresent()) {
    
      Subscription subscription = optional.get();
      List<Node> nodes = databaseService.getSubscribedNodes(subscription);
      
      for(Node node : nodes) {
      
        //TODO makes this meaningful somehow
        boolean success = false;
        
        if(!success) {
          
          UndeliveredEvent undeliveredEvent = new UndeliveredEvent();
          
          undeliveredEvent.setEventId(event.getId());
          undeliveredEvent.setNodeId(node.getId());
          
          databaseService.saveUndeliveredEvent(undeliveredEvent);
        }
      }
    }
  }
  
  @Scheduled(fixedDelay = 1000)
  public void cleanUpNonSubscribedTopic() {
    
    logger.info("Purging non-subscribed subscriptions");
    
    //TODO
  }
}

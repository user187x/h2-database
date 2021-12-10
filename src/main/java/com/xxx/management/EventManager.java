package com.xxx.management;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
import com.xxx.util.Requester;

@Component
@EnableScheduling
public class EventManager {

  private static final Logger logger = Logger.getLogger(EventManager.class.getSimpleName());
  
  @Autowired
  private DatabaseService databaseService;
  
  public void setDatabaseService(DatabaseService databaseService) {
    this.databaseService = databaseService;
  }
  
  public CompletableFuture<Void> asyncBroadastEvent(Event event) { 
    return CompletableFuture.runAsync(() -> broadCastEvent(event));
  }
  
  public synchronized void broadCastEvent(Event event) {

    Optional<Subscription> subscription = databaseService.getSubscriptionById(event.getSubscriptionId());
    
    if(subscription.isPresent()) {
    
      for(Node node : databaseService.getSubscribedNodes(subscription.get())) {
      
        if(!Requester.sendEvent(node, event)) {
          
          UndeliveredEvent undeliveredEvent = new UndeliveredEvent();
          
          undeliveredEvent.setEventId(event.getId());
          undeliveredEvent.setNodeId(node.getId());
          
          databaseService.saveUndeliveredEvent(undeliveredEvent);
        }
      }
    }
  }
  
  @Scheduled(fixedDelay = 30000)
  public void cleanUpNonSubscribedTopic() {
    
    logger.info("Looking for non-subscribed subscriptions to purge");
    
    List<Subscription> nonSubscribedSubcriptions = databaseService.getNonSubscribedSubscriptions();
    
    for(Subscription subscription : nonSubscribedSubcriptions) {
      
      if(databaseService.deleteSubscription(subscription)) {
        
        String topic = subscription.getTopic();
        String channel = subscription.getChannel();
        
        logger.info("Purged subscription [" + subscription.getId() + "] -> No subscribers for [" + channel + " <-> " + topic + "]");
        
        //Clean up any event under this subscription
        List<Event> deadEvents = databaseService.getSubscriptionEvents(subscription);
        
        if(deadEvents != null && !deadEvents.isEmpty()) {
          
          logger.info("Purging [" + deadEvents.size() + "] dead events");
          
          for(Event event : deadEvents) {
            
            logger.info("Flushing any undelivered events for [" + event.getId() + "]");
            List<UndeliveredEvent> undeliveredEvents = databaseService.getUndeliveredEvents(event);
            
            boolean allUndeliveredDeleteSuccess = true;
            
            for (UndeliveredEvent undeliveredEvent : undeliveredEvents) {

              if (databaseService.deleteUndeliveredEvent(undeliveredEvent))
                logger.info("Undelivered event [" + undeliveredEvent.getId() + "] purged");
              else
                allUndeliveredDeleteSuccess = false;
            }
            
            if (allUndeliveredDeleteSuccess) {

              boolean eventDeleteSuccess = databaseService.deleteEvent(event);

              if (eventDeleteSuccess) {
                logger.info("Event [" + event.getId() + "] purged");
              }
            }
          }
        }
      }
      else {
        logger.warning("Non-subscribed subscription failed to be purged -> " + subscription.getId());
      }
    }
  }
}

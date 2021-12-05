package com.xxx.management;

import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.xxx.model.Node;
import com.xxx.model.NodeHealth;
import com.xxx.service.DatabaseService;
import com.xxx.util.Requester;

@Component
@EnableScheduling
public class NodeManager {

  private static final Logger logger = Logger.getLogger(NodeManager.class.getSimpleName());
  
  @Value("${nodeHealth.max-attempt-count}")
  private int maxAttemptCount;
  
  @Autowired
  private DatabaseService databaseService;
  
  public void setDatabaseService(DatabaseService databaseService) {
    this.databaseService = databaseService;
  }
  
  @Scheduled(fixedDelay = 60000)
  public void checkNodes() {
    
    List<Node> nodes = databaseService.getNodes();
    
    for(Node node : nodes) {
      
      String endpoint = node.getEndpoint();
      boolean success = Requester.isActive(endpoint);
 
      if(success)
        logger.info("Node [" + node.getName() + "] is healthy");
      else
        logger.info("Node [" + node.getName() + "] is unhealthy");
      
      Optional<NodeHealth> optional = databaseService.getNodeHealth(node);
      NodeHealth nodeHealth = optional.orElse(new NodeHealth());
        
      if(nodeHealth.getAttemptCount() > maxAttemptCount) {
        
        logger.warning("Evicting node [" + node.getId() + "] -> Max attempts have been exceeded");
        
        if(databaseService.cascadeDeleteNode(node)) {
          
          logger.info("Node [" + node.getId() + "] has been evicted");
          continue;
        }
      }
      
      if (success && optional.isPresent()) {

        nodeHealth.setHealthy(true);
        nodeHealth.setLastSeen();
        nodeHealth.resetAttemptCount();
      } 
      else if (success && optional.isEmpty()) {

        nodeHealth.setHealthy(true);
        nodeHealth.setLastSeen();
        nodeHealth.setNodeId(node.getId());
      }
      else if (!success && optional.isPresent()) {

        nodeHealth.setHealthy(false);
        nodeHealth.incrementAttemptCount();
        nodeHealth.setLastAttempt();
      }
      else {

        nodeHealth.setHealthy(false);
        nodeHealth.setNodeId(node.getId());
        nodeHealth.incrementAttemptCount();
        nodeHealth.setLastAttempt();
      }
      
      databaseService.updateNodeHealth(node, nodeHealth);
    }
  }
  
  @Scheduled(fixedDelay = 1000)
  public void checkUnhealthyNodes() {
  
    List<Node> unhealthyNodes = databaseService.getUnhealthyNodes();
    
    for(Node node : unhealthyNodes) {
      
      String endpoint = node.getEndpoint();
      boolean success = Requester.isHealthy(endpoint);
      
      if(success) {
        
        logger.info("Node [" + node.getId() + "] is back online");
      }
      else {
        
        Optional<NodeHealth> oNodeHealth = databaseService.getNodeHealth(node);
        
        if(oNodeHealth.isPresent()) {
          
          NodeHealth nodeHealth = oNodeHealth.get();
          nodeHealth.incrementAttemptCount();
          nodeHealth.setLastSeen();
          
          databaseService.saveNodeHealth(nodeHealth);
        }
      }     
    }
  }
}

package com.xxx;

import java.util.Optional;
import com.xxx.model.Event;

public class App {

  public static void main(String[] args) {
    
    DatabaseService databaseService = new DatabaseService();
    
    Event event = new Event();
    event.setMessage("Hello World");
    
    boolean success = databaseService.insert(event);
    
    System.out.println("Insert success was " + success);
    
    Optional<Event> result = databaseService.getEventById(event.getId());
    
    if(result.isPresent())
      System.out.println(result.get());
  }
}

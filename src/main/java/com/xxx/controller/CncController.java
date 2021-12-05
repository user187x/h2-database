package com.xxx.controller;

import java.util.Arrays;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.xxx.service.DatabaseService;

@RestController
public class CncController {

  private Gson gson = new GsonBuilder().setPrettyPrinting().create();
  
  @Autowired
  private DatabaseService databaseService;
  
  @RequestMapping(value = "/db/{value}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> exposeOnDbGUI(@PathVariable String value) {

    boolean enable = Arrays.asList("on","enable","true").stream()
    .map(String::toLowerCase)
    .anyMatch(v -> v.equalsIgnoreCase(value));
    
    if(enable) {
      
      if(databaseService.isGuiActive()) {
      
        JsonObject response = new JsonObject();
        response.addProperty("activated", true);
        response.addProperty("user", databaseService.getUser());
        response.addProperty("password", databaseService.getPassword());
        response.addProperty("path", "http://localhost:" + databaseService.getDatbaseGuiPort());
        
        return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(response));
      }
      else {
        
        databaseService.startGUIServer();
        
        JsonObject response = new JsonObject();
        response.addProperty("activated", true);
        response.addProperty("user", databaseService.getUser());
        response.addProperty("password", databaseService.getPassword());
        response.addProperty("path", "http://localhost:" + databaseService.getDatbaseGuiPort());
        
        return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(response));
      }
    }
    else {
        
      if(databaseService.isGuiActive()) {
        
        databaseService.stopGUIServer();
        
        JsonObject response = new JsonObject();
        response.addProperty("activated", false);
        
        return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(response));
      }
      else {
        
        JsonObject response = new JsonObject();
        response.addProperty("activated", false);
        
        return ResponseEntity.status(HttpStatus.OK).body(gson.toJson(response));
      }
    }
  }
}

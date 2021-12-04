package com.xxx.config;

import java.util.logging.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Initializer {

  private static final Logger logger = Logger.getLogger(Initializer.class.getSimpleName());
  
  @Autowired
  private ApplicationContext context;
}
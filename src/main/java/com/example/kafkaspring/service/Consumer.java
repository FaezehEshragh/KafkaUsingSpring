package com.example.kafkaspring.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
  @KafkaListener(topics="my-topic",groupId = "myGroup")
  public void consumeFromTopic(String message){
    System.out.println("Consumed Message by first endpoint: "+message);
  }
}

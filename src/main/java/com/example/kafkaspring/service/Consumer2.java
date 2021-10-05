package com.example.kafkaspring.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer2 {
  @KafkaListener(topics="my-topic")
  public void consumeFromTopic(String message){
    System.out.println("Consumed Message by second endpoint: "+message);
  }
}

package com.example.kafkaspring.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {
  @Autowired
  private KafkaTemplate<String, String> kafkaTemp;

  @Autowired
  private DynamicKafkaConsumerService dynamicKafkaConsumerService;

  public void publishToTopic(String message){
    String[] parts = message.split(":", 2);
    if (parts.length < 2) {
      System.out.println("Invalid message format. Expected format: 'topic:message'");
      return;
    }
    String dynamicTopic = parts[0];
    String actualMessage = parts[1];
    System.out.println("Publishing to topic " + dynamicTopic);
    this.kafkaTemp.send(dynamicTopic, actualMessage);
    dynamicKafkaConsumerService.startConsumer(dynamicTopic);
  }
}

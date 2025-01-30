package com.example.kafkaspring.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

@Service
public class DynamicKafkaConsumerService {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    private final Set<String> activeTopics = new HashSet<>();

    public void startConsumer(String topicName) {
        if (activeTopics.contains(topicName)) {
            System.out.println("Consumer for topic " + topicName + " is already running.");
            return;
        }

        ContainerProperties containerProperties = new ContainerProperties(topicName);
        containerProperties.setMessageListener((MessageListener<String, String>) this::consumeRecord);


        ConcurrentMessageListenerContainer<String, String> container =
            new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setBeanName(topicName);
        container.start();

        registry.getListenerContainer(container.getListenerId());
        activeTopics.add(topicName);
    }

    public void stopConsumer(String topicName) {
        MessageListenerContainer container = registry.getListenerContainer(topicName);
        if (container != null) {
            container.stop();
            activeTopics.remove(topicName);
            System.out.println("Stopped consumer for topic: " + topicName);
        }
    }

    private void consumeRecord(ConsumerRecord<String, String> record) {
        System.out.println("Received message: " + record.value() + " from topic: " + record.topic());
        System.out.println("Consuming the record:" + record.value() + " from topic: " + record.topic());
        System.out.println("Saved record in db");
        System.out.printf("Sent record to WCar");
    }
}

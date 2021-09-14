package com.learn.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

public class ConsumerThread extends Thread {
	
	private String topicName = null;
	
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:9092");
        props.put(
          ConsumerConfig.GROUP_ID_CONFIG,  "test-group");
        props.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        props.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }
    
	
	public void run() {
		System.out.println("ConsumerThread.run() started for topic "+ topicName);
		
		ContainerProperties containerProperties = new ContainerProperties(topicName);
		containerProperties.setMessageListener(
		                (MessageListener<String, String>) record -> System.out.println("ConsumerThread.run()" + record.value()));

		ConcurrentMessageListenerContainer container =
		        new ConcurrentMessageListenerContainer<>(
		        		consumerFactory(),
		                containerProperties);

		container.start();
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
}
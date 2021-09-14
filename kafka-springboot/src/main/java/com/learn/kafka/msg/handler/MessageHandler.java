package com.learn.kafka.msg.handler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component("MessageHandler")
public class MessageHandler {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String topicName, String msg) {
		kafkaTemplate.send(topicName, msg);

	}
	
	public void getMessage(String topicName, String msg) {
		kafkaTemplate.send(topicName, msg);
	}

	

}

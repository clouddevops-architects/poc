package com.learn.kafka.api;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.learn.kafka.consumer.ConsumerThread;
import com.learn.kafka.msg.handler.MessageHandler;

@RestController()
@RequestMapping("/KafkaService")
public class MessageController {
	
	@Autowired
	KafkaAdmin kafkaAdmin;
	
	@Autowired
	AdminClient adminClient;
	
	@Autowired
	MessageHandler handler; 
	
	@PostMapping("createTopic")
	public ResponseEntity<String> createTopic(@RequestBody String jsonStr) throws JsonMappingException, JsonProcessingException {
		JSONObject jsonObject = new JSONObject(jsonStr);
		String topicNmae = (String) jsonObject.get("topicName");
		System.out.println("MessageController.createTopic() kafkaAdmin: "+kafkaAdmin);
		kafkaAdmin.createOrModifyTopics(new NewTopic(topicNmae, 1, (short)1));
		
		ConsumerThread consumerThread = new ConsumerThread();
		consumerThread.setTopicName(topicNmae);

		consumerThread.start();
		
		return new ResponseEntity<String>(topicNmae +" Created Successfully", HttpStatus.OK);
	}
	
	@PostMapping("addMessage")
	public ResponseEntity<String> addMessage(@RequestBody String jsonStr) throws JsonMappingException, JsonProcessingException {
		JSONObject jsonObject = new JSONObject(jsonStr);
		handler.sendMessage(jsonObject.getString("topicName"), jsonObject.getString("message"));
		return new ResponseEntity<String>("Message Added Successfully", HttpStatus.OK);
	}
	
	@PostMapping("getMessages")
	public ResponseEntity<String> getMessages(@RequestBody String jsonStr) throws JsonMappingException, JsonProcessingException {
		JSONObject jsonObject = new JSONObject(jsonStr);
		handler.sendMessage(jsonObject.getString("topicName"), jsonObject.getString("message"));
		return new ResponseEntity<String>("Message Added Successfully", HttpStatus.OK);
	}

	
	@GetMapping("topicList")
	public ResponseEntity<String> topicList() throws JsonMappingException, JsonProcessingException, InterruptedException, ExecutionException {
		return new ResponseEntity<String>(adminClient.listTopics().names().get().toString() , HttpStatus.OK);
	}
	
	@GetMapping("test")
	public ResponseEntity<String> test() {
		return new ResponseEntity<>("Working...", HttpStatus.OK);
	}
	
}

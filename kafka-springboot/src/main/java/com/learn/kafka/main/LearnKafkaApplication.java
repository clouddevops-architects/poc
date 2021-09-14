package com.learn.kafka.main;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = { "com" })
public class LearnKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(LearnKafkaApplication.class, args);
	}

}

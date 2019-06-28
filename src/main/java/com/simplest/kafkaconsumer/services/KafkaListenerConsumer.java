package com.simplest.kafkaconsumer.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaListenerConsumer {
	private final Logger logger = LoggerFactory.getLogger(KafkaListenerConsumer.class);

	@KafkaListener(topics = "test")
	public void consume(String message) {
		logger.info(String.format("Consumed Message using KafkaListener -> %s", message));
	}
}
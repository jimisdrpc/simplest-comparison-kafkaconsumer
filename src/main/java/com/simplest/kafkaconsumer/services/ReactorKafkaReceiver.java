package com.simplest.kafkaconsumer.services;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

@Service
public class ReactorKafkaReceiver {

	private static final Logger logger = LoggerFactory.getLogger(ReactorKafkaReceiver.class.getName());

	private KafkaReceiver kafkaReceiver;

	public ReactorKafkaReceiver() {

		final Map<String, Object> consumerProps = new HashMap<>();
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "test");
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id-reactor-kafkareceiver");
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		ReceiverOptions<Object, Object> consumerOptions = ReceiverOptions.create(consumerProps)
				.subscription(Collections.singleton("test"))
				.addAssignListener(partitions -> logger.debug("onPartitionsAssigned {}", partitions))
				.addRevokeListener(partitions -> logger.debug("onPartitionsRevoked {}", partitions));

		kafkaReceiver = KafkaReceiver.create(consumerOptions);

		((Flux<ReceiverRecord>) kafkaReceiver.receive()).doOnNext(r -> {
			logger.info(String.format("Consumed Message using KafkaListener -> %s", r.value()));
			r.receiverOffset().acknowledge();
		}).subscribe();
	}

}

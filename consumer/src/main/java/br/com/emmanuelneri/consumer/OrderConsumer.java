package br.com.emmanuelneri.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "${kafka.order.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumer(final ConsumerRecord<String, br.com.emmanuelneri.schema.orders.Order> consumerRecord) {
        log.info("key: " + consumerRecord.key());
        log.info("Headers: " + consumerRecord.headers());
        log.info("Partition: " + consumerRecord.partition());
        log.info("Order: " + consumerRecord.value());
    }
}
package br.com.emmanuelneri.producer.component;

import br.com.emmanuelneri.schema.orders.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class OrderProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderProducer.class);

    @Value("${kafka.order.topic}")
    private String orderTopic;

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducer(final KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(final Order order) {
        kafkaTemplate.send(orderTopic, order.getIdentifier().toString(), order).addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(final Throwable throwable) {
                LOGGER.error("fail to send kafka message " + order.getIdentifier(), throwable);
            }

            @Override
            public void onSuccess(final SendResult<String, Order> objectObjectSendResult) {
                LOGGER.info(String.format("message %s sent to %s topic", order.getIdentifier(), orderTopic));
            }
        });
    }

}


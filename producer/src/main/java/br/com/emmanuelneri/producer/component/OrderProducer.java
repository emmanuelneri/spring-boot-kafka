package br.com.emmanuelneri.producer.component;

import br.com.emmanuelneri.producer.OrderRequest;
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

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OrderProducer(final KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(final OrderRequest order) {
        kafkaTemplate.send(orderTopic, order.getIdentifier(), order).addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(final Throwable throwable) {
                LOGGER.error("fail on send kafka message " + order.getIdentifier(), throwable);
            }

            @Override
            public void onSuccess(final SendResult<Object, Object> objectObjectSendResult) {
                LOGGER.info(String.format("message %s sent to %s topic", order.getIdentifier(), orderTopic));
            }
        });
    }

}


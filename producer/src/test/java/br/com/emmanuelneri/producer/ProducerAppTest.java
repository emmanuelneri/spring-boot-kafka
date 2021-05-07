package br.com.emmanuelneri.producer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "spring.profiles.active=test")
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class ProducerAppTest {

    private static final String CONSUMER_GROUP = "test-group";

    @LocalServerPort
    int randomPort;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${kafka.order.topic}")
    private String orderTopic;

    @Test
    public void shouldProduceMessageWhenRequestAPI() {
        final RestTemplate restTemplate = new RestTemplate();
        final String url = String.format("http://localhost:%d/orders", randomPort);

        final OrderRequest orderRequest = new OrderRequest("123", "customer", BigDecimal.ZERO);
        final ResponseEntity response = restTemplate.postForEntity(url, orderRequest, ResponseEntity.class);

        Assertions.assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());

        final Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(CONSUMER_GROUP, "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final JsonDeserializer<OrderRequest> orderRequestJsonDeserializer = new JsonDeserializer<>();
        orderRequestJsonDeserializer.addTrustedPackages("br.com.emmanuelneri.producer");

        final Consumer<String, OrderRequest> consumer = new DefaultKafkaConsumerFactory<>(consumerProps,
                new StringDeserializer(), orderRequestJsonDeserializer)
                .createConsumer();

        consumer.subscribe(Collections.singletonList(orderTopic));

        final ConsumerRecord<String, OrderRequest> singleRecord = KafkaTestUtils.getSingleRecord(consumer, orderTopic);
        Assertions.assertNotNull(singleRecord);
        Assertions.assertEquals(singleRecord.key(), "123");
    }
}

package br.com.emmanuelneri.producer.controller;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "spring.profiles.active=test")
@Testcontainers
public class ProducerAppTest {

    private static final String CONSUMER_GROUP = "test-group";

    private static final String SCHEMA_REGISTRY_SERVICE_NAME = "schema-registry_1";
    private static final String KAFKA_SERVICE_NAME = "kafka_1";

    private static final int KAFKA_PORT = 9092;
    private static final int SCHEMA_REGISTRY_PORT = 8081;

    @LocalServerPort
    int randomPort;

    @Container
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("../docker-compose.yml"))
                    .withExposedService(KAFKA_SERVICE_NAME, KAFKA_PORT, Wait.forListeningPort())
                    .withExposedService("zookeeper_1", 2181, Wait.forListeningPort())
                    .withExposedService(SCHEMA_REGISTRY_SERVICE_NAME, SCHEMA_REGISTRY_PORT, Wait.forListeningPort());

    @DynamicPropertySource
    static void kafkaProperties(final DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.properties.bootstrap-servers", ProducerAppTest::getKafkaBroker);
        registry.add("spring.kafka.properties.schema.registry.url", ProducerAppTest::getSchemaRegistry);
    }

    @Value("${kafka.order.topic}")
    private String orderTopic;

    @Test
    public void shouldProduceMessageWhenRequestAPI() {
        final RestTemplate restTemplate = new RestTemplate();
        final String url = String.format("http://localhost:%d/orders", randomPort);

        final OrderRequest orderRequest = new OrderRequest("123", "customer", BigDecimal.ZERO);
        final ResponseEntity response = restTemplate.postForEntity(url, orderRequest, ResponseEntity.class);

        Assertions.assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());

        final String kafkaBroker = getKafkaBroker();
        final String schemaRegistry = getSchemaRegistry();

        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.put("schema.registry.url", schemaRegistry);

        final Consumer<String, Object> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(orderTopic));

        final LocalDateTime timeout = LocalDateTime.now().plusMinutes(1);

        int total = 0;
        while (LocalDateTime.now().isBefore(timeout)) {
            if (total > 0) {
                return;
            }

            final ConsumerRecords<String, Object> consumerRecords = consumer.poll(Duration.ofMillis(100L));

            for (final ConsumerRecord<String, Object> consumerRecord : consumerRecords) {
                Assertions.assertNotNull(consumerRecord);
                Assertions.assertEquals(consumerRecord.topic(), orderTopic);
                Assertions.assertEquals(consumerRecord.key(), "123");
                total++;
            }
        }
        consumer.close();
        Assertions.assertEquals(1, total);
    }

    private static String getKafkaBroker() {
        return environment.getServiceHost(KAFKA_SERVICE_NAME, KAFKA_PORT) + ":" + KAFKA_PORT;
    }

    private static String getSchemaRegistry() {
        return "http://" + environment.getServiceHost(SCHEMA_REGISTRY_SERVICE_NAME, SCHEMA_REGISTRY_PORT) + ":" + SCHEMA_REGISTRY_PORT;
    }
}

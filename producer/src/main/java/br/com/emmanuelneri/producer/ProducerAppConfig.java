package br.com.emmanuelneri.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class ProducerAppConfig {

    public static void main(String[] args) {
        SpringApplication.run(ProducerAppConfig.class, args);
    }

    @Bean
    @ConditionalOnProperty(value = "spring.kafka.admin.fail-fast", havingValue = "true")
    public NewTopic buildTopicOnStartup(@Value("${order.topic}") final String orderTopic) {
        return TopicBuilder.name(orderTopic).build();
    }
}
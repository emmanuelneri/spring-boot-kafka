# spring-boot-kafka

---- 

This project is an example of using Kafka with Spring Boot.

1. Start Kafka environment with Docker

`docker-compose up`

2. Start Spring Boot services

- Consumer: `mvn -f consumer/pom.xml spring-boot:run`
- Producer: `mvn -f producer/pom.xml spring-boot:run`


3. Send data to produce/consume in Kafka
- Use `./send-orders` script to request producer service
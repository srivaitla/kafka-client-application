package com.nord.kafka.client.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaClientStarter {

    public static void main(String[] args) {
        SpringApplication.run(KafkaClientStarter.class, args);
    }

}

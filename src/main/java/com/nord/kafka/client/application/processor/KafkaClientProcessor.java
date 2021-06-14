package com.nord.kafka.client.application.processor;

import com.nord.kafka.client.application.producer.KafkaClientProducer;
import com.nord.kafka.consumer.dto.ConsumerNotificationRequest;
import com.nord.kafka.consumer.dto.ConsumerProfileRequest;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaClientProcessor {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClientProcessor.class);

    @Autowired
    private KafkaClientProducer producer;

    private static final String LOG_NOTIFICATION = "Processor-Notification";
    private static final String LOG_PROFILE = "Processor-Profile";

    public void process(String topicName, ConsumerNotificationRequest request, List<Header> headers) {
        LOGGER.info(LOG_NOTIFICATION + " ----- ----- Started : " + request + "\n");

        producer.publishToTopic(topicName, request, headers, request.getNotificationId().toString(), "-Notification");

        LOGGER.info(LOG_NOTIFICATION + " ----- ----- Completed : " + request + "\n");
    }

    public void process(String topicName, ConsumerProfileRequest request, List<Header> headers) {
        LOGGER.info(LOG_PROFILE + " ----- ----- Started : " + request + "\n");

        producer.publishToTopic(topicName, request, headers, request.getProfileId().toString(), "-Profile");

        LOGGER.info(LOG_PROFILE + " ----- ----- Completed : " + request + "\n");
    }
}

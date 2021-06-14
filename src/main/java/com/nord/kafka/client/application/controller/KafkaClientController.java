package com.nord.kafka.client.application.controller;

import com.nord.kafka.client.application.processor.KafkaClientProcessor;
import com.nord.kafka.consumer.dto.ConsumerNotificationRequest;
import com.nord.kafka.consumer.dto.ConsumerProfileRequest;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@RestController
public class KafkaClientController {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClientController.class);

    @Autowired
    private KafkaClientProcessor processor;

    @Value("${kafka.topic.name.notification}")
    private String topicNameNotification;

    @Value("${kafka.topic.name.profile}")
    private String topicNameProfile;

    private static final String LOG_NOTIFICATION = "Controller-Notification";
    private static final String LOG_PROFILE = "Controller-Profile";

    @PostMapping(value = "/consumer/notification",
            consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public String consumerNotification(@RequestBody ConsumerNotificationRequest request,
                                       @RequestParam(name = "id") String id, @RequestParam(name = "appId") String appId) {
        LOGGER.info(LOG_NOTIFICATION + " ----- ----- Started\n");
        LOGGER.info(request + "\n");

        try {
            processor.process(topicNameNotification, request, buildHeaders(id, appId));
        } catch (Exception ex) {
            LOGGER.info(LOG_NOTIFICATION + " ----- ----- " + ex.getMessage() + "\n\n\n");
            return request.toString();
        }

        LOGGER.info(LOG_NOTIFICATION + " ----- ----- Completed\n\n\n");
        return request.toString();
    }

    @PostMapping(value = "/consumer/profile",
            consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public String consumerProfile(@RequestBody ConsumerProfileRequest request,
                                  @RequestParam(name = "id") String id, @RequestParam(name = "appId") String appId) {
        LOGGER.info(LOG_PROFILE + " ----- ----- Started\n");
        LOGGER.info(request + "\n");

        try {
            processor.process(topicNameProfile, request, buildHeaders(id, appId));
        } catch (Exception ex) {
            LOGGER.info(LOG_PROFILE + " ----- ----- " + ex.getMessage() + "\n\n\n");
            return request.toString();
        }

        LOGGER.info(LOG_PROFILE + " ----- ----- Completed\n\n\n");
        return request.toString();
    }


    private List<Header> buildHeaders(String id, String appId) {
        final List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("ID", id.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("AppId", appId.getBytes(StandardCharsets.UTF_8)));
        return headers;
    }
}

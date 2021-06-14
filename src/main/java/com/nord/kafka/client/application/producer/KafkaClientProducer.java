package com.nord.kafka.client.application.producer;

import com.nord.kafka.client.application.util.RetryClientLogUtil;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaClientProducer {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClientProducer.class);

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    private final RetryClientLogUtil logUtility;

    private static final String LOG = "Producer-";

    @Autowired
    public KafkaClientProducer(KafkaTemplate<String, SpecificRecordBase> kafkaTemplate,
                               RetryClientLogUtil logUtility) {
        this.kafkaTemplate = kafkaTemplate;
        this.logUtility = logUtility;
    }

    public void publishToTopic(String topicName, SpecificRecordBase request, List<Header> headers, String requestId, String logName) {
        final ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(topicName, null, requestId, request, headers);
        LOGGER.info(LOG + logName + " ----- ----- Started : " + logUtility.logProducerRecord(record) + "\n");
        try {
            kafkaTemplate.send(record);
            LOGGER.info(LOG + logName + " ----- ----- Completed : " + logUtility.logProducerRecord(record) + "\n\n\n");
        } catch (Exception ex) {
            LOGGER.info(LOG + logName + " ----- ----- Exception : " + logUtility.logProducerRecord(record, ex) + "\n\n\n");
            throw ex;
        }
    }
}

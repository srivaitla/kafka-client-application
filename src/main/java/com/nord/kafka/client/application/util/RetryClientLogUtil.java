package com.nord.kafka.client.application.util;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@Component
public class RetryClientLogUtil {

    public String logProducerRecord(ProducerRecord<String, SpecificRecordBase> record) {
        return "ProducerRecord[Topic=" + record.topic() + ", " + record.value()
                + ", Headers=" + getHeadersAsString(record.headers()) + "]";
    }

    public String logProducerRecord(ProducerRecord<String, SpecificRecordBase> record, Exception ex) {
        return logProducerRecord(record) + ", Exception= " + Arrays.toString(ex.getStackTrace());
    }

    public String getHeadersAsString(Headers recordHeaders) {
        final StringBuilder headersBuilder = new StringBuilder().append('{');
        for (Header header : recordHeaders) {
            headersBuilder.append(header.key()).append('=').append(new String(header.value(), StandardCharsets.UTF_8)).append("; ");
        }
        headersBuilder.append('}');
        return headersBuilder.toString();
    }

}

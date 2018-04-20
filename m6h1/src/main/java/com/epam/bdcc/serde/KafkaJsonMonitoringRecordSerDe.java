package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMonitoringRecordSerDe.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final String value = mapper.writeValueAsString(data);
            return value.getBytes();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        MonitoringRecord record = null;
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final MonitoringRecord monitoringRecord = mapper.readValue(new String(data), MonitoringRecord.class);
            return monitoringRecord;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {
    }
}
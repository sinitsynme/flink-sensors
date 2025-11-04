package ru.sinitsynme.sensors.alert;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class AlertValueSerializationSchema implements SerializationSchema<AlertEvent> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(AlertEvent element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            System.err.println("Couldn't serialize value: " + e);
            return null;
        }
    }
}

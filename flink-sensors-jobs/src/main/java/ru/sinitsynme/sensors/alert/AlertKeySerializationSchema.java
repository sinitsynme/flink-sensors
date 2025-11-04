package ru.sinitsynme.sensors.alert;

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;

public class AlertKeySerializationSchema implements SerializationSchema<AlertEvent> {

    @Override
    public byte[] serialize(AlertEvent element) {
        return element.machineId().toString().getBytes(StandardCharsets.UTF_8);
    }
}

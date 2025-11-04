package ru.sinitsynme.sensors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class SensorMetricMappingFunction implements MapFunction<String, SensorMetric> {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public SensorMetric map(String value) throws Exception {
        try {
            return objectMapper.readValue(value, SensorMetric.class);
        } catch (Exception e) {
            System.err.println("Failed to parse JSON: " + value);
            return null;
        }

    }
}

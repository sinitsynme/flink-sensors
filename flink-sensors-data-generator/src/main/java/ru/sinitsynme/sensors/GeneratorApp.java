package ru.sinitsynme.sensors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.sinitsynme.util.AppProperties;

import java.security.SecureRandom;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static ru.sinitsynme.sensors.SensorType.*;

public class GeneratorApp {
    private static final Random random = new SecureRandom();
    private static final AppProperties appProperties = AppProperties.getInstance();
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public static final UUID BROKEN_SENSOR_ID_1 = UUID.fromString("08ec7162-91df-4afa-a90b-7ee834af276a");
    public static final UUID BROKEN_SENSOR_ID_2 = UUID.fromString("2eba0ee5-7650-4139-b0f1-725a782e1ea8");
    public static final List<UUID> BROKEN_SENSOR_IDS = List.of(BROKEN_SENSOR_ID_1, BROKEN_SENSOR_ID_2);

    public static void main(String[] args) {
        List<Sensor> sensors = generateSensors();
        SensorMetricGenerator metricGenerator = new SensorMetricGenerator(random, appProperties);

        Properties producerProps = arrangeKafkaProducerProperties();
        var sensorMetricTopic = appProperties.getProperty("kafka.metrics-data.topic");
        var sleepDurationMs = appProperties.getProperty("app.sleep-duration-ms");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps)) {
            System.out.println("Started generation");
            while (true) {
                sensors.forEach(sensor -> {
                    var metric = metricGenerator.generate(sensor.sensorId(), sensor.machineId(), sensor.type());
                    try {
                        String key = sensor.sensorId() + "-" + sensor.type().name();
                        var payload = objectMapper.writeValueAsString(metric);
                        var record = new ProducerRecord<>(sensorMetricTopic, key, payload);
                        kafkaProducer.send(record).get();
                        System.out.println("Sent: " + payload);
                    } catch (Exception e) {
                        System.err.println("Something went wrong during sending: " + e.getMessage());
                    }
                });
                Thread.sleep(Long.parseLong(sleepDurationMs));
            }
        } catch (Exception e) {
            System.err.println("Something went wrong: " + e);
        }
    }

    private static List<Sensor> generateSensors() {
        var machineId1 = UUID.fromString("db804a74-01d3-7851-8c20-48441167cc4e");
        var machineId2 = UUID.fromString("90f5cebb-96b5-4a09-9fb3-d9492f8600ed");
        var machineId3 = UUID.fromString("db5ea46e-7336-40c5-a181-22b1b99e6f92");
        var machineId4 = UUID.fromString("75302036-61d5-4a56-af0b-884368999734");

        var sensor1tempId = UUID.fromString("fe725846-9679-4951-8a14-2b01151175e4");
        var sensor1presId = UUID.fromString("076df5f3-6454-4294-a4b7-c2234abb3dc3");
        var sensor1vibId = UUID.fromString("bb3cfc98-f554-4e95-8521-5a9e53c3c777");
        var sensor2tempId = UUID.fromString("1264e1a0-af87-4c5e-98fc-a5ce9beb7be6");
        var sensor2presId = UUID.fromString("9583d087-af80-4bb5-8f45-03bf5b84ff10");
        var sensor3tempId = UUID.fromString("7529d04e-f33f-42f5-999f-7e895df1c987");
        var sensor3vibId = UUID.fromString("30c8c8b0-7f1b-4e19-b03a-0fc5e147d5ec");
        var sensor4tempId = UUID.fromString("30aafb95-4d13-4b9a-b63a-883b6ba71d39");
        var sensor4presId = UUID.fromString("0b398019-cafb-4673-91cf-6f97e1bbb4e2");
        var sensor4vibId = UUID.fromString("f0cadcfb-02b6-465c-bb24-51aa76a2e3ab");

        return List.of(
                new Sensor(sensor1tempId, machineId1, TEMPERATURE),
                new Sensor(sensor1presId, machineId1, PRESSURE),
                new Sensor(sensor1vibId, machineId1, VIBRATION),

                new Sensor(sensor2tempId, machineId2, TEMPERATURE),
                new Sensor(sensor2presId, machineId2, PRESSURE),
                new Sensor(BROKEN_SENSOR_ID_2, machineId2, VIBRATION),

                new Sensor(sensor3tempId, machineId3, TEMPERATURE),
                new Sensor(BROKEN_SENSOR_ID_1, machineId3, PRESSURE),
                new Sensor(sensor3vibId, machineId3, VIBRATION),

                new Sensor(sensor4tempId, machineId4, TEMPERATURE),
                new Sensor(sensor4presId, machineId4, PRESSURE),
                new Sensor(sensor4vibId, machineId4, VIBRATION)
        );
    }

    private static Properties arrangeKafkaProducerProperties() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", appProperties.getProperty("kafka.demo-cluster.bootstrap-servers"));
        producerProps.put("key.serializer", appProperties.getProperty("kafka.sensor-metrics-data.key-serializer"));
        producerProps.put("value.serializer", appProperties.getProperty("kafka.sensor-metrics-data.value-serializer"));
        producerProps.put("delivery.timeout.ms", "1000");
        producerProps.put("request.timeout.ms", "1000");
        producerProps.put("auto.offset.reset", "earliest");
        return producerProps;
    }
}
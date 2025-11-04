package ru.sinitsynme.sensors.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import ru.sinitsynme.sensors.*;
import ru.sinitsynme.sensors.alert.*;
import ru.sinitsynme.util.AppProperties;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest;
import static ru.sinitsynme.sensors.SensorType.TEMPERATURE;

public class IoTMonitoringJob {

    private static final String SENSOR_METRICS_DATA_SOURCE_NAME = "Sensor metrics data source";
    private static final String ALERT_DATA_SINK_NAME = "Sensor alerts data sink";
    private static final String IOT_SENSOR_MONITORING_JOB_NAME = "IoTMonitoringJob";

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    private static final AppProperties appProperties = AppProperties.getInstance();


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int checkpointingInterval = Integer.parseInt(appProperties.getProperty("iot-monitoring-job.checkpoint-interval-ms"));
        env.enableCheckpointing(checkpointingInterval);

        int maxParallelism = Integer.parseInt(appProperties.getProperty("iot-monitoring-job.max-parallelism"));

        SensorMetricsSourceProperties sourceProperties = new SensorMetricsSourceProperties(appProperties);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(sourceProperties.getBootstrapServers())
                .setTopics(sourceProperties.getTopic())
                .setGroupId(sourceProperties.getGroupId())
                .setStartingOffsets(earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        int watermarkSeconds = Integer.parseInt(appProperties.getProperty("iot-monitoring-job.watermark-seconds"));
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(watermarkSeconds)),
                SENSOR_METRICS_DATA_SOURCE_NAME
        );

        DataStream<SensorMetric> sensorMetricsData = kafkaStream
                .map(sensorMetricsMappingFunction())
                .filter(Objects::nonNull);

        DataStream<SensorMetric> averageMetrics = sensorMetricsData
                .keyBy(metric -> metric.machineId() + "-" + metric.type())
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new AverageMetricAggregate())
                .setParallelism(maxParallelism);

        DataStream<String> averageMetricsText = averageMetrics
                .map(avg -> String.format(
                        "AVG: Machine=%s, sensor=%s Type=%s, Average=%.2f, Timestamp=%s",
                        avg.machineId(), avg.sensorId(), avg.type(), avg.value(), avg.timestamp()
                ));

        double temperatureAlertThreshold = appProperties.getDoubleProperty("iot-monitoring-job.temperature-alert-threshold");
        DataStream<AlertEvent> temperatureAlerts = averageMetrics
                .filter(sensorMetric -> sensorMetric.type() == TEMPERATURE)
                .keyBy(metric -> metric.machineId() + "-" + metric.type())
                .process(new AlertFunction(temperatureAlertThreshold));

        DataStream<String> healthScores = sensorMetricsData
                .keyBy(SensorMetric::machineId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
                .process(new HealthScoreProcessFunction());

        averageMetricsText.print().name("Average Readings");
        temperatureAlerts.print().name("Temperature Alerts");
        healthScores.print().name("Health Scores");

        AlertsSinkProperties alertsSinkProperties = new AlertsSinkProperties(appProperties);
        KafkaSink<AlertEvent> alertSink = KafkaSink.<AlertEvent>builder()
                .setBootstrapServers(alertsSinkProperties.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(alertsSinkProperties.getTopic())
                        .setKeySerializationSchema(new AlertKeySerializationSchema())
                        .setValueSerializationSchema(new AlertValueSerializationSchema())
                        .build()
                ).build();

        temperatureAlerts.sinkTo(alertSink).name(ALERT_DATA_SINK_NAME);
        env.execute(IOT_SENSOR_MONITORING_JOB_NAME);
    }

    private static MapFunction<String, SensorMetric> sensorMetricsMappingFunction() {
        return value -> {
            try {
                return objectMapper.readValue(value, SensorMetric.class);
            } catch (Exception e) {
                System.err.println("Failed to parse JSON: " + value);
                return null;
            }
        };
    }
}

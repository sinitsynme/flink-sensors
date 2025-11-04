package ru.sinitsynme.sensors.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.prometheus.sink.PrometheusSink;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import ru.sinitsynme.sensors.*;
import ru.sinitsynme.sensors.alert.AlertEvent;
import ru.sinitsynme.sensors.alert.AlertFunction;
import ru.sinitsynme.sensors.alert.AlertKeySerializationSchema;
import ru.sinitsynme.sensors.alert.AlertValueSerializationSchema;
import ru.sinitsynme.sensors.average.AverageMetricAggregate;
import ru.sinitsynme.sensors.average.AverageMetricPrometheusMappingFunction;
import ru.sinitsynme.sensors.healthscore.HealthScoreProcessFunction;
import ru.sinitsynme.util.AppProperties;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.latest;
import static ru.sinitsynme.sensors.SensorType.TEMPERATURE;

public class IoTMonitoringJob {

    private static final String SENSOR_METRICS_DATA_SOURCE_NAME = "Sensor metrics data source";
    private static final String ALERT_DATA_SINK_NAME = "Sensor alerts data sink";
    private static final String PROMETHEUS_AVG_METRIC_SINK_NAME = "Prometheus average metrics sink";
    private static final String IOT_SENSOR_MONITORING_JOB_NAME = "IoTMonitoringJob";

    private static final AppProperties appProperties = AppProperties.getInstance();


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int checkpointingInterval = Integer.parseInt(appProperties.getProperty("iot-monitoring-job.checkpoint-interval-ms"));
        env.enableCheckpointing(checkpointingInterval);

        int maxParallelism = Integer.parseInt(appProperties.getProperty("iot-monitoring-job.max-parallelism"));

        DataStream<String> kafkaStream = getSourceKafkaStream(env);
        DataStream<SensorMetric> sensorMetricsData = kafkaStream
                .map(new SensorMetricMappingFunction())
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

        DataStream<PrometheusTimeSeries> prometheusAverageMetrics = averageMetrics
                .map(new AverageMetricPrometheusMappingFunction());

        double temperatureAlertThreshold = appProperties.getDoubleProperty("iot-monitoring-job.temperature-alert-threshold");
        DataStream<AlertEvent> temperatureAlerts = averageMetrics
                .filter(sensorMetric -> sensorMetric.type() == TEMPERATURE)
                .keyBy(metric -> metric.machineId() + "-" + metric.type())
                .process(new AlertFunction(temperatureAlertThreshold));

        DataStream<String> healthScores = averageMetrics
                .keyBy(SensorMetric::machineId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(30)))
                .process(new HealthScoreProcessFunction());

        averageMetricsText.print().name("Average Metrics");
        temperatureAlerts.print().name("Temperature Alerts");
        healthScores.print().name("Health Scores");

        KafkaSink<AlertEvent> alertSink = getAlertEventKafkaSink();
        PrometheusSink prometheusSink = getPrometheusSink();

        temperatureAlerts.sinkTo(alertSink).name(ALERT_DATA_SINK_NAME);
        prometheusAverageMetrics.sinkTo(prometheusSink).name(PROMETHEUS_AVG_METRIC_SINK_NAME);
        env.execute(IOT_SENSOR_MONITORING_JOB_NAME);
    }

    private static PrometheusSink getPrometheusSink() {
        PrometheusSinkProperties prometheusSinkProperties = new PrometheusSinkProperties(appProperties);
        return (PrometheusSink) PrometheusSink.builder()
                .setPrometheusRemoteWriteUrl(prometheusSinkProperties.getPrometheusPushGatewayUrl())
                .build();
    }

    private static KafkaSink<AlertEvent> getAlertEventKafkaSink() {
        AlertsSinkProperties alertsSinkProperties = new AlertsSinkProperties(appProperties);
        return KafkaSink.<AlertEvent>builder()
                .setBootstrapServers(alertsSinkProperties.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(alertsSinkProperties.getTopic())
                        .setKeySerializationSchema(new AlertKeySerializationSchema())
                        .setValueSerializationSchema(new AlertValueSerializationSchema())
                        .build()
                ).build();
    }

    private static DataStream<String> getSourceKafkaStream(StreamExecutionEnvironment env) {
        SensorMetricsSourceProperties sourceProperties = new SensorMetricsSourceProperties(appProperties);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(sourceProperties.getBootstrapServers())
                .setTopics(sourceProperties.getTopic())
                .setGroupId(sourceProperties.getGroupId())
                .setStartingOffsets(latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        int watermarkSeconds = Integer.parseInt(appProperties.getProperty("iot-monitoring-job.watermark-seconds"));
        return env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(watermarkSeconds)),
                SENSOR_METRICS_DATA_SOURCE_NAME
        );
    }
}

package ru.sinitsynme.sensors.average;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import ru.sinitsynme.sensors.SensorMetric;

import java.time.ZoneId;
import java.time.ZoneOffset;

public class AverageMetricPrometheusMappingFunction implements MapFunction<SensorMetric, PrometheusTimeSeries> {

    private static final String AVERAGE_SENSOR_METRIC_NAME = "sensor_stats";
    private static final String MACHINE_ID_LABEL = "machineId";
    private static final String SENSOR_ID_LABEL = "sensorId";
    private static final String METRIC_TYPE_LABEL = "metricType";

    @Override
    public PrometheusTimeSeries map(SensorMetric metric) throws Exception {
        return PrometheusTimeSeries.builder()
                .withMetricName(AVERAGE_SENSOR_METRIC_NAME)
                .addLabel(MACHINE_ID_LABEL, metric.machineId().toString())
                .addLabel(SENSOR_ID_LABEL, metric.sensorId().toString())
                .addLabel(METRIC_TYPE_LABEL, metric.type().toString())
                .addSample(metric.value(), metric.timestamp().toInstant(ZoneOffset.ofHours(3)).toEpochMilli())
                .build();
    }
}

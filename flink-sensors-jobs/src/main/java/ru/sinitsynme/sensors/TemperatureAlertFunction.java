package ru.sinitsynme.sensors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class TemperatureAlertFunction extends KeyedProcessFunction<UUID, SensorMetric, String> {

    public static final String TEMPERATURE_ALERT_STATE = "temperatureAlertState";
    private final double temperatureThreshold;
    private ValueState<Boolean> alertState;


    public TemperatureAlertFunction(double temperatureThreshold) {
        this.temperatureThreshold = temperatureThreshold;
    }

    @Override
    public void processElement(SensorMetric metric, Context ctx, Collector<String> out) throws Exception {
        Boolean isAlerting = alertState.value();
        if (isAlerting == null) {
            isAlerting = false;
        }

        if (metric.value() > temperatureThreshold) {
            if (!isAlerting) {
                String alert = String.format(
                        "(!) HIGH TEMP ALERT: Machine=%s, sensorId=%s, temperature=%.2f°C (Threshold=%.1f°C)",
                        metric.machineId(), metric.sensorId(), metric.value(), temperatureThreshold
                );
                out.collect(alert);
                alertState.update(true);
            }
        } else {
            if (isAlerting) {
                String recovery = String.format(
                        "TEMP NORMAL: Machine=%s, sensorId=%s, temperature=%.2f°C",
                        metric.machineId(), metric.sensorId(), metric.value()
                );
                out.collect(recovery);
                alertState.update(false);
            }
        }
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> alertStateDescriptor =
                new ValueStateDescriptor<>(TEMPERATURE_ALERT_STATE, TypeInformation.of(Boolean.class));
        alertState = getRuntimeContext().getState(alertStateDescriptor);
    }
}

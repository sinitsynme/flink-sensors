package ru.sinitsynme.sensors.alert;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import ru.sinitsynme.sensors.SensorMetric;

public class AlertFunction extends KeyedProcessFunction<String, SensorMetric, AlertEvent> {

    public static final String ALERT_STATE = "alertState";
    private final double alertThreshold;
    private ValueState<Boolean> alertState;


    public AlertFunction(double alertThreshold) {
        this.alertThreshold = alertThreshold;
    }

    @Override
    public void processElement(SensorMetric metric, Context ctx, Collector<AlertEvent> out) throws Exception {
        Boolean isAlerting = alertState.value();
        if (isAlerting == null) {
            isAlerting = false;
        }

        if (metric.value() > alertThreshold && !isAlerting) {
            String alert = String.format(
                    "(!) HIGH %s ALERT: Machine=%s, sensorId=%s, temperature=%.2f°C (Threshold=%.1f°C)",
                    metric.type().toString().toUpperCase(),
                    metric.machineId(),
                    metric.sensorId(),
                    metric.value(),
                    alertThreshold
            );
            var alertEvent = new AlertEvent(
                    metric.machineId(),
                    metric.sensorId(),
                    metric.type(),
                    metric.value(),
                    true,
                    alert
            );
            out.collect(alertEvent);
            alertState.update(true);
            return;
        }

        if (metric.value() <= alertThreshold && isAlerting) {
            String recovery = String.format(
                    "%s NORMAL: Machine=%s, sensorId=%s, temperature=%.2f°C",
                    metric.type().toString().toUpperCase(),
                    metric.machineId(),
                    metric.sensorId(),
                    metric.value()
            );
            var recoveryEvent = new AlertEvent(
                    metric.machineId(),
                    metric.sensorId(),
                    metric.type(),
                    metric.value(),
                    false,
                    recovery
            );
            out.collect(recoveryEvent);
            alertState.update(false);
        }
    }


    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> alertStateDescriptor =
                new ValueStateDescriptor<>(ALERT_STATE, TypeInformation.of(Boolean.class));
        alertState = getRuntimeContext().getState(alertStateDescriptor);
    }
}

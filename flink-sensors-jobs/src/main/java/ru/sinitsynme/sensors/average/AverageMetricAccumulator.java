package ru.sinitsynme.sensors.average;

import ru.sinitsynme.sensors.SensorType;

import java.time.LocalDateTime;
import java.util.UUID;

public class AverageMetricAccumulator {
    private UUID machineId;
    private UUID sensorId;
    private SensorType type;
    private double sum = 0;
    private int count = 0;
    private LocalDateTime lastTimestamp;

    public UUID getMachineId() {
        return machineId;
    }

    public void setMachineId(UUID machineId) {
        this.machineId = machineId;
    }

    public UUID getSensorId() {
        return sensorId;
    }

    public void setSensorId(UUID sensorId) {
        this.sensorId = sensorId;
    }

    public SensorType getType() {
        return type;
    }

    public void setType(SensorType type) {
        this.type = type;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public LocalDateTime getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(LocalDateTime lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }
}


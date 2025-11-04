package ru.sinitsynme.sensors.alert;

import ru.sinitsynme.sensors.SensorType;

import java.util.UUID;

public record AlertEvent(
        UUID machineId,
        UUID sensorId,
        SensorType type,
        double temperatureValue,
        boolean isAlertState,
        String message
) {}
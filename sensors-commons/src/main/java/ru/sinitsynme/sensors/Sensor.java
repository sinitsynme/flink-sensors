package ru.sinitsynme.sensors;

import java.util.UUID;

public record Sensor(
        UUID sensorId,
        UUID machineId,
        SensorType type
) {
}

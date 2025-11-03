package ru.sinitsynme.sensors;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.UUID;

public record SensorMetric(
     UUID sensorId,
     UUID machineId,
     SensorType type,
     double value,
     @JsonFormat(shape=JsonFormat.Shape.STRING)
     LocalDateTime timestamp
) { }


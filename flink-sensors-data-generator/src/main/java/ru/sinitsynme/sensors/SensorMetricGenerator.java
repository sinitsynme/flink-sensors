package ru.sinitsynme.sensors;

import ru.sinitsynme.util.AppProperties;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Random;
import java.util.UUID;

public class SensorMetricGenerator {
    private final double anomalyPercentage;
    private final double anomalyCoefficient;

    private final Random random;
    private final GeneratorStrategy temperatureGeneratorStrategy;
    private final GeneratorStrategy pressureGeneratorStrategy;
    private final GeneratorStrategy vibrationGeneratorStrategy;
    private final Clock clock;

    public SensorMetricGenerator(Random random, AppProperties properties) {
        this.random = random;
        this.anomalyPercentage = properties.getDoubleProperty("generator.anomaly-percentage");
        this.anomalyCoefficient = properties.getDoubleProperty("generator.anomaly-coefficient");
        this.clock = Clock.system(ZoneId.of("Europe/Moscow"));

        temperatureGeneratorStrategy = new TemperatureGeneratorStrategy(random);
        pressureGeneratorStrategy = new PressureGeneratorStrategy(random);
        vibrationGeneratorStrategy = new VibrationGeneratorStrategy(random);
    }

    public SensorMetric generate(UUID sensorId, UUID machineId, SensorType type) {
        var datetime = LocalDateTime.now(clock);
        var strategy = getStrategyForType(type);
        var generatedValue = strategy.generate();

        var anomalyChance = random.nextDouble() < anomalyPercentage;
        if (anomalyChance) {
            System.out.println("ANOMALY HAPPENED");
            generatedValue *= anomalyCoefficient;
        }

        return new SensorMetric(
                sensorId,
                machineId,
                type,
                generatedValue,
                datetime
        );
    }

    private GeneratorStrategy getStrategyForType(SensorType type) {
        return switch (type) {
            case TEMPERATURE -> temperatureGeneratorStrategy;
            case PRESSURE -> pressureGeneratorStrategy;
            case VIBRATION -> vibrationGeneratorStrategy;
        };
    }
}

package ru.sinitsynme.sensors;

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

abstract class GeneratorStrategy {
    protected final Random random;
    private final int baseValue;
    private final int maxDeviation;

    public GeneratorStrategy(Random random, int baseValue, int maxDeviation) {
        this.random = random;
        this.baseValue = baseValue;
        this.maxDeviation = maxDeviation;
    }

    public double generate() {
        return baseValue + random.nextDouble() * maxDeviation;
    }
}

class TemperatureGeneratorStrategy extends GeneratorStrategy {
    private static final int BASE_VALUE = 70;
    private static final int MAX_DEVIATION = 100;

    public TemperatureGeneratorStrategy(Random random) {
        super(random, BASE_VALUE, MAX_DEVIATION);
    }
}

class PressureGeneratorStrategy extends GeneratorStrategy {
    private static final int BASE_VALUE = 30;
    private static final int MAX_DEVIATION = 20;

    public PressureGeneratorStrategy(Random random) {
        super(random, BASE_VALUE, MAX_DEVIATION);
    }
}

class VibrationGeneratorStrategy extends GeneratorStrategy {
    private static final int BASE_VALUE = 1000;
    private static final int MAX_DEVIATION = 2000;

    public VibrationGeneratorStrategy(Random random) {
        super(random, BASE_VALUE, MAX_DEVIATION);
    }
}


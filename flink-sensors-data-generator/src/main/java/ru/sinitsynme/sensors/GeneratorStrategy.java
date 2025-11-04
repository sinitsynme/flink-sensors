package ru.sinitsynme.sensors;

import java.util.Random;

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
    private static final int BASE_VALUE = 40;
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

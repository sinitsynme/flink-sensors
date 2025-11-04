package ru.sinitsynme.sensors.average;

import org.apache.flink.api.common.functions.AggregateFunction;
import ru.sinitsynme.sensors.SensorMetric;

public class AverageMetricAggregate implements AggregateFunction<SensorMetric, AverageMetricAccumulator, SensorMetric> {

    @Override
    public AverageMetricAccumulator createAccumulator() {
        return new AverageMetricAccumulator();
    }

    @Override
    public AverageMetricAccumulator add(SensorMetric sensorMetric, AverageMetricAccumulator accumulator) {
        if (accumulator.getSensorId() == null) {
            accumulator.setMachineId(sensorMetric.machineId());
            accumulator.setSensorId(sensorMetric.sensorId());
            accumulator.setType(sensorMetric.type());
            accumulator.setLastTimestamp(sensorMetric.timestamp());
        }
        accumulator.setSum(accumulator.getSum() + sensorMetric.value());
        accumulator.setCount(accumulator.getCount() + 1);

        var accTimestamp = accumulator.getLastTimestamp();
        var sensorTimestamp = sensorMetric.timestamp();
        accumulator.setLastTimestamp(sensorTimestamp.isAfter(accTimestamp) ? sensorTimestamp : accTimestamp);

        return accumulator;
    }

    @Override
    public SensorMetric getResult(AverageMetricAccumulator accumulator) {
        double average = accumulator.getCount() > 0 ? accumulator.getSum() / accumulator.getCount() : 0;

        return new SensorMetric(
                accumulator.getSensorId(),
                accumulator.getMachineId(),
                accumulator.getType(),
                average,
                accumulator.getLastTimestamp()
        );
    }

    @Override
    public AverageMetricAccumulator merge(AverageMetricAccumulator accumulatorA, AverageMetricAccumulator accumulatorB) {
        accumulatorA.setSum(accumulatorA.getSum() + accumulatorB.getSum());
        accumulatorA.setCount(accumulatorA.getCount() + accumulatorB.getCount());
        accumulatorA.setLastTimestamp(accumulatorA.getLastTimestamp()
                .isAfter(accumulatorB.getLastTimestamp())
                ? accumulatorA.getLastTimestamp() : accumulatorB.getLastTimestamp());
        return accumulatorA;
    }
}


package ru.sinitsynme.sensors.healthscore;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ru.sinitsynme.sensors.SensorMetric;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class HealthScoreProcessFunction extends ProcessWindowFunction<SensorMetric, String, UUID, TimeWindow> {

    @Override
    public void process(
            UUID machineId,
            Context context,
            Iterable<SensorMetric> metrics,
            Collector<String> out
    ) {
        List<SensorMetric> metricList = new ArrayList<>();
        metrics.forEach(metricList::add);

        if (metricList.isEmpty()) {
            return;
        }

        double healthScore = calculateHealthScore(metricList);
        String status = getHealthStatus(healthScore);

        String result = String.format(
                "HEALTH: Machine=%s, Score=%.1f/100, Status=%s, metrics=%d",
                machineId, healthScore, status, metricList.size()
        );

        out.collect(result);
    }

    private double calculateHealthScore(List<SensorMetric> metrics) {
        double score = 100.0;

        for (SensorMetric metric : metrics) {
            switch (metric.type()) {
                case TEMPERATURE:
                    if (metric.value() > 95) score -= 10;
                    else if (metric.value() > 85) score -= 5;
                    break;
                case PRESSURE:
                    if (metric.value() > 45) score -= 8;
                    else if (metric.value() < 25) score -= 8;
                    break;
                case VIBRATION:
                    if (metric.value() > 2500) score -= 7;
                    break;
            }
        }

        return Math.max(0, score);
    }

    private String getHealthStatus(double score) {
        if (score >= 90) return "EXCELLENT";
        if (score >= 75) return "GOOD";
        if (score >= 60) return "FAIR";
        if (score >= 40) return "POOR";
        return "CRITICAL";
    }
}

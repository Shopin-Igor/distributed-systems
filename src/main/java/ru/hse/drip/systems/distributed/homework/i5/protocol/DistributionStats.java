package ru.hse.drip.systems.distributed.homework.i5.protocol;

import java.util.LinkedHashMap;
import java.util.Map;

public class DistributionStats {
    public int totalKeys;
    public Map<String, Integer> keyCounts = new LinkedHashMap<>();
    public double mean;
    public double stddev;
    public double stddevOverMean;
    public double maxOverAverage;

    public DistributionStats() {
    }

    public static DistributionStats from(Map<String, Integer> counts) {
        DistributionStats stats = new DistributionStats();
        stats.keyCounts = new LinkedHashMap<>(counts);
        stats.totalKeys = counts.values().stream().mapToInt(Integer::intValue).sum();

        if (counts.isEmpty()) {
            return stats;
        }

        stats.mean = stats.totalKeys / (double) counts.size();
        double variance = 0.0;
        int max = 0;
        for (int count : counts.values()) {
            double delta = count - stats.mean;
            variance += delta * delta;
            max = Math.max(max, count);
        }

        stats.stddev = Math.sqrt(variance / counts.size());
        if (stats.mean > 0.0) {
            stats.stddevOverMean = stats.stddev / stats.mean;
            stats.maxOverAverage = max / stats.mean;
        }
        return stats;
    }
}

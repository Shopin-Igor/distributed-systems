package ru.hse.drip.systems.distributed.homework.i5.benchmarks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class LatencyRecorder {
    private final List<Long> latenciesNanos = new CopyOnWriteArrayList<>();

    public void record(long nanos) {
        latenciesNanos.add(nanos);
    }

    public Snapshot snapshot() {
        if (latenciesNanos.isEmpty()) {
            return new Snapshot(0, 0, 0, 0);
        }

        List<Long> sorted = new ArrayList<>(latenciesNanos);
        Collections.sort(sorted);

        double avg = sorted.stream().mapToDouble(Long::doubleValue).average().orElse(0.0) / 1_000_000.0;
        return new Snapshot(
                avg,
                percentile(sorted, 0.50),
                percentile(sorted, 0.95),
                percentile(sorted, 0.99)
        );
    }

    private double percentile(List<Long> sorted, double percentile) {
        int index = Math.min(sorted.size() - 1, (int) Math.ceil(percentile * sorted.size()) - 1);
        return sorted.get(Math.max(0, index)) / 1_000_000.0;
    }

    public record Snapshot(double avgMs, double p50Ms, double p95Ms, double p99Ms) {
    }
}

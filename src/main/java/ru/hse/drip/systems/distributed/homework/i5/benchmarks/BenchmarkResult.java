package ru.hse.drip.systems.distributed.homework.i5.benchmarks;

public class BenchmarkResult {
    public String part;
    public String strategy;
    public String workload;
    public int nodeCount;
    public int keyCount;
    public int threads;
    public double putRatio;
    public double throughputOpsSec;
    public double avgLatencyMs;
    public double p50LatencyMs;
    public double p95LatencyMs;
    public double p99LatencyMs;
    public double stddevOverMean;
    public double maxOverAverage;
    public double migratedPercent;
    public long migrationDurationMs;

    public String csvHeader() {
        return "part,strategy,workload,nodeCount,keyCount,threads,putRatio,throughputOpsSec,"
                + "avgLatencyMs,p50LatencyMs,p95LatencyMs,p99LatencyMs,stddevOverMean,maxOverAverage,"
                + "migratedPercent,migrationDurationMs";
    }

    public String toCsvRow() {
        return String.join(",",
                part,
                strategy,
                workload,
                Integer.toString(nodeCount),
                Integer.toString(keyCount),
                Integer.toString(threads),
                String.format("%.3f", putRatio),
                String.format("%.2f", throughputOpsSec),
                String.format("%.4f", avgLatencyMs),
                String.format("%.4f", p50LatencyMs),
                String.format("%.4f", p95LatencyMs),
                String.format("%.4f", p99LatencyMs),
                String.format("%.6f", stddevOverMean),
                String.format("%.6f", maxOverAverage),
                String.format("%.4f", migratedPercent),
                Long.toString(migrationDurationMs)
        );
    }
}

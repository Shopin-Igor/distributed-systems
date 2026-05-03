package ru.hse.drip.systems.distributed.homework.i5.protocol;

public class MigrationReport {
    public String reason;
    public String strategy;
    public int movedKeys;
    public int totalKeys;
    public long durationMs;

    public MigrationReport() {
    }

    public MigrationReport(String reason, String strategy, int movedKeys, int totalKeys, long durationMs) {
        this.reason = reason;
        this.strategy = strategy;
        this.movedKeys = movedKeys;
        this.totalKeys = totalKeys;
        this.durationMs = durationMs;
    }

    public double movedPercent() {
        if (totalKeys == 0) {
            return 0.0;
        }
        return movedKeys * 100.0 / totalKeys;
    }
}

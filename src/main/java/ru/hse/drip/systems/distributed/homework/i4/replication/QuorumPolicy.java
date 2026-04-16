package ru.hse.drip.systems.distributed.homework.i4.replication;

public class QuorumPolicy {
    public static int requiredAcks(ReplicationMode mode, int rf, int k) {
        if (rf <= 1) return 0;

        return switch (mode) {
            case ASYNC -> 0;
            case SEMI_SYNC -> Math.max(1, Math.min(k, rf - 1));
            case SYNC -> rf - 1;
        };
    }
}

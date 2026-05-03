package ru.hse.drip.systems.distributed.homework.i5.benchmarks;

public final class WorkloadGenerator {
    private WorkloadGenerator() {
    }

    public static String key(int index) {
        return String.format("key:%05d", index);
    }

    public static String value(int index) {
        return "value-" + index;
    }

    public static String rangeBoundary(int keyIndex) {
        return key(keyIndex);
    }
}

package ru.hse.drip.systems.distributed.homework.i5.protocol;

import java.util.Locale;

public enum ShardingStrategyType {
    RANGE,
    HASH_MOD_N,
    CONSISTENT_HASHING;

    public static ShardingStrategyType fromCli(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("strategy is required");
        }

        String normalized = raw.trim()
                .replace('-', '_')
                .toUpperCase(Locale.ROOT);

        return switch (normalized) {
            case "RANGE", "RANGE_BASED" -> RANGE;
            case "HASH", "HASH_MOD", "HASH_MOD_N" -> HASH_MOD_N;
            case "CONSISTENT", "CONSISTENT_HASH", "CONSISTENT_HASHING" -> CONSISTENT_HASHING;
            default -> throw new IllegalArgumentException("unknown strategy: " + raw);
        };
    }

    public String cliName() {
        return switch (this) {
            case RANGE -> "range";
            case HASH_MOD_N -> "hash_mod_n";
            case CONSISTENT_HASHING -> "consistent_hashing";
        };
    }
}

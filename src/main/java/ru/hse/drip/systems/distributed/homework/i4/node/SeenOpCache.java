package ru.hse.drip.systems.distributed.homework.i4.node;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SeenOpCache {
    private final long ttlMs;
    private final Map<UUID, Long> seenUntil = new ConcurrentHashMap<>();

    public SeenOpCache(long ttlMs) {
        this.ttlMs = ttlMs;
    }

    public boolean markIfNew(UUID opId) {
        long now = System.currentTimeMillis();
        cleanup(now);

        Long prev = seenUntil.putIfAbsent(opId, now + ttlMs);
        return prev == null;
    }

    public void cleanup(long now) {
        for (var e : seenUntil.entrySet()) {
            if (e.getValue() < now) seenUntil.remove(e.getKey());
        }
    }

    public void clear() {
        seenUntil.clear();
    }
}

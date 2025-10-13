package ru.hse.drip.systems.distributed.seminar.i3.peer;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;

public class DedupStore {
    private static final long TTL_MS = 5 * 60_000;
    private final ConcurrentHashMap<String, Long> seen = new ConcurrentHashMap<>();

    public boolean firstTimeSeen(String messageId) {
        long now = System.currentTimeMillis();
        Long prev = seen.putIfAbsent(messageId, now);
        return prev == null;
    }

    public void cleanupExpired() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> e : seen.entrySet()) {
            if (now - e.getValue() > TTL_MS) {
                seen.remove(e.getKey(), e.getValue());
            }
        }
    }

    public static ScheduledExecutorService startCleaner(DedupStore store) {
        ScheduledExecutorService sch = Executors.newSingleThreadScheduledExecutor();
        sch.scheduleAtFixedRate(store::cleanupExpired, 1, 60, TimeUnit.SECONDS);
        return sch;
    }
}

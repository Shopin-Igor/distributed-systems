package ru.hse.drip.systems.distributed.homework.i4.replication;

import ru.hse.drip.systems.distributed.homework.i4.protocol.ReplicaAck;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PendingAcks {

    public static class Waiter {
        public final int required;
        public final AtomicInteger okCount = new AtomicInteger(0);
        public final CompletableFuture<Boolean> done = new CompletableFuture<>();

        public Waiter(int required) {
            this.required = required;
            if (required == 0) done.complete(true);
        }

        public void onAck(ReplicaAck ack) {
            if (done.isDone()) return;
            if (ack != null && ack.ok) {
                int now = okCount.incrementAndGet();
                if (now >= required) done.complete(true);
            }
        }
    }

    private final Map<UUID, Waiter> waiters = new ConcurrentHashMap<>();

    public Waiter register(UUID opId, int required) {
        Waiter w = new Waiter(required);
        waiters.put(opId, w);
        return w;
    }

    public void onAck(ReplicaAck ack) {
        if (ack == null || ack.opId == null) return;
        Waiter w = waiters.get(ack.opId);
        if (w != null) {
            w.onAck(ack);
            if (w.done.isDone()) {
                waiters.remove(ack.opId);
            }
        }
    }
}

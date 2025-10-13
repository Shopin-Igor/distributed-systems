package ru.hse.drip.systems.distributed.seminar.i3.balancer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RoundRobinPicker {
    private final AtomicReference<List<InetSocketAddress>> aliveRef;
    private final AtomicInteger idx = new AtomicInteger(0);

    public RoundRobinPicker(AtomicReference<List<InetSocketAddress>> aliveRef) {
        this.aliveRef = aliveRef;
    }

    public InetSocketAddress nextOrNull() {
        List<InetSocketAddress> alive = aliveRef.get();
        if (alive == null || alive.isEmpty()) return null;
        int i = Math.floorMod(idx.getAndIncrement(), alive.size());
        return alive.get(i);
    }
}

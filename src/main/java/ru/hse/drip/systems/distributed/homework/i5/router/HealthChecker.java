package ru.hse.drip.systems.distributed.homework.i5.router;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HealthChecker implements AutoCloseable {
    private final ClusterState state;
    private final NodeClient nodeClient;
    private final int intervalMs;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public HealthChecker(ClusterState state, NodeClient nodeClient, int intervalMs) {
        this.state = state;
        this.nodeClient = nodeClient;
        this.intervalMs = intervalMs;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::checkOnce, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void checkOnce() {
        for (ClusterNode node : state.nodesSorted()) {
            boolean alive = nodeClient.canConnect(node);
            state.markAlive(node.nodeId, alive);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}

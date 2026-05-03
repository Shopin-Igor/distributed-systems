package ru.hse.drip.systems.distributed.homework.i5.router;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MigrationReport;
import ru.hse.drip.systems.distributed.homework.i5.sharding.ShardingStrategy;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public class MigrationService {
    private final ClusterState state;
    private final NodeClient nodeClient;
    private final Supplier<ShardingStrategy> strategySupplier;

    public MigrationService(ClusterState state, NodeClient nodeClient, Supplier<ShardingStrategy> strategySupplier) {
        this.state = state;
        this.nodeClient = nodeClient;
        this.strategySupplier = strategySupplier;
    }

    public MigrationReport rebalanceAll(String reason) {
        return rebalance(reason, List.of());
    }

    public MigrationReport rebalanceWithExtraSource(String reason, ClusterNode extraSource) {
        return rebalance(reason, List.of(extraSource));
    }

    public void clearAllStores() {
        List<String> failures = new ArrayList<>();
        for (ClusterNode node : state.nodesSorted()) {
            if (!nodeClient.canConnect(node)) {
                state.markAlive(node.nodeId, false);
                failures.add(node.nodeId + " is unavailable");
                continue;
            }
            try {
                nodeClient.clearData(node);
                state.markAlive(node.nodeId, true);
            } catch (RuntimeException e) {
                state.markAlive(node.nodeId, false);
                failures.add(node.nodeId + ": " + e.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException("failed to clear all shard data: " + String.join("; ", failures));
        }
    }

    private MigrationReport rebalance(String reason, List<ClusterNode> extraSources) {
        long startedAt = System.currentTimeMillis();
        ShardingStrategy strategy = strategySupplier.get();
        List<ClusterNode> sources = migrationSources(extraSources);

        int total = 0;
        int moved = 0;
        Set<String> processedKeys = new LinkedHashSet<>();
        for (ClusterNode source : sources) {
            Map<String, String> data = safeDump(source);
            for (Map.Entry<String, String> entry : data.entrySet()) {
                Optional<ClusterNode> owner = strategy.route(entry.getKey(), state);
                if (owner.isEmpty()) {
                    continue;
                }
                ClusterNode target = owner.get();

                if (!processedKeys.add(entry.getKey())) {
                    deleteDuplicate(source, target, entry.getKey());
                    continue;
                }
                total++;

                if (target.nodeId.equals(source.nodeId)) {
                    continue;
                }
                if (!target.alive) {
                    continue;
                }

                if (move(source, target, entry.getKey(), entry.getValue())) {
                    moved++;
                }
            }
        }

        return new MigrationReport(
                reason,
                state.strategyType().cliName(),
                moved,
                total,
                System.currentTimeMillis() - startedAt
        );
    }

    private List<ClusterNode> migrationSources(List<ClusterNode> extraSources) {
        List<ClusterNode> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (ClusterNode node : state.nodesSorted()) {
            result.add(node);
            seen.add(node.nodeId);
        }
        for (ClusterNode node : extraSources) {
            if (node != null && seen.add(node.nodeId)) {
                result.add(node);
            }
        }
        return result;
    }

    private Map<String, String> safeDump(ClusterNode node) {
        try {
            return nodeClient.dumpData(node);
        } catch (RuntimeException ignored) {
            state.markAlive(node.nodeId, false);
            return Map.of();
        }
    }

    private void deleteDuplicate(ClusterNode source, ClusterNode target, String key) {
        if (target.nodeId.equals(source.nodeId)) {
            return;
        }
        try {
            nodeClient.deleteIfPresent(source, key);
        } catch (RuntimeException ignored) {
            state.markAlive(source.nodeId, false);
        }
    }

    private boolean move(ClusterNode source, ClusterNode target, String key, String value) {
        try {
            nodeClient.putData(target, key, value);
        } catch (RuntimeException ignored) {
            state.markAlive(target.nodeId, false);
            return false;
        }

        try {
            nodeClient.deleteIfPresent(source, key);
            return true;
        } catch (RuntimeException ignored) {
            state.markAlive(source.nodeId, false);
            return false;
        }
    }
}

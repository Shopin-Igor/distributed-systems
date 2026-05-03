package ru.hse.drip.systems.distributed.homework.i5.router;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.DistributionStats;

import java.util.LinkedHashMap;
import java.util.Map;

public class StatsService {
    private final ClusterState state;
    private final NodeClient nodeClient;

    public StatsService(ClusterState state, NodeClient nodeClient) {
        this.state = state;
        this.nodeClient = nodeClient;
    }

    public DistributionStats distributionStats() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (ClusterNode node : state.nodesSorted()) {
            counts.put(node.nodeId, keyCount(node));
        }
        return DistributionStats.from(counts);
    }

    public Map<String, Map<String, String>> clusterDump() {
        Map<String, Map<String, String>> dump = new LinkedHashMap<>();
        for (ClusterNode node : state.nodesSorted()) {
            if (!node.alive) {
                dump.put(node.nodeId, Map.of());
                continue;
            }
            try {
                dump.put(node.nodeId, new LinkedHashMap<>(nodeClient.dumpData(node)));
            } catch (RuntimeException ignored) {
                dump.put(node.nodeId, Map.of());
                state.markAlive(node.nodeId, false);
            }
        }
        return dump;
    }

    public boolean allStoresEmpty() {
        return distributionStats().totalKeys == 0;
    }

    private int keyCount(ClusterNode node) {
        if (!node.alive) {
            return 0;
        }
        try {
            return nodeClient.dumpData(node).size();
        } catch (RuntimeException ignored) {
            state.markAlive(node.nodeId, false);
            return 0;
        }
    }
}

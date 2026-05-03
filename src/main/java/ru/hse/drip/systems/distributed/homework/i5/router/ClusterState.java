package ru.hse.drip.systems.distributed.homework.i5.router;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ShardingStrategyType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ClusterState {
    private final Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    private ShardingStrategyType strategyType = ShardingStrategyType.CONSISTENT_HASHING;
    private List<String> rangeBoundaries = new ArrayList<>();
    private int vnodeCount = 3;

    public synchronized void addNode(ClusterNode node) {
        requireNodeId(node.nodeId);
        if (node.port <= 0 || node.port > 65535) {
            throw new IllegalArgumentException("port must be in 1..65535");
        }
        if (node.weight < 1) {
            node.weight = 1;
        }
        nodes.put(node.nodeId, node.copy());
    }

    public synchronized Optional<ClusterNode> removeNode(String nodeId) {
        ClusterNode removed = nodes.remove(nodeId);
        return removed == null ? Optional.empty() : Optional.of(removed.copy());
    }

    public synchronized Optional<ClusterNode> findNode(String nodeId) {
        ClusterNode node = nodes.get(nodeId);
        return node == null ? Optional.empty() : Optional.of(node.copy());
    }

    public synchronized boolean containsNode(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    public synchronized void markAlive(String nodeId, boolean alive) {
        ClusterNode node = nodes.get(nodeId);
        if (node != null) {
            node.alive = alive;
        }
    }

    public synchronized void setWeight(String nodeId, int weight) {
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1");
        }
        ClusterNode node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("unknown node: " + nodeId);
        }
        node.weight = weight;
    }

    public synchronized List<ClusterNode> nodesSorted() {
        return nodes.values().stream()
                .map(ClusterNode::copy)
                .sorted(Comparator.comparing(node -> node.nodeId))
                .toList();
    }

    public synchronized List<ClusterNode> liveNodesSorted() {
        return nodes.values().stream()
                .filter(node -> node.alive)
                .map(ClusterNode::copy)
                .sorted(Comparator.comparing(node -> node.nodeId))
                .toList();
    }

    public synchronized boolean hasNodes() {
        return !nodes.isEmpty();
    }

    public synchronized ShardingStrategyType strategyType() {
        return strategyType;
    }

    public synchronized void setStrategyType(ShardingStrategyType strategyType) {
        this.strategyType = strategyType;
    }

    public synchronized List<String> rangeBoundaries() {
        return new ArrayList<>(rangeBoundaries);
    }

    public synchronized void setRangeBoundaries(List<String> boundaries) {
        List<String> copy = new ArrayList<>(boundaries);
        for (int i = 1; i < copy.size(); i++) {
            if (copy.get(i - 1).compareTo(copy.get(i)) >= 0) {
                throw new IllegalArgumentException("range boundaries must be strictly increasing");
            }
        }
        this.rangeBoundaries = copy;
    }

    public synchronized int vnodeCount() {
        return vnodeCount;
    }

    public synchronized void setVnodeCount(int vnodeCount) {
        if (vnodeCount < 1) {
            throw new IllegalArgumentException("vnode count must be >= 1");
        }
        this.vnodeCount = vnodeCount;
    }

    public synchronized boolean rangeBoundariesMatchNodeCount() {
        return rangeBoundaries.size() == Math.max(0, nodes.size() - 1);
    }

    private void requireNodeId(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId is required");
        }
    }
}

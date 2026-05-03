package ru.hse.drip.systems.distributed.homework.i5.sharding;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ShardingStrategyType;
import ru.hse.drip.systems.distributed.homework.i5.router.ClusterState;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConsistentHashingStrategy implements ShardingStrategy {
    private volatile String cachedSignature = "";
    private volatile HashRing cachedRing = HashRing.build(List.of(), 1);

    @Override
    public ShardingStrategyType type() {
        return ShardingStrategyType.CONSISTENT_HASHING;
    }

    @Override
    public Optional<ClusterNode> route(String key, ClusterState state) {
        return ringFor(state).lookupNodeId(key).flatMap(state::findNode);
    }

    @Override
    public Map<String, List<Long>> ringTokens(ClusterState state) {
        return ringFor(state).tokensByNode();
    }

    private HashRing ringFor(ClusterState state) {
        List<ClusterNode> nodes = state.nodesSorted();
        String signature = signature(nodes, state.vnodeCount());
        HashRing ring = cachedRing;
        if (signature.equals(cachedSignature)) {
            return ring;
        }

        synchronized (this) {
            if (!signature.equals(cachedSignature)) {
                cachedRing = HashRing.build(nodes, state.vnodeCount());
                cachedSignature = signature;
            }
            return cachedRing;
        }
    }

    private String signature(List<ClusterNode> nodes, int vnodeCount) {
        StringBuilder signature = new StringBuilder();
        signature.append(vnodeCount);
        for (ClusterNode node : nodes) {
            signature.append('|')
                    .append(node.nodeId)
                    .append(':')
                    .append(node.weight);
        }
        return signature.toString();
    }
}

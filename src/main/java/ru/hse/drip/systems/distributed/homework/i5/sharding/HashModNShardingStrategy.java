package ru.hse.drip.systems.distributed.homework.i5.sharding;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ShardingStrategyType;
import ru.hse.drip.systems.distributed.homework.i5.router.ClusterState;

import java.util.List;
import java.util.Optional;

public class HashModNShardingStrategy implements ShardingStrategy {
    @Override
    public ShardingStrategyType type() {
        return ShardingStrategyType.HASH_MOD_N;
    }

    @Override
    public Optional<ClusterNode> route(String key, ClusterState state) {
        List<ClusterNode> nodes = state.nodesSorted();
        if (nodes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(nodes.get(HashUtil.moduloNodeIndex(key, nodes.size())));
    }
}

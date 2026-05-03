package ru.hse.drip.systems.distributed.homework.i5.sharding;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ShardingStrategyType;
import ru.hse.drip.systems.distributed.homework.i5.router.ClusterState;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ShardingStrategy {
    ShardingStrategyType type();

    Optional<ClusterNode> route(String key, ClusterState state);

    default List<ClusterNode> routeRange(String fromKey, String toKey, ClusterState state) {
        return state.nodesSorted();
    }

    default Map<String, List<Long>> ringTokens(ClusterState state) {
        return Map.of();
    }
}

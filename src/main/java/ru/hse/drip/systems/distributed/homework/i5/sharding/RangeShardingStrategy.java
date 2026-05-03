package ru.hse.drip.systems.distributed.homework.i5.sharding;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ShardingStrategyType;
import ru.hse.drip.systems.distributed.homework.i5.router.ClusterState;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RangeShardingStrategy implements ShardingStrategy {
    @Override
    public ShardingStrategyType type() {
        return ShardingStrategyType.RANGE;
    }

    @Override
    public Optional<ClusterNode> route(String key, ClusterState state) {
        List<ClusterNode> nodes = state.nodesSorted();
        if (nodes.isEmpty()) {
            return Optional.empty();
        }
        List<String> boundaries = state.rangeBoundaries();
        if (boundaries.size() != nodes.size() - 1) {
            throw new IllegalStateException("range boundaries count must be node count - 1");
        }

        for (int index = 0; index < boundaries.size(); index++) {
            if (key.compareTo(boundaries.get(index)) < 0) {
                return Optional.of(nodes.get(index));
            }
        }
        return Optional.of(nodes.get(nodes.size() - 1));
    }

    @Override
    public List<ClusterNode> routeRange(String fromKey, String toKey, ClusterState state) {
        List<ClusterNode> nodes = state.nodesSorted();
        List<String> boundaries = state.rangeBoundaries();
        if (nodes.isEmpty()) {
            return List.of();
        }
        if (boundaries.size() != nodes.size() - 1) {
            throw new IllegalStateException("range boundaries count must be node count - 1");
        }

        List<ClusterNode> result = new ArrayList<>();
        for (int index = 0; index < nodes.size(); index++) {
            String lower = index == 0 ? "" : boundaries.get(index - 1);
            String upper = index < boundaries.size() ? boundaries.get(index) : null;
            if (intersects(fromKey, toKey, lower, upper)) {
                result.add(nodes.get(index));
            }
        }
        return result;
    }

    private boolean intersects(String fromKey, String toKey, String lowerInclusive, String upperExclusive) {
        boolean startsBeforeUpper = upperExclusive == null || fromKey.compareTo(upperExclusive) < 0;
        boolean endsAfterLower = toKey.compareTo(lowerInclusive) > 0;
        return startsBeforeUpper && endsAfterLower;
    }
}

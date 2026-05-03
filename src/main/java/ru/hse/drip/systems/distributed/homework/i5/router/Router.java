package ru.hse.drip.systems.distributed.homework.i5.router;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ErrorCode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MessageType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MigrationReport;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ResponseStatus;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ShardingStrategyType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;
import ru.hse.drip.systems.distributed.homework.i5.sharding.ConsistentHashingStrategy;
import ru.hse.drip.systems.distributed.homework.i5.sharding.HashModNShardingStrategy;
import ru.hse.drip.systems.distributed.homework.i5.sharding.RangeShardingStrategy;
import ru.hse.drip.systems.distributed.homework.i5.sharding.ShardingStrategy;
import ru.hse.drip.systems.distributed.homework.i5.transport.LineConnection;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpClient;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Router implements AutoCloseable {
    private final JsonCodec codec;
    private final ClusterState state;
    private final NodeClient nodeClient;
    private final StatsService statsService;
    private final MigrationService migrationService;
    private final HealthChecker healthChecker;
    private final Map<ShardingStrategyType, ShardingStrategy> strategies = new EnumMap<>(ShardingStrategyType.class);

    public Router(RouterConfig config) {
        this.codec = new JsonCodec();
        this.state = new ClusterState();
        this.nodeClient = new NodeClient(codec, new TcpClient(), config.requestTimeoutMs);
        this.statsService = new StatsService(state, nodeClient);
        this.migrationService = new MigrationService(state, nodeClient, this::activeStrategy);
        this.healthChecker = new HealthChecker(state, nodeClient, config.healthCheckIntervalMs);

        register(new RangeShardingStrategy());
        register(new HashModNShardingStrategy());
        register(new ConsistentHashingStrategy());
    }

    public void startBackgroundJobs() {
        healthChecker.start();
    }

    public void handleConnection(LineConnection connection) {
        try {
            String line = connection.readLine();
            if (line == null) {
                return;
            }

            WireMessage request = codec.fromLine(line, WireMessage.class);
            WireMessage response = handle(request);
            connection.writeLine(codec.toLine(response));
        } catch (Exception e) {
            WireMessage response = WireMessage.error(null, ErrorCode.BAD_REQUEST, e.getMessage());
            connection.writeLine(codec.toLine(response));
        }
    }

    public WireMessage handle(WireMessage request) {
        if (request == null || request.type == null) {
            return WireMessage.error(null, ErrorCode.BAD_REQUEST, "message type is required");
        }

        try {
            return switch (request.type) {
                case CLIENT_PUT -> clientPut(request);
                case CLIENT_GET -> clientGet(request);
                case CLIENT_DELETE -> clientDelete(request);
                case CLIENT_GET_RANGE -> clientGetRange(request);
                case ADD_NODE -> addNode(request);
                case REMOVE_NODE -> removeNode(request);
                case SET_STRATEGY -> setStrategy(request);
                case SET_RANGES -> setRanges(request);
                case SET_VNODES -> setVnodes(request);
                case SET_WEIGHT -> setWeight(request);
                case MIGRATE_DATA -> migrateData(request);
                case CLUSTER_INFO, LIST_NODES -> clusterInfo(request);
                case DUMP -> dump(request);
                case CLUSTER_DUMP -> clusterDump(request);
                case STATS -> stats(request);
                case RING_INFO -> ringInfo(request);
                default -> WireMessage.error(request.requestId, ErrorCode.BAD_REQUEST,
                        "unsupported router message: " + request.type);
            };
        } catch (RouterException e) {
            return WireMessage.error(request.requestId, e.errorCode, e.getMessage());
        } catch (IllegalArgumentException e) {
            return WireMessage.error(request.requestId, ErrorCode.BAD_REQUEST, e.getMessage());
        } catch (IllegalStateException e) {
            return WireMessage.error(request.requestId, ErrorCode.INVALID_CONFIG, e.getMessage());
        } catch (NodeUnavailableException e) {
            return WireMessage.error(request.requestId, ErrorCode.NODE_UNAVAILABLE, e.getMessage());
        }
    }

    private WireMessage clientPut(WireMessage request) {
        requireKey(request.key);
        if (request.value == null) {
            throw new IllegalArgumentException("value is required");
        }

        ClusterNode owner = requireAvailableOwner(request.key);
        WireMessage response = nodeClient.put(owner, request.key, request.value);
        response.nodeId = owner.nodeId;
        return response;
    }

    private WireMessage clientGet(WireMessage request) {
        requireKey(request.key);
        ClusterNode owner = requireAvailableOwner(request.key);
        WireMessage response = nodeClient.get(owner, request.key);
        response.nodeId = owner.nodeId;
        return response;
    }

    private WireMessage clientDelete(WireMessage request) {
        requireKey(request.key);
        ClusterNode owner = requireAvailableOwner(request.key);
        WireMessage response = nodeClient.delete(owner, request.key);
        response.nodeId = owner.nodeId;
        return response;
    }

    private WireMessage clientGetRange(WireMessage request) {
        requireKey(request.fromKey);
        requireKey(request.toKey);
        if (request.fromKey.compareTo(request.toKey) >= 0) {
            throw new IllegalArgumentException("fromKey must be lower than toKey");
        }
        if (state.strategyType() != ShardingStrategyType.RANGE) {
            throw new RouterException(ErrorCode.INVALID_CONFIG, "GET_RANGE is supported only for range strategy");
        }

        Map<String, String> values = new LinkedHashMap<>();
        for (ClusterNode node : activeStrategy().routeRange(request.fromKey, request.toKey, state)) {
            if (!node.alive) {
                throw new RouterException(ErrorCode.NODE_UNAVAILABLE, "range shard is unavailable: " + node.nodeId);
            }
            WireMessage shardResponse = nodeClient.getRange(node, request.fromKey, request.toKey);
            if (shardResponse.status != ResponseStatus.OK) {
                return shardResponse;
            }
            values.putAll(shardResponse.data);
        }

        WireMessage response = WireMessage.ok(request.requestId);
        response.data = values;
        response.keys = List.copyOf(values.keySet());
        return response;
    }

    private WireMessage addNode(WireMessage request) {
        String nodeId = requireText(request.nodeId, "nodeId");
        String host = requireText(request.host, "host");
        if (request.port == null) {
            throw new IllegalArgumentException("port is required");
        }
        if (state.containsNode(nodeId)) {
            throw new RouterException(ErrorCode.BAD_REQUEST,
                    "node already exists: " + nodeId + ". Use listNodes before loading data to verify cluster membership");
        }

        ClusterNode node = new ClusterNode(nodeId, host, request.port);
        if (request.weight != null) {
            node.weight = request.weight;
        }
        node.alive = nodeClient.canConnect(node);
        if (node.alive) {
            nodeClient.clearData(node);
        }
        state.addNode(node);

        WireMessage response = clusterInfo(request);
        response.message = "node added";
        if (state.strategyType() == ShardingStrategyType.CONSISTENT_HASHING
                || state.strategyType() == ShardingStrategyType.HASH_MOD_N) {
            response.migration = migrationService.rebalanceAll("addNode " + nodeId);
        }
        return response;
    }

    private WireMessage removeNode(WireMessage request) {
        String nodeId = requireText(request.nodeId, "nodeId");
        Optional<ClusterNode> removed = state.removeNode(nodeId);
        if (removed.isEmpty()) {
            throw new IllegalArgumentException("unknown node: " + nodeId);
        }

        WireMessage response = clusterInfo(request);
        response.message = "node removed";
        if (state.strategyType() == ShardingStrategyType.CONSISTENT_HASHING
                || state.strategyType() == ShardingStrategyType.HASH_MOD_N) {
            response.migration = migrationService.rebalanceWithExtraSource("removeNode " + nodeId, removed.get());
        }
        return response;
    }

    private WireMessage setStrategy(WireMessage request) {
        ShardingStrategyType strategyType = ShardingStrategyType.fromCli(requireText(request.strategy, "strategy"));
        if (!statsService.allStoresEmpty()) {
            if (!request.isForce()) {
                throw new RouterException(ErrorCode.INVALID_CONFIG,
                        "strategy can be changed only when all shards are empty; repeat as: setStrategy "
                                + strategyType.cliName() + " --force. This clears all shard data");
            }
            migrationService.clearAllStores();
        }

        state.setStrategyType(strategyType);
        WireMessage response = clusterInfo(request);
        response.message = "strategy set to " + strategyType.cliName();
        return response;
    }

    private WireMessage setRanges(WireMessage request) {
        List<String> boundaries = request.boundaries == null ? List.of() : request.boundaries;
        int expected = Math.max(0, state.nodesSorted().size() - 1);
        if (boundaries.size() != expected) {
            throw new RouterException(ErrorCode.INVALID_CONFIG,
                    "range boundaries count must be " + expected + ", got " + boundaries.size());
        }

        state.setRangeBoundaries(boundaries);
        WireMessage response = clusterInfo(request);
        response.message = "range boundaries updated";
        return response;
    }

    private WireMessage setVnodes(WireMessage request) {
        if (request.count == null) {
            throw new IllegalArgumentException("count is required");
        }
        state.setVnodeCount(request.count);

        WireMessage response = clusterInfo(request);
        response.message = "vnode count updated";
        if (state.strategyType() == ShardingStrategyType.CONSISTENT_HASHING) {
            response.migration = migrationService.rebalanceAll("setVnodes " + request.count);
        }
        return response;
    }

    private WireMessage setWeight(WireMessage request) {
        String nodeId = requireText(request.nodeId, "nodeId");
        if (request.weight == null) {
            throw new IllegalArgumentException("weight is required");
        }

        state.setWeight(nodeId, request.weight);
        WireMessage response = clusterInfo(request);
        response.message = "weight updated";
        if (state.strategyType() == ShardingStrategyType.CONSISTENT_HASHING) {
            response.migration = migrationService.rebalanceAll("setWeight " + nodeId + " " + request.weight);
        }
        return response;
    }

    private WireMessage migrateData(WireMessage request) {
        if (state.strategyType() == ShardingStrategyType.RANGE && !state.rangeBoundariesMatchNodeCount()) {
            throw new RouterException(ErrorCode.INVALID_CONFIG, "range boundaries are not configured");
        }

        MigrationReport report = migrationService.rebalanceAll("manual migrateData");
        WireMessage response = WireMessage.ok(request.requestId);
        response.migration = report;
        response.message = "migration finished";
        return response;
    }

    private WireMessage clusterInfo(WireMessage request) {
        WireMessage response = WireMessage.ok(request.requestId);
        response.strategy = state.strategyType().cliName();
        response.count = state.vnodeCount();
        response.boundaries = state.rangeBoundaries();
        response.nodes = state.nodesSorted();
        return response;
    }

    private WireMessage dump(WireMessage request) {
        String targetNodeId = request.targetNodeId != null ? request.targetNodeId : request.nodeId;
        if (targetNodeId == null || targetNodeId.isBlank()) {
            return clusterDump(request);
        }

        ClusterNode node = state.findNode(targetNodeId)
                .orElseThrow(() -> new IllegalArgumentException("unknown node: " + targetNodeId));
        if (!node.alive) {
            throw new RouterException(ErrorCode.NODE_UNAVAILABLE, "node unavailable: " + node.nodeId);
        }

        WireMessage response = nodeClient.dump(node);
        response.nodeId = node.nodeId;
        return response;
    }

    private WireMessage clusterDump(WireMessage request) {
        WireMessage response = WireMessage.ok(request.requestId);
        response.clusterData = statsService.clusterDump();
        return response;
    }

    private WireMessage stats(WireMessage request) {
        WireMessage response = WireMessage.ok(request.requestId);
        response.stats = statsService.distributionStats();
        return response;
    }

    private WireMessage ringInfo(WireMessage request) {
        if (state.strategyType() != ShardingStrategyType.CONSISTENT_HASHING) {
            throw new RouterException(ErrorCode.INVALID_CONFIG, "ringInfo is available only for consistent_hashing");
        }
        WireMessage response = WireMessage.ok(request.requestId);
        response.ringTokens = activeStrategy().ringTokens(state);
        response.count = state.vnodeCount();
        return response;
    }

    private ClusterNode requireAvailableOwner(String key) {
        ClusterNode owner = activeStrategy().route(key, state)
                .orElseThrow(() -> new RouterException(ErrorCode.NO_AVAILABLE_NODES, "no nodes in cluster"));
        if (!owner.alive) {
            throw new RouterException(ErrorCode.NODE_UNAVAILABLE, "owner node is unavailable: " + owner.nodeId);
        }
        return owner;
    }

    private ShardingStrategy activeStrategy() {
        return strategies.get(state.strategyType());
    }

    private void register(ShardingStrategy strategy) {
        strategies.put(strategy.type(), strategy);
    }

    private void requireKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is required");
        }
        if (key.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("key must not contain whitespace");
        }
    }

    private String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    @Override
    public void close() {
        healthChecker.close();
    }

    private static class RouterException extends RuntimeException {
        private final ErrorCode errorCode;

        private RouterException(ErrorCode errorCode, String message) {
            super(message);
            this.errorCode = errorCode;
        }
    }
}

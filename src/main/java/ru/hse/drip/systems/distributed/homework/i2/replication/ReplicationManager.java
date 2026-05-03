package ru.hse.drip.systems.distributed.homework.i2.replication;
import ru.hse.drip.systems.distributed.homework.i2.node.NodeConfig;
import ru.hse.drip.systems.distributed.homework.i2.node.NodeState;
import ru.hse.drip.systems.distributed.homework.i2.protocol.*;
import ru.hse.drip.systems.distributed.homework.i2.replication.ReplicationMode;
import ru.hse.drip.systems.distributed.homework.i2.transport.TcpClient;

import java.util.*;
import java.util.concurrent.*;

public class ReplicationManager implements AutoCloseable {
    private final NodeConfig cfg;
    private final NodeState state;
    private final JsonCodec codec;
    private final TcpClient client = new TcpClient();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    private final ConcurrentHashMap<UUID, Pending> pending = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Deque<UUID>> perFollowerQueue = new ConcurrentHashMap<>();

    public ReplicationManager(NodeConfig cfg, NodeState state, JsonCodec codec) {
        this.cfg = cfg;
        this.state = state;
        this.codec = codec;

        scheduler.scheduleAtFixedRate(this::retryTick, 50, cfg.retryIntervalMs, TimeUnit.MILLISECONDS);
    }

    public ClientResponse replicateDelete(UUID requestId, String key, long version) {
        return replicateMutation(requestId, "DELETE", key, null, version);
    }

    private ClientResponse replicateMutation(UUID requestId, String opType, String key, String value, long version) {
        if (requestId == null) {
            return ClientResponse.error(null, ErrorCode.BAD_REQUEST, "requestId не задан");
        }
        if (opType == null || opType.isBlank()) {
            return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, "opType не задан");
        }
        if (key == null) {
            return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, "key не задан");
        }

        int clusterSize = state.nodes.size();
        if (clusterSize <= 0) {
            return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, "кластер пуст");
        }

        int rf = state.rf;
        if (rf < 1 || rf > clusterSize) {
            return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, "RF должен быть в диапазоне 1..clusterSize");
        }

        if (state.replicationMode == ReplicationMode.SEMI_SYNC) {
            int k = state.semiSyncAcksK;
            if (k < 1 || k > rf - 1) {
                return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, "Для semi-sync K должен быть в диапазоне 1..RF-1");
            }
        }

        UUID opId = UUID.randomUUID();
        Pending p = new Pending(opId);
        p.opType = opType;
        p.key = key;
        p.value = value;
        p.version = version;

        int requiredFull = rf - 1;
        int requiredHot;
        if (state.replicationMode == ReplicationMode.SYNC) {
            requiredHot = requiredFull;
        } else if (state.replicationMode == ReplicationMode.SEMI_SYNC) {
            requiredHot = state.semiSyncAcksK;
        } else {
            requiredHot = 0;
        }
        p.requiredHot = requiredHot;
        p.requiredFull = requiredFull;

        pending.put(opId, p);

        for (ClusterNode n : state.nodes.values()) {
            if (n.nodeId.equals(cfg.nodeId)) continue;
            perFollowerQueue
                    .computeIfAbsent(n.nodeId, __ -> new java.util.concurrent.ConcurrentLinkedDeque<>())
                    .addLast(opId);
        }

        if (state.replicationMode == ReplicationMode.ASYNC || rf == 1) {
            return ClientResponse.ok(requestId);
        }

        try {
            boolean ok = p.hotPathDone.get(state.hotPathTimeoutMs, TimeUnit.MILLISECONDS);
            if (ok) {
                return ClientResponse.ok(requestId);
            }
            return ClientResponse.error(requestId, ErrorCode.NOT_ENOUGH_REPLICAS, "недостаточно подтверждений");
        } catch (TimeoutException te) {
            return ClientResponse.error(requestId, ErrorCode.NOT_ENOUGH_REPLICAS, "не удалось набрать нужное число реплик до таймаута");
        } catch (Exception e) {
            return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, "ошибка репликации: " + e.getMessage());
        }
    }
    public ClientResponse replicatePut(UUID requestId, String key, String value, long version) {
        int clusterSize = state.nodes.size();
        if (state.rf < 1 || state.rf > clusterSize) {
            return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, "RF должен быть в диапазоне 1..clusterSize");
        }
        if (state.replicationMode == ReplicationMode.SEMI_SYNC) {
            int k = state.semiSyncAcksK;
            if (k < 1 || k > state.rf - 1) {
                return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, "Для semi-sync K должен быть в диапазоне 1..RF-1");
            }
        }

        UUID opId = UUID.randomUUID();

        int requiredFull = state.rf - 1;
        int requiredHot = switch (state.replicationMode) {
            case SYNC -> requiredFull;
            case SEMI_SYNC -> state.semiSyncAcksK;
            case ASYNC -> 0;
        };

        Pending p = new Pending(opId);
        p.opType = "PUT";
        p.key = key;
        p.value = value;
        p.version = version;
        p.requiredHot = requiredHot;
        p.requiredFull = requiredFull;

        pending.put(opId, p);

        for (ClusterNode n : state.nodes.values()) {
            if (n.nodeId.equals(cfg.nodeId)) continue;
            perFollowerQueue.computeIfAbsent(n.nodeId, __ -> new ConcurrentLinkedDeque<>()).addLast(opId);
        }

        if (state.replicationMode == ReplicationMode.ASYNC || state.rf == 1) {
            return ClientResponse.ok(requestId);
        }

        try {
            boolean ok = p.hotPathDone.get(state.hotPathTimeoutMs, TimeUnit.MILLISECONDS);
            return ok ? ClientResponse.ok(requestId)
                    : ClientResponse.error(requestId, ErrorCode.NOT_ENOUGH_REPLICAS, "недостаточно подтверждений");
        } catch (TimeoutException te) {
            return ClientResponse.error(requestId, ErrorCode.NOT_ENOUGH_REPLICAS, "не удалось набрать нужное число реплик до таймаута");
        } catch (Exception e) {
            return ClientResponse.error(requestId, ErrorCode.BAD_REQUEST, e.getMessage());
        }
    }


    public void onAck(ReplAck ack, String followerNodeId) {
        if (ack == null || ack.operationId == null) return;
        Pending p = pending.get(ack.operationId);
        if (p == null) return;

        if (ack.ok) {
            p.ackedFollowers.add(followerNodeId);

            Deque<UUID> q = perFollowerQueue.get(followerNodeId);
            if (q != null) {
                q.remove(ack.operationId);
            }

            int requiredHot = p.requiredHot;
            if (requiredHot > 0 && p.ackedFollowers.size() >= requiredHot) {
                p.hotPathDone.complete(true);
            }
            if (p.requiredFull > 0 && p.ackedFollowers.size() >= p.requiredFull) {
                p.fullDone = true;
            }
        }
    }

    private void retryTick() {
        for (var entry : perFollowerQueue.entrySet()) {
            String followerId = entry.getKey();
            Deque<UUID> q = entry.getValue();
            UUID opId = q.peekFirst();
            if (opId == null) continue;

            ClusterNode follower = state.nodes.get(followerId);
            if (follower == null) { q.pollFirst(); continue; }

            Pending p = pending.get(opId);
            if (p == null) { q.pollFirst(); continue; }

            if (p.ackedFollowers.contains(followerId)) {
                q.pollFirst();
                continue;
            }


            ReplPut repl = "DELETE".equals(p.opType)
                    ? ReplPut.delete(opId, cfg.nodeId, p.key, p.version)
                    : ReplPut.put(opId, cfg.nodeId, p.key, p.value, p.version);

            MessageEnvelope env = MessageEnvelope.of(MessageType.REPL_PUT, codec.toTree(repl));
            String line = codec.toLine(env);

            scheduler.execute(() -> {
                try {
                    String respLine = client.callLine(follower.host, follower.port, line, state.hotPathTimeoutMs);
                    if (respLine == null) return;
                    MessageEnvelope respEnv = codec.fromLine(respLine, MessageEnvelope.class);
                    if (respEnv.type == MessageType.REPL_ACK) {
                        ReplAck ack = codec.treeToValue(respEnv.payload, ReplAck.class);
                        onAck(ack, followerId);
                    }
                } catch (Exception ignored) {}
            });
        }

        for (var e : pending.entrySet()) {
            if (e.getValue().fullDone) pending.remove(e.getKey());
        }
    }

    public void registerOperationData(UUID opId, String key, String value, int requiredHot, int requiredFull) {
        Pending p = pending.get(opId);
        if (p != null) {
            p.key = key;
            p.value = value;
            p.requiredHot = requiredHot;
            p.requiredFull = requiredFull;
        }
    }

    private static class Pending {
        volatile String opType;
        final UUID opId;
        volatile String key;
        volatile String value;
        volatile long version;

        volatile int requiredHot = 0;
        volatile int requiredFull = 0;

        final Set<String> ackedFollowers = ConcurrentHashMap.newKeySet();
        final CompletableFuture<Boolean> hotPathDone = new CompletableFuture<>();
        volatile boolean fullDone = false;

        Pending(UUID opId) { this.opId = opId; }
    }
}

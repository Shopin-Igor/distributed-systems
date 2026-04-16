package ru.hse.drip.systems.distributed.homework.i4.replication;

import ru.hse.drip.systems.distributed.homework.i4.node.NodeConfig;
import ru.hse.drip.systems.distributed.homework.i4.node.NodeState;
import ru.hse.drip.systems.distributed.homework.i4.protocol.*;
import ru.hse.drip.systems.distributed.homework.i4.transport.TcpClient;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiLeaderReplicationManager implements AutoCloseable {
    private final NodeConfig cfg;
    private final NodeState state;
    private final JsonCodec codec;
    private final TcpClient client = new TcpClient();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Random rnd = new Random();

    public MultiLeaderReplicationManager(NodeConfig cfg, NodeState state, JsonCodec codec) {
        this.cfg = cfg;
        this.state = state;
        this.codec = codec;
    }

    public void replicateLocalPut(UUID operationId, String key, String value, Version version) {
        forward(ReplPut.put(operationId, cfg.nodeId, cfg.nodeId, key, value, version), null, true);
    }

    public void forward(ReplPut put, String senderNodeId, boolean localOrigin) {
        for (ClusterNode target : computeTargets(put, senderNodeId, localOrigin)) {
            if (target == null || cfg.nodeId.equals(target.nodeId)) {
                continue;
            }
            executor.submit(() -> sendWithDelay(target, put));
        }
    }

    private void sendWithDelay(ClusterNode target, ReplPut source) {
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                int min = Math.max(0, state.delayMinMs);
                int max = Math.max(min, state.delayMaxMs);
                if (max > 0) {
                    int delay = (min == max) ? min : (min + rnd.nextInt(max - min + 1));
                    Thread.sleep(delay);
                }

                ReplPut outbound = ReplPut.put(
                        source.operationId,
                        source.originNodeId,
                        cfg.nodeId,
                        source.key,
                        source.value,
                        source.version
                );
                MessageEnvelope env = MessageEnvelope.of(MessageType.REPL_PUT, codec.toTree(outbound));
                client.callLine(target.host, target.port, codec.toLine(env), state.hotPathTimeoutMs);
                return;
            } catch (Exception ignored) {
            }
        }
    }

    private List<ClusterNode> computeTargets(ReplPut put, String senderNodeId, boolean localOrigin) {
        return switch (state.topology) {
            case MESH -> meshTargets(localOrigin, senderNodeId);
            case RING -> ringTargets(senderNodeId);
            case STAR -> starTargets(put, senderNodeId, localOrigin);
        };
    }

    private List<ClusterNode> meshTargets(boolean localOrigin, String senderNodeId) {
        List<ClusterNode> targets = new ArrayList<>();
        for (ClusterNode node : state.nodes.values()) {
            if (cfg.nodeId.equals(node.nodeId)) {
                continue;
            }
            if (!localOrigin && senderNodeId != null && senderNodeId.equals(node.nodeId)) {
                continue;
            }
            targets.add(node);
        }
        return targets;
    }

    private List<ClusterNode> ringTargets(String senderNodeId) {
        List<ClusterNode> ordered = orderedNodes();
        if (ordered.size() <= 1) {
            return List.of();
        }
        int currentIndex = -1;
        for (int i = 0; i < ordered.size(); i++) {
            if (cfg.nodeId.equals(ordered.get(i).nodeId)) {
                currentIndex = i;
                break;
            }
        }
        if (currentIndex < 0) {
            return List.of();
        }
        ClusterNode next = ordered.get((currentIndex + 1) % ordered.size());
        if (next.nodeId.equals(senderNodeId)) {
            return List.of();
        }
        return List.of(next);
    }

    private List<ClusterNode> starTargets(ReplPut put, String senderNodeId, boolean localOrigin) {
        String centerNodeId = state.starCenterNodeId;
        if (centerNodeId == null || !state.nodes.containsKey(centerNodeId)) {
            return List.of();
        }
        if (cfg.nodeId.equals(centerNodeId)) {
            List<ClusterNode> targets = new ArrayList<>();
            for (ClusterNode node : state.nodes.values()) {
                if (cfg.nodeId.equals(node.nodeId)) {
                    continue;
                }
                if (senderNodeId != null && senderNodeId.equals(node.nodeId)) {
                    continue;
                }
                if (put.originNodeId != null && put.originNodeId.equals(node.nodeId) && !localOrigin) {
                    continue;
                }
                targets.add(node);
            }
            return targets;
        }
        if (!localOrigin) {
            return List.of();
        }
        ClusterNode center = state.nodes.get(centerNodeId);
        return center == null ? List.of() : List.of(center);
    }

    private List<ClusterNode> orderedNodes() {
        List<ClusterNode> ordered = new ArrayList<>(state.nodes.values());
        ordered.sort(Comparator.comparing(node -> node.nodeId));
        return ordered;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}

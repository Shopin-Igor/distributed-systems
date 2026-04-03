package ru.hse.drip.systems.distributed.homework.i2.node;

import com.fasterxml.jackson.databind.JsonNode;
import ru.hse.drip.systems.distributed.homework.i2.protocol.*;
import ru.hse.drip.systems.distributed.homework.i2.replication.ReplicationManager;
import ru.hse.drip.systems.distributed.homework.i2.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i2.store.StoreApplier;
import ru.hse.drip.systems.distributed.homework.i2.transport.LineConnection;

import java.util.Map;

public class Node {
    private final NodeConfig cfg;
    private final NodeState state;
    private final JsonCodec codec;

    private final InMemoryKeyValueStore store;
    private final CommandHandler command;
    private final ReplicaHandler replica;
    private final ReplicationManager replMgr;

    public Node(NodeConfig cfg, NodeState state, InMemoryKeyValueStore store, JsonCodec codec) {
        this.cfg = cfg;
        this.state = state;
        this.store = store;
        this.codec = codec;

        this.command = new CommandHandler(store);
        this.replica = new ReplicaHandler(new StoreApplier(store), state);
        this.replMgr = new ReplicationManager(cfg, state, codec);
    }

    public void handleConnection(LineConnection c) {
        try {
            String line;
            while ((line = c.readLine()) != null) {
                MessageEnvelope env = codec.fromLine(line, MessageEnvelope.class);
                String out = dispatch(env);
                c.writeLine(out);
            }
        } catch (Exception ignored) {}
    }

    private String dispatch(MessageEnvelope env) {
        try {
            if (env == null || env.type == null) {
                return codec.toLine(MessageEnvelope.of(
                        MessageType.CLIENT_GET, codec.toTree(ClientResponse.error(null, ErrorCode.BAD_REQUEST, "некорректный конверт сообщения"))));
            }

            return switch (env.type) {
                case CLIENT_PUT, CLIENT_GET, CLIENT_DUMP, CLIENT_DELETE -> handleClient(env.type, env.payload);
                case REPL_PUT -> handleReplPut(env.payload);
                case REPL_ACK -> handleReplAck(env.payload);
                case CLUSTER_UPDATE -> handleClusterUpdate(env.payload);
            };
        } catch (Exception e) {
            ClientResponse r = ClientResponse.error(null, ErrorCode.BAD_REQUEST, e.getMessage());
            return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_GET, codec.toTree(r)));
        }
    }

    private String handleClient(MessageType type, JsonNode payload) {
        ClientRequest req = codec.treeToValue(payload, ClientRequest.class);
        if (req == null || req.requestId == null || req.clientId == null) {
            ClientResponse r = ClientResponse.error(null, ErrorCode.BAD_REQUEST, "отсутствует requestId/clientId");
            return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_GET, codec.toTree(r)));
        }

        if (type == MessageType.CLIENT_GET) {
            String v = command.get(req.key);

            ClientResponse r = ClientResponse.okValue(req.requestId, v);
            return codec.toLine(MessageEnvelope.of(type, codec.toTree(r)));
        }

        if (type == MessageType.CLIENT_DUMP) {
            Map<String,String> snap = store.snapshot();
            ClientResponse r = ClientResponse.okDump(req.requestId, snap);
            return codec.toLine(MessageEnvelope.of(type, codec.toTree(r)));
        }

        if (state.role != Role.LEADER) {
            ClusterNode leader = state.nodes.get(state.leaderNodeId);
            ClientResponse r = ClientResponse.notLeader(req.requestId,
                    state.leaderNodeId,
                    leader == null ? null : leader.host,
                    leader == null ? null : leader.port);
            return codec.toLine(MessageEnvelope.of(type, codec.toTree(r)));
        }

        if (type == MessageType.CLIENT_DELETE) {
            long version = state.nextMutationVersion.incrementAndGet();
            command.delete(req.key, version);
            ClientResponse resp = replMgr.replicateDelete(req.requestId, req.key, version);
            return codec.toLine(MessageEnvelope.of(type, codec.toTree(resp)));
        }


        long version = state.nextMutationVersion.incrementAndGet();
        command.put(req.key, req.value, version);

        ClientResponse resp = replMgr.replicatePut(req.requestId, req.key, req.value, version);
        return codec.toLine(MessageEnvelope.of(type, codec.toTree(resp)));
    }

    private String handleReplPut(JsonNode payload) {
        ReplPut put = codec.treeToValue(payload, ReplPut.class);
        ReplAck ack = replica.onReplPut(put);
        return codec.toLine(MessageEnvelope.of(MessageType.REPL_ACK, codec.toTree(ack)));
    }

    private String handleReplAck(JsonNode payload) {
        ReplAck ack = codec.treeToValue(payload, ReplAck.class);
        ClientResponse r = ClientResponse.ok(null);
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_GET, codec.toTree(r)));
    }

    private String handleClusterUpdate(JsonNode payload) {
        ClusterUpdate upd = codec.treeToValue(payload, ClusterUpdate.class);
        if (upd == null || upd.nodes == null) {
            ClientResponse r = ClientResponse.error(null, ErrorCode.BAD_REQUEST, "некорректное обновление кластера");
            return codec.toLine(MessageEnvelope.of(MessageType.CLUSTER_UPDATE, codec.toTree(r)));
        }

        state.nodes.clear();
        for (ClusterNode n : upd.nodes) state.nodes.put(n.nodeId, n);

        state.leaderNodeId = upd.leaderNodeId;
        state.role = cfg.nodeId.equals(upd.leaderNodeId) ? Role.LEADER : Role.FOLLOWER;

        state.replicationMode = upd.replicationMode;
        state.rf = upd.rf;
        state.semiSyncAcksK = upd.semiSyncAcksK;
        state.delayMinMs = upd.replicationDelayMinMs;
        state.delayMaxMs = upd.replicationDelayMaxMs;
        state.hotPathTimeoutMs = upd.hotPathTimeoutMs;

        ClientResponse ok = ClientResponse.ok(null);
        return codec.toLine(MessageEnvelope.of(MessageType.CLUSTER_UPDATE, codec.toTree(ok)));
    }
}

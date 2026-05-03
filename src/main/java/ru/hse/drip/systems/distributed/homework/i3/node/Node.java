package ru.hse.drip.systems.distributed.homework.i3.node;

import com.fasterxml.jackson.databind.JsonNode;
import ru.hse.drip.systems.distributed.homework.i3.protocol.*;
import ru.hse.drip.systems.distributed.homework.i3.replication.MultiLeaderReplicationManager;
import ru.hse.drip.systems.distributed.homework.i3.replication.ReplicationManager;
import ru.hse.drip.systems.distributed.homework.i3.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i3.store.StoreApplier;
import ru.hse.drip.systems.distributed.homework.i3.transport.LineConnection;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Node {
    private final NodeConfig cfg;
    private final NodeState state;
    private final JsonCodec codec;

    private final InMemoryKeyValueStore store;
    private final CommandHandler command;
    private final ReplicaHandler replica;
    private final ReplicationManager replMgr;
    private final MultiLeaderReplicationManager multiReplMgr;

    public Node(NodeConfig cfg, NodeState state, InMemoryKeyValueStore store, JsonCodec codec) {
        this.cfg = cfg;
        this.state = state;
        this.store = store;
        this.codec = codec;

        this.command = new CommandHandler(store);
        this.replica = new ReplicaHandler(cfg.nodeId, new StoreApplier(store), state, command);
        this.replMgr = new ReplicationManager(cfg, state, codec);
        this.multiReplMgr = new MultiLeaderReplicationManager(cfg, state, codec);
    }

    public void handleConnection(LineConnection c) {
        try {
            String line;
            while ((line = c.readLine()) != null) {
                MessageEnvelope env = codec.fromLine(line, MessageEnvelope.class);
                String out = dispatch(env);
                c.writeLine(out);
            }
        } catch (Exception ignored) {
        }
    }

    private String dispatch(MessageEnvelope env) {
        try {
            if (env == null || env.type == null) {
                return errorEnvelope(MessageType.CLIENT_GET, null, ErrorCode.BAD_REQUEST, "некорректный envelope");
            }

            return switch (env.type) {
                case CLIENT_PUT, CLIENT_GET, CLIENT_DUMP, CLIENT_DELETE -> handleClient(env.type, env.payload);
                case REPL_PUT -> handleReplPut(env.payload);
                case REPL_ACK -> handleReplAck(env.payload);
                case CLUSTER_UPDATE -> handleClusterUpdate(env.payload);
            };
        } catch (Exception e) {
            return errorEnvelope(MessageType.CLIENT_GET, null, ErrorCode.BAD_REQUEST, e.getMessage());
        }
    }

    private String handleClient(MessageType type, JsonNode payload) {
        ClientRequest req = codec.treeToValue(payload, ClientRequest.class);
        if (req == null || req.requestId == null || req.clientId == null) {
            return errorEnvelope(type, null, ErrorCode.BAD_REQUEST, "отсутствует requestId/clientId");
        }

        if (type == MessageType.CLIENT_GET) {
            return handleGet(req);
        }

        if (type == MessageType.CLIENT_DUMP) {
            return handleDump(req);
        }

        if (type == MessageType.CLIENT_DELETE) {
            return handleDelete(req);
        }

        return handlePut(req);
    }

    private String handleGet(ClientRequest req) {
        ClientResponse response = state.clusterMode == ClusterMode.MULTI
                ? ClientResponse.okVersionedValue(req.requestId, command.getVersioned(req.key))
                : ClientResponse.okValue(req.requestId, command.get(req.key));
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_GET, codec.toTree(response)));
    }

    private String handleDump(ClientRequest req) {
        ClientResponse response = state.clusterMode == ClusterMode.MULTI
                ? ClientResponse.okVersionedDump(req.requestId, command.dumpVersioned())
                : ClientResponse.okDump(req.requestId, store.snapshot());
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_DUMP, codec.toTree(response)));
    }

    private String handleDelete(ClientRequest req) {
        if (state.clusterMode == ClusterMode.MULTI) {
            return errorEnvelope(MessageType.CLIENT_DELETE, req.requestId, ErrorCode.BAD_REQUEST, "DELETE не поддерживается в multi-режиме");
        }
        if (state.role != Role.LEADER) {
            return notLeaderEnvelope(MessageType.CLIENT_DELETE, req.requestId, ErrorCode.NOT_LEADER, state.leaderNodeId);
        }
        command.delete(req.key);
        ClientResponse resp = replMgr.replicateDelete(req.requestId, req.key);
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_DELETE, codec.toTree(resp)));
    }

    private String handlePut(ClientRequest req) {
        if (state.clusterMode == ClusterMode.SINGLE) {
            if (state.role != Role.LEADER) {
                return notLeaderEnvelope(MessageType.CLIENT_PUT, req.requestId, ErrorCode.NOT_LEADER, state.leaderNodeId);
            }
            command.put(req.key, req.value);
            ClientResponse resp = replMgr.replicatePut(req.requestId, req.key, req.value);
            return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_PUT, codec.toTree(resp)));
        }

        if (state.role != Role.LEADER) {
            return notLeaderEnvelope(MessageType.CLIENT_PUT, req.requestId, ErrorCode.NOT_LEADER_FOR_WRITE, pickAnyLeaderNodeId());
        }

        long lamport = state.lamportCounter.incrementAndGet();
        UUID operationId = UUID.randomUUID();
        replica.markLocalOperation(operationId);

        Version version = new Version(lamport, cfg.nodeId);
        command.putVersioned(req.key, new VersionedValue(req.value, version));
        multiReplMgr.replicateLocalPut(operationId, req.key, req.value, version);

        ClientResponse response = ClientResponse.okVersionedValue(req.requestId, new VersionedValue(req.value, version));
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_PUT, codec.toTree(response)));
    }

    private String handleReplPut(JsonNode payload) {
        ReplPut put = codec.treeToValue(payload, ReplPut.class);
        ReplicaHandler.ReplicaApplyResult result = replica.onReplPut(put);
        if (result.isNew && state.clusterMode == ClusterMode.MULTI) {
            multiReplMgr.forward(put, put.sourceNodeId, false);
        }
        return codec.toLine(MessageEnvelope.of(MessageType.REPL_ACK, codec.toTree(result.ack)));
    }

    private String handleReplAck(JsonNode payload) {
        ReplAck ack = codec.treeToValue(payload, ReplAck.class);
        if (ack != null && ack.fromNodeId != null) {
            replMgr.onAck(ack, ack.fromNodeId);
        }
        ClientResponse r = ClientResponse.ok(null);
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_GET, codec.toTree(r)));
    }

    private String handleClusterUpdate(JsonNode payload) {
        ClusterUpdate upd = codec.treeToValue(payload, ClusterUpdate.class);
        if (upd == null || upd.nodes == null) {
            return errorEnvelope(MessageType.CLUSTER_UPDATE, null, ErrorCode.BAD_REQUEST, "некорректный cluster update");
        }

        state.nodes.clear();
        for (ClusterNode n : upd.nodes) {
            state.nodes.put(n.nodeId, n);
        }

        state.clusterMode = upd.clusterMode == null ? ClusterMode.SINGLE : upd.clusterMode;
        state.leaderNodeId = upd.leaderNodeId;
        state.leaderNodeIds = upd.leaderNodeIds == null ? List.of() : List.copyOf(upd.leaderNodeIds);
        state.topology = upd.topology == null ? TopologyType.MESH : upd.topology;
        state.starCenterNodeId = upd.starCenterNodeId;

        state.replicationMode = upd.replicationMode;
        state.rf = upd.rf;
        state.semiSyncAcksK = upd.semiSyncAcksK;
        state.delayMinMs = upd.replicationDelayMinMs;
        state.delayMaxMs = upd.replicationDelayMaxMs;
        state.hotPathTimeoutMs = upd.hotPathTimeoutMs;

        state.role = resolveRole(upd);

        ClientResponse ok = ClientResponse.ok(null);
        return codec.toLine(MessageEnvelope.of(MessageType.CLUSTER_UPDATE, codec.toTree(ok)));
    }

    private Role resolveRole(ClusterUpdate upd) {
        if (state.clusterMode == ClusterMode.MULTI) {
            ClusterNode self = state.nodes.get(cfg.nodeId);
            if (self != null && "leader".equalsIgnoreCase(self.role)) {
                return Role.LEADER;
            }
            return Role.FOLLOWER;
        }
        return cfg.nodeId.equals(upd.leaderNodeId) ? Role.LEADER : Role.FOLLOWER;
    }

    private String pickAnyLeaderNodeId() {
        if (state.leaderNodeIds != null && !state.leaderNodeIds.isEmpty()) {
            return state.leaderNodeIds.getFirst();
        }
        return state.leaderNodeId;
    }

    private String notLeaderEnvelope(MessageType type, UUID requestId, ErrorCode errorCode, String leaderNodeId) {
        ClusterNode leader = state.nodes.get(leaderNodeId);
        ClientResponse response = errorCode == ErrorCode.NOT_LEADER_FOR_WRITE
                ? ClientResponse.notLeaderForWrite(requestId, leaderNodeId, leader == null ? null : leader.host, leader == null ? null : leader.port)
                : ClientResponse.notLeader(requestId, leaderNodeId, leader == null ? null : leader.host, leader == null ? null : leader.port);
        return codec.toLine(MessageEnvelope.of(type, codec.toTree(response)));
    }

    private String errorEnvelope(MessageType type, UUID requestId, ErrorCode code, String message) {
        return codec.toLine(MessageEnvelope.of(type, codec.toTree(ClientResponse.error(requestId, code, message))));
    }
}

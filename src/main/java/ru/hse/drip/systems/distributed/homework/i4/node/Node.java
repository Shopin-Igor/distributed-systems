package ru.hse.drip.systems.distributed.homework.i4.node;

import com.fasterxml.jackson.databind.JsonNode;
import ru.hse.drip.systems.distributed.homework.i4.leaderless.LeaderlessService;
import ru.hse.drip.systems.distributed.homework.i4.protocol.*;
import ru.hse.drip.systems.distributed.homework.i4.replication.MultiLeaderReplicationManager;
import ru.hse.drip.systems.distributed.homework.i4.replication.ReplicationManager;
import ru.hse.drip.systems.distributed.homework.i4.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i4.store.StoreApplier;
import ru.hse.drip.systems.distributed.homework.i4.transport.LineConnection;

import java.util.LinkedHashMap;
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
    private final LeaderlessService leaderless;

    public Node(NodeConfig cfg, NodeState state, InMemoryKeyValueStore store, JsonCodec codec) {
        this.cfg = cfg;
        this.state = state;
        this.store = store;
        this.codec = codec;

        this.command = new CommandHandler(store);
        this.replica = new ReplicaHandler(cfg.nodeId, new StoreApplier(store), state, command);
        this.replMgr = new ReplicationManager(cfg, state, codec);
        this.multiReplMgr = new MultiLeaderReplicationManager(cfg, state, codec);
        this.leaderless = new LeaderlessService(cfg, state, store, codec);
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
                case CLIENT_PUT, CLIENT_GET, CLIENT_DUMP, CLIENT_DUMP_HINTS, CLIENT_NODE_STATS, CLIENT_SHOW_MERKLE_ROOT,
                        CLIENT_RUN_HINTED_HANDOFF, CLIENT_WIPE_NODE_DATA, CLIENT_RUN_ANTI_ENTROPY,
                        CLIENT_DELETE -> handleClient(env.type, env.payload);
                case REPL_PUT -> handleReplPut(env.payload);
                case REPL_ACK -> handleReplAck(env.payload);
                case REPL_WRITE -> handleReplWrite(env.payload);
                case READ_QUERY -> handleReadQuery(env.payload);
                case HINTED_HANDOFF_TRANSFER -> handleHintedHandoffTransfer(env.payload);
                case MERKLE_ROOT_REQUEST -> handleMerkleRootRequest(env.payload);
                case MERKLE_DIFF_REQUEST -> handleMerkleDiffRequest(env.payload);
                case MERKLE_RECORDS_TRANSFER -> handleMerkleRecordsTransfer(env.payload);
                case CLUSTER_UPDATE -> handleClusterUpdate(env.payload);
                case REPL_WRITE_ACK, READ_RESPONSE, HINTED_HANDOFF_ACK, MERKLE_ROOT_RESPONSE,
                        MERKLE_DIFF_RESPONSE, MERKLE_RECORDS_ACK ->
                        errorEnvelope(MessageType.CLIENT_GET, null, ErrorCode.BAD_REQUEST, "неожиданный входящий тип " + env.type);
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

        return switch (type) {
            case CLIENT_PUT -> handlePut(req);
            case CLIENT_GET -> handleGet(req);
            case CLIENT_DUMP -> handleDump(req);
            case CLIENT_DUMP_HINTS -> leaderlessEnvelope(type, leaderless.handleDumpHints(req.requestId));
            case CLIENT_NODE_STATS -> handleNodeStats(req);
            case CLIENT_SHOW_MERKLE_ROOT -> leaderlessEnvelope(type, leaderless.handleShowMerkleRoot(req.requestId));
            case CLIENT_RUN_HINTED_HANDOFF -> leaderlessEnvelope(type, leaderless.handleRunHintedHandoff(req.requestId));
            case CLIENT_WIPE_NODE_DATA -> leaderlessEnvelope(type, leaderless.handleWipeNodeData(req.requestId));
            case CLIENT_RUN_ANTI_ENTROPY -> leaderlessEnvelope(type, leaderless.handleRunAntiEntropyCluster(req.requestId));
            case CLIENT_DELETE -> handleDelete(req);
            default -> errorEnvelope(type, req.requestId, ErrorCode.BAD_REQUEST, "неподдерживаемый клиентский тип " + type);
        };
    }

    private String handleGet(ClientRequest req) {
        if (state.clusterMode == ClusterMode.LEADERLESS) {
            return leaderlessEnvelope(MessageType.CLIENT_GET, leaderless.handleGet(req));
        }
        ClientResponse response = state.clusterMode == ClusterMode.MULTI
                ? ClientResponse.okVersionedValue(req.requestId, command.getVersioned(req.key))
                : ClientResponse.okValue(req.requestId, command.get(req.key));
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_GET, codec.toTree(response)));
    }

    private String handleDump(ClientRequest req) {
        if (state.clusterMode == ClusterMode.LEADERLESS) {
            return leaderlessEnvelope(MessageType.CLIENT_DUMP, leaderless.handleDump(req.requestId));
        }
        ClientResponse response = state.clusterMode == ClusterMode.MULTI
                ? ClientResponse.okVersionedDump(req.requestId, command.dumpVersioned())
                : ClientResponse.okDump(req.requestId, store.snapshot());
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_DUMP, codec.toTree(response)));
    }

    private String handleNodeStats(ClientRequest req) {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("staleReadCount", state.staleReadCount.get());
        stats.put("readRepairCount", state.readRepairCount.get());
        stats.put("hintsCreated", state.hintsCreated.get());
        stats.put("hintsDelivered", state.hintsDelivered.get());
        stats.put("antiEntropyRuns", state.antiEntropyRuns.get());
        stats.put("antiEntropyRecoveredKeys", state.antiEntropyRecoveredKeys.get());
        stats.put("storeKeys", (long) store.versionedSnapshot().size());
        stats.put("hintCount", (long) store.hintSnapshot().size());
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_NODE_STATS, codec.toTree(ClientResponse.okStats(req.requestId, stats))));
    }

    private String handleDelete(ClientRequest req) {
        if (state.clusterMode == ClusterMode.MULTI || state.clusterMode == ClusterMode.LEADERLESS) {
            return errorEnvelope(MessageType.CLIENT_DELETE, req.requestId, ErrorCode.BAD_REQUEST,
                    state.clusterMode == ClusterMode.MULTI
                            ? "DELETE не поддерживается в multi-режиме"
                            : "DELETE не поддерживается в leaderless-режиме");
        }
        if (state.role != Role.LEADER) {
            return notLeaderEnvelope(MessageType.CLIENT_DELETE, req.requestId, ErrorCode.NOT_LEADER, state.leaderNodeId);
        }
        command.delete(req.key);
        ClientResponse resp = replMgr.replicateDelete(req.requestId, req.key);
        return codec.toLine(MessageEnvelope.of(MessageType.CLIENT_DELETE, codec.toTree(resp)));
    }

    private String handlePut(ClientRequest req) {
        if (state.clusterMode == ClusterMode.LEADERLESS) {
            return leaderlessEnvelope(MessageType.CLIENT_PUT, leaderless.handlePut(req));
        }

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

    private String handleReplWrite(JsonNode payload) {
        ReplWrite write = codec.treeToValue(payload, ReplWrite.class);
        ReplWriteAck ack = leaderless.handleReplWrite(write);
        return codec.toLine(MessageEnvelope.of(MessageType.REPL_WRITE_ACK, codec.toTree(ack)));
    }

    private String handleReadQuery(JsonNode payload) {
        ReadQuery query = codec.treeToValue(payload, ReadQuery.class);
        ReadResponse response = leaderless.handleReadQuery(query);
        return codec.toLine(MessageEnvelope.of(MessageType.READ_RESPONSE, codec.toTree(response)));
    }

    private String handleHintedHandoffTransfer(JsonNode payload) {
        HintedHandoffTransfer transfer = codec.treeToValue(payload, HintedHandoffTransfer.class);
        HintedHandoffAck ack = leaderless.handleHintedHandoffTransfer(transfer);
        return codec.toLine(MessageEnvelope.of(MessageType.HINTED_HANDOFF_ACK, codec.toTree(ack)));
    }

    private String handleMerkleRootRequest(JsonNode payload) {
        MerkleRootRequest request = codec.treeToValue(payload, MerkleRootRequest.class);
        MerkleRootResponse response = leaderless.handleMerkleRootRequest(request);
        return codec.toLine(MessageEnvelope.of(MessageType.MERKLE_ROOT_RESPONSE, codec.toTree(response)));
    }

    private String handleMerkleDiffRequest(JsonNode payload) {
        MerkleDiffRequest request = codec.treeToValue(payload, MerkleDiffRequest.class);
        MerkleDiffResponse response = leaderless.handleMerkleDiffRequest(request);
        return codec.toLine(MessageEnvelope.of(MessageType.MERKLE_DIFF_RESPONSE, codec.toTree(response)));
    }

    private String handleMerkleRecordsTransfer(JsonNode payload) {
        MerkleRecordsTransfer transfer = codec.treeToValue(payload, MerkleRecordsTransfer.class);
        MerkleRecordsAck ack = leaderless.handleMerkleRecordsTransfer(transfer);
        return codec.toLine(MessageEnvelope.of(MessageType.MERKLE_RECORDS_ACK, codec.toTree(ack)));
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
        state.homeReplicaNodeIds = upd.homeReplicaNodeIds == null ? List.of() : List.copyOf(upd.homeReplicaNodeIds);
        state.spareNodeIds = upd.spareNodeIds == null ? List.of() : List.copyOf(upd.spareNodeIds);
        state.writeQuorum = upd.writeQuorum;
        state.readQuorum = upd.readQuorum;
        state.writeQuorumMode = upd.writeQuorumMode == null ? WriteQuorumMode.STRICT : upd.writeQuorumMode;

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
        if (state.clusterMode == ClusterMode.LEADERLESS) {
            return Role.LEADER;
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

    private String leaderlessEnvelope(MessageType type, ClientResponse response) {
        return codec.toLine(MessageEnvelope.of(type, codec.toTree(response)));
    }
}

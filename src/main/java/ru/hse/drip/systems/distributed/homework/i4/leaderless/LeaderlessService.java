package ru.hse.drip.systems.distributed.homework.i4.leaderless;

import ru.hse.drip.systems.distributed.homework.i4.node.NodeConfig;
import ru.hse.drip.systems.distributed.homework.i4.node.NodeState;
import ru.hse.drip.systems.distributed.homework.i4.node.SeenOpCache;
import ru.hse.drip.systems.distributed.homework.i4.protocol.*;
import ru.hse.drip.systems.distributed.homework.i4.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i4.transport.TcpClient;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class LeaderlessService {
    private static final int HOME_REPLICA_COUNT = 5;
    private static final int SPARE_NODE_COUNT = 2;

    private final NodeConfig cfg;
    private final NodeState state;
    private final InMemoryKeyValueStore store;
    private final JsonCodec codec;
    private final SeenOpCache seen;
    private final TcpClient client = new TcpClient();
    private final ExecutorService asyncExecutor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setName("i4-leaderless-rpc-" + THREAD_COUNTER.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    public LeaderlessService(NodeConfig cfg, NodeState state, InMemoryKeyValueStore store, JsonCodec codec) {
        this.cfg = cfg;
        this.state = state;
        this.store = store;
        this.codec = codec;
        this.seen = new SeenOpCache(5 * 60_000L);
    }

    public ClientResponse handlePut(ClientRequest req) {
        try {
            validateLeaderlessConfig();
            if (req.key == null || req.value == null) {
                return ClientResponse.error(req.requestId, ErrorCode.BAD_REQUEST, "PUT требует key и value");
            }

            long lamport = state.lamportCounter.incrementAndGet();
            Version version = new Version(lamport, cfg.nodeId);
            UUID operationId = UUID.randomUUID();

            ReplWrite write = new ReplWrite();
            write.operationId = operationId;
            write.sourceNodeId = cfg.nodeId;
            write.key = req.key;
            write.value = req.value;
            write.version = version;

            int successfulAcks = 0;
            List<ClusterNode> failedHomes = new ArrayList<>();
            for (RpcResult<ReplWriteAck> result : callAll(homeNodes(), homeNode -> sendReplWrite(homeNode, write))) {
                ReplWriteAck ack = result.response();
                if (ack != null && ack.ok) {
                    successfulAcks++;
                } else {
                    failedHomes.add(result.node());
                }
            }

            if (state.writeQuorumMode == WriteQuorumMode.SLOPPY && successfulAcks < state.writeQuorum) {
                List<ClusterNode> spares = spareNodes();
                for (ClusterNode failedHome : failedHomes) {
                    if (successfulAcks >= state.writeQuorum) {
                        break;
                    }
                    for (ClusterNode spareNode : spares) {
                        ReplWrite hintWrite = new ReplWrite();
                        hintWrite.operationId = hintOperationId(operationId, failedHome.nodeId);
                        hintWrite.sourceNodeId = cfg.nodeId;
                        hintWrite.key = req.key;
                        hintWrite.value = req.value;
                        hintWrite.version = version;
                        hintWrite.storeAsHint = true;
                        hintWrite.intendedHomeNodeId = failedHome.nodeId;

                        ReplWriteAck ack = sendReplWrite(spareNode, hintWrite);
                        if (ack != null && ack.ok) {
                            successfulAcks++;
                            break;
                        }
                    }
                }
            }

            if (successfulAcks < state.writeQuorum) {
                return ClientResponse.error(req.requestId, ErrorCode.NOT_ENOUGH_REPLICAS,
                        "не удалось набрать W=" + state.writeQuorum + ", получено ACK=" + successfulAcks);
            }

            return ClientResponse.okVersionedValue(req.requestId, new VersionedValue(req.value, version, operationId.toString()));
        } catch (IllegalStateException e) {
            return ClientResponse.error(req.requestId, ErrorCode.INVALID_CONFIG, e.getMessage());
        }
    }

    public ClientResponse handleGet(ClientRequest req) {
        try {
            validateLeaderlessConfig();
            if (req.key == null) {
                return ClientResponse.error(req.requestId, ErrorCode.BAD_REQUEST, "GET требует key");
            }

            List<ReadResponse> successfulResponses = new ArrayList<>();
            for (RpcResult<ReadResponse> result : callAll(homeNodes(), homeNode -> sendReadQuery(homeNode, req.requestId, req.key))) {
                ReadResponse response = result.response();
                if (response != null && response.ok) {
                    successfulResponses.add(response);
                }
            }

            if (successfulResponses.size() < state.readQuorum) {
                return ClientResponse.error(req.requestId, ErrorCode.NOT_ENOUGH_REPLICAS,
                        "не удалось набрать R=" + state.readQuorum + ", получено ответов=" + successfulResponses.size());
            }

            ReadResponse latest = successfulResponses.stream()
                    .max(Comparator.comparing(response -> response.value, this::compareValues))
                    .orElse(null);
            VersionedValue latestValue = latest == null ? null : latest.value;

            if (latestValue != null) {
                List<ClusterNode> staleNodes = new ArrayList<>();
                for (ReadResponse response : successfulResponses) {
                    if (compareValues(response.value, latestValue) < 0) {
                        ClusterNode staleNode = nodeById(response.fromNodeId);
                        if (staleNode != null) {
                            staleNodes.add(staleNode);
                        }
                    }
                }
                if (!staleNodes.isEmpty()) {
                    state.staleReadCount.addAndGet(staleNodes.size());
                    scheduleReadRepair(req.key, latestValue, staleNodes);
                }
            }

            return ClientResponse.okVersionedValue(req.requestId, latestValue);
        } catch (IllegalStateException e) {
            return ClientResponse.error(req.requestId, ErrorCode.INVALID_CONFIG, e.getMessage());
        }
    }

    public ClientResponse handleDump(UUID requestId) {
        return ClientResponse.okVersionedDump(requestId, store.versionedSnapshot());
    }

    public ClientResponse handleDumpHints(UUID requestId) {
        return ClientResponse.okHints(requestId, store.hintSnapshot().stream()
                .sorted(Comparator.comparing(HintRecord::hintId))
                .toList());
    }

    public ClientResponse handleRunHintedHandoff(UUID requestId) {
        int delivered = 0;
        for (HintRecord hint : store.hintSnapshot()) {
            ClusterNode target = nodeById(hint.intendedHomeNodeId);
            if (target == null) {
                continue;
            }
            HintedHandoffAck ack = sendHintedHandoff(target, hint);
            if (ack != null && ack.ok) {
                store.removeHint(hint.hintId());
                state.hintsDelivered.incrementAndGet();
                delivered++;
            }
        }
        return ClientResponse.okMessage(requestId, "hinted handoff завершён", delivered);
    }

    public ClientResponse handleShowMerkleRoot(UUID requestId) {
        return ClientResponse.okMerkleRoot(requestId, currentMerkleRoot());
    }

    public ClientResponse handleWipeNodeData(UUID requestId) {
        store.clearAll();
        seen.clear();
        state.lamportCounter.set(0);
        return ClientResponse.okMessage(requestId, "данные узла очищены", 0);
    }

    public ClientResponse handleRunAntiEntropyCluster(UUID requestId) {
        try {
            validateLeaderlessConfig();
            state.antiEntropyRuns.incrementAndGet();

            Map<String, MerkleRootResponse> roots = new LinkedHashMap<>();
            for (ClusterNode homeNode : homeNodes()) {
                MerkleRootResponse response = fetchMerkleRoot(homeNode);
                if (response != null && response.ok) {
                    roots.put(homeNode.nodeId, response);
                }
            }

            int recoveredRecords = 0;
            List<String> aliveHomes = new ArrayList<>(roots.keySet());
            for (int i = 0; i < aliveHomes.size(); i++) {
                for (int j = i + 1; j < aliveHomes.size(); j++) {
                    String leftId = aliveHomes.get(i);
                    String rightId = aliveHomes.get(j);
                    MerkleRootResponse leftRoot = roots.get(leftId);
                    MerkleRootResponse rightRoot = roots.get(rightId);
                    if (Objects.equals(leftRoot.rootHash, rightRoot.rootHash)) {
                        continue;
                    }
                    recoveredRecords += reconcilePair(nodeById(leftId), nodeById(rightId));
                }
            }

            if (recoveredRecords > 0) {
                state.antiEntropyRecoveredKeys.addAndGet(recoveredRecords);
            }
            return ClientResponse.okMessage(requestId, "anti-entropy pass завершён", recoveredRecords);
        } catch (IllegalStateException e) {
            return ClientResponse.error(requestId, ErrorCode.INVALID_CONFIG, e.getMessage());
        }
    }

    public ReplWriteAck handleReplWrite(ReplWrite write) {
        if (write == null || write.operationId == null || write.key == null || write.version == null) {
            return ReplWriteAck.error(write == null ? null : write.operationId, cfg.nodeId, "некорректный REPL_WRITE");
        }

        boolean isNew = seen.markIfNew(write.operationId);
        if (!isNew) {
            return ReplWriteAck.ok(write.operationId, cfg.nodeId, write.storeAsHint);
        }

        updateLamport(write.version);

        if (write.storeAsHint) {
            if (write.intendedHomeNodeId == null) {
                return ReplWriteAck.error(write.operationId, cfg.nodeId, "для hint не указан intendedHomeNodeId");
            }
            store.storeHint(new HintRecord(write.key, write.value, write.version, write.operationId, write.intendedHomeNodeId));
            state.hintsCreated.incrementAndGet();
            return ReplWriteAck.ok(write.operationId, cfg.nodeId, true);
        }

        applyIfNewer(write.key, new VersionedValue(write.value, write.version, write.operationId.toString()));
        return ReplWriteAck.ok(write.operationId, cfg.nodeId, false);
    }

    public ReadResponse handleReadQuery(ReadQuery query) {
        if (query == null || query.requestId == null || query.key == null) {
            return ReadResponse.error(query == null ? null : query.requestId, cfg.nodeId, query == null ? null : query.key, "некорректный READ_QUERY");
        }
        return ReadResponse.ok(query.requestId, cfg.nodeId, query.key, store.getVersioned(query.key));
    }

    public HintedHandoffAck handleHintedHandoffTransfer(HintedHandoffTransfer transfer) {
        if (transfer == null || transfer.operationId == null || transfer.key == null || transfer.version == null) {
            return HintedHandoffAck.error(transfer == null ? null : transfer.operationId, cfg.nodeId, "некорректный HINTED_HANDOFF_TRANSFER");
        }

        boolean isNew = seen.markIfNew(transfer.operationId);
        if (isNew) {
            updateLamport(transfer.version);
            applyIfNewer(transfer.key, new VersionedValue(transfer.value, transfer.version, transfer.operationId.toString()));
        }
        return HintedHandoffAck.ok(transfer.operationId, cfg.nodeId);
    }

    public MerkleRootResponse handleMerkleRootRequest(MerkleRootRequest request) {
        MerkleRootResponse response = new MerkleRootResponse();
        response.requestId = request == null ? null : request.requestId;
        response.fromNodeId = cfg.nodeId;
        response.ok = true;
        response.rootHash = currentMerkleRoot();
        return response;
    }

    public MerkleDiffResponse handleMerkleDiffRequest(MerkleDiffRequest request) {
        MerkleDiffResponse response = new MerkleDiffResponse();
        response.requestId = request == null ? null : request.requestId;
        response.fromNodeId = cfg.nodeId;
        response.ok = true;

        Map<String, VersionedValue> snapshot = store.versionedSnapshot();
        Map<Integer, String> bucketHashes = MerkleTreeUtil.bucketHashes(snapshot);
        response.rootHash = MerkleTreeUtil.rootHash(bucketHashes);

        List<Integer> requestedBuckets = request == null || request.bucketIndexes == null || request.bucketIndexes.isEmpty()
                ? new ArrayList<>(bucketHashes.keySet())
                : request.bucketIndexes;

        Map<Integer, String> filteredHashes = new LinkedHashMap<>();
        for (Integer bucket : requestedBuckets) {
            filteredHashes.put(bucket, bucketHashes.get(bucket));
        }
        response.bucketHashes = filteredHashes;

        if (request != null && request.includeRecords) {
            response.records = MerkleTreeUtil.recordsForBuckets(snapshot, requestedBuckets);
        }
        return response;
    }

    public MerkleRecordsAck handleMerkleRecordsTransfer(MerkleRecordsTransfer transfer) {
        MerkleRecordsAck ack = new MerkleRecordsAck();
        ack.requestId = transfer == null ? null : transfer.requestId;
        ack.fromNodeId = cfg.nodeId;
        ack.ok = true;

        if (transfer == null || transfer.records == null) {
            ack.ok = false;
            ack.error = "некорректный MERKLE_RECORDS_TRANSFER";
            return ack;
        }

        int applied = 0;
        for (MerkleRecord record : transfer.records) {
            UUID operationId = operationIdFrom(record.operationId, record.key, record.version);
            boolean isNew = seen.markIfNew(operationId);
            if (!isNew) {
                continue;
            }
            updateLamport(record.version);
            if (applyIfNewer(record.key, new VersionedValue(record.value, record.version, operationId.toString()))) {
                applied++;
            }
        }

        ack.appliedRecords = applied;
        return ack;
    }

    private int reconcilePair(ClusterNode leftNode, ClusterNode rightNode) {
        MerkleDiffResponse leftHashes = fetchMerkleDiff(leftNode, List.of(), false);
        MerkleDiffResponse rightHashes = fetchMerkleDiff(rightNode, List.of(), false);
        if (leftHashes == null || rightHashes == null || !leftHashes.ok || !rightHashes.ok) {
            return 0;
        }

        List<Integer> diffBuckets = new ArrayList<>();
        for (int bucket = 0; bucket < MerkleTreeUtil.BUCKET_COUNT; bucket++) {
            String leftHash = leftHashes.bucketHashes.get(bucket);
            String rightHash = rightHashes.bucketHashes.get(bucket);
            if (!Objects.equals(leftHash, rightHash)) {
                diffBuckets.add(bucket);
            }
        }
        if (diffBuckets.isEmpty()) {
            return 0;
        }

        MerkleDiffResponse leftRecords = fetchMerkleDiff(leftNode, diffBuckets, true);
        MerkleDiffResponse rightRecords = fetchMerkleDiff(rightNode, diffBuckets, true);
        if (leftRecords == null || rightRecords == null || !leftRecords.ok || !rightRecords.ok) {
            return 0;
        }

        Map<String, MerkleRecord> leftMap = recordsByKey(leftRecords.records);
        Map<String, MerkleRecord> rightMap = recordsByKey(rightRecords.records);

        List<MerkleRecord> sendToLeft = new ArrayList<>();
        List<MerkleRecord> sendToRight = new ArrayList<>();
        LinkedHashSet<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(leftMap.keySet());
        allKeys.addAll(rightMap.keySet());

        for (String key : allKeys) {
            MerkleRecord left = leftMap.get(key);
            MerkleRecord right = rightMap.get(key);
            MerkleRecord latest = compareRecords(left, right) >= 0 ? left : right;
            if (latest == null) {
                continue;
            }
            if (compareRecords(left, latest) < 0) {
                sendToLeft.add(latest);
            }
            if (compareRecords(right, latest) < 0) {
                sendToRight.add(latest);
            }
        }

        int applied = 0;
        if (!sendToLeft.isEmpty()) {
            MerkleRecordsAck ack = sendMerkleRecords(leftNode, sendToLeft);
            if (ack != null && ack.ok) {
                applied += ack.appliedRecords;
            }
        }
        if (!sendToRight.isEmpty()) {
            MerkleRecordsAck ack = sendMerkleRecords(rightNode, sendToRight);
            if (ack != null && ack.ok) {
                applied += ack.appliedRecords;
            }
        }
        return applied;
    }

    private void scheduleReadRepair(String key, VersionedValue latestValue, List<ClusterNode> staleNodes) {
        asyncExecutor.submit(() -> {
            for (ClusterNode staleNode : staleNodes) {
                ReplWrite repair = new ReplWrite();
                repair.operationId = operationIdFrom(latestValue.operationId, key, latestValue.version);
                repair.sourceNodeId = cfg.nodeId;
                repair.key = key;
                repair.value = latestValue.value;
                repair.version = latestValue.version;
                sendReplWrite(staleNode, repair);
                state.readRepairCount.incrementAndGet();
            }
        });
    }

    private ReplWriteAck sendReplWrite(ClusterNode node, ReplWrite write) {
        if (node == null) {
            return null;
        }
        if (cfg.nodeId.equals(node.nodeId)) {
            return handleReplWrite(write);
        }
        delayIfNeeded(MessageType.REPL_WRITE);
        return send(node, MessageType.REPL_WRITE, write, ReplWriteAck.class);
    }

    private ReadResponse sendReadQuery(ClusterNode node, UUID requestId, String key) {
        if (node == null) {
            return null;
        }
        ReadQuery query = new ReadQuery();
        query.requestId = requestId;
        query.key = key;
        if (cfg.nodeId.equals(node.nodeId)) {
            return handleReadQuery(query);
        }
        return send(node, MessageType.READ_QUERY, query, ReadResponse.class);
    }

    private HintedHandoffAck sendHintedHandoff(ClusterNode node, HintRecord hint) {
        if (node == null) {
            return null;
        }
        HintedHandoffTransfer transfer = new HintedHandoffTransfer();
        transfer.operationId = hint.operationId;
        transfer.key = hint.key;
        transfer.value = hint.value;
        transfer.version = hint.version;
        transfer.sourceNodeId = cfg.nodeId;
        transfer.intendedHomeNodeId = hint.intendedHomeNodeId;
        if (cfg.nodeId.equals(node.nodeId)) {
            return handleHintedHandoffTransfer(transfer);
        }
        delayIfNeeded(MessageType.HINTED_HANDOFF_TRANSFER);
        return send(node, MessageType.HINTED_HANDOFF_TRANSFER, transfer, HintedHandoffAck.class);
    }

    private MerkleRootResponse fetchMerkleRoot(ClusterNode node) {
        if (node == null) {
            return null;
        }
        MerkleRootRequest request = new MerkleRootRequest();
        request.requestId = UUID.randomUUID();
        if (cfg.nodeId.equals(node.nodeId)) {
            return handleMerkleRootRequest(request);
        }
        return send(node, MessageType.MERKLE_ROOT_REQUEST, request, MerkleRootResponse.class);
    }

    private MerkleDiffResponse fetchMerkleDiff(ClusterNode node, List<Integer> buckets, boolean includeRecords) {
        if (node == null) {
            return null;
        }
        MerkleDiffRequest request = new MerkleDiffRequest();
        request.requestId = UUID.randomUUID();
        request.bucketIndexes = buckets;
        request.includeRecords = includeRecords;
        if (cfg.nodeId.equals(node.nodeId)) {
            return handleMerkleDiffRequest(request);
        }
        return send(node, MessageType.MERKLE_DIFF_REQUEST, request, MerkleDiffResponse.class);
    }

    private MerkleRecordsAck sendMerkleRecords(ClusterNode node, List<MerkleRecord> records) {
        if (node == null || records.isEmpty()) {
            return null;
        }
        MerkleRecordsTransfer transfer = new MerkleRecordsTransfer();
        transfer.requestId = UUID.randomUUID();
        transfer.sourceNodeId = cfg.nodeId;
        transfer.records = records;
        if (cfg.nodeId.equals(node.nodeId)) {
            return handleMerkleRecordsTransfer(transfer);
        }
        delayIfNeeded(MessageType.MERKLE_RECORDS_TRANSFER);
        return send(node, MessageType.MERKLE_RECORDS_TRANSFER, transfer, MerkleRecordsAck.class);
    }

    private <T> T send(ClusterNode node, MessageType type, Object payload, Class<T> responseType) {
        try {
            MessageEnvelope env = MessageEnvelope.of(type, codec.toTree(payload));
            String responseLine = client.callLine(node.host, node.port, codec.toLine(env), state.hotPathTimeoutMs);
            MessageEnvelope responseEnv = codec.fromLine(responseLine, MessageEnvelope.class);
            return codec.treeToValue(responseEnv.payload, responseType);
        } catch (Exception e) {
            return null;
        }
    }

    private <T> List<RpcResult<T>> callAll(List<ClusterNode> nodes, Function<ClusterNode, T> call) {
        List<CompletableFuture<RpcResult<T>>> futures = new ArrayList<>();
        for (ClusterNode node : nodes) {
            futures.add(CompletableFuture.supplyAsync(() -> new RpcResult<>(node, call.apply(node)), asyncExecutor));
        }

        List<RpcResult<T>> results = new ArrayList<>();
        for (CompletableFuture<RpcResult<T>> future : futures) {
            RpcResult<T> result = joinQuietly(future);
            if (result != null) {
                results.add(result);
            }
        }
        return results;
    }

    private <T> T joinQuietly(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            return null;
        }
    }

    private UUID hintOperationId(UUID operationId, String intendedHomeNodeId) {
        String source = operationId + "|" + intendedHomeNodeId;
        return UUID.nameUUIDFromBytes(source.getBytes(StandardCharsets.UTF_8));
    }

    private boolean applyIfNewer(String key, VersionedValue incoming) {
        VersionedValue local = store.getVersioned(key);
        if (compareValues(incoming, local) > 0) {
            store.putVersioned(key, incoming);
            return true;
        }
        return false;
    }

    private void updateLamport(Version incoming) {
        if (incoming == null) {
            return;
        }
        state.lamportCounter.updateAndGet(current -> Math.max(current, incoming.lamport));
    }

    private int compareValues(VersionedValue left, VersionedValue right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left.version == null && right.version == null) {
            return 0;
        }
        if (left.version == null) {
            return -1;
        }
        return left.version.compareTo(right.version);
    }

    private int compareRecords(MerkleRecord left, MerkleRecord right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left.version == null && right.version == null) {
            return 0;
        }
        if (left.version == null) {
            return -1;
        }
        return left.version.compareTo(right.version);
    }

    private Map<String, MerkleRecord> recordsByKey(List<MerkleRecord> records) {
        Map<String, MerkleRecord> result = new LinkedHashMap<>();
        if (records == null) {
            return result;
        }
        for (MerkleRecord record : records) {
            result.put(record.key, record);
        }
        return result;
    }

    private UUID operationIdFrom(String operationId, String key, Version version) {
        if (operationId != null && !operationId.isBlank()) {
            return UUID.fromString(operationId);
        }
        String fallback = key + "|" + (version == null ? 0 : version.lamport) + "|" + (version == null ? "" : version.nodeId);
        return UUID.nameUUIDFromBytes(fallback.getBytes(StandardCharsets.UTF_8));
    }

    private void delayIfNeeded(MessageType type) {
        if (type != MessageType.REPL_WRITE
                && type != MessageType.HINTED_HANDOFF_TRANSFER
                && type != MessageType.MERKLE_RECORDS_TRANSFER) {
            return;
        }
        int min = Math.max(0, state.delayMinMs);
        int max = Math.max(min, state.delayMaxMs);
        if (max == 0) {
            return;
        }
        int delay = ThreadLocalRandom.current().nextInt(min, max + 1);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String currentMerkleRoot() {
        Map<String, VersionedValue> snapshot = store.versionedSnapshot();
        return MerkleTreeUtil.rootHash(MerkleTreeUtil.bucketHashes(snapshot));
    }

    private List<ClusterNode> homeNodes() {
        return state.homeReplicaNodeIds.stream()
                .map(this::nodeById)
                .filter(Objects::nonNull)
                .toList();
    }

    private List<ClusterNode> spareNodes() {
        return state.spareNodeIds.stream()
                .map(this::nodeById)
                .filter(Objects::nonNull)
                .toList();
    }

    private ClusterNode nodeById(String nodeId) {
        return state.nodes.get(nodeId);
    }

    private void validateLeaderlessConfig() {
        if (state.clusterMode != ClusterMode.LEADERLESS) {
            throw new IllegalStateException("leaderless-операция вызвана вне режима leaderless");
        }
        if (state.homeReplicaNodeIds.size() != HOME_REPLICA_COUNT) {
            throw new IllegalStateException("в leaderless должно быть ровно 5 home-реплик");
        }
        if (state.spareNodeIds.size() != SPARE_NODE_COUNT) {
            throw new IllegalStateException("в leaderless должно быть ровно 2 spare-узла");
        }
        if (state.writeQuorum < 1 || state.writeQuorum > HOME_REPLICA_COUNT) {
            throw new IllegalStateException("W должен быть в диапазоне 1..5");
        }
        if (state.readQuorum < 1 || state.readQuorum > HOME_REPLICA_COUNT) {
            throw new IllegalStateException("R должен быть в диапазоне 1..5");
        }
        if (state.writeQuorum + state.readQuorum <= HOME_REPLICA_COUNT) {
            throw new IllegalStateException("должно выполняться W + R > 5");
        }
    }

    private record RpcResult<T>(ClusterNode node, T response) {}
}

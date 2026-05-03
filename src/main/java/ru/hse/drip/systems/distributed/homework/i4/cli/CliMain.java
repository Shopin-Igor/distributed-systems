package ru.hse.drip.systems.distributed.homework.i4.cli;

import ru.hse.drip.systems.distributed.homework.i4.protocol.*;
import ru.hse.drip.systems.distributed.homework.i4.replication.ReplicationMode;
import ru.hse.drip.systems.distributed.homework.i4.transport.TcpClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CliMain {
    private static final int CLIENT_TIMEOUT_MS = 5_000;

    public static void main(String[] args) throws Exception {
        ControllerState st = new ControllerState();
        TcpClient client = new TcpClient();
        JsonCodec codec = new JsonCodec();
        CliParser parser = new CliParser();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("CLI готов. Команды: addNode/removeNode/listNodes/setMode/setLeader/setTopology/setStarCenter/setFollowerLeader/setReplication/setRF/setSemiSyncAcks/setReplicationDelayMs setHomeReplicas setSpareNodes setQuorum setWriteQuorumMode put/get/delete/dump/getAll/clusterDump/dumpHints/runHintedHandoff/showMerkleRoot/wipeNodeData/runAntiEntropyCluster");
        while (true) {
            System.out.print("> ");
            String line = br.readLine();
            if (line == null) {
                break;
            }
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            try {
                CliParser.Parsed cmd = parser.parse(line);
                if (cmd == null) {
                    continue;
                }

                switch (cmd.cmd) {
                    case "addNode" -> {
                        ControllerState candidate = copyState(st);
                        String nodeId = cmd.args.getFirst();
                        String host = cmd.args.get(1);
                        int port = Integer.parseInt(cmd.args.get(2));
                        candidate.nodes.put(nodeId, new ClusterNode(nodeId, host, port));
                        if (candidate.leaderNodeId == null) {
                            candidate.leaderNodeId = nodeId;
                        }
                        rebalanceRoles(candidate);
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "removeNode" -> {
                        ControllerState candidate = copyState(st);
                        String nodeId = cmd.args.getFirst();
                        candidate.nodes.remove(nodeId);
                        candidate.homeReplicaNodeIds.remove(nodeId);
                        candidate.spareNodeIds.remove(nodeId);
                        if (nodeId.equals(candidate.leaderNodeId)) {
                            candidate.leaderNodeId = candidate.nodes.keySet().stream().sorted().findFirst().orElse(null);
                        }
                        if (nodeId.equals(candidate.starCenterNodeId)) {
                            candidate.starCenterNodeId = candidate.nodes.keySet().stream().sorted().findFirst().orElse(null);
                        }
                        rebalanceRoles(candidate);
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "listNodes" -> printNodes(st);
                    case "setMode" -> {
                        ControllerState candidate = copyState(st);
                        candidate.clusterMode = parseClusterMode(cmd.args.getFirst());
                        rebalanceRoles(candidate);
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setLeader" -> {
                        ControllerState candidate = copyState(st);
                        String nodeId = cmd.args.getFirst();
                        if (!candidate.nodes.containsKey(nodeId)) {
                            throw new IllegalArgumentException("неизвестный узел");
                        }
                        candidate.leaderNodeId = nodeId;
                        rebalanceRoles(candidate);
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setTopology" -> {
                        ControllerState candidate = copyState(st);
                        candidate.topology = parseTopology(cmd.args.getFirst());
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setStarCenter" -> {
                        ControllerState candidate = copyState(st);
                        String nodeId = cmd.args.getFirst();
                        if (!candidate.nodes.containsKey(nodeId)) {
                            throw new IllegalArgumentException("неизвестный узел");
                        }
                        candidate.starCenterNodeId = nodeId;
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setFollowerLeader" -> {
                        ControllerState candidate = copyState(st);
                        if (candidate.clusterMode != ClusterMode.MULTI) {
                            throw new IllegalArgumentException("команда доступна только в multi-режиме");
                        }
                        String followerId = cmd.args.getFirst();
                        String leaderId = cmd.args.get(1);
                        if (!candidate.nodes.containsKey(followerId)) {
                            throw new IllegalArgumentException("неизвестный follower-узел");
                        }
                        rebalanceRoles(candidate);
                        if (!candidate.leaderNodeIds.contains(leaderId)) {
                            throw new IllegalArgumentException("leader должен входить в список multi-лидеров");
                        }
                        if (candidate.leaderNodeIds.contains(followerId)) {
                            throw new IllegalArgumentException("leader-узел нельзя привязать как follower");
                        }
                        candidate.nodes.get(followerId).assignedLeaderNodeId = leaderId;
                        rebalanceRoles(candidate);
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setReplication" -> {
                        ControllerState candidate = copyState(st);
                        candidate.mode = parseReplicationMode(cmd.args.getFirst());
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setRF" -> {
                        ControllerState candidate = copyState(st);
                        candidate.rf = Integer.parseInt(cmd.args.getFirst());
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setSemiSyncAcks" -> {
                        ControllerState candidate = copyState(st);
                        candidate.k = Integer.parseInt(cmd.args.getFirst());
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setReplicationDelayMs" -> {
                        ControllerState candidate = copyState(st);
                        candidate.delayMinMs = Integer.parseInt(cmd.args.getFirst());
                        candidate.delayMaxMs = Integer.parseInt(cmd.args.get(1));
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setHomeReplicas" -> {
                        ControllerState candidate = copyState(st);
                        candidate.homeReplicaNodeIds = new ArrayList<>(cmd.args);
                        rebalanceRoles(candidate);
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setSpareNodes" -> {
                        ControllerState candidate = copyState(st);
                        candidate.spareNodeIds = new ArrayList<>(cmd.args);
                        rebalanceRoles(candidate);
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setQuorum" -> {
                        ControllerState candidate = copyState(st);
                        candidate.writeQuorum = Integer.parseInt(cmd.args.getFirst());
                        candidate.readQuorum = Integer.parseInt(cmd.args.get(1));
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setWriteQuorumMode" -> {
                        ControllerState candidate = copyState(st);
                        candidate.writeQuorumMode = parseWriteQuorumMode(cmd.args.getFirst());
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "put", "get", "delete", "dump" -> executeClientCommand(st, client, codec, cmd);
                    case "getAll" -> executeGetAll(st, client, codec, cmd.args.getFirst());
                    case "clusterDump" -> executeClusterDump(st, client, codec);
                    case "dumpHints" -> executeDumpHints(st, client, codec, cmd.opts.get("--target"));
                    case "runHintedHandoff" -> executeRunHintedHandoff(st, client, codec, cmd.opts.get("--target"));
                    case "showMerkleRoot" -> executeShowMerkleRoot(st, client, codec, cmd.args.getFirst());
                    case "wipeNodeData" -> executeWipeNodeData(st, client, codec, cmd.args.getFirst());
                    case "runAntiEntropyCluster" -> executeRunAntiEntropyCluster(st, client, codec);
                    default -> System.out.println("Неизвестная команда: " + cmd.cmd);
                }
            } catch (Exception e) {
                System.out.println("ОШИБКА: " + e.getMessage());
            }
        }
    }

    private static void printNodes(ControllerState st) {
        st.nodes.values().forEach(n -> System.out.println(nodeLine(st, n)));
        System.out.println("режим=" + st.clusterMode.name().toLowerCase());
        if (st.clusterMode == ClusterMode.LEADERLESS) {
            System.out.println("homeReplicas=" + st.homeReplicaNodeIds);
            System.out.println("spareNodes=" + st.spareNodeIds);
            System.out.println("quorum W=" + st.writeQuorum + " R=" + st.readQuorum + " writeMode=" + st.writeQuorumMode.name().toLowerCase());
        } else {
            System.out.println("лидер=" + st.leaderNodeId);
            System.out.println("лидеры=" + st.leaderNodeIds);
            System.out.println("топология=" + st.topology.name().toLowerCase());
            System.out.println("центрЗвезды=" + st.starCenterNodeId);
        }
    }

    private static String nodeLine(ControllerState st, ClusterNode node) {
        String role = effectiveRole(st, node.nodeId);
        String line = node.nodeId + " " + node.host + ":" + node.port + " роль=" + role;
        if (st.clusterMode == ClusterMode.MULTI && "follower".equals(role)) {
            line += " leader=" + node.assignedLeaderNodeId;
        }
        return line;
    }

    private static void executeClientCommand(ControllerState st, TcpClient client, JsonCodec codec, CliParser.Parsed cmd) throws Exception {
        String target = resolveTarget(st, cmd);
        String clientId = cmd.opts.getOrDefault("--client", "cli");
        ClusterNode node = st.nodes.get(target);
        if (node == null) {
            throw new IllegalArgumentException("неизвестный target-узел");
        }

        ClientRequest req = switch (cmd.cmd) {
            case "put" -> ClientRequest.put(clientId, cmd.args.getFirst(), cmd.args.get(1));
            case "delete" -> ClientRequest.delete(clientId, cmd.args.getFirst());
            case "get" -> ClientRequest.get(clientId, cmd.args.getFirst());
            case "dump" -> ClientRequest.dump(clientId);
            default -> throw new IllegalArgumentException("неизвестная клиентская команда " + cmd.cmd);
        };

        boolean canUseAnyLeaderlessCoordinator = st.clusterMode == ClusterMode.LEADERLESS
                && cmd.opts.get("--target") == null
                && ("put".equals(cmd.cmd) || "get".equals(cmd.cmd));
        ClientResponse response = canUseAnyLeaderlessCoordinator
                ? callFirstAvailable(leaderlessCoordinatorCandidates(st), req, client, codec)
                : call(node, req, client, codec);
        System.out.println(codec.toLine(response));
    }

    private static void executeGetAll(ControllerState st, TcpClient client, JsonCodec codec, String key) {
        for (ClusterNode node : st.nodes.values()) {
            try {
                ClientResponse resp = call(node, ClientRequest.get("cli", key), client, codec);
                String version = resp.versionLamport == null ? "-" : resp.versionLamport + "," + resp.versionNodeId;
                System.out.println(node.nodeId + " значение=" + resp.value + " версия=" + version + " opId=" + resp.operationId);
            } catch (Exception e) {
                System.out.println(node.nodeId + " ОШИБКА " + e.getMessage());
            }
        }
    }

    private static void executeClusterDump(ControllerState st, TcpClient client, JsonCodec codec) {
        for (ClusterNode node : st.nodes.values()) {
            try {
                ClientResponse resp = call(node, ClientRequest.dump("cli"), client, codec);
                System.out.println(node.nodeId + " " + codec.toLine(resp));
            } catch (Exception e) {
                System.out.println(node.nodeId + " ОШИБКА " + e.getMessage());
            }
        }
    }

    private static void executeDumpHints(ControllerState st, TcpClient client, JsonCodec codec, String targetNodeId) {
        List<ClusterNode> targets = targetNodeId == null ? new ArrayList<>(st.nodes.values()) : List.of(requireNode(st, targetNodeId));
        for (ClusterNode target : targets) {
            try {
                ClientResponse resp = call(target, ClientRequest.dumpHints("cli"), client, codec);
                System.out.println(target.nodeId + " " + codec.toLine(resp));
            } catch (Exception e) {
                System.out.println(target.nodeId + " ОШИБКА " + e.getMessage());
            }
        }
    }

    private static void executeRunHintedHandoff(ControllerState st, TcpClient client, JsonCodec codec, String targetNodeId) {
        List<ClusterNode> targets = targetNodeId == null ? new ArrayList<>(st.nodes.values()) : List.of(requireNode(st, targetNodeId));
        for (ClusterNode target : targets) {
            try {
                ClientResponse resp = call(target, ClientRequest.runHintedHandoff("cli"), client, codec);
                System.out.println(target.nodeId + " " + codec.toLine(resp));
            } catch (Exception e) {
                System.out.println(target.nodeId + " ОШИБКА " + e.getMessage());
            }
        }
    }

    private static void executeShowMerkleRoot(ControllerState st, TcpClient client, JsonCodec codec, String targetNodeId) throws Exception {
        ClusterNode node = requireNode(st, targetNodeId);
        ClientResponse resp = call(node, ClientRequest.showMerkleRoot("cli"), client, codec);
        System.out.println(node.nodeId + " root=" + resp.merkleRoot);
    }

    private static void executeWipeNodeData(ControllerState st, TcpClient client, JsonCodec codec, String targetNodeId) throws Exception {
        ClusterNode node = requireNode(st, targetNodeId);
        ClientResponse resp = call(node, ClientRequest.wipeNodeData("cli"), client, codec);
        System.out.println(node.nodeId + " " + codec.toLine(resp));
    }

    private static void executeRunAntiEntropyCluster(ControllerState st, TcpClient client, JsonCodec codec) throws Exception {
        String targetNodeId = !st.homeReplicaNodeIds.isEmpty()
                ? st.homeReplicaNodeIds.getFirst()
                : defaultTarget(st, "put");
        ClusterNode node = requireNode(st, targetNodeId);
        ClientResponse resp = call(node, ClientRequest.runAntiEntropy("cli"), client, codec);
        System.out.println(node.nodeId + " " + codec.toLine(resp));
    }

    private static ClusterNode requireNode(ControllerState st, String nodeId) {
        ClusterNode node = st.nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("неизвестный узел " + nodeId);
        }
        return node;
    }

    private static ClientResponse callFirstAvailable(List<ClusterNode> candidates, ClientRequest req, TcpClient client, JsonCodec codec) throws Exception {
        Exception lastError = null;
        for (ClusterNode candidate : candidates) {
            try {
                return call(candidate, req, client, codec);
            } catch (Exception e) {
                lastError = e;
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new IllegalArgumentException("нет coordinator-узлов для leaderless-запроса");
    }

    private static List<ClusterNode> leaderlessCoordinatorCandidates(ControllerState st) {
        List<ClusterNode> candidates = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        addCoordinatorCandidates(st, candidates, seen, st.homeReplicaNodeIds);
        addCoordinatorCandidates(st, candidates, seen, st.spareNodeIds);
        for (ClusterNode node : st.nodes.values()) {
            if (seen.add(node.nodeId)) {
                candidates.add(node);
            }
        }
        return candidates;
    }

    private static void addCoordinatorCandidates(ControllerState st, List<ClusterNode> candidates, Set<String> seen, List<String> nodeIds) {
        for (String nodeId : nodeIds) {
            ClusterNode node = st.nodes.get(nodeId);
            if (node != null && seen.add(node.nodeId)) {
                candidates.add(node);
            }
        }
    }

    private static ClientResponse call(ClusterNode node, ClientRequest req, TcpClient client, JsonCodec codec) throws Exception {
        MessageEnvelope env = MessageEnvelope.of(req.type, codec.toTree(req));
        String respLine = client.callLine(node.host, node.port, codec.toLine(env), CLIENT_TIMEOUT_MS);
        MessageEnvelope respEnv = codec.fromLine(respLine, MessageEnvelope.class);
        return codec.treeToValue(respEnv.payload, ClientResponse.class);
    }

    private static void broadcastClusterUpdate(ControllerState st, TcpClient client, JsonCodec codec) {
        validateControllerState(st);
        rebalanceRoles(st);

        ClusterUpdate upd = new ClusterUpdate();
        upd.nodes = List.copyOf(st.nodes.values());
        upd.leaderNodeId = st.leaderNodeId;
        upd.leaderNodeIds = List.copyOf(st.leaderNodeIds);
        upd.clusterMode = st.clusterMode;
        upd.topology = st.topology;
        upd.starCenterNodeId = st.starCenterNodeId;
        upd.replicationMode = st.mode;
        upd.rf = st.rf;
        upd.semiSyncAcksK = st.k;
        upd.replicationDelayMinMs = st.delayMinMs;
        upd.replicationDelayMaxMs = st.delayMaxMs;
        upd.hotPathTimeoutMs = st.hotPathTimeoutMs;
        upd.homeReplicaNodeIds = List.copyOf(st.homeReplicaNodeIds);
        upd.spareNodeIds = List.copyOf(st.spareNodeIds);
        upd.writeQuorum = st.writeQuorum;
        upd.readQuorum = st.readQuorum;
        upd.writeQuorumMode = st.writeQuorumMode;

        MessageEnvelope env = MessageEnvelope.of(MessageType.CLUSTER_UPDATE, codec.toTree(upd));
        String line = codec.toLine(env);

        for (ClusterNode n : st.nodes.values()) {
            try {
                client.callLine(n.host, n.port, line, 500);
            } catch (Exception ignored) {
            }
        }
    }

    private static void validateControllerState(ControllerState st) {
        if (st.nodes.isEmpty()) {
            return;
        }

        if (st.clusterMode == ClusterMode.LEADERLESS) {
            if (st.nodes.size() != 7) {
                throw new IllegalArgumentException("leaderless требует ровно 7 узлов");
            }
            if (st.homeReplicaNodeIds.size() != 5) {
                throw new IllegalArgumentException("leaderless требует ровно 5 home-реплик");
            }
            if (st.spareNodeIds.size() != 2) {
                throw new IllegalArgumentException("leaderless требует ровно 2 spare-узла");
            }

            Set<String> assigned = new HashSet<>();
            for (String nodeId : st.homeReplicaNodeIds) {
                if (!st.nodes.containsKey(nodeId)) {
                    throw new IllegalArgumentException("неизвестная home-реплика " + nodeId);
                }
                if (!assigned.add(nodeId)) {
                    throw new IllegalArgumentException("home/spare-конфигурация содержит дубликаты");
                }
            }
            for (String nodeId : st.spareNodeIds) {
                if (!st.nodes.containsKey(nodeId)) {
                    throw new IllegalArgumentException("неизвестный spare-узел " + nodeId);
                }
                if (!assigned.add(nodeId)) {
                    throw new IllegalArgumentException("home/spare-конфигурация содержит пересечения");
                }
            }
            if (assigned.size() != 7) {
                throw new IllegalArgumentException("home+spare должны покрывать все 7 узлов");
            }
            if (st.writeQuorum < 1 || st.writeQuorum > 5 || st.readQuorum < 1 || st.readQuorum > 5) {
                throw new IllegalArgumentException("W и R должны быть в диапазоне 1..5");
            }
            if (st.writeQuorum + st.readQuorum <= 5) {
                throw new IllegalArgumentException("должно выполняться W + R > 5");
            }
            return;
        }

        if (st.rf < 1 || st.rf > st.nodes.size()) {
            throw new IllegalArgumentException("RF должен быть в диапазоне 1..clusterSize");
        }
        if (st.mode == ReplicationMode.SEMI_SYNC && (st.k < 1 || st.k > st.rf - 1)) {
            throw new IllegalArgumentException("K должен быть в диапазоне 1..RF-1");
        }
        if (st.clusterMode == ClusterMode.MULTI) {
            if (st.leaderNodeIds.isEmpty()) {
                throw new IllegalArgumentException("multi-режим требует хотя бы одного leader");
            }
            Set<String> leaders = new HashSet<>(st.leaderNodeIds);
            for (String leaderId : st.leaderNodeIds) {
                if (!st.nodes.containsKey(leaderId)) {
                    throw new IllegalArgumentException("неизвестный multi-leader " + leaderId);
                }
            }
            if (st.topology == TopologyType.STAR && (st.starCenterNodeId == null || !leaders.contains(st.starCenterNodeId))) {
                throw new IllegalArgumentException("topology star требует leader-узел в качестве центра");
            }
            for (ClusterNode node : st.nodes.values()) {
                if (leaders.contains(node.nodeId)) {
                    continue;
                }
                if (node.assignedLeaderNodeId == null || !leaders.contains(node.assignedLeaderNodeId)) {
                    throw new IllegalArgumentException("follower " + node.nodeId + " должен быть привязан к multi-leader");
                }
            }
        }
    }

    private static void rebalanceRoles(ControllerState st) {
        List<String> orderedIds = st.nodes.keySet().stream().sorted().toList();

        if (st.clusterMode == ClusterMode.LEADERLESS) {
            boolean currentAssignmentsValid = st.homeReplicaNodeIds.size() == 5
                    && st.spareNodeIds.size() == 2
                    && st.homeReplicaNodeIds.stream().allMatch(st.nodes::containsKey)
                    && st.spareNodeIds.stream().allMatch(st.nodes::containsKey);
            if (!currentAssignmentsValid && orderedIds.size() >= 7) {
                st.homeReplicaNodeIds = new ArrayList<>(orderedIds.subList(0, 5));
                st.spareNodeIds = new ArrayList<>(orderedIds.subList(5, 7));
            }
            st.leaderNodeId = orderedIds.isEmpty() ? null : orderedIds.getFirst();
            st.leaderNodeIds = new ArrayList<>();
            for (ClusterNode node : st.nodes.values()) {
                node.role = effectiveRole(st, node.nodeId);
                node.assignedLeaderNodeId = null;
            }
        } else if (st.clusterMode == ClusterMode.MULTI) {
            int leaderCount = Math.min(4, orderedIds.size());
            st.leaderNodeIds = new ArrayList<>(orderedIds.subList(0, leaderCount));
            if (st.starCenterNodeId == null || !st.leaderNodeIds.contains(st.starCenterNodeId)) {
                st.starCenterNodeId = st.leaderNodeIds.isEmpty() ? null : st.leaderNodeIds.getFirst();
            }
            assignMultiFollowerLeaders(st, orderedIds);
        } else {
            if (st.leaderNodeId == null && !orderedIds.isEmpty()) {
                st.leaderNodeId = orderedIds.getFirst();
            }
            st.leaderNodeIds = st.leaderNodeId == null ? new ArrayList<>() : new ArrayList<>(List.of(st.leaderNodeId));
            for (ClusterNode node : st.nodes.values()) {
                node.role = effectiveRole(st, node.nodeId);
                node.assignedLeaderNodeId = null;
            }
        }
    }

    private static void assignMultiFollowerLeaders(ControllerState st, List<String> orderedIds) {
        Set<String> leaders = new HashSet<>(st.leaderNodeIds);
        if (leaders.isEmpty()) {
            for (ClusterNode node : st.nodes.values()) {
                node.role = "follower";
                node.assignedLeaderNodeId = null;
            }
            return;
        }

        int autoIndex = 0;
        for (String nodeId : orderedIds) {
            ClusterNode node = st.nodes.get(nodeId);
            if (node == null) {
                continue;
            }
            if (leaders.contains(nodeId)) {
                node.role = "leader";
                node.assignedLeaderNodeId = null;
                continue;
            }

            node.role = "follower";
            if (node.assignedLeaderNodeId == null || !leaders.contains(node.assignedLeaderNodeId)) {
                node.assignedLeaderNodeId = st.leaderNodeIds.get(autoIndex % st.leaderNodeIds.size());
            }
            autoIndex++;
        }
    }

    private static String effectiveRole(ControllerState st, String nodeId) {
        if (st.clusterMode == ClusterMode.LEADERLESS) {
            if (st.homeReplicaNodeIds.contains(nodeId)) {
                return "home";
            }
            if (st.spareNodeIds.contains(nodeId)) {
                return "spare";
            }
            return "unassigned";
        }
        if (st.clusterMode == ClusterMode.MULTI) {
            return st.leaderNodeIds.contains(nodeId) ? "leader" : "follower";
        }
        return nodeId.equals(st.leaderNodeId) ? "leader" : "follower";
    }

    private static ControllerState copyState(ControllerState st) {
        ControllerState copy = new ControllerState();
        for (ClusterNode node : st.nodes.values()) {
            copy.nodes.put(node.nodeId, new ClusterNode(node.nodeId, node.host, node.port, node.role, node.assignedLeaderNodeId));
        }
        copy.leaderNodeId = st.leaderNodeId;
        copy.clusterMode = st.clusterMode;
        copy.topology = st.topology;
        copy.starCenterNodeId = st.starCenterNodeId;
        copy.leaderNodeIds = new ArrayList<>(st.leaderNodeIds);
        copy.mode = st.mode;
        copy.rf = st.rf;
        copy.k = st.k;
        copy.delayMinMs = st.delayMinMs;
        copy.delayMaxMs = st.delayMaxMs;
        copy.hotPathTimeoutMs = st.hotPathTimeoutMs;
        copy.homeReplicaNodeIds = new ArrayList<>(st.homeReplicaNodeIds);
        copy.spareNodeIds = new ArrayList<>(st.spareNodeIds);
        copy.writeQuorum = st.writeQuorum;
        copy.readQuorum = st.readQuorum;
        copy.writeQuorumMode = st.writeQuorumMode;
        return copy;
    }

    private static void applyState(ControllerState target, ControllerState source) {
        target.nodes.clear();
        target.nodes.putAll(source.nodes);
        target.leaderNodeId = source.leaderNodeId;
        target.clusterMode = source.clusterMode;
        target.topology = source.topology;
        target.starCenterNodeId = source.starCenterNodeId;
        target.leaderNodeIds = new ArrayList<>(source.leaderNodeIds);
        target.mode = source.mode;
        target.rf = source.rf;
        target.k = source.k;
        target.delayMinMs = source.delayMinMs;
        target.delayMaxMs = source.delayMaxMs;
        target.hotPathTimeoutMs = source.hotPathTimeoutMs;
        target.homeReplicaNodeIds = new ArrayList<>(source.homeReplicaNodeIds);
        target.spareNodeIds = new ArrayList<>(source.spareNodeIds);
        target.writeQuorum = source.writeQuorum;
        target.readQuorum = source.readQuorum;
        target.writeQuorumMode = source.writeQuorumMode;
    }

    private static String defaultTarget(ControllerState st, String command) {
        if (st.clusterMode == ClusterMode.LEADERLESS) {
            if (!st.homeReplicaNodeIds.isEmpty()) {
                return st.homeReplicaNodeIds.getFirst();
            }
            return st.nodes.keySet().stream().sorted().findFirst().orElse(null);
        }
        if ("put".equals(command) && st.clusterMode == ClusterMode.MULTI && !st.leaderNodeIds.isEmpty()) {
            return st.leaderNodeIds.getFirst();
        }
        return st.leaderNodeId;
    }

    private static String resolveTarget(ControllerState st, CliParser.Parsed cmd) {
        String explicit = cmd.opts.get("--target");
        if (explicit != null) {
            return explicit;
        }
        if ("get".equals(cmd.cmd) && "leader".equalsIgnoreCase(cmd.opts.get("--read"))) {
            return st.leaderNodeId;
        }
        if ("delete".equals(cmd.cmd)) {
            return st.leaderNodeId;
        }
        return defaultTarget(st, cmd.cmd);
    }

    private static ClusterMode parseClusterMode(String value) {
        return switch (value) {
            case "single" -> ClusterMode.SINGLE;
            case "multi" -> ClusterMode.MULTI;
            case "leaderless" -> ClusterMode.LEADERLESS;
            default -> throw new IllegalArgumentException("режим должен быть single|multi|leaderless");
        };
    }

    private static TopologyType parseTopology(String value) {
        return switch (value) {
            case "mesh" -> TopologyType.MESH;
            case "ring" -> TopologyType.RING;
            case "star" -> TopologyType.STAR;
            default -> throw new IllegalArgumentException("топология должна быть mesh|ring|star");
        };
    }

    private static ReplicationMode parseReplicationMode(String value) {
        return switch (value) {
            case "async" -> ReplicationMode.ASYNC;
            case "sync" -> ReplicationMode.SYNC;
            case "semi-sync" -> ReplicationMode.SEMI_SYNC;
            default -> throw new IllegalArgumentException("режим должен быть async|sync|semi-sync");
        };
    }

    private static WriteQuorumMode parseWriteQuorumMode(String value) {
        return switch (value) {
            case "strict" -> WriteQuorumMode.STRICT;
            case "sloppy" -> WriteQuorumMode.SLOPPY;
            default -> throw new IllegalArgumentException("write quorum mode должен быть strict|sloppy");
        };
    }
}

package ru.hse.drip.systems.distributed.homework.i3.cli;

import ru.hse.drip.systems.distributed.homework.i3.protocol.*;
import ru.hse.drip.systems.distributed.homework.i3.replication.ReplicationMode;
import ru.hse.drip.systems.distributed.homework.i3.transport.TcpClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CliMain {

    public static void main(String[] args) throws Exception {
        ControllerState st = new ControllerState();
        TcpClient client = new TcpClient();
        JsonCodec codec = new JsonCodec();
        CliParser parser = new CliParser();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("CLI готов. Команды: addNode/removeNode/listNodes/setMode/setLeader/setTopology/setStarCenter/setReplication/setRF/setSemiSyncAcks/setReplicationDelayMs put/get/delete/dump/getAll/clusterDump");
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
                        String nodeId = cmd.args.getFirst();
                        String host = cmd.args.get(1);
                        int port = Integer.parseInt(cmd.args.get(2));
                        st.nodes.put(nodeId, new ClusterNode(nodeId, host, port));
                        if (st.leaderNodeId == null) {
                            st.leaderNodeId = nodeId;
                        }
                        rebalanceRoles(st);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "removeNode" -> {
                        String nodeId = cmd.args.getFirst();
                        ControllerState candidate = copyState(st);
                        candidate.nodes.remove(nodeId);
                        if (nodeId.equals(candidate.leaderNodeId)) {
                            candidate.leaderNodeId = candidate.nodes.keySet().stream().findFirst().orElse(null);
                        }
                        if (nodeId.equals(candidate.starCenterNodeId)) {
                            candidate.starCenterNodeId = candidate.nodes.keySet().stream().findFirst().orElse(null);
                        }
                        rebalanceRoles(candidate);
                        validateControllerState(candidate);
                        applyState(st, candidate);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "listNodes" -> {
                        st.nodes.values().forEach(n -> System.out.println(n.nodeId + " " + n.host + ":" + n.port + " роль=" + effectiveRole(st, n.nodeId)));
                        System.out.println("режим=" + st.clusterMode.name().toLowerCase());
                        System.out.println("лидер=" + st.leaderNodeId);
                        System.out.println("лидеры=" + st.leaderNodeIds);
                        System.out.println("топология=" + st.topology.name().toLowerCase());
                        System.out.println("центрЗвезды=" + st.starCenterNodeId);
                    }
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
                        String nodeId = cmd.args.getFirst();
                        if (!st.nodes.containsKey(nodeId)) {
                            System.out.println("ОШИБКА: неизвестный узел");
                            continue;
                        }
                        st.leaderNodeId = nodeId;
                        rebalanceRoles(st);
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
                        String nodeId = cmd.args.getFirst();
                        if (!st.nodes.containsKey(nodeId)) {
                            System.out.println("ОШИБКА: неизвестный узел");
                            continue;
                        }
                        st.starCenterNodeId = nodeId;
                        validateControllerState(st);
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setReplication" -> {
                        ReplicationMode newMode = switch (cmd.args.getFirst()) {
                            case "async" -> ReplicationMode.ASYNC;
                            case "sync" -> ReplicationMode.SYNC;
                            case "semi-sync" -> ReplicationMode.SEMI_SYNC;
                            default -> throw new IllegalArgumentException("режим должен быть async|sync|semi-sync");
                        };
                        ControllerState candidate = copyState(st);
                        candidate.mode = newMode;
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
                        st.delayMinMs = Integer.parseInt(cmd.args.getFirst());
                        st.delayMaxMs = Integer.parseInt(cmd.args.get(1));
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "put", "get", "delete", "dump" -> executeClientCommand(st, client, codec, cmd);
                    case "getAll" -> executeGetAll(st, client, codec, cmd.args.getFirst());
                    case "clusterDump" -> executeClusterDump(st, client, codec);
                    default -> System.out.println("Неизвестная команда: " + cmd.cmd);
                }
            } catch (Exception e) {
                System.out.println("ОШИБКА: " + e.getMessage());
            }
        }
    }

    private static void executeClientCommand(ControllerState st, TcpClient client, JsonCodec codec, CliParser.Parsed cmd) throws Exception {
        String target = resolveTarget(st, cmd);
        String clientId = cmd.opts.getOrDefault("--client", "cli");
        ClusterNode node = st.nodes.get(target);
        if (node == null) {
            System.out.println("ОШИБКА: неизвестный target-узел");
            return;
        }

        ClientRequest req;
        if (cmd.cmd.equals("put")) {
            req = ClientRequest.put(clientId, cmd.args.getFirst(), cmd.args.get(1));
        } else if (cmd.cmd.equals("delete")) {
            req = ClientRequest.delete(clientId, cmd.args.getFirst());
        } else if (cmd.cmd.equals("get")) {
            req = ClientRequest.get(clientId, cmd.args.getFirst());
        } else {
            req = ClientRequest.dump(clientId);
        }

        System.out.println(codec.toLine(call(node, req, client, codec)));
    }

    private static void executeGetAll(ControllerState st, TcpClient client, JsonCodec codec, String key) {
        for (ClusterNode node : st.nodes.values()) {
            try {
                ClientResponse resp = call(node, ClientRequest.get("cli", key), client, codec);
                String version = resp.versionLamport == null ? "-" : resp.versionLamport + "," + resp.versionNodeId;
                System.out.println(node.nodeId + " значение=" + resp.value + " версия=" + version);
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

    private static ClientResponse call(ClusterNode node, ClientRequest req, TcpClient client, JsonCodec codec) throws Exception {
        MessageEnvelope env = MessageEnvelope.of(req.type, codec.toTree(req));
        String respLine = client.callLine(node.host, node.port, codec.toLine(env), 1000);
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
        if (st.rf < 1 || st.rf > st.nodes.size()) {
            throw new IllegalArgumentException("RF должен быть в диапазоне 1..clusterSize");
        }
        if (st.mode == ReplicationMode.SEMI_SYNC) {
            if (st.k < 1 || st.k > st.rf - 1) {
                throw new IllegalArgumentException("K должен быть в диапазоне 1..RF-1");
            }
        }
        if (st.clusterMode == ClusterMode.MULTI) {
            if (st.leaderNodeIds.isEmpty()) {
                throw new IllegalArgumentException("multi-режим требует хотя бы одного leader");
            }
            if (st.topology == TopologyType.STAR) {
                if (st.starCenterNodeId == null || !st.nodes.containsKey(st.starCenterNodeId)) {
                    throw new IllegalArgumentException("topology star требует существующий центр");
                }
            }
        }
    }

    private static void rebalanceRoles(ControllerState st) {
        List<String> orderedIds = st.nodes.keySet().stream().sorted().toList();
        if (st.clusterMode == ClusterMode.MULTI) {
            int leaderCount = Math.min(4, orderedIds.size());
            st.leaderNodeIds = new ArrayList<>(orderedIds.subList(0, leaderCount));
            if (st.starCenterNodeId == null && !orderedIds.isEmpty()) {
                st.starCenterNodeId = orderedIds.getFirst();
            }
        } else {
            if (st.leaderNodeId == null && !orderedIds.isEmpty()) {
                st.leaderNodeId = orderedIds.getFirst();
            }
            st.leaderNodeIds = st.leaderNodeId == null ? new ArrayList<>() : new ArrayList<>(List.of(st.leaderNodeId));
        }

        for (ClusterNode node : st.nodes.values()) {
            node.role = effectiveRole(st, node.nodeId);
        }
    }

    private static String effectiveRole(ControllerState st, String nodeId) {
        if (st.clusterMode == ClusterMode.MULTI) {
            return st.leaderNodeIds.contains(nodeId) ? "leader" : "follower";
        }
        return nodeId.equals(st.leaderNodeId) ? "leader" : "follower";
    }

    private static ControllerState copyState(ControllerState st) {
        ControllerState copy = new ControllerState();
        for (ClusterNode node : st.nodes.values()) {
            copy.nodes.put(node.nodeId, new ClusterNode(node.nodeId, node.host, node.port, node.role));
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
    }

    private static String defaultTarget(ControllerState st, String command) {
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
            default -> throw new IllegalArgumentException("режим должен быть single|multi");
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
}

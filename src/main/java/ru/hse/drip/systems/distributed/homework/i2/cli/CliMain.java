package ru.hse.drip.systems.distributed.homework.i2.cli;

import ru.hse.drip.systems.distributed.homework.i2.cli.ControllerState;
import ru.hse.drip.systems.distributed.homework.i2.protocol.*;
import ru.hse.drip.systems.distributed.homework.i2.replication.ReplicationMode;
import ru.hse.drip.systems.distributed.homework.i2.transport.TcpClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

public class CliMain {

    public static void main(String[] args) throws Exception {
        ControllerState st = new ControllerState();
        TcpClient client = new TcpClient();
        JsonCodec codec = new JsonCodec();
        CliParser parser = new CliParser();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("CLI готов. Команды: addNode/removeNode/listNodes/setLeader/setReplication/setRF/setSemiSyncAcks/setReplicationDelayMs put/get/delete/dump");
        while (true) {
            System.out.print("> ");
            String line = br.readLine();
            if (line == null) break;
            line = line.trim();
            if (line.isEmpty()) continue;

            try {
                CliParser.Parsed cmd = parser.parse(line);
                if (cmd == null) continue;

                switch (cmd.cmd) {
                    case "addNode" -> {
                        String nodeId = cmd.args.getFirst();
                        String host = cmd.args.get(1);
                        int port = Integer.parseInt(cmd.args.get(2));
                        st.nodes.put(nodeId, new ClusterNode(nodeId, host, port));
                        if (st.leaderNodeId == null) st.leaderNodeId = nodeId;
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
                        validateControllerState(candidate);
                        st.nodes.clear();
                        st.nodes.putAll(candidate.nodes);
                        st.leaderNodeId = candidate.leaderNodeId;
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "listNodes" -> {
                        st.nodes.values().forEach(n -> System.out.println(n.nodeId + " " + n.host + ":" + n.port));
                        System.out.println("лидер=" + st.leaderNodeId);
                    }
                    case "setLeader" -> {
                        String nodeId = cmd.args.getFirst();
                        if (!st.nodes.containsKey(nodeId)) {
                            System.out.println("ОШИБКА: неизвестный узел");
                            continue;
                        }
                        st.leaderNodeId = nodeId;
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setReplication" -> {
                        String m = cmd.args.getFirst();
                        ReplicationMode newMode = switch (m) {
                            case "async" -> ReplicationMode.ASYNC;
                            case "sync" -> ReplicationMode.SYNC;
                            case "semi-sync" -> ReplicationMode.SEMI_SYNC;
                            default -> throw new IllegalArgumentException("режим должен быть async|sync|semi-sync");
                        };
                        ControllerState candidate = copyState(st);
                        candidate.mode = newMode;
                        validateControllerState(candidate);
                        st.mode = newMode;
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setRF" -> {
                        int newRf = Integer.parseInt(cmd.args.getFirst());
                        ControllerState candidate = copyState(st);
                        candidate.rf = newRf;
                        validateControllerState(candidate);
                        st.rf = newRf;
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setSemiSyncAcks" ->    {
                        int newK = Integer.parseInt(cmd.args.getFirst());
                        ControllerState candidate = copyState(st);
                        candidate.k = newK;
                        validateControllerState(candidate);
                        st.k = newK;
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "setReplicationDelayMs" -> {
                        st.delayMinMs = Integer.parseInt(cmd.args.getFirst());
                        st.delayMaxMs = Integer.parseInt(cmd.args.get(1));
                        broadcastClusterUpdate(st, client, codec);
                        System.out.println("OK");
                    }
                    case "put", "get", "delete", "dump" -> {
                        String target = cmd.opts.getOrDefault("--target", st.leaderNodeId);
                        if (cmd.cmd.equals("get") && "leader".equalsIgnoreCase(cmd.opts.getOrDefault("--read", ""))) {
                            target = st.leaderNodeId;
                        }
                        String clientId = cmd.opts.getOrDefault("--client", "cli");
                        ClusterNode node = st.nodes.get(target);
                        if (node == null) {
                            System.out.println("ОШИБКА: неизвестный target-узел");
                            continue;
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

                        MessageEnvelope env = MessageEnvelope.of(req.type, codec.toTree(req));
                        String respLine = client.callLine(node.host, node.port, codec.toLine(env), 1000);
                        MessageEnvelope respEnv = codec.fromLine(respLine, MessageEnvelope.class);
                        ClientResponse resp = codec.treeToValue(respEnv.payload, ClientResponse.class);

                        System.out.println(codec.toLine(resp));
                    }
                    default -> System.out.println("Неизвестная команда: " + cmd.cmd);
                }

            } catch (Exception e) {
                System.out.println("ОШИБКА: " + e.getMessage());
            }
        }
    }

    private static void broadcastClusterUpdate(ControllerState st, TcpClient client, JsonCodec codec) {
        validateControllerState(st);

        ClusterUpdate upd = new ClusterUpdate();
        upd.nodes = List.copyOf(st.nodes.values());
        upd.leaderNodeId = st.leaderNodeId;
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
            } catch (Exception ignored) {}
        }
    }

    private static void validateControllerState(ControllerState st) {
        if (st.rf < 1 || st.rf > st.nodes.size()) {
            throw new IllegalArgumentException("RF должен быть в диапазоне 1..clusterSize");
        }
        if (st.mode == ReplicationMode.SEMI_SYNC) {
            if (st.k < 1 || st.k > st.rf - 1) {
                throw new IllegalArgumentException("K должен быть в диапазоне 1..RF-1");
            }
        }
    }

    private static ControllerState copyState(ControllerState st) {
        ControllerState copy = new ControllerState();
        copy.nodes.putAll(st.nodes);
        copy.leaderNodeId = st.leaderNodeId;
        copy.mode = st.mode;
        copy.rf = st.rf;
        copy.k = st.k;
        copy.delayMinMs = st.delayMinMs;
        copy.delayMaxMs = st.delayMaxMs;
        copy.hotPathTimeoutMs = st.hotPathTimeoutMs;
        return copy;
    }
}

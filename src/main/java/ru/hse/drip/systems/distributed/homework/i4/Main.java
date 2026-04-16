package ru.hse.drip.systems.distributed.homework.i4;

import ru.hse.drip.systems.distributed.homework.i4.node.Node;
import ru.hse.drip.systems.distributed.homework.i4.node.NodeConfig;
import ru.hse.drip.systems.distributed.homework.i4.node.NodeState;
import ru.hse.drip.systems.distributed.homework.i4.node.Role;
import ru.hse.drip.systems.distributed.homework.i4.protocol.*;
import ru.hse.drip.systems.distributed.homework.i4.replication.ReplicationMode;
import ru.hse.drip.systems.distributed.homework.i4.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i4.transport.TcpClient;
import ru.hse.drip.systems.distributed.homework.i4.transport.TcpServer;

import java.net.Socket;
import java.util.List;
import java.util.Objects;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length >= 1 && args[0].equalsIgnoreCase("test")) {
            selfTest();
            return;
        }

        if (args.length < 2) {
            System.out.println("Использование:");
            System.out.println("  Main <host> <port> [leader|follower]");
            System.out.println("  Main test   (запустить self-test; добавьте -ea для assert)");
            return;
        }

        NodeConfig cfg = new NodeConfig();
        cfg.host = args[0];
        cfg.port = Integer.parseInt(args[1]);
        cfg.nodeId = cfg.host + ":" + cfg.port;

        NodeState state = new NodeState();
        state.role = (args.length >= 3 && args[2].equalsIgnoreCase("follower")) ? Role.FOLLOWER : Role.LEADER;
        state.clusterMode = ClusterMode.SINGLE;

        ClusterNode self = new ClusterNode(cfg.nodeId, cfg.host, cfg.port, state.role == Role.LEADER ? "leader" : "follower");
        state.nodes.put(cfg.nodeId, self);
        state.leaderNodeId = cfg.nodeId;
        state.leaderNodeIds = List.of(cfg.nodeId);
        state.replicationMode = ReplicationMode.ASYNC;

        JsonCodec codec = new JsonCodec();
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        Node node = new Node(cfg, state, store, codec);

        try (TcpServer server = new TcpServer(cfg.port, 32)) {
            server.start(node::handleConnection);
            System.out.println("Узел запущен на " + cfg.host + ":" + cfg.port + " с ролью " + state.role);
            Thread.currentThread().join();
        }
    }

    private static void selfTest() throws Exception {
        String host = "127.0.0.1";
        int port = 19001;

        NodeConfig cfg = new NodeConfig();
        cfg.host = host;
        cfg.port = port;
        cfg.nodeId = cfg.host + ":" + cfg.port;

        NodeState state = new NodeState();
        state.role = Role.LEADER;
        state.clusterMode = ClusterMode.SINGLE;
        state.leaderNodeId = cfg.nodeId;
        state.leaderNodeIds = List.of(cfg.nodeId);
        state.nodes.put(cfg.nodeId, new ClusterNode(cfg.nodeId, host, port, "leader"));
        state.rf = 1;
        state.replicationMode = ReplicationMode.ASYNC;

        JsonCodec codec = new JsonCodec();
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        Node node = new Node(cfg, state, store, codec);

        try (TcpServer server = new TcpServer(cfg.port, 16)) {
            server.start(node::handleConnection);
            waitUntilPortOpen(host, port, 1500);

            TcpClient client = new TcpClient();
            String clientId = "test";

            ClientResponse put = call(client, codec, host, port, ClientRequest.put(clientId, "a", "1"));
            assert put.status == ResponseStatus.OK : "PUT должен завершиться с OK";

            ClientResponse get = call(client, codec, host, port, ClientRequest.get(clientId, "a"));
            assert get.status == ResponseStatus.OK : "GET должен завершиться с OK";
            assert Objects.equals(get.value, "1") : "GET a должен вернуть 1";

            System.out.println("[SELFTEST] OK (запускайте с -ea, чтобы включить assert)");
        }
    }

    private static ClientResponse call(TcpClient client, JsonCodec codec, String host, int port, ClientRequest req) throws Exception {
        MessageEnvelope env = MessageEnvelope.of(req.type, codec.toTree(req));
        String respLine = client.callLine(host, port, codec.toLine(env), 1000);
        MessageEnvelope respEnv = codec.fromLine(respLine, MessageEnvelope.class);
        return codec.treeToValue(respEnv.payload, ClientResponse.class);
    }

    private static void waitUntilPortOpen(String host, int port, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket(host, port)) {
                return;
            } catch (Exception ignored) {
                Thread.sleep(50);
            }
        }
        throw new IllegalStateException("Сервер не запустился на " + host + ":" + port);
    }
}

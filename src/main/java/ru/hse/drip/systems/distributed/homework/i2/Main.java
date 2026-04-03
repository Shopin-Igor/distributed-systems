package ru.hse.drip.systems.distributed.homework.i2;

import ru.hse.drip.systems.distributed.homework.i2.node.*;
import ru.hse.drip.systems.distributed.homework.i2.protocol.*;
import ru.hse.drip.systems.distributed.homework.i2.replication.ReplicationMode;
import ru.hse.drip.systems.distributed.homework.i2.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i2.transport.TcpClient;
import ru.hse.drip.systems.distributed.homework.i2.transport.TcpServer;

import java.net.Socket;
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
        if (args.length >= 3 && args[2].equalsIgnoreCase("follower")) {
            state.role = Role.FOLLOWER;
        } else {
            state.role = Role.LEADER;
        }

        state.nodes.put("127.0.0.1:9001", new ClusterNode("127.0.0.1:9001", "127.0.0.1", 9001));
        state.nodes.put("127.0.0.1:9002", new ClusterNode("127.0.0.1:9002", "127.0.0.1", 9002));
        state.nodes.put("127.0.0.1:9003", new ClusterNode("127.0.0.1:9003", "127.0.0.1", 9003));

        state.leaderNodeId = "127.0.0.1:9001";

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

        state.leaderNodeId = cfg.nodeId;
        state.nodes.put(cfg.nodeId, new ClusterNode(cfg.nodeId, host, port));

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
            System.out.println("Ответ PUT = " + codec.toLine(put));
            assert put.status == ResponseStatus.OK : "PUT должен завершиться с OK";

            ClientResponse get = call(client, codec, host, port, ClientRequest.get(clientId, "a"));
            System.out.println("Ответ GET = " + codec.toLine(get));
            assert get.status == ResponseStatus.OK : "GET должен завершиться с OK";
            assert Objects.equals(get.value, "1") : "GET a должен вернуть 1";

            ClientResponse del = call(client, codec, host, port, ClientRequest.delete(clientId, "a"));
            System.out.println("Ответ DELETE = " + codec.toLine(del));
            assert del.status == ResponseStatus.OK : "DELETE должен завершиться с OK";

            ClientResponse get2 = call(client, codec, host, port, ClientRequest.get(clientId, "a"));
            System.out.println("Ответ GET = " + codec.toLine(get));
            assert get2.status == ResponseStatus.OK : "GET после delete должен завершиться с OK";
            assert get2.value == null : "GET после delete должен вернуть null";


            System.out.println("[SELFTEST] OK (запускайте с -ea, чтобы включить assert)");
        }
    }

    private static ClientResponse call(TcpClient client, JsonCodec codec,
                                       String host, int port, ClientRequest req) throws Exception {
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

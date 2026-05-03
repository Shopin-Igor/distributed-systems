package ru.hse.drip.systems.distributed.homework.i5;

import ru.hse.drip.systems.distributed.homework.i5.benchmarks.BenchmarkMain;
import ru.hse.drip.systems.distributed.homework.i5.cli.CliClient;
import ru.hse.drip.systems.distributed.homework.i5.cli.CliMain;
import ru.hse.drip.systems.distributed.homework.i5.node.ShardCommandHandler;
import ru.hse.drip.systems.distributed.homework.i5.node.ShardNode;
import ru.hse.drip.systems.distributed.homework.i5.node.ShardNodeMain;
import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MessageType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ResponseStatus;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;
import ru.hse.drip.systems.distributed.homework.i5.router.Router;
import ru.hse.drip.systems.distributed.homework.i5.router.RouterConfig;
import ru.hse.drip.systems.distributed.homework.i5.router.RouterMain;
import ru.hse.drip.systems.distributed.homework.i5.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i5.tools.LoadKeysMain;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpClient;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpServer;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        switch (args[0].toLowerCase()) {
            case "router" -> RouterMain.run(args);
            case "node" -> ShardNodeMain.run(args);
            case "cli" -> CliMain.run(args);
            case "loadkeys", "load-keys" -> LoadKeysMain.run(args);
            case "benchmark" -> BenchmarkMain.run(args);
            case "test" -> selfTest();
            default -> printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("""
                Usage:
                  Main router <host> <port>
                  Main node <nodeId> <host> <port>
                  Main cli <routerHost> <routerPort>
                  Main loadKeys [--count <n>] [--key-prefix <prefix>] [--start <n>]
                  Main benchmark [--output <path>]
                  Main test
                """);
    }

    private static void selfTest() throws Exception {
        JsonCodec codec = new JsonCodec();
        int routerPort = 19300;
        List<TcpServer> servers = List.of(
                shardServer("A", 19301, codec),
                shardServer("B", 19302, codec),
                shardServer("C", 19303, codec),
                shardServer("D", 19304, codec),
                shardServer("E", 19305, codec)
        );

        RouterConfig routerConfig = new RouterConfig("127.0.0.1", routerPort, 1_000, 1_000);
        try (Router router = new Router(routerConfig);
             TcpServer routerServer = new TcpServer(routerPort, 32)) {
            router.startBackgroundJobs();
            routerServer.start(router::handleConnection);
            Thread.sleep(150);

            CliClient client = new CliClient("127.0.0.1", routerPort, codec, new TcpClient());
            ok(client, addNode("A", 19301));
            ok(client, addNode("B", 19302));
            ok(client, addNode("C", 19303));
            ok(client, setStrategy("hash_mod_n", false));
            ok(client, put("user:1", "Alice"));

            WireMessage get = client.send(get("user:1"));
            require(get.status == ResponseStatus.OK && "Alice".equals(get.value), "GET must return Alice");

            ok(client, setStrategy("range", true));
            ok(client, setRanges("m", "z"));
            ok(client, put("a", "left"));
            ok(client, put("n", "middle"));

            WireMessage range = client.send(getRange("a", "z"));
            require(range.status == ResponseStatus.OK && range.data.size() == 2, "range query must return 2 keys");

            LoadKeysMain.run(new String[]{
                    "--router-port", Integer.toString(routerPort),
                    "--count", "2",
                    "--start", "10",
                    "--key-prefix", "bulk:",
                    "--value-prefix", "v-",
                    "--width", "2",
                    "--report-every", "0"
            });
            WireMessage loaded = client.send(get("bulk:10"));
            require(loaded.status == ResponseStatus.OK && "v-10".equals(loaded.value), "loader must write bulk keys");

            ok(client, setStrategy("consistent_hashing", true));
            WireMessage cleared = client.send(WireMessage.request(MessageType.STATS));
            require(cleared.status == ResponseStatus.OK && cleared.stats.totalKeys == 0,
                    "setStrategy --force must clear all shard stores");

            ok(client, addNode("D", 19304));
            ok(client, addNode("E", 19305));
            LoadKeysMain.run(new String[]{
                    "--router-port", Integer.toString(routerPort),
                    "--count", "50",
                    "--start", "1",
                    "--key-prefix", "scenario:",
                    "--value-prefix", "value-",
                    "--width", "3",
                    "--report-every", "0"
            });
            WireMessage fiveNodeStats = client.send(WireMessage.request(MessageType.STATS));
            require(fiveNodeStats.status == ResponseStatus.OK && fiveNodeStats.stats.totalKeys == 50,
                    "five-node scenario must keep the expected key count");

            ok(client, setStrategy("consistent_hashing", true));
            WireMessage fiveNodeCleared = client.send(WireMessage.request(MessageType.STATS));
            require(fiveNodeCleared.status == ResponseStatus.OK && fiveNodeCleared.stats.totalKeys == 0,
                    "setStrategy --force must clear all stores in the five-node scenario");

            System.out.println("[SELFTEST] OK");
        } finally {
            for (TcpServer server : servers) {
                server.close();
            }
        }
    }

    private static TcpServer shardServer(String nodeId, int port, JsonCodec codec) throws Exception {
        ShardCommandHandler handler = new ShardCommandHandler(nodeId, new InMemoryKeyValueStore());
        ShardNode node = new ShardNode(codec, handler);
        TcpServer server = new TcpServer(port, 16);
        server.start(node::handleConnection);
        return server;
    }

    private static WireMessage addNode(String nodeId, int port) {
        WireMessage request = WireMessage.request(MessageType.ADD_NODE);
        request.nodeId = nodeId;
        request.host = "127.0.0.1";
        request.port = port;
        return request;
    }

    private static WireMessage setStrategy(String strategy, boolean force) {
        WireMessage request = WireMessage.request(MessageType.SET_STRATEGY);
        request.strategy = strategy;
        request.force = force;
        return request;
    }

    private static WireMessage setRanges(String... boundaries) {
        WireMessage request = WireMessage.request(MessageType.SET_RANGES);
        request.boundaries = List.of(boundaries);
        return request;
    }

    private static WireMessage put(String key, String value) {
        WireMessage request = WireMessage.request(MessageType.CLIENT_PUT);
        request.key = key;
        request.value = value;
        return request;
    }

    private static WireMessage get(String key) {
        WireMessage request = WireMessage.request(MessageType.CLIENT_GET);
        request.key = key;
        return request;
    }

    private static WireMessage getRange(String fromKey, String toKey) {
        WireMessage request = WireMessage.request(MessageType.CLIENT_GET_RANGE);
        request.fromKey = fromKey;
        request.toKey = toKey;
        return request;
    }

    private static void ok(CliClient client, WireMessage request) {
        WireMessage response = client.send(request);
        require(response.status == ResponseStatus.OK,
                "expected OK for " + request.type + ", got " + response.errorCode + ": " + response.error);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}

package ru.hse.drip.systems.distributed.homework.i5.node;

import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpServer;

public final class ShardNodeMain {
    private ShardNodeMain() {
    }

    public static void run(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: Main node <nodeId> <host> <port>");
            return;
        }

        ShardNodeConfig config = new ShardNodeConfig(args[1], args[2], Integer.parseInt(args[3]));
        JsonCodec codec = new JsonCodec();
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        ShardCommandHandler handler = new ShardCommandHandler(config.nodeId, store);
        ShardNode node = new ShardNode(codec, handler);

        try (TcpServer server = new TcpServer(config.port, 32)) {
            server.start(node::handleConnection);
            System.out.println("[ShardNode] " + config.nodeId + " listening on " + config.host + ":" + config.port);
            Thread.currentThread().join();
        }
    }
}

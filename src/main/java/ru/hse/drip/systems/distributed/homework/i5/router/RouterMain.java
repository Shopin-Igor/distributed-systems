package ru.hse.drip.systems.distributed.homework.i5.router;

import ru.hse.drip.systems.distributed.homework.i5.transport.TcpServer;

public final class RouterMain {
    private RouterMain() {
    }

    public static void run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: Main router <host> <port>");
            return;
        }

        RouterConfig config = new RouterConfig(args[1], Integer.parseInt(args[2]));
        try (Router router = new Router(config);
             TcpServer server = new TcpServer(config.port, 64)) {
            router.startBackgroundJobs();
            server.start(router::handleConnection);
            System.out.println("[Router] listening on " + config.host + ":" + config.port);
            Thread.currentThread().join();
        }
    }
}

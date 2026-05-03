package ru.hse.drip.systems.distributed.homework.i5.benchmarks;

import ru.hse.drip.systems.distributed.homework.i5.cli.CliClient;
import ru.hse.drip.systems.distributed.homework.i5.node.ShardCommandHandler;
import ru.hse.drip.systems.distributed.homework.i5.node.ShardNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.DistributionStats;
import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MessageType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ResponseStatus;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ShardingStrategyType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;
import ru.hse.drip.systems.distributed.homework.i5.router.Router;
import ru.hse.drip.systems.distributed.homework.i5.router.RouterConfig;
import ru.hse.drip.systems.distributed.homework.i5.store.InMemoryKeyValueStore;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpClient;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpServer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BenchmarkMain {
    private static final int DEFAULT_KEY_COUNT = 10_000;
    private static final int DEFAULT_THREADS = 16;
    private static final AtomicInteger NEXT_BASE_PORT = new AtomicInteger(23000);

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        BenchmarkOptions options = BenchmarkOptions.parse(args);
        List<BenchmarkResult> results = new ArrayList<>();

        runThroughputPart(options, results);
        runMigrationPart(options, results);
        runVnodePart(options, results);

        new CsvResultWriter().write(options.output, results);
        System.out.println("[Benchmark] wrote " + results.size() + " rows to " + options.output);
    }

    private static void runThroughputPart(BenchmarkOptions options, List<BenchmarkResult> results) throws Exception {
        for (ShardingStrategyType strategy : List.of(
                ShardingStrategyType.RANGE,
                ShardingStrategyType.HASH_MOD_N,
                ShardingStrategyType.CONSISTENT_HASHING)) {
            for (double putRatio : List.of(0.8, 0.2)) {
                try (LocalCluster cluster = LocalCluster.start(5)) {
                    cluster.configure(strategy, 150, options.keyCount);
                    cluster.loadKeys(options.keyCount);

                    BenchmarkResult result = runMixedWorkload(
                            cluster,
                            "A",
                            strategy.cliName(),
                            putRatio >= 0.5 ? "write-heavy" : "read-heavy",
                            5,
                            options.keyCount,
                            options.threads,
                            putRatio
                    );
                    results.add(result);
                    System.out.println("[Benchmark] " + result.part + " " + result.strategy + " " + result.workload);
                }
            }
        }
    }

    private static void runMigrationPart(BenchmarkOptions options, List<BenchmarkResult> results) throws Exception {
        for (ShardingStrategyType strategy : List.of(
                ShardingStrategyType.RANGE,
                ShardingStrategyType.HASH_MOD_N,
                ShardingStrategyType.CONSISTENT_HASHING)) {
            try (LocalCluster cluster = LocalCluster.start(4)) {
                cluster.configure(strategy, 150, options.keyCount);
                cluster.loadKeys(options.keyCount);

                WireMessage addResponse = cluster.addNode("N5");
                if (strategy == ShardingStrategyType.RANGE) {
                    cluster.setRangesForNodeCount(5, options.keyCount);
                    addResponse = cluster.call(WireMessage.request(MessageType.MIGRATE_DATA));
                }

                DistributionStats stats = cluster.stats();
                BenchmarkResult result = baseResult("B", strategy.cliName(), "add-node", 5,
                        options.keyCount, options.threads, 0.0, stats);
                if (addResponse.migration != null) {
                    result.migratedPercent = addResponse.migration.movedPercent();
                    result.migrationDurationMs = addResponse.migration.durationMs;
                }
                results.add(result);
                System.out.println("[Benchmark] B " + strategy.cliName() + " migration");
            }
        }

        for (ShardingStrategyType strategy : List.of(
                ShardingStrategyType.HASH_MOD_N,
                ShardingStrategyType.CONSISTENT_HASHING)) {
            try (LocalCluster cluster = LocalCluster.start(5)) {
                cluster.configure(strategy, 150, options.keyCount);
                cluster.loadKeys(options.keyCount);

                WireMessage removeResponse = cluster.removeNode("N5");
                DistributionStats stats = cluster.stats();
                BenchmarkResult result = baseResult("B", strategy.cliName(), "remove-node", 4,
                        options.keyCount, options.threads, 0.0, stats);
                if (removeResponse.migration != null) {
                    result.migratedPercent = removeResponse.migration.movedPercent();
                    result.migrationDurationMs = removeResponse.migration.durationMs;
                }
                results.add(result);
                System.out.println("[Benchmark] B " + strategy.cliName() + " remove migration");
            }
        }
    }

    private static void runVnodePart(BenchmarkOptions options, List<BenchmarkResult> results) throws Exception {
        for (int vnodes : List.of(1, 3, 50, 150)) {
            try (LocalCluster cluster = LocalCluster.start(5)) {
                cluster.configure(ShardingStrategyType.CONSISTENT_HASHING, vnodes, options.keyCount);
                cluster.loadKeys(options.keyCount);

                DistributionStats stats = cluster.stats();
                BenchmarkResult result = baseResult("C", "consistent_hashing", "vnodes-" + vnodes, 5,
                        options.keyCount, options.threads, 0.0, stats);
                results.add(result);
                System.out.println("[Benchmark] C vnodes=" + vnodes);
            }
        }
    }

    private static BenchmarkResult runMixedWorkload(
            LocalCluster cluster,
            String part,
            String strategy,
            String workload,
            int nodeCount,
            int keyCount,
            int threads,
            double putRatio
    ) throws InterruptedException {
        int operations = keyCount;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger nextOperation = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        LatencyRecorder latencies = new LatencyRecorder();

        for (int thread = 0; thread < threads; thread++) {
            executor.submit(() -> {
                await(start);
                while (true) {
                    int operation = nextOperation.getAndIncrement();
                    if (operation >= operations) {
                        return;
                    }
                    int keyIndex = ThreadLocalRandom.current().nextInt(keyCount);
                    boolean put = ThreadLocalRandom.current().nextDouble() < putRatio;
                    WireMessage request = put
                            ? put(WorkloadGenerator.key(keyIndex), WorkloadGenerator.value(operation))
                            : get(WorkloadGenerator.key(keyIndex));

                    long startedAt = System.nanoTime();
                    WireMessage response = cluster.call(request);
                    latencies.record(System.nanoTime() - startedAt);
                    if (response.status != ResponseStatus.OK) {
                        throw new IllegalStateException("benchmark operation failed: " + response.error);
                    }
                }
            });
        }

        long startedAt = System.nanoTime();
        start.countDown();
        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
            executor.shutdownNow();
            throw new IllegalStateException("benchmark timed out");
        }
        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;

        DistributionStats stats = cluster.stats();
        BenchmarkResult result = baseResult(part, strategy, workload, nodeCount, keyCount, threads, putRatio, stats);
        LatencyRecorder.Snapshot snapshot = latencies.snapshot();
        result.throughputOpsSec = operations / elapsedSeconds;
        result.avgLatencyMs = snapshot.avgMs();
        result.p50LatencyMs = snapshot.p50Ms();
        result.p95LatencyMs = snapshot.p95Ms();
        result.p99LatencyMs = snapshot.p99Ms();
        return result;
    }

    private static BenchmarkResult baseResult(
            String part,
            String strategy,
            String workload,
            int nodeCount,
            int keyCount,
            int threads,
            double putRatio,
            DistributionStats stats
    ) {
        BenchmarkResult result = new BenchmarkResult();
        result.part = part;
        result.strategy = strategy;
        result.workload = workload;
        result.nodeCount = nodeCount;
        result.keyCount = keyCount;
        result.threads = threads;
        result.putRatio = putRatio;
        result.stddevOverMean = stats.stddevOverMean;
        result.maxOverAverage = stats.maxOverAverage;
        return result;
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

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    private static class LocalCluster implements AutoCloseable {
        private final JsonCodec codec = new JsonCodec();
        private final List<TcpServer> shardServers = new ArrayList<>();
        private final Router router;
        private final TcpServer routerServer;
        private final CliClient client;
        private final int basePort;
        private int nodeCount;

        private LocalCluster(int initialNodeCount, int basePort) throws Exception {
            this.basePort = basePort;
            this.router = new Router(new RouterConfig("127.0.0.1", basePort, 2_000, 1_000));
            this.routerServer = new TcpServer(basePort, 64);
            this.client = new CliClient("127.0.0.1", basePort, codec, new TcpClient(), 120_000);

            for (int i = 1; i <= initialNodeCount; i++) {
                startShardServer("N" + i, portForNode(i));
            }
            router.startBackgroundJobs();
            routerServer.start(router::handleConnection);
            Thread.sleep(150);

            for (int i = 1; i <= initialNodeCount; i++) {
                call(addNodeRequest("N" + i, portForNode(i)));
            }
            this.nodeCount = initialNodeCount;
        }

        static LocalCluster start(int initialNodeCount) throws Exception {
            int basePort = NEXT_BASE_PORT.getAndAdd(100);
            return new LocalCluster(initialNodeCount, basePort);
        }

        void configure(ShardingStrategyType strategy, int vnodes, int keyCount) {
            WireMessage setStrategy = WireMessage.request(MessageType.SET_STRATEGY);
            setStrategy.strategy = strategy.cliName();
            setStrategy.force = true;
            call(setStrategy);

            if (strategy == ShardingStrategyType.RANGE) {
                setRangesForNodeCount(nodeCount, keyCount);
            }
            if (strategy == ShardingStrategyType.CONSISTENT_HASHING) {
                WireMessage setVnodes = WireMessage.request(MessageType.SET_VNODES);
                setVnodes.count = vnodes;
                call(setVnodes);
            }
        }

        WireMessage addNode(String nodeId) throws Exception {
            nodeCount++;
            startShardServer(nodeId, portForNode(nodeCount));
            Thread.sleep(100);
            return call(addNodeRequest(nodeId, portForNode(nodeCount)));
        }

        WireMessage removeNode(String nodeId) {
            nodeCount--;
            WireMessage request = WireMessage.request(MessageType.REMOVE_NODE);
            request.nodeId = nodeId;
            return call(request);
        }

        void setRangesForNodeCount(int nodes, int keyCount) {
            WireMessage request = WireMessage.request(MessageType.SET_RANGES);
            for (int i = 1; i < nodes; i++) {
                int boundary = Math.max(1, keyCount * i / nodes);
                request.boundaries.add(WorkloadGenerator.rangeBoundary(boundary));
            }
            call(request);
        }

        void loadKeys(int keyCount) {
            for (int i = 0; i < keyCount; i++) {
                call(put(WorkloadGenerator.key(i), WorkloadGenerator.value(i)));
            }
        }

        DistributionStats stats() {
            WireMessage response = call(WireMessage.request(MessageType.STATS));
            return response.stats;
        }

        WireMessage call(WireMessage request) {
            WireMessage response = client.send(request);
            if (response.status != ResponseStatus.OK) {
                throw new IllegalStateException("request failed: " + request.type + " -> "
                        + response.errorCode + ": " + response.error);
            }
            return response;
        }

        private WireMessage addNodeRequest(String nodeId, int port) {
            WireMessage request = WireMessage.request(MessageType.ADD_NODE);
            request.nodeId = nodeId;
            request.host = "127.0.0.1";
            request.port = port;
            return request;
        }

        private void startShardServer(String nodeId, int port) throws Exception {
            ShardCommandHandler handler = new ShardCommandHandler(nodeId, new InMemoryKeyValueStore());
            ShardNode node = new ShardNode(codec, handler);
            TcpServer server = new TcpServer(port, 32);
            server.start(node::handleConnection);
            shardServers.add(server);
        }

        private int portForNode(int index) {
            return basePort + index;
        }

        @Override
        public void close() throws Exception {
            router.close();
            routerServer.close();
            for (TcpServer shardServer : shardServers) {
                shardServer.close();
            }
        }
    }

    private record BenchmarkOptions(Path output, int keyCount, int threads) {
        static BenchmarkOptions parse(String[] args) {
            Path output = Path.of("src/main/java/ru/hse/drip/systems/distributed/homework/i5/benchmarks/results.csv");
            int keyCount = DEFAULT_KEY_COUNT;
            int threads = DEFAULT_THREADS;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i].toLowerCase(Locale.ROOT);
                switch (arg) {
                    case "--output" -> output = Path.of(args[++i]);
                    case "--keys" -> keyCount = Integer.parseInt(args[++i]);
                    case "--threads" -> threads = Integer.parseInt(args[++i]);
                    case "--quick" -> {
                        keyCount = 1_000;
                        threads = 8;
                    }
                    default -> {
                    }
                }
            }
            return new BenchmarkOptions(output, keyCount, threads);
        }
    }
}

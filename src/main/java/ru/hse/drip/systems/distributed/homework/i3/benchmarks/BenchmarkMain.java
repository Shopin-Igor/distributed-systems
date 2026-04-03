package ru.hse.drip.systems.distributed.homework.i3.benchmarks;

import ru.hse.drip.systems.distributed.homework.i3.protocol.*;
import ru.hse.drip.systems.distributed.homework.i3.replication.ReplicationMode;
import ru.hse.drip.systems.distributed.homework.i3.transport.TcpClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkMain {
    public static void main(String[] args) throws Exception {
        BenchmarkConfig cfg = BenchmarkConfig.parse(args);
        BenchmarkMain main = new BenchmarkMain();
        BenchmarkResult result = main.run(cfg);
        System.out.println(result.toHumanString());
        if (cfg.outputFile != null && !cfg.outputFile.isBlank()) {
            main.appendCsv(cfg, result);
            System.out.println("Строка добавлена в CSV: " + cfg.outputFile);
        }
    }

    private final TcpClient client = new TcpClient();
    private final JsonCodec codec = new JsonCodec();

    public BenchmarkResult run(BenchmarkConfig cfg) throws Exception {
        ClusterLayout layout = ClusterLayout.fromConfig(cfg);
        broadcastClusterUpdate(layout, cfg);

        long[] latenciesNs = new long[cfg.totalOps];
        AtomicInteger latencyIndex = new AtomicInteger();
        Set<String> touchedKeys = ConcurrentHashMap.newKeySet();

        ExecutorService pool = Executors.newFixedThreadPool(cfg.threads);
        List<Future<Void>> futures = new ArrayList<>();
        AtomicLong startedAt = new AtomicLong(System.nanoTime());

        for (int workerId = 0; workerId < cfg.threads; workerId++) {
            futures.add(pool.submit(worker(cfg, layout, latenciesNs, latencyIndex, touchedKeys)));
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);

        long finishedAt = System.nanoTime();
        long totalNs = finishedAt - startedAt.get();

        List<Long> latenciesMs = new ArrayList<>(latenciesNs.length);
        for (long latencyNs : latenciesNs) {
            latenciesMs.add(TimeUnit.NANOSECONDS.toMillis(latencyNs));
        }
        Collections.sort(latenciesMs);

        long convergenceMs = measureConvergence(cfg, layout, touchedKeys);
        return BenchmarkResult.from(cfg, totalNs, latenciesMs, convergenceMs);
    }

    private Callable<Void> worker(
            BenchmarkConfig cfg,
            ClusterLayout layout,
            long[] latenciesNs,
            AtomicInteger latencyIndex,
            Set<String> touchedKeys
    ) {
        return () -> {
            while (true) {
                int opIndex = latencyIndex.getAndIncrement();
                if (opIndex >= cfg.totalOps) {
                    return null;
                }

                boolean isPut = ThreadLocalRandom.current().nextDouble() < cfg.putRatio;
                String key = "k" + ThreadLocalRandom.current().nextInt(cfg.keySpace);
                long started = System.nanoTime();
                if (isPut) {
                    ClusterNode node = layout.randomWriteTarget();
                    String value = "v" + opIndex + "-" + UUID.randomUUID();
                    touchedKeys.add(key);
                    call(node, ClientRequest.put("bench", key, value), cfg.requestTimeoutMs);
                } else {
                    ClusterNode node = layout.randomReadTarget();
                    call(node, ClientRequest.get("bench", key), cfg.requestTimeoutMs);
                }
                latenciesNs[opIndex] = System.nanoTime() - started;
            }
        };
    }

    private void broadcastClusterUpdate(ClusterLayout layout, BenchmarkConfig cfg) {
        ClusterUpdate update = new ClusterUpdate();
        update.nodes = List.copyOf(layout.nodes);
        update.leaderNodeId = layout.singleLeaderNodeId;
        update.leaderNodeIds = List.copyOf(layout.multiLeaderNodeIds);
        update.clusterMode = cfg.mode;
        update.topology = cfg.topology;
        update.starCenterNodeId = layout.starCenterNodeId;
        update.replicationMode = cfg.replicationMode;
        update.rf = cfg.rf;
        update.semiSyncAcksK = cfg.semiSyncAcks;
        update.replicationDelayMinMs = cfg.delayMinMs;
        update.replicationDelayMaxMs = cfg.delayMaxMs;
        update.hotPathTimeoutMs = cfg.requestTimeoutMs;

        MessageEnvelope env = MessageEnvelope.of(MessageType.CLUSTER_UPDATE, codec.toTree(update));
        String line = codec.toLine(env);
        for (ClusterNode node : layout.nodes) {
            try {
                client.callLine(node.host, node.port, line, cfg.requestTimeoutMs);
            } catch (Exception ignored) {
            }
        }
    }

    private ClientResponse call(ClusterNode node, ClientRequest request, int timeoutMs) throws IOException {
        MessageEnvelope env = MessageEnvelope.of(request.type, codec.toTree(request));
        String responseLine = client.callLine(node.host, node.port, codec.toLine(env), timeoutMs);
        MessageEnvelope responseEnv = codec.fromLine(responseLine, MessageEnvelope.class);
        return codec.treeToValue(responseEnv.payload, ClientResponse.class);
    }

    private long measureConvergence(BenchmarkConfig cfg, ClusterLayout layout, Set<String> touchedKeys) throws Exception {
        List<String> sampleKeys = new ArrayList<>(touchedKeys);
        if (sampleKeys.isEmpty()) {
            return 0;
        }
        Collections.shuffle(sampleKeys);
        int limit = Math.min(cfg.convergenceSampleSize, sampleKeys.size());
        List<String> selected = sampleKeys.subList(0, limit);

        long started = System.nanoTime();
        long deadline = started + TimeUnit.MILLISECONDS.toNanos(cfg.convergenceTimeoutMs);

        while (System.nanoTime() < deadline) {
            boolean allConverged = true;
            for (String key : selected) {
                if (!isConverged(cfg, layout, key)) {
                    allConverged = false;
                    break;
                }
            }
            if (allConverged) {
                return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            }
            Thread.sleep(cfg.convergencePollMs);
        }

        return cfg.convergenceTimeoutMs;
    }

    private boolean isConverged(BenchmarkConfig cfg, ClusterLayout layout, String key) throws Exception {
        String first = null;
        for (ClusterNode node : layout.nodes) {
            ClientResponse response = call(node, ClientRequest.get("bench", key), cfg.requestTimeoutMs);
            String normalized = response.value + "|" + response.versionLamport + "|" + response.versionNodeId;
            if (first == null) {
                first = normalized;
            } else if (!first.equals(normalized)) {
                return false;
            }
        }
        return true;
    }

    private void appendCsv(BenchmarkConfig cfg, BenchmarkResult result) throws IOException {
        Path output = Path.of(cfg.outputFile);
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }
        String line = String.join(",",
                cfg.runId,
                cfg.mode.name().toLowerCase(Locale.ROOT),
                cfg.mode == ClusterMode.MULTI ? cfg.topology.name().toLowerCase(Locale.ROOT) : cfg.replicationMode.name().toLowerCase(Locale.ROOT),
                formatDouble(cfg.putRatio),
                String.valueOf(cfg.threads),
                String.valueOf(cfg.totalOps),
                String.valueOf(cfg.keySpace),
                String.valueOf(cfg.delayMaxMs),
                formatDouble(result.throughputOpsSec),
                formatDouble(result.avgMs),
                String.valueOf(result.p50Ms),
                String.valueOf(result.p75Ms),
                String.valueOf(result.p95Ms),
                String.valueOf(result.p99Ms),
                String.valueOf(result.convergenceMs),
                cfg.notes
        ) + System.lineSeparator();
        Files.writeString(output, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static String formatDouble(double value) {
        return String.format(Locale.US, "%.2f", value);
    }

    private record ClusterLayout(
            List<ClusterNode> nodes,
            List<ClusterNode> writeTargets,
            String singleLeaderNodeId,
            List<String> multiLeaderNodeIds,
            String starCenterNodeId
    ) {
        static ClusterLayout fromConfig(BenchmarkConfig cfg) {
            List<ClusterNode> nodes = new ArrayList<>();
            for (String endpoint : cfg.nodes) {
                String[] hostPort = endpoint.split(":");
                String host = hostPort[0];
                int port = Integer.parseInt(hostPort[1]);
                String nodeId = host + ":" + port;
                nodes.add(new ClusterNode(nodeId, host, port));
            }
            nodes.sort((a, b) -> a.nodeId.compareTo(b.nodeId));

            String singleLeader = nodes.getFirst().nodeId;
            List<String> multiLeaders = new ArrayList<>();
            int limit = Math.min(4, nodes.size());
            for (int i = 0; i < limit; i++) {
                multiLeaders.add(nodes.get(i).nodeId);
            }

            for (ClusterNode node : nodes) {
                node.role = cfg.mode == ClusterMode.MULTI
                        ? (multiLeaders.contains(node.nodeId) ? "leader" : "follower")
                        : (node.nodeId.equals(singleLeader) ? "leader" : "follower");
            }

            List<ClusterNode> writeTargets = new ArrayList<>();
            for (ClusterNode node : nodes) {
                if ("leader".equals(node.role)) {
                    writeTargets.add(node);
                }
            }

            String starCenter = cfg.starCenterNodeId != null && !cfg.starCenterNodeId.isBlank()
                    ? cfg.starCenterNodeId
                    : nodes.getFirst().nodeId;

            return new ClusterLayout(nodes, writeTargets, singleLeader, multiLeaders, starCenter);
        }

        ClusterNode randomWriteTarget() {
            return writeTargets.get(ThreadLocalRandom.current().nextInt(writeTargets.size()));
        }

        ClusterNode randomReadTarget() {
            return nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
        }
    }

    private static final class BenchmarkConfig {
        final String runId;
        final List<String> nodes;
        final ClusterMode mode;
        final TopologyType topology;
        final ReplicationMode replicationMode;
        final double putRatio;
        final int threads;
        final int totalOps;
        final int keySpace;
        final int rf;
        final int semiSyncAcks;
        final int delayMinMs;
        final int delayMaxMs;
        final int requestTimeoutMs;
        final int convergenceTimeoutMs;
        final int convergencePollMs;
        final int convergenceSampleSize;
        final String starCenterNodeId;
        final String outputFile;
        final String notes;

        BenchmarkConfig(
                String runId,
                List<String> nodes,
                ClusterMode mode,
                TopologyType topology,
                ReplicationMode replicationMode,
                double putRatio,
                int threads,
                int totalOps,
                int keySpace,
                int rf,
                int semiSyncAcks,
                int delayMinMs,
                int delayMaxMs,
                int requestTimeoutMs,
                int convergenceTimeoutMs,
                int convergencePollMs,
                int convergenceSampleSize,
                String starCenterNodeId,
                String outputFile,
                String notes
        ) {
            this.runId = runId;
            this.nodes = nodes;
            this.mode = mode;
            this.topology = topology;
            this.replicationMode = replicationMode;
            this.putRatio = putRatio;
            this.threads = threads;
            this.totalOps = totalOps;
            this.keySpace = keySpace;
            this.rf = rf;
            this.semiSyncAcks = semiSyncAcks;
            this.delayMinMs = delayMinMs;
            this.delayMaxMs = delayMaxMs;
            this.requestTimeoutMs = requestTimeoutMs;
            this.convergenceTimeoutMs = convergenceTimeoutMs;
            this.convergencePollMs = convergencePollMs;
            this.convergenceSampleSize = convergenceSampleSize;
            this.starCenterNodeId = starCenterNodeId;
            this.outputFile = outputFile;
            this.notes = notes;
        }

        static BenchmarkConfig parse(String[] args) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (!arg.startsWith("--")) {
                    continue;
                }
                String key = arg.substring(2);
                String value = i + 1 < args.length ? args[++i] : "";
                values.put(key, value);
            }

            if (!values.containsKey("nodes")) {
                throw new IllegalArgumentException("Не указан --nodes host:port,...");
            }

            return new BenchmarkConfig(
                    values.getOrDefault("runId", "manual"),
                    List.of(values.get("nodes").split(",")),
                    parseMode(values.getOrDefault("mode", "multi")),
                    parseTopology(values.getOrDefault("topology", "mesh")),
                    parseReplication(values.getOrDefault("replication", "async")),
                    Double.parseDouble(values.getOrDefault("putRatio", "0.8")),
                    Integer.parseInt(values.getOrDefault("threads", "16")),
                    Integer.parseInt(values.getOrDefault("totalOps", "10000")),
                    Integer.parseInt(values.getOrDefault("keySpace", "1000")),
                    Integer.parseInt(values.getOrDefault("rf", "1")),
                    Integer.parseInt(values.getOrDefault("semiSyncAcks", "1")),
                    Integer.parseInt(values.getOrDefault("delayMinMs", "0")),
                    Integer.parseInt(values.getOrDefault("delayMaxMs", values.getOrDefault("delayMinMs", "0"))),
                    Integer.parseInt(values.getOrDefault("requestTimeoutMs", "2000")),
                    Integer.parseInt(values.getOrDefault("convergenceTimeoutMs", "10000")),
                    Integer.parseInt(values.getOrDefault("convergencePollMs", "100")),
                    Integer.parseInt(values.getOrDefault("convergenceSampleSize", "10")),
                    values.get("starCenter"),
                    values.get("output"),
                    values.getOrDefault("notes", "")
            );
        }

        private static ClusterMode parseMode(String value) {
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

        private static ReplicationMode parseReplication(String value) {
            return switch (value) {
                case "async" -> ReplicationMode.ASYNC;
                case "sync" -> ReplicationMode.SYNC;
                case "semi-sync" -> ReplicationMode.SEMI_SYNC;
                default -> throw new IllegalArgumentException("режим репликации должен быть async|sync|semi-sync");
            };
        }
    }

    private static final class BenchmarkResult {
        final double throughputOpsSec;
        final double avgMs;
        final long p50Ms;
        final long p75Ms;
        final long p95Ms;
        final long p99Ms;
        final long convergenceMs;
        final Duration elapsed;

        BenchmarkResult(
                double throughputOpsSec,
                double avgMs,
                long p50Ms,
                long p75Ms,
                long p95Ms,
                long p99Ms,
                long convergenceMs,
                Duration elapsed
        ) {
            this.throughputOpsSec = throughputOpsSec;
            this.avgMs = avgMs;
            this.p50Ms = p50Ms;
            this.p75Ms = p75Ms;
            this.p95Ms = p95Ms;
            this.p99Ms = p99Ms;
            this.convergenceMs = convergenceMs;
            this.elapsed = elapsed;
        }

        static BenchmarkResult from(BenchmarkConfig cfg, long totalNs, List<Long> latenciesMs, long convergenceMs) {
            long sum = 0;
            for (Long latency : latenciesMs) {
                sum += latency;
            }
            double avgMs = latenciesMs.isEmpty() ? 0.0 : (double) sum / latenciesMs.size();
            double seconds = totalNs / 1000000000.0;
            double throughput = seconds == 0.0 ? 0.0 : cfg.totalOps / seconds;
            return new BenchmarkResult(
                    throughput,
                    avgMs,
                    percentile(latenciesMs, 0.50),
                    percentile(latenciesMs, 0.75),
                    percentile(latenciesMs, 0.95),
                    percentile(latenciesMs, 0.99),
                    convergenceMs,
                    Duration.ofNanos(totalNs)
            );
        }

        static long percentile(List<Long> values, double percentile) {
            if (values.isEmpty()) {
                return 0;
            }
            int index = (int) Math.min(values.size() - 1, Math.ceil(percentile * values.size()) - 1);
            return values.get(index);
        }

        String toHumanString() {
            return String.join(System.lineSeparator(),
                    "операцийВСек=" + formatDouble(throughputOpsSec),
                    "среднееМс=" + formatDouble(avgMs),
                    "p50Мс=" + p50Ms,
                    "p75Мс=" + p75Ms,
                    "p95Мс=" + p95Ms,
                    "p99Мс=" + p99Ms,
                    "сходимостьМс=" + convergenceMs,
                    "время=" + elapsed
            );
        }
    }
}

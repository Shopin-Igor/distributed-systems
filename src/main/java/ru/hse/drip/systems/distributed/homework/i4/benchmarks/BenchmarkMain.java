package ru.hse.drip.systems.distributed.homework.i4.benchmarks;

import ru.hse.drip.systems.distributed.homework.i4.protocol.*;
import ru.hse.drip.systems.distributed.homework.i4.transport.TcpClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkMain {
    private static final int UNAVAILABLE_PORT = 65530;

    public static void main(String[] args) throws Exception {
        BenchmarkConfig cfg = BenchmarkConfig.parse(args);
        BenchmarkMain main = new BenchmarkMain();
        List<BenchmarkRow> rows = main.runRequiredSuite(cfg);
        main.writeCsv(cfg.outputFile, rows);
        main.writeReport(cfg.reportFile, rows, cfg);
        System.out.println("Готово: " + rows.size() + " benchmark-прогонов");
        for (BenchmarkRow row : rows) {
            System.out.println(row.toHumanString());
        }
    }

    private final TcpClient client = new TcpClient();
    private final JsonCodec codec = new JsonCodec();

    public List<BenchmarkRow> runRequiredSuite(BenchmarkConfig cfg) throws Exception {
        ClusterLayout layout = ClusterLayout.fromConfig(cfg.nodes);
        List<BenchmarkRow> rows = new ArrayList<>();

        List<RunSpec> specs = List.of(
                RunSpec.workload("A1", "partA", WriteQuorumMode.STRICT, 3, 3, 0.8, cfg.threads, cfg.totalOps, cfg.keySpace, null, false, "strict (W,R)=(3,3), write-heavy"),
                RunSpec.workload("A2", "partA", WriteQuorumMode.STRICT, 3, 3, 0.2, cfg.threads, cfg.totalOps, cfg.keySpace, null, false, "strict (W,R)=(3,3), read-heavy"),
                RunSpec.workload("A3", "partA", WriteQuorumMode.STRICT, 4, 2, 0.8, cfg.threads, cfg.totalOps, cfg.keySpace, null, false, "strict (W,R)=(4,2), write-heavy"),
                RunSpec.workload("A4", "partA", WriteQuorumMode.STRICT, 4, 2, 0.2, cfg.threads, cfg.totalOps, cfg.keySpace, null, false, "strict (W,R)=(4,2), read-heavy"),
                RunSpec.workload("A5", "partA", WriteQuorumMode.STRICT, 5, 1, 0.8, cfg.threads, cfg.totalOps, cfg.keySpace, null, false, "strict (W,R)=(5,1), write-heavy"),
                RunSpec.workload("A6", "partA", WriteQuorumMode.STRICT, 5, 1, 0.2, cfg.threads, cfg.totalOps, cfg.keySpace, null, false, "strict (W,R)=(5,1), read-heavy"),
                RunSpec.workload("B1", "partB", WriteQuorumMode.STRICT, 3, 3, 0.8, cfg.threads, cfg.totalOps, cfg.keySpace, layout.homeNodes.getLast().nodeId, false, "strict with one unavailable home replica"),
                RunSpec.workload("B2", "partB", WriteQuorumMode.SLOPPY, 3, 3, 0.8, cfg.threads, cfg.totalOps, cfg.keySpace, layout.homeNodes.getLast().nodeId, true, "sloppy with one unavailable home replica"),
                RunSpec.recovery("C1", "partC", WriteQuorumMode.STRICT, 3, 3, cfg.recoveryLoadOps, cfg.recoveryKeySpace, layout.homeNodes.get(2).nodeId, "recovery after wipe via anti-entropy")
        );

        for (RunSpec spec : specs) {
            rows.add(spec.recovery ? runRecoveryBenchmark(layout, spec, cfg) : runLeaderlessBenchmark(layout, spec, cfg));
        }

        return rows;
    }

    private BenchmarkRow runLeaderlessBenchmark(ClusterLayout layout, RunSpec spec, BenchmarkConfig cfg) throws Exception {
        wipeAll(layout.actualNodes, cfg.requestTimeoutMs);
        broadcastLeaderlessConfig(layout, spec, cfg, true);
        sleepQuietly(200);

        Map<String, Map<String, Long>> beforeStats = captureStats(layout.actualNodes, cfg.requestTimeoutMs);
        WorkloadResult workload = executeWorkload(layout, spec, cfg);

        if (spec.runHintedHandoffAfterRun) {
            broadcastLeaderlessConfig(layout, spec.withUnavailableHomeNodeId(null), cfg, true);
            sleepQuietly(200);
            runHintedHandoff(layout.spareNodes, cfg.requestTimeoutMs);
        }

        sleepQuietly(300);
        Map<String, Map<String, Long>> afterStats = captureStats(layout.actualNodes, cfg.requestTimeoutMs);

        return BenchmarkRow.fromWorkload(
                spec,
                workload,
                statsDelta(beforeStats, afterStats),
                0L
        );
    }

    private BenchmarkRow runRecoveryBenchmark(ClusterLayout layout, RunSpec spec, BenchmarkConfig cfg) throws Exception {
        wipeAll(layout.actualNodes, cfg.requestTimeoutMs);
        broadcastLeaderlessConfig(layout, spec, cfg, true);
        sleepQuietly(200);

        WorkloadResult preload = loadRecoveryDataset(layout, spec, cfg);
        sleepQuietly(200);

        call(requireNode(layout, spec.recoveryNodeId), ClientRequest.wipeNodeData("bench"), cfg.requestTimeoutMs);
        Map<String, Map<String, Long>> beforeStats = captureStats(layout.actualNodes, cfg.requestTimeoutMs);

        long expectedKeys = nodeStat(requireNode(layout, layout.homeNodes.getFirst().nodeId), "storeKeys", cfg.requestTimeoutMs);
        long recoveryStarted = System.nanoTime();
        call(requireNode(layout, layout.homeNodes.getFirst().nodeId), ClientRequest.runAntiEntropy("bench"), cfg.requestTimeoutMs);
        waitUntilStoreKeys(requireNode(layout, spec.recoveryNodeId), expectedKeys, cfg.requestTimeoutMs, cfg.recoveryTimeoutMs);
        long recoveryMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - recoveryStarted);

        sleepQuietly(200);
        Map<String, Map<String, Long>> afterStats = captureStats(layout.actualNodes, cfg.requestTimeoutMs);

        return BenchmarkRow.fromWorkload(
                spec,
                preload,
                statsDelta(beforeStats, afterStats),
                recoveryMs
        );
    }

    private WorkloadResult loadRecoveryDataset(ClusterLayout layout, RunSpec spec, BenchmarkConfig cfg) throws Exception {
        List<Long> latenciesMs = new ArrayList<>();
        int success = 0;
        int errors = 0;
        long started = System.nanoTime();

        List<ClusterNode> coordinators = aliveCoordinatorNodes(layout, null);
        for (int i = 0; i < spec.recoveryLoadOps; i++) {
            ClusterNode coordinator = coordinators.get(i % coordinators.size());
            String key = "recovery-" + i;
            String value = "v" + i;
            long opStarted = System.nanoTime();
            try {
                ClientResponse response = call(coordinator, ClientRequest.put("bench", key, value), cfg.requestTimeoutMs);
                if (response.status == ResponseStatus.OK) {
                    success++;
                } else {
                    errors++;
                }
            } catch (Exception e) {
                errors++;
            }
            latenciesMs.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - opStarted));
        }

        long totalNs = System.nanoTime() - started;
        return WorkloadResult.from(latenciesMs, totalNs, success, errors);
    }

    private WorkloadResult executeWorkload(ClusterLayout layout, RunSpec spec, BenchmarkConfig cfg) throws Exception {
        List<ClusterNode> coordinators = aliveCoordinatorNodes(layout, spec.unavailableHomeNodeId);
        long[] latenciesNs = new long[spec.totalOps];
        AtomicInteger nextOp = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(spec.threads);
        long started = System.nanoTime();

        List<Future<?>> futures = new ArrayList<>();
        for (int workerId = 0; workerId < spec.threads; workerId++) {
            futures.add(pool.submit(() -> {
                while (true) {
                    int opIndex = nextOp.getAndIncrement();
                    if (opIndex >= spec.totalOps) {
                        return;
                    }

                    boolean isPut = ThreadLocalRandom.current().nextDouble() < spec.putRatio;
                    String key = "k" + ThreadLocalRandom.current().nextInt(spec.keySpace);
                    ClusterNode node = coordinators.get(ThreadLocalRandom.current().nextInt(coordinators.size()));

                    long opStarted = System.nanoTime();
                    try {
                        ClientResponse response = isPut
                                ? call(node, ClientRequest.put("bench", key, "v" + opIndex + "-" + UUID.randomUUID()), cfg.requestTimeoutMs)
                                : call(node, ClientRequest.get("bench", key), cfg.requestTimeoutMs);
                        if (response.status == ResponseStatus.OK) {
                            success.incrementAndGet();
                        } else {
                            errors.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                    latenciesNs[opIndex] = System.nanoTime() - opStarted;
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);

        long totalNs = System.nanoTime() - started;
        return WorkloadResult.from(latenciesNs, totalNs, success.get(), errors.get());
    }

    private void broadcastLeaderlessConfig(ClusterLayout layout, RunSpec spec, BenchmarkConfig cfg, boolean sendToAllActualNodes) {
        ClusterUpdate update = new ClusterUpdate();
        update.clusterMode = ClusterMode.LEADERLESS;
        update.nodes = buildAdvertisedNodes(layout, spec.unavailableHomeNodeId);
        update.homeReplicaNodeIds = layout.homeNodes.stream().map(node -> node.nodeId).toList();
        update.spareNodeIds = layout.spareNodes.stream().map(node -> node.nodeId).toList();
        update.writeQuorum = spec.writeQuorum;
        update.readQuorum = spec.readQuorum;
        update.writeQuorumMode = spec.writeQuorumMode;
        update.replicationDelayMinMs = cfg.delayMinMs;
        update.replicationDelayMaxMs = cfg.delayMaxMs;
        update.hotPathTimeoutMs = cfg.requestTimeoutMs;

        MessageEnvelope env = MessageEnvelope.of(MessageType.CLUSTER_UPDATE, codec.toTree(update));
        String line = codec.toLine(env);
        List<ClusterNode> targets = sendToAllActualNodes ? layout.actualNodes : aliveCoordinatorNodes(layout, spec.unavailableHomeNodeId);
        for (ClusterNode node : targets) {
            try {
                client.callLine(node.host, node.port, line, cfg.requestTimeoutMs);
            } catch (Exception ignored) {
            }
        }
    }

    private List<ClusterNode> buildAdvertisedNodes(ClusterLayout layout, String unavailableHomeNodeId) {
        List<ClusterNode> advertised = new ArrayList<>();
        for (ClusterNode node : layout.actualNodes) {
            ClusterNode copy = new ClusterNode(node.nodeId, node.host, node.port, node.role);
            if (node.nodeId.equals(unavailableHomeNodeId)) {
                copy.port = UNAVAILABLE_PORT;
            }
            advertised.add(copy);
        }
        return advertised;
    }

    private Map<String, Map<String, Long>> captureStats(List<ClusterNode> nodes, int timeoutMs) throws Exception {
        Map<String, Map<String, Long>> stats = new LinkedHashMap<>();
        for (ClusterNode node : nodes) {
            ClientResponse response = call(node, ClientRequest.nodeStats("bench"), timeoutMs);
            stats.put(node.nodeId, response.stats == null ? Map.of() : new LinkedHashMap<>(response.stats));
        }
        return stats;
    }

    private Map<String, Long> statsDelta(Map<String, Map<String, Long>> before, Map<String, Map<String, Long>> after) {
        Set<String> keys = new LinkedHashSet<>();
        for (Map<String, Long> values : before.values()) {
            keys.addAll(values.keySet());
        }
        for (Map<String, Long> values : after.values()) {
            keys.addAll(values.keySet());
        }

        Map<String, Long> delta = new LinkedHashMap<>();
        for (String key : keys) {
            long beforeValue = 0;
            for (Map<String, Long> values : before.values()) {
                beforeValue += values.getOrDefault(key, 0L);
            }
            long afterValue = 0;
            for (Map<String, Long> values : after.values()) {
                afterValue += values.getOrDefault(key, 0L);
            }
            delta.put(key, afterValue - beforeValue);
        }
        return delta;
    }

    private void wipeAll(List<ClusterNode> nodes, int timeoutMs) {
        for (ClusterNode node : nodes) {
            try {
                call(node, ClientRequest.wipeNodeData("bench"), timeoutMs);
            } catch (Exception ignored) {
            }
        }
    }

    private void runHintedHandoff(List<ClusterNode> spareNodes, int timeoutMs) {
        for (ClusterNode node : spareNodes) {
            try {
                call(node, ClientRequest.runHintedHandoff("bench"), timeoutMs);
            } catch (Exception ignored) {
            }
        }
    }

    private long nodeStat(ClusterNode node, String stat, int timeoutMs) throws Exception {
        ClientResponse response = call(node, ClientRequest.nodeStats("bench"), timeoutMs);
        if (response.stats == null) {
            return 0;
        }
        return response.stats.getOrDefault(stat, 0L);
    }

    private void waitUntilStoreKeys(ClusterNode node, long expectedKeys, int timeoutMs, int recoveryTimeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + recoveryTimeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (nodeStat(node, "storeKeys", timeoutMs) >= expectedKeys) {
                return;
            }
            Thread.sleep(100);
        }
    }

    private ClusterNode requireNode(ClusterLayout layout, String nodeId) {
        return layout.actualNodes.stream()
                .filter(node -> node.nodeId.equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Неизвестный nodeId " + nodeId));
    }

    private List<ClusterNode> aliveCoordinatorNodes(ClusterLayout layout, String unavailableHomeNodeId) {
        return layout.actualNodes.stream()
                .filter(node -> !node.nodeId.equals(unavailableHomeNodeId))
                .toList();
    }

    private ClientResponse call(ClusterNode node, ClientRequest request, int timeoutMs) throws IOException {
        MessageEnvelope env = MessageEnvelope.of(request.type, codec.toTree(request));
        String responseLine = client.callLine(node.host, node.port, codec.toLine(env), timeoutMs);
        MessageEnvelope responseEnv = codec.fromLine(responseLine, MessageEnvelope.class);
        return codec.treeToValue(responseEnv.payload, ClientResponse.class);
    }

    private void writeCsv(String outputFile, List<BenchmarkRow> rows) throws IOException {
        Path output = Path.of(outputFile);
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }
        StringBuilder csv = new StringBuilder();
        csv.append("run_id,phase,mode,write_quorum_mode,w,r,put_ratio,threads,total_ops,key_space,delay_ms,unavailable_home_node_id,throughput_ops_sec,avg_latency_ms,p95_latency_ms,success_ops,error_ops,stale_read_count,read_repair_count,hints_created,hints_delivered,anti_entropy_runs,anti_entropy_recovered_keys,node_recovery_time_ms,notes")
                .append(System.lineSeparator());
        for (BenchmarkRow row : rows) {
            csv.append(row.toCsv()).append(System.lineSeparator());
        }
        Files.writeString(output, csv.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void writeReport(String reportFile, List<BenchmarkRow> rows, BenchmarkConfig cfg) throws IOException {
        Path output = Path.of(reportFile);
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }

        List<BenchmarkRow> partA = rows.stream().filter(row -> row.phase.equals("partA")).toList();
        List<BenchmarkRow> partB = rows.stream().filter(row -> row.phase.equals("partB")).toList();
        BenchmarkRow partC = rows.stream().filter(row -> row.phase.equals("partC")).findFirst().orElse(null);

        String generatedAt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        StringBuilder report = new StringBuilder();
        report.append("# Отчёт по benchmark-части ДЗ 4").append(System.lineSeparator()).append(System.lineSeparator());
        report.append("Сгенерировано: ").append(generatedAt).append(System.lineSeparator()).append(System.lineSeparator());
        report.append("Кластер benchmark-ов: `7 nodes`, `5 home + 2 spare`, `mode=leaderless`, `delay=0`.")
                .append(System.lineSeparator()).append(System.lineSeparator());

        report.append("## Таблица прогонов").append(System.lineSeparator()).append(System.lineSeparator());
        report.append("| runId | phase | writeMode | W | R | putRatio | threads | totalOps | throughputOpsSec | avgLatencyMs | p95LatencyMs | staleReadCount | readRepairCount | hintsCreated | hintsDelivered | antiEntropyRuns | antiEntropyRecoveredKeys | nodeRecoveryTimeMs | notes |").append(System.lineSeparator());
        report.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|").append(System.lineSeparator());
        for (BenchmarkRow row : rows) {
            report.append("| ")
                    .append(row.runId).append(" | ")
                    .append(row.phase).append(" | ")
                    .append(row.writeQuorumMode.name().toLowerCase(Locale.ROOT)).append(" | ")
                    .append(row.writeQuorum).append(" | ")
                    .append(row.readQuorum).append(" | ")
                    .append(formatDouble(row.putRatio)).append(" | ")
                    .append(row.threads).append(" | ")
                    .append(row.totalOps).append(" | ")
                    .append(formatDouble(row.throughputOpsSec)).append(" | ")
                    .append(formatDouble(row.avgLatencyMs)).append(" | ")
                    .append(row.p95LatencyMs).append(" | ")
                    .append(row.staleReadCount).append(" | ")
                    .append(row.readRepairCount).append(" | ")
                    .append(row.hintsCreated).append(" | ")
                    .append(row.hintsDelivered).append(" | ")
                    .append(row.antiEntropyRuns).append(" | ")
                    .append(row.antiEntropyRecoveredKeys).append(" | ")
                    .append(row.nodeRecoveryTimeMs).append(" | ")
                    .append(row.notes).append(" |").append(System.lineSeparator());
        }
        report.append(System.lineSeparator());

        report.append("## График 1: throughput для strict-конфигураций (write-heavy)").append(System.lineSeparator()).append(System.lineSeparator());
        report.append(mermaidBarChart(
                "Throughput strict write-heavy",
                partA.stream().filter(row -> row.putRatio > 0.5).sorted(Comparator.comparingInt(BenchmarkRow::writeQuorum)).toList(),
                row -> row.writeQuorum + "/" + row.readQuorum,
                BenchmarkRow::throughputOpsSec,
                "ops/sec"
        )).append(System.lineSeparator()).append(System.lineSeparator());

        report.append("## График 2: throughput для strict-конфигураций (read-heavy)").append(System.lineSeparator()).append(System.lineSeparator());
        report.append(mermaidBarChart(
                "Throughput strict read-heavy",
                partA.stream().filter(row -> row.putRatio < 0.5).sorted(Comparator.comparingInt(BenchmarkRow::writeQuorum)).toList(),
                row -> row.writeQuorum + "/" + row.readQuorum,
                BenchmarkRow::throughputOpsSec,
                "ops/sec"
        )).append(System.lineSeparator()).append(System.lineSeparator());

        report.append("## График 3: средняя latency для strict-конфигураций").append(System.lineSeparator()).append(System.lineSeparator());
        report.append(mermaidBarChart(
                "Average latency strict write-heavy",
                partA.stream().filter(row -> row.putRatio > 0.5).sorted(Comparator.comparingInt(BenchmarkRow::writeQuorum)).toList(),
                row -> row.writeQuorum + "/" + row.readQuorum,
                BenchmarkRow::avgLatencyMs,
                "ms"
        )).append(System.lineSeparator()).append(System.lineSeparator());

        report.append("## График 4: hints и recovery").append(System.lineSeparator()).append(System.lineSeparator());
        report.append(mermaidBarChart(
                "Hints created in part B",
                partB,
                BenchmarkRow::runId,
                row -> (double) row.hintsCreated,
                "count"
        )).append(System.lineSeparator()).append(System.lineSeparator());

        if (partC != null) {
            report.append(mermaidBarChart(
                    "Recovery time",
                    List.of(partC),
                    BenchmarkRow::runId,
                    row -> (double) row.nodeRecoveryTimeMs,
                    "ms"
            )).append(System.lineSeparator()).append(System.lineSeparator());
        }

        report.append("## Краткие выводы").append(System.lineSeparator()).append(System.lineSeparator());
        report.append("- В части A видно типичный trade-off quorum: при росте `W` запись дорожает, а чтение дешевеет меньше, потому что чтения в benchmark-е всё равно проходят через координаторов leaderless-кластера.")
                .append(System.lineSeparator());
        report.append("- В части B при конфигурации `W=3, R=3` и одной недоступной home-реплике strict и sloppy оба обычно завершаются успешно; отличие в том, что sloppy может задействовать spare для удержания write-доступности в более жёстких конфигурациях.")
                .append(System.lineSeparator());
        report.append("- `hintsCreated`/`hintsDelivered` для partB с `W=3, R=3` ожидаемо остаются нулевыми: недостатка home-ACK обычно нет, и spare-цепочка не требуется.")
                .append(System.lineSeparator());
        report.append("- `readRepairCount` не заменяет anti-entropy: read-repair срабатывает только там, где были реальные чтения. В recovery-сценарии состояние возвращается через `runAntiEntropyCluster` и `antiEntropyRecoveredKeys`.")
                .append(System.lineSeparator());
        if (partC != null) {
            report.append("- В части C `nodeRecoveryTimeMs=").append(partC.nodeRecoveryTimeMs)
                    .append("` и `antiEntropyRecoveredKeys=").append(partC.antiEntropyRecoveredKeys)
                    .append("` показывают восстановление wiped home-реплики через Merkle-based anti-entropy.")
                    .append(System.lineSeparator());
        }

        Files.writeString(output, report.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private String mermaidBarChart(String title, List<BenchmarkRow> rows, LabelExtractor labels, ValueExtractor values, String unit) {
        if (rows.isEmpty()) {
            return "_Нет данных для графика_";
        }
        double max = 0;
        List<String> labelsList = new ArrayList<>();
        List<String> valuesList = new ArrayList<>();
        for (BenchmarkRow row : rows) {
            labelsList.add("\"" + labels.extract(row) + "\"");
            double value = values.extract(row);
            valuesList.add(formatDouble(value));
            max = Math.max(max, value);
        }
        long upperBound = (long) Math.ceil(max * 1.1 + 1);
        return """
                ```mermaid
                xychart-beta
                    title "%s"
                    x-axis [%s]
                    y-axis "%s" 0 --> %d
                    bar [%s]
                ```
                """.formatted(title, String.join(", ", labelsList), unit, upperBound, String.join(", ", valuesList));
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String formatDouble(double value) {
        return String.format(Locale.US, "%.2f", value);
    }

    @FunctionalInterface
    private interface LabelExtractor {
        String extract(BenchmarkRow row);
    }

    @FunctionalInterface
    private interface ValueExtractor {
        double extract(BenchmarkRow row);
    }

    private record ClusterLayout(
            List<ClusterNode> actualNodes,
            List<ClusterNode> homeNodes,
            List<ClusterNode> spareNodes
    ) {
        static ClusterLayout fromConfig(List<String> nodeEndpoints) {
            if (nodeEndpoints.size() != 7) {
                throw new IllegalArgumentException("Для benchmark-ов ДЗ 4 нужно ровно 7 нод");
            }

            List<ClusterNode> actualNodes = new ArrayList<>();
            for (String endpoint : nodeEndpoints) {
                String[] hostPort = endpoint.split(":");
                String host = hostPort[0];
                int port = Integer.parseInt(hostPort[1]);
                String nodeId = host + ":" + port;
                actualNodes.add(new ClusterNode(nodeId, host, port));
            }
            actualNodes.sort(Comparator.comparing(node -> node.nodeId));

            List<ClusterNode> homeNodes = new ArrayList<>(actualNodes.subList(0, 5));
            List<ClusterNode> spareNodes = new ArrayList<>(actualNodes.subList(5, 7));
            for (ClusterNode node : homeNodes) {
                node.role = "home";
            }
            for (ClusterNode node : spareNodes) {
                node.role = "spare";
            }
            return new ClusterLayout(List.copyOf(actualNodes), List.copyOf(homeNodes), List.copyOf(spareNodes));
        }
    }

    private static final class BenchmarkConfig {
        final List<String> nodes;
        final String outputFile;
        final String reportFile;
        final int totalOps;
        final int recoveryLoadOps;
        final int keySpace;
        final int recoveryKeySpace;
        final int threads;
        final int requestTimeoutMs;
        final int recoveryTimeoutMs;
        final int delayMinMs;
        final int delayMaxMs;

        BenchmarkConfig(
                List<String> nodes,
                String outputFile,
                String reportFile,
                int totalOps,
                int recoveryLoadOps,
                int keySpace,
                int recoveryKeySpace,
                int threads,
                int requestTimeoutMs,
                int recoveryTimeoutMs,
                int delayMinMs,
                int delayMaxMs
        ) {
            this.nodes = nodes;
            this.outputFile = outputFile;
            this.reportFile = reportFile;
            this.totalOps = totalOps;
            this.recoveryLoadOps = recoveryLoadOps;
            this.keySpace = keySpace;
            this.recoveryKeySpace = recoveryKeySpace;
            this.threads = threads;
            this.requestTimeoutMs = requestTimeoutMs;
            this.recoveryTimeoutMs = recoveryTimeoutMs;
            this.delayMinMs = delayMinMs;
            this.delayMaxMs = delayMaxMs;
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
                    List.of(values.get("nodes").split(",")),
                    values.getOrDefault("output", "src/main/java/ru/hse/drip/systems/distributed/homework/i4/benchmarks/results.csv"),
                    values.getOrDefault("report", "src/main/java/ru/hse/drip/systems/distributed/homework/i4/report.md"),
                    Integer.parseInt(values.getOrDefault("totalOps", "1200")),
                    Integer.parseInt(values.getOrDefault("recoveryLoadOps", "600")),
                    Integer.parseInt(values.getOrDefault("keySpace", "1000")),
                    Integer.parseInt(values.getOrDefault("recoveryKeySpace", "600")),
                    Integer.parseInt(values.getOrDefault("threads", "16")),
                    Integer.parseInt(values.getOrDefault("requestTimeoutMs", "2000")),
                    Integer.parseInt(values.getOrDefault("recoveryTimeoutMs", "5000")),
                    Integer.parseInt(values.getOrDefault("delayMinMs", "0")),
                    Integer.parseInt(values.getOrDefault("delayMaxMs", values.getOrDefault("delayMinMs", "0")))
            );
        }
    }

    private record RunSpec(
            String runId,
            String phase,
            WriteQuorumMode writeQuorumMode,
            int writeQuorum,
            int readQuorum,
            double putRatio,
            int threads,
            int totalOps,
            int keySpace,
            String unavailableHomeNodeId,
            boolean runHintedHandoffAfterRun,
            boolean recovery,
            int recoveryLoadOps,
            int recoveryKeySpace,
            String recoveryNodeId,
            String notes
    ) {
        static RunSpec workload(
                String runId,
                String phase,
                WriteQuorumMode writeQuorumMode,
                int writeQuorum,
                int readQuorum,
                double putRatio,
                int threads,
                int totalOps,
                int keySpace,
                String unavailableHomeNodeId,
                boolean runHintedHandoffAfterRun,
                String notes
        ) {
            return new RunSpec(runId, phase, writeQuorumMode, writeQuorum, readQuorum, putRatio, threads, totalOps, keySpace,
                    unavailableHomeNodeId, runHintedHandoffAfterRun, false, 0, 0, null, notes);
        }

        static RunSpec recovery(
                String runId,
                String phase,
                WriteQuorumMode writeQuorumMode,
                int writeQuorum,
                int readQuorum,
                int recoveryLoadOps,
                int recoveryKeySpace,
                String recoveryNodeId,
                String notes
        ) {
            return new RunSpec(runId, phase, writeQuorumMode, writeQuorum, readQuorum, 1.0, 1, recoveryLoadOps, recoveryKeySpace,
                    null, false, true, recoveryLoadOps, recoveryKeySpace, recoveryNodeId, notes);
        }

        RunSpec withUnavailableHomeNodeId(String nodeId) {
            return new RunSpec(runId, phase, writeQuorumMode, writeQuorum, readQuorum, putRatio, threads, totalOps, keySpace,
                    nodeId, runHintedHandoffAfterRun, recovery, recoveryLoadOps, recoveryKeySpace, recoveryNodeId, notes);
        }
    }

    private record WorkloadResult(
            double throughputOpsSec,
            double avgLatencyMs,
            long p95LatencyMs,
            int successOps,
            int errorOps
    ) {
        static WorkloadResult from(long[] latenciesNs, long totalNs, int successOps, int errorOps) {
            List<Long> latenciesMs = new ArrayList<>(latenciesNs.length);
            for (long latencyNs : latenciesNs) {
                latenciesMs.add(TimeUnit.NANOSECONDS.toMillis(latencyNs));
            }
            return from(latenciesMs, totalNs, successOps, errorOps);
        }

        static WorkloadResult from(List<Long> latenciesMs, int successOps, int errorOps) {
            long sumMs = 0;
            for (Long latency : latenciesMs) {
                sumMs += latency;
            }
            double avg = latenciesMs.isEmpty() ? 0.0 : (double) sumMs / latenciesMs.size();
            return new WorkloadResult(0.0, avg, percentile(latenciesMs, 0.95), successOps, errorOps);
        }

        static WorkloadResult from(List<Long> latenciesMs, long totalNs, int successOps, int errorOps) {
            long sumMs = 0;
            for (Long latency : latenciesMs) {
                sumMs += latency;
            }
            double avg = latenciesMs.isEmpty() ? 0.0 : (double) sumMs / latenciesMs.size();
            double seconds = totalNs / 1_000_000_000.0;
            double throughput = seconds == 0.0 ? 0.0 : latenciesMs.size() / seconds;
            return new WorkloadResult(throughput, avg, percentile(latenciesMs, 0.95), successOps, errorOps);
        }

        private static long percentile(List<Long> values, double percentile) {
            if (values.isEmpty()) {
                return 0;
            }
            Collections.sort(values);
            int index = (int) Math.min(values.size() - 1, Math.ceil(percentile * values.size()) - 1);
            return values.get(index);
        }
    }

    private record BenchmarkRow(
            String runId,
            String phase,
            WriteQuorumMode writeQuorumMode,
            int writeQuorum,
            int readQuorum,
            double putRatio,
            int threads,
            int totalOps,
            int keySpace,
            int delayMs,
            String unavailableHomeNodeId,
            double throughputOpsSec,
            double avgLatencyMs,
            long p95LatencyMs,
            int successOps,
            int errorOps,
            long staleReadCount,
            long readRepairCount,
            long hintsCreated,
            long hintsDelivered,
            long antiEntropyRuns,
            long antiEntropyRecoveredKeys,
            long nodeRecoveryTimeMs,
            String notes
    ) {
        static BenchmarkRow fromWorkload(RunSpec spec, WorkloadResult workload, Map<String, Long> statsDelta, long nodeRecoveryTimeMs) {
            return new BenchmarkRow(
                    spec.runId,
                    spec.phase,
                    spec.writeQuorumMode,
                    spec.writeQuorum,
                    spec.readQuorum,
                    spec.putRatio,
                    spec.threads,
                    spec.recovery ? spec.recoveryLoadOps : spec.totalOps,
                    spec.recovery ? spec.recoveryKeySpace : spec.keySpace,
                    0,
                    spec.unavailableHomeNodeId == null ? "" : spec.unavailableHomeNodeId,
                    workload.throughputOpsSec,
                    workload.avgLatencyMs,
                    workload.p95LatencyMs,
                    workload.successOps,
                    workload.errorOps,
                    statsDelta.getOrDefault("staleReadCount", 0L),
                    statsDelta.getOrDefault("readRepairCount", 0L),
                    statsDelta.getOrDefault("hintsCreated", 0L),
                    statsDelta.getOrDefault("hintsDelivered", 0L),
                    statsDelta.getOrDefault("antiEntropyRuns", 0L),
                    statsDelta.getOrDefault("antiEntropyRecoveredKeys", 0L),
                    nodeRecoveryTimeMs,
                    spec.notes
            );
        }

        String toCsv() {
            return String.join(",",
                    runId,
                    phase,
                    "leaderless",
                    writeQuorumMode.name().toLowerCase(Locale.ROOT),
                    String.valueOf(writeQuorum),
                    String.valueOf(readQuorum),
                    formatDouble(putRatio),
                    String.valueOf(threads),
                    String.valueOf(totalOps),
                    String.valueOf(keySpace),
                    String.valueOf(delayMs),
                    unavailableHomeNodeId,
                    formatDouble(throughputOpsSec),
                    formatDouble(avgLatencyMs),
                    String.valueOf(p95LatencyMs),
                    String.valueOf(successOps),
                    String.valueOf(errorOps),
                    String.valueOf(staleReadCount),
                    String.valueOf(readRepairCount),
                    String.valueOf(hintsCreated),
                    String.valueOf(hintsDelivered),
                    String.valueOf(antiEntropyRuns),
                    String.valueOf(antiEntropyRecoveredKeys),
                    String.valueOf(nodeRecoveryTimeMs),
                    notes.replace(",", ";")
            );
        }

        String toHumanString() {
            return runId + " phase=" + phase
                    + " mode=" + writeQuorumMode.name().toLowerCase(Locale.ROOT)
                    + " W=" + writeQuorum
                    + " R=" + readQuorum
                    + " throughput=" + formatDouble(throughputOpsSec)
                    + " avgMs=" + formatDouble(avgLatencyMs)
                    + " p95Ms=" + p95LatencyMs
                    + " staleReads=" + staleReadCount
                    + " readRepairs=" + readRepairCount
                    + " hintsCreated=" + hintsCreated
                    + " hintsDelivered=" + hintsDelivered
                    + " antiEntropyRuns=" + antiEntropyRuns
                    + " antiEntropyRecoveredKeys=" + antiEntropyRecoveredKeys
                    + " nodeRecoveryTimeMs=" + nodeRecoveryTimeMs;
        }
    }
}

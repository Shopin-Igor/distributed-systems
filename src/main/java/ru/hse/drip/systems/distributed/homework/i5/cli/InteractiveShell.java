package ru.hse.drip.systems.distributed.homework.i5.cli;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MigrationReport;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ResponseStatus;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;

import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class InteractiveShell {
    private final CliParser parser;
    private final CliClient client;
    private final JsonCodec codec;

    public InteractiveShell(CliParser parser, CliClient client, JsonCodec codec) {
        this.parser = parser;
        this.client = client;
        this.codec = codec;
    }

    public void run() {
        printHelp();
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("i5> ");
            if (!scanner.hasNextLine()) {
                return;
            }

            String line = scanner.nextLine().trim();
            if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                return;
            }
            if (line.equalsIgnoreCase("help")) {
                printHelp();
                continue;
            }
            try {
                parser.parse(line)
                        .map(client::send)
                        .ifPresent(response -> System.out.println(format(response)));
            } catch (Exception e) {
                System.out.println("ERROR BAD_REQUEST: " + e.getMessage());
            }
        }
    }

    public String format(WireMessage response) {
        if (response.status == ResponseStatus.ERROR) {
            return "ERROR " + response.errorCode + ": " + response.error;
        }

        StringBuilder out = new StringBuilder("OK");
        appendLine(out, response.message);
        if (response.nodeId != null) {
            appendLine(out, "node=" + response.nodeId);
        }
        if (response.value != null) {
            appendLine(out, "value=" + response.value);
        }
        if (response.strategy != null) {
            appendLine(out, "strategy=" + response.strategy + ", vnodes=" + response.count);
        }
        if (response.boundaries != null && !response.boundaries.isEmpty()) {
            appendLine(out, "ranges=" + response.boundaries);
        }
        if (response.nodes != null && !response.nodes.isEmpty()) {
            appendNodes(out, response.nodes);
        }
        if (response.migration != null) {
            appendMigration(out, response.migration);
        }
        if (response.stats != null) {
            appendLine(out, codec.toPrettyJson(response.stats));
        }
        if (response.data != null && !response.data.isEmpty()) {
            appendLine(out, codec.toPrettyJson(response.data));
        }
        if (response.clusterData != null && !response.clusterData.isEmpty()) {
            appendLine(out, codec.toPrettyJson(response.clusterData));
        }
        if (response.ringTokens != null && !response.ringTokens.isEmpty()) {
            appendRing(out, response.ringTokens);
        }
        return out.toString();
    }

    private void appendNodes(StringBuilder out, List<ClusterNode> nodes) {
        appendLine(out, "nodes:");
        for (ClusterNode node : nodes) {
            appendLine(out, "  " + node.nodeId + " " + node.address()
                    + " alive=" + node.alive + " weight=" + node.weight);
        }
    }

    private void appendMigration(StringBuilder out, MigrationReport report) {
        appendLine(out, "migration: moved=" + report.movedKeys + "/" + report.totalKeys
                + String.format(" (%.2f%%)", report.movedPercent())
                + ", durationMs=" + report.durationMs
                + ", reason=" + report.reason);
    }

    private void appendRing(StringBuilder out, Map<String, List<Long>> ringTokens) {
        appendLine(out, "ring:");
        for (Map.Entry<String, List<Long>> entry : ringTokens.entrySet()) {
            appendLine(out, "  " + entry.getKey() + " tokens=" + entry.getValue());
        }
    }

    private void appendLine(StringBuilder out, String line) {
        if (line == null || line.isBlank()) {
            return;
        }
        out.append(System.lineSeparator()).append(line);
    }

    private void printHelp() {
        System.out.println("""
                Commands:
                  addNode <nodeId> <host> <port> [weight]
                  removeNode <nodeId>
                  deleteNode <nodeId>  (alias for removeNode)
                  listNodes
                  setStrategy range|hash_mod_n|consistent_hashing [--force]
                  setRanges <boundary1> <boundary2> ...
                  setVnodes <count>
                  setWeight <nodeId> <weight>
                  migrateData
                  put <key> <value>
                  get <key>
                  delete <key>
                  getRange <fromKey> <toKey>
                  dump [--target <nodeId>]
                  clusterDump
                  stats
                  ringInfo
                  help
                  exit
                """);
    }
}

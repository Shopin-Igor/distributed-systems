package ru.hse.drip.systems.distributed.homework.i5.cli;

import ru.hse.drip.systems.distributed.homework.i5.protocol.MessageType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class CliParser {
    public Optional<WireMessage> parse(String line) {
        List<String> tokens = tokenize(line);
        if (tokens.isEmpty()) {
            return Optional.empty();
        }

        String command = tokens.getFirst().toLowerCase(Locale.ROOT);
        return Optional.of(switch (command) {
            case "addnode" -> addNode(tokens);
            case "removenode", "deletenode" -> oneNode(tokens, MessageType.REMOVE_NODE);
            case "listnodes", "clusterinfo" -> WireMessage.request(MessageType.CLUSTER_INFO);
            case "setstrategy" -> setStrategy(tokens);
            case "setranges" -> setRanges(tokens);
            case "setvnodes" -> oneInteger(tokens, MessageType.SET_VNODES, "count");
            case "setweight" -> setWeight(tokens);
            case "migratedata" -> WireMessage.request(MessageType.MIGRATE_DATA);
            case "put" -> put(tokens);
            case "get" -> oneKey(tokens, MessageType.CLIENT_GET);
            case "delete" -> oneKey(tokens, MessageType.CLIENT_DELETE);
            case "getrange" -> getRange(tokens);
            case "dump" -> dump(tokens);
            case "clusterdump" -> WireMessage.request(MessageType.CLUSTER_DUMP);
            case "stats" -> WireMessage.request(MessageType.STATS);
            case "ringinfo" -> WireMessage.request(MessageType.RING_INFO);
            default -> throw new IllegalArgumentException("unknown command: " + tokens.getFirst());
        });
    }

    private WireMessage addNode(List<String> tokens) {
        requireSize(tokens, 4, "addNode <nodeId> <host> <port> [weight]");
        WireMessage request = WireMessage.request(MessageType.ADD_NODE);
        request.nodeId = tokens.get(1);
        request.host = tokens.get(2);
        request.port = parseInt(tokens.get(3), "port");
        if (tokens.size() >= 5) {
            request.weight = parseInt(tokens.get(4), "weight");
        }
        return request;
    }

    private WireMessage oneNode(List<String> tokens, MessageType type) {
        requireSize(tokens, 2, tokens.getFirst() + " <nodeId>");
        WireMessage request = WireMessage.request(type);
        request.nodeId = tokens.get(1);
        return request;
    }

    private WireMessage setStrategy(List<String> tokens) {
        requireSize(tokens, 2, "setStrategy range|hash_mod_n|consistent_hashing [--force]");
        WireMessage request = WireMessage.request(MessageType.SET_STRATEGY);
        request.strategy = tokens.get(1);
        request.force = tokens.stream().anyMatch("--force"::equalsIgnoreCase);
        return request;
    }

    private WireMessage setRanges(List<String> tokens) {
        WireMessage request = WireMessage.request(MessageType.SET_RANGES);
        request.boundaries = new ArrayList<>(tokens.subList(1, tokens.size()));
        return request;
    }

    private WireMessage oneInteger(List<String> tokens, MessageType type, String fieldName) {
        requireSize(tokens, 2, tokens.getFirst() + " <" + fieldName + ">");
        WireMessage request = WireMessage.request(type);
        request.count = parseInt(tokens.get(1), fieldName);
        return request;
    }

    private WireMessage setWeight(List<String> tokens) {
        requireSize(tokens, 3, "setWeight <nodeId> <weight>");
        WireMessage request = WireMessage.request(MessageType.SET_WEIGHT);
        request.nodeId = tokens.get(1);
        request.weight = parseInt(tokens.get(2), "weight");
        return request;
    }

    private WireMessage put(List<String> tokens) {
        requireSize(tokens, 3, "put <key> <value>");
        WireMessage request = WireMessage.request(MessageType.CLIENT_PUT);
        request.key = tokens.get(1);
        request.value = String.join(" ", tokens.subList(2, tokens.size()));
        return request;
    }

    private WireMessage oneKey(List<String> tokens, MessageType type) {
        requireSize(tokens, 2, tokens.getFirst() + " <key>");
        WireMessage request = WireMessage.request(type);
        request.key = tokens.get(1);
        return request;
    }

    private WireMessage getRange(List<String> tokens) {
        requireSize(tokens, 3, "getRange <fromKey> <toKey>");
        WireMessage request = WireMessage.request(MessageType.CLIENT_GET_RANGE);
        request.fromKey = tokens.get(1);
        request.toKey = tokens.get(2);
        return request;
    }

    private WireMessage dump(List<String> tokens) {
        WireMessage request = WireMessage.request(MessageType.DUMP);
        for (int i = 1; i < tokens.size(); i++) {
            if ("--target".equalsIgnoreCase(tokens.get(i))) {
                if (i + 1 >= tokens.size()) {
                    throw new IllegalArgumentException("dump --target requires nodeId");
                }
                request.targetNodeId = tokens.get(i + 1);
                i++;
            } else {
                request.targetNodeId = tokens.get(i);
            }
        }
        return request;
    }

    private List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                quoted = !quoted;
                continue;
            }
            if (Character.isWhitespace(ch) && !quoted) {
                flushToken(tokens, current);
                continue;
            }
            current.append(ch);
        }
        flushToken(tokens, current);

        if (quoted) {
            throw new IllegalArgumentException("unclosed quote");
        }
        return tokens;
    }

    private void flushToken(List<String> tokens, StringBuilder current) {
        if (!current.isEmpty()) {
            tokens.add(current.toString());
            current.setLength(0);
        }
    }

    private void requireSize(List<String> tokens, int minSize, String usage) {
        if (tokens.size() < minSize) {
            throw new IllegalArgumentException("usage: " + usage);
        }
    }

    private int parseInt(String raw, String fieldName) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(fieldName + " must be an integer: " + raw);
        }
    }
}

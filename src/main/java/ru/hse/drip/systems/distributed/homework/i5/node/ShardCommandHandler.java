package ru.hse.drip.systems.distributed.homework.i5.node;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ErrorCode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MessageType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;
import ru.hse.drip.systems.distributed.homework.i5.store.KeyValueStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class ShardCommandHandler {
    private final String nodeId;
    private final KeyValueStore store;

    public ShardCommandHandler(String nodeId, KeyValueStore store) {
        this.nodeId = nodeId;
        this.store = store;
    }

    public WireMessage handle(WireMessage request) {
        if (request == null || request.type == null) {
            return WireMessage.error(null, ErrorCode.BAD_REQUEST, "message type is required");
        }

        try {
            return switch (request.type) {
                case SHARD_PUT -> put(request);
                case SHARD_GET -> get(request);
                case SHARD_DELETE -> delete(request);
                case SHARD_GET_KEYS -> keys(request);
                case SHARD_DUMP -> dump(request);
                case SHARD_GET_RANGE -> getRange(request);
                case SHARD_CLEAR -> clear(request);
                default -> WireMessage.error(request.requestId, ErrorCode.BAD_REQUEST,
                        "unsupported shard message: " + request.type);
            };
        } catch (IllegalArgumentException e) {
            return WireMessage.error(request.requestId, ErrorCode.BAD_REQUEST, e.getMessage());
        }
    }

    private WireMessage put(WireMessage request) {
        requireKey(request.key);
        if (request.value == null) {
            throw new IllegalArgumentException("value is required");
        }
        store.put(request.key, request.value);

        WireMessage response = WireMessage.ok(request.requestId);
        response.nodeId = nodeId;
        response.message = "stored";
        return response;
    }

    private WireMessage get(WireMessage request) {
        requireKey(request.key);
        Optional<String> value = store.get(request.key);
        if (value.isEmpty()) {
            return WireMessage.error(request.requestId, ErrorCode.KEY_NOT_FOUND, "key not found: " + request.key);
        }

        WireMessage response = WireMessage.ok(request.requestId);
        response.nodeId = nodeId;
        response.key = request.key;
        response.value = value.get();
        return response;
    }

    private WireMessage delete(WireMessage request) {
        requireKey(request.key);
        boolean removed = store.delete(request.key);
        if (!removed) {
            return WireMessage.error(request.requestId, ErrorCode.KEY_NOT_FOUND, "key not found: " + request.key);
        }

        WireMessage response = WireMessage.ok(request.requestId);
        response.nodeId = nodeId;
        response.message = "deleted";
        return response;
    }

    private WireMessage keys(WireMessage request) {
        WireMessage response = WireMessage.ok(request.requestId);
        response.nodeId = nodeId;
        response.keys = new ArrayList<>(store.keys());
        return response;
    }

    private WireMessage dump(WireMessage request) {
        WireMessage response = WireMessage.ok(request.requestId);
        response.nodeId = nodeId;
        response.data = new LinkedHashMap<>(store.snapshot());
        response.keys = new ArrayList<>(response.data.keySet());
        return response;
    }

    private WireMessage getRange(WireMessage request) {
        requireKey(request.fromKey);
        requireKey(request.toKey);
        if (request.fromKey.compareTo(request.toKey) >= 0) {
            throw new IllegalArgumentException("fromKey must be lower than toKey");
        }

        Map<String, String> values = store.range(request.fromKey, request.toKey);
        WireMessage response = WireMessage.ok(request.requestId);
        response.nodeId = nodeId;
        response.data = new LinkedHashMap<>(values);
        response.keys = new ArrayList<>(values.keySet());
        return response;
    }

    private WireMessage clear(WireMessage request) {
        store.clear();
        WireMessage response = WireMessage.ok(request.requestId);
        response.nodeId = nodeId;
        response.message = "cleared";
        return response;
    }

    private void requireKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is required");
        }
        if (key.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("key must not contain whitespace");
        }
    }
}

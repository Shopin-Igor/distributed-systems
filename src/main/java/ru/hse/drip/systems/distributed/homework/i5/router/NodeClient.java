package ru.hse.drip.systems.distributed.homework.i5.router;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ErrorCode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MessageType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ResponseStatus;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpClient;

import java.io.IOException;
import java.util.Map;

public class NodeClient {
    private final JsonCodec codec;
    private final TcpClient tcpClient;
    private final int timeoutMs;

    public NodeClient(JsonCodec codec, TcpClient tcpClient, int timeoutMs) {
        this.codec = codec;
        this.tcpClient = tcpClient;
        this.timeoutMs = timeoutMs;
    }

    public WireMessage put(ClusterNode node, String key, String value) {
        WireMessage request = WireMessage.request(MessageType.SHARD_PUT);
        request.key = key;
        request.value = value;
        return call(node, request);
    }

    public WireMessage get(ClusterNode node, String key) {
        WireMessage request = WireMessage.request(MessageType.SHARD_GET);
        request.key = key;
        return call(node, request);
    }

    public WireMessage delete(ClusterNode node, String key) {
        WireMessage request = WireMessage.request(MessageType.SHARD_DELETE);
        request.key = key;
        return call(node, request);
    }

    public WireMessage keys(ClusterNode node) {
        return call(node, WireMessage.request(MessageType.SHARD_GET_KEYS));
    }

    public WireMessage dump(ClusterNode node) {
        return call(node, WireMessage.request(MessageType.SHARD_DUMP));
    }

    public WireMessage getRange(ClusterNode node, String fromKey, String toKey) {
        WireMessage request = WireMessage.request(MessageType.SHARD_GET_RANGE);
        request.fromKey = fromKey;
        request.toKey = toKey;
        return call(node, request);
    }

    public WireMessage clear(ClusterNode node) {
        return call(node, WireMessage.request(MessageType.SHARD_CLEAR));
    }

    public void clearData(ClusterNode node) {
        requireOk(node, clear(node), "clear");
    }

    public void putData(ClusterNode node, String key, String value) {
        requireOk(node, put(node, key, value), "put " + key);
    }

    public boolean deleteIfPresent(ClusterNode node, String key) {
        WireMessage response = delete(node, key);
        if (response.status == ResponseStatus.OK) {
            return true;
        }
        if (response.errorCode == ErrorCode.KEY_NOT_FOUND) {
            return false;
        }
        throw failedResponse(node, response, "delete " + key);
    }

    public boolean canConnect(ClusterNode node) {
        return tcpClient.canConnect(node.host, node.port, timeoutMs);
    }

    public WireMessage call(ClusterNode node, WireMessage request) {
        try {
            String line = tcpClient.callLine(node.host, node.port, codec.toLine(request), timeoutMs);
            if (line == null) {
                throw new NodeUnavailableException("node closed connection: " + node.nodeId);
            }
            return codec.fromLine(line, WireMessage.class);
        } catch (IOException e) {
            throw new NodeUnavailableException("node unavailable: " + node.nodeId + " at " + node.address(), e);
        }
    }

    public Map<String, String> dumpData(ClusterNode node) {
        WireMessage response = dump(node);
        if (response.status != ResponseStatus.OK) {
            throw new IllegalStateException("dump failed on " + node.nodeId + ": " + response.error);
        }
        return response.data;
    }

    private void requireOk(ClusterNode node, WireMessage response, String operation) {
        if (response.status != ResponseStatus.OK) {
            throw failedResponse(node, response, operation);
        }
    }

    private IllegalStateException failedResponse(ClusterNode node, WireMessage response, String operation) {
        return new IllegalStateException(operation + " failed on " + node.nodeId
                + ": " + response.errorCode + " " + response.error);
    }
}

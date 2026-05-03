package ru.hse.drip.systems.distributed.homework.i5.node;

public class ShardNodeConfig {
    public final String nodeId;
    public final String host;
    public final int port;

    public ShardNodeConfig(String nodeId, String host, int port) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId is required");
        }
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }
}

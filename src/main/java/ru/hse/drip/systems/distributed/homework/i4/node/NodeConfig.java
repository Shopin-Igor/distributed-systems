package ru.hse.drip.systems.distributed.homework.i4.node;

import ru.hse.drip.systems.distributed.homework.i4.replication.ReplicationMode;

public class NodeConfig {
    public int hotPathTimeoutMs = 500;
    public int retryIntervalMs = 200;
    public String nodeId;
    public String host;
    public int port;

    public ReplicationMode replicationMode = ReplicationMode.ASYNC;
    public int rf = 1;
    public int k = 1;
    public int timeoutMs = 500;

    public NodeConfig() {}
}

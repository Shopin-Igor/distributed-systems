package ru.hse.drip.systems.distributed.homework.i2.protocol;

public class ClusterNode {
    public String nodeId;
    public String host;
    public int port;

    public ClusterNode() {}
    public ClusterNode(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }
}

package ru.hse.drip.systems.distributed.homework.i4.protocol;

public class ClusterNode {
    public String nodeId;
    public String host;
    public int port;
    public String role;

    public ClusterNode() {}
    public ClusterNode(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public ClusterNode(String nodeId, String host, int port, String role) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.role = role;
    }
}

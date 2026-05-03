package ru.hse.drip.systems.distributed.homework.i5.protocol;

import java.util.Objects;

public class ClusterNode {
    public String nodeId;
    public String host;
    public int port;
    public int weight = 1;
    public boolean alive = true;

    public ClusterNode() {
    }

    public ClusterNode(String nodeId, String host, int port) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    public ClusterNode copy() {
        ClusterNode copy = new ClusterNode(nodeId, host, port);
        copy.weight = weight;
        copy.alive = alive;
        return copy;
    }

    public String address() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return nodeId + "(" + address() + ", weight=" + weight + ", alive=" + alive + ")";
    }
}

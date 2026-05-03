package ru.hse.drip.systems.distributed.homework.i3.protocol;

import ru.hse.drip.systems.distributed.homework.i3.replication.ReplicationMode;

import java.util.List;

public class ClusterUpdate {
    public List<ClusterNode> nodes;
    public String leaderNodeId;
    public List<String> leaderNodeIds;
    public ClusterMode clusterMode = ClusterMode.SINGLE;
    public TopologyType topology = TopologyType.MESH;
    public String starCenterNodeId;

    public ReplicationMode replicationMode;
    public int rf;
    public int semiSyncAcksK;

    public int replicationDelayMinMs;
    public int replicationDelayMaxMs;

    public int hotPathTimeoutMs = 500;

    public ClusterUpdate() {}
}

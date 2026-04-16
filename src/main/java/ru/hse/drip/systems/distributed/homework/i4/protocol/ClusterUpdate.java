package ru.hse.drip.systems.distributed.homework.i4.protocol;

import ru.hse.drip.systems.distributed.homework.i4.replication.ReplicationMode;

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
    public List<String> homeReplicaNodeIds = List.of();
    public List<String> spareNodeIds = List.of();
    public int writeQuorum = 3;
    public int readQuorum = 3;
    public WriteQuorumMode writeQuorumMode = WriteQuorumMode.STRICT;

    public ClusterUpdate() {}
}

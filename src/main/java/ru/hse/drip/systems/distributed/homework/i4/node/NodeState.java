package ru.hse.drip.systems.distributed.homework.i4.node;

import ru.hse.drip.systems.distributed.homework.i4.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i4.protocol.ClusterMode;
import ru.hse.drip.systems.distributed.homework.i4.protocol.TopologyType;
import ru.hse.drip.systems.distributed.homework.i4.protocol.WriteQuorumMode;
import ru.hse.drip.systems.distributed.homework.i4.replication.ReplicationMode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NodeState {
    public volatile String leaderNodeId;
    public volatile Role role = Role.FOLLOWER;
    public volatile ClusterMode clusterMode = ClusterMode.SINGLE;
    public volatile TopologyType topology = TopologyType.MESH;
    public volatile String starCenterNodeId;
    public volatile List<String> leaderNodeIds = List.of();
    public final AtomicLong lamportCounter = new AtomicLong();

    public final Map<String, ClusterNode> nodes = new ConcurrentHashMap<>();

    public volatile ReplicationMode replicationMode = ReplicationMode.ASYNC;
    public volatile int rf = 1;
    public volatile int semiSyncAcksK = 1;
    public volatile int delayMinMs = 0;
    public volatile int delayMaxMs = 0;
    public volatile int hotPathTimeoutMs = 500;
    public volatile List<String> homeReplicaNodeIds = List.of();
    public volatile List<String> spareNodeIds = List.of();
    public volatile int writeQuorum = 3;
    public volatile int readQuorum = 3;
    public volatile WriteQuorumMode writeQuorumMode = WriteQuorumMode.STRICT;

    public final AtomicLong staleReadCount = new AtomicLong();
    public final AtomicLong readRepairCount = new AtomicLong();
    public final AtomicLong hintsCreated = new AtomicLong();
    public final AtomicLong hintsDelivered = new AtomicLong();
    public final AtomicLong antiEntropyRuns = new AtomicLong();
    public final AtomicLong antiEntropyRecoveredKeys = new AtomicLong();
}

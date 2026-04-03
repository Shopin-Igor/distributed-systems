package ru.hse.drip.systems.distributed.homework.i2.node;

import ru.hse.drip.systems.distributed.homework.i2.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i2.replication.ReplicationMode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class NodeState {
    public volatile String leaderNodeId;
    public volatile Role role = Role.FOLLOWER;

    public final Map<String, ClusterNode> nodes = new ConcurrentHashMap<>();

    public volatile ReplicationMode replicationMode = ReplicationMode.ASYNC;
    public volatile int rf = 1;
    public volatile int semiSyncAcksK = 1;
    public volatile int delayMinMs = 0;
    public volatile int delayMaxMs = 0;
    public volatile int hotPathTimeoutMs = 500;

    public final AtomicLong nextMutationVersion = new AtomicLong();
}

package ru.hse.drip.systems.distributed.homework.i2.cli;
import ru.hse.drip.systems.distributed.homework.i2.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i2.replication.ReplicationMode;

import java.util.LinkedHashMap;
import java.util.Map;

public class ControllerState {
    public final Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    public String leaderNodeId;

    public ReplicationMode mode = ReplicationMode.ASYNC;
    public int rf = 1;
    public int k = 1;
    public int delayMinMs = 0;
    public int delayMaxMs = 0;
    public int hotPathTimeoutMs = 500;
}

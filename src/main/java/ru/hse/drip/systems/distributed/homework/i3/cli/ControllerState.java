package ru.hse.drip.systems.distributed.homework.i3.cli;
import ru.hse.drip.systems.distributed.homework.i3.protocol.ClusterMode;
import ru.hse.drip.systems.distributed.homework.i3.protocol.ClusterNode;
import ru.hse.drip.systems.distributed.homework.i3.protocol.TopologyType;
import ru.hse.drip.systems.distributed.homework.i3.replication.ReplicationMode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ControllerState {
    public final Map<String, ClusterNode> nodes = new LinkedHashMap<>();
    public String leaderNodeId;
    public ClusterMode clusterMode = ClusterMode.SINGLE;
    public TopologyType topology = TopologyType.MESH;
    public String starCenterNodeId;
    public List<String> leaderNodeIds = new ArrayList<>();

    public ReplicationMode mode = ReplicationMode.ASYNC;
    public int rf = 1;
    public int k = 1;
    public int delayMinMs = 0;
    public int delayMaxMs = 0;
    public int hotPathTimeoutMs = 500;
}

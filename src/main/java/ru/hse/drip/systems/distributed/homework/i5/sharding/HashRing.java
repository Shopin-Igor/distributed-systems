package ru.hse.drip.systems.distributed.homework.i5.sharding;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ClusterNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class HashRing {
    private final TreeMap<Long, String> tokenToNode = new TreeMap<>();
    private final Map<String, List<Long>> nodeToTokens = new LinkedHashMap<>();

    public static HashRing build(List<ClusterNode> nodes, int vnodeCount) {
        HashRing ring = new HashRing();
        for (ClusterNode node : nodes) {
            int effectiveVnodes = vnodeCount * Math.max(1, node.weight);
            for (int vnodeIndex = 0; vnodeIndex < effectiveVnodes; vnodeIndex++) {
                long token = uniqueToken(node.nodeId + "#" + vnodeIndex, ring.tokenToNode);
                ring.tokenToNode.put(token, node.nodeId);
                ring.nodeToTokens.computeIfAbsent(node.nodeId, ignored -> new ArrayList<>()).add(token);
            }
        }
        ring.nodeToTokens.values().forEach(Collections::sort);
        return ring;
    }

    public Optional<String> lookupNodeId(String key) {
        if (tokenToNode.isEmpty()) {
            return Optional.empty();
        }

        long token = HashUtil.sha256ToLong(key);
        Map.Entry<Long, String> entry = tokenToNode.ceilingEntry(token);
        if (entry == null) {
            entry = tokenToNode.firstEntry();
        }
        return Optional.of(entry.getValue());
    }

    public Map<String, List<Long>> tokensByNode() {
        Map<String, List<Long>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<Long>> entry : nodeToTokens.entrySet()) {
            copy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return copy;
    }

    private static long uniqueToken(String seed, TreeMap<Long, String> existing) {
        long token = HashUtil.sha256ToLong(seed);
        int collision = 0;
        while (existing.containsKey(token)) {
            collision++;
            token = HashUtil.sha256ToLong(seed + "@" + collision);
        }
        return token;
    }
}

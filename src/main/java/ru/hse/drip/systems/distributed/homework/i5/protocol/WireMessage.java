package ru.hse.drip.systems.distributed.homework.i5.protocol;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class WireMessage {
    public String requestId;
    public MessageType type;

    public String key;
    public String value;
    public String fromKey;
    public String toKey;

    public String nodeId;
    public String host;
    public Integer port;
    public Integer weight;
    public String targetNodeId;

    public String strategy;
    public Integer count;
    public Boolean force;
    public List<String> boundaries = new ArrayList<>();

    public ResponseStatus status;
    public ErrorCode errorCode;
    public String error;
    public String message;

    public List<String> keys = new ArrayList<>();
    public Map<String, String> data = new LinkedHashMap<>();
    public List<ClusterNode> nodes = new ArrayList<>();
    public Map<String, Map<String, String>> clusterData = new LinkedHashMap<>();
    public MigrationReport migration;
    public DistributionStats stats;
    public Map<String, List<Long>> ringTokens = new LinkedHashMap<>();

    public WireMessage() {
    }

    public static WireMessage request(MessageType type) {
        WireMessage message = new WireMessage();
        message.requestId = UUID.randomUUID().toString();
        message.type = type;
        return message;
    }

    public static WireMessage ok(String requestId) {
        WireMessage message = new WireMessage();
        message.requestId = requestId;
        message.type = MessageType.RESPONSE;
        message.status = ResponseStatus.OK;
        return message;
    }

    public static WireMessage error(String requestId, ErrorCode code, String text) {
        WireMessage message = new WireMessage();
        message.requestId = requestId;
        message.type = MessageType.RESPONSE;
        message.status = ResponseStatus.ERROR;
        message.errorCode = code;
        message.error = text;
        return message;
    }

    public boolean isForce() {
        return Boolean.TRUE.equals(force);
    }
}

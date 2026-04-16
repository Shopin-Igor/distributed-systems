package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class ReplPut {
    public UUID operationId;
    public String originNodeId;
    public String sourceNodeId;
    public String operationType;
    public String key;
    public String value;
    public Version version;

    public ReplPut() {}

    public static ReplPut put(UUID opId, String originNodeId, String key, String value) {
        return put(opId, originNodeId, originNodeId, key, value, null);
    }

    public static ReplPut put(UUID opId, String originNodeId, String sourceNodeId, String key, String value, Version version) {
        ReplPut r = new ReplPut();
        r.operationId = opId;
        r.originNodeId = originNodeId;
        r.sourceNodeId = sourceNodeId;
        r.operationType = "PUT";
        r.key = key;
        r.value = value;
        r.version = version;
        return r;
    }

    public static ReplPut delete(java.util.UUID opId, String originNodeId, String key) {
        ReplPut r = new ReplPut();
        r.operationId = opId;
        r.originNodeId = originNodeId;
        r.sourceNodeId = originNodeId;
        r.operationType = "DELETE";
        r.key = key;
        r.value = null;
        return r;
    }

}

package ru.hse.drip.systems.distributed.homework.i2.protocol;

import java.util.UUID;

public class ReplPut {
    public UUID operationId;
    public String originNodeId;
    public String operationType;
    public String key;
    public String value;
    public long version;

    public ReplPut() {}

    public static ReplPut put(UUID opId, String originNodeId, String key, String value, long version) {
        ReplPut r = new ReplPut();
        r.operationId = opId;
        r.originNodeId = originNodeId;
        r.operationType = "PUT";
        r.key = key;
        r.value = value;
        r.version = version;
        return r;
    }

    public static ReplPut delete(java.util.UUID opId, String originNodeId, String key, long version) {
        ReplPut r = new ReplPut();
        r.operationId = opId;
        r.originNodeId = originNodeId;
        r.operationType = "DELETE";
        r.key = key;
        r.value = null;
        r.version = version;
        return r;
    }

}

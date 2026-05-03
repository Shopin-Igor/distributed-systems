package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class HintRecord {
    public String key;
    public String value;
    public Version version;
    public UUID operationId;
    public String intendedHomeNodeId;

    public HintRecord() {}

    public HintRecord(String key, String value, Version version, UUID operationId, String intendedHomeNodeId) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.operationId = operationId;
        this.intendedHomeNodeId = intendedHomeNodeId;
    }

    public String hintId() {
        return operationId + "|" + intendedHomeNodeId + "|" + key;
    }

    public HintRecord copy() {
        return new HintRecord(key, value, version, operationId, intendedHomeNodeId);
    }
}

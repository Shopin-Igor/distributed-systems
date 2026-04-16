package ru.hse.drip.systems.distributed.homework.i4.protocol;

public class MerkleRecord {
    public String key;
    public String value;
    public Version version;
    public String operationId;

    public MerkleRecord() {}

    public MerkleRecord(String key, String value, Version version, String operationId) {
        this.key = key;
        this.value = value;
        this.version = version;
        this.operationId = operationId;
    }
}

package ru.hse.drip.systems.distributed.homework.i4.protocol;

public class VersionedValue {
    public String value;
    public Version version;
    public String operationId;

    public VersionedValue() {}

    public VersionedValue(String value, Version version) {
        this(value, version, null);
    }

    public VersionedValue(String value, Version version, String operationId) {
        this.value = value;
        this.version = version;
        this.operationId = operationId;
    }
}

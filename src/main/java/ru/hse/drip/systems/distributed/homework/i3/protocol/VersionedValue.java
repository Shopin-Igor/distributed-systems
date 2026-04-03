package ru.hse.drip.systems.distributed.homework.i3.protocol;

public class VersionedValue {
    public String value;
    public Version version;

    public VersionedValue() {}

    public VersionedValue(String value, Version version) {
        this.value = value;
        this.version = version;
    }
}

package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class ReplWrite {
    public UUID operationId;
    public String sourceNodeId;
    public String key;
    public String value;
    public Version version;
    public boolean storeAsHint;
    public String intendedHomeNodeId;

    public ReplWrite() {}
}

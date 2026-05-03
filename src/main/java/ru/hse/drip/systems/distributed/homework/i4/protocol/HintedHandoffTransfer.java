package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class HintedHandoffTransfer {
    public UUID operationId;
    public String key;
    public String value;
    public Version version;
    public String sourceNodeId;
    public String intendedHomeNodeId;

    public HintedHandoffTransfer() {}
}

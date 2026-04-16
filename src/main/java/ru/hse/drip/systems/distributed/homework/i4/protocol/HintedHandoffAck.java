package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class HintedHandoffAck {
    public UUID operationId;
    public boolean ok;
    public String fromNodeId;
    public String error;

    public HintedHandoffAck() {}

    public static HintedHandoffAck ok(UUID operationId, String fromNodeId) {
        HintedHandoffAck ack = new HintedHandoffAck();
        ack.operationId = operationId;
        ack.ok = true;
        ack.fromNodeId = fromNodeId;
        return ack;
    }

    public static HintedHandoffAck error(UUID operationId, String fromNodeId, String error) {
        HintedHandoffAck ack = new HintedHandoffAck();
        ack.operationId = operationId;
        ack.ok = false;
        ack.fromNodeId = fromNodeId;
        ack.error = error;
        return ack;
    }
}

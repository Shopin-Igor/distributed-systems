package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class ReplWriteAck {
    public UUID operationId;
    public boolean ok;
    public String fromNodeId;
    public boolean storedAsHint;
    public String error;

    public ReplWriteAck() {}

    public static ReplWriteAck ok(UUID operationId, String fromNodeId, boolean storedAsHint) {
        ReplWriteAck ack = new ReplWriteAck();
        ack.operationId = operationId;
        ack.ok = true;
        ack.fromNodeId = fromNodeId;
        ack.storedAsHint = storedAsHint;
        return ack;
    }

    public static ReplWriteAck error(UUID operationId, String fromNodeId, String error) {
        ReplWriteAck ack = new ReplWriteAck();
        ack.operationId = operationId;
        ack.ok = false;
        ack.fromNodeId = fromNodeId;
        ack.error = error;
        return ack;
    }
}

package ru.hse.drip.systems.distributed.homework.i3.protocol;

import java.util.UUID;

public class ReplAck {
    public UUID operationId;
    public boolean ok;
    public String error;
    public String fromNodeId;

    public ReplAck() {}

    public static ReplAck ok(UUID opId) {
        return ok(opId, null);
    }

    public static ReplAck ok(UUID opId, String fromNodeId) {
        ReplAck a = new ReplAck();
        a.operationId = opId;
        a.ok = true;
        a.fromNodeId = fromNodeId;
        return a;
    }

    public static ReplAck error(UUID opId, String msg) {
        return error(opId, null, msg);
    }

    public static ReplAck error(UUID opId, String fromNodeId, String msg) {
        ReplAck a = new ReplAck();
        a.operationId = opId;
        a.ok = false;
        a.fromNodeId = fromNodeId;
        a.error = msg;
        return a;
    }
}

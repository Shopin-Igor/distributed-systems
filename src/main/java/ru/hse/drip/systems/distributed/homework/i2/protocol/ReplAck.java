package ru.hse.drip.systems.distributed.homework.i2.protocol;

import java.util.UUID;

public class ReplAck {
    public UUID operationId;
    public boolean ok;
    public String error;

    public ReplAck() {}

    public static ReplAck ok(UUID opId) {
        ReplAck a = new ReplAck();
        a.operationId = opId;
        a.ok = true;
        return a;
    }

    public static ReplAck error(UUID opId, String msg) {
        ReplAck a = new ReplAck();
        a.operationId = opId;
        a.ok = false;
        a.error = msg;
        return a;
    }
}

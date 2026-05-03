package ru.hse.drip.systems.distributed.homework.i2.protocol;

import java.util.UUID;

public class ReplicaAck {
    public UUID opId;
    public long lsn;
    public boolean ok;
    public String error;

    public ReplicaAck() {}

    public static ReplicaAck ok(UUID opId, long lsn) {
        ReplicaAck a = new ReplicaAck();
        a.opId = opId;
        a.lsn = lsn;
        a.ok = true;
        return a;
    }

    public static ReplicaAck error(UUID opId, long lsn, String msg) {
        ReplicaAck a = new ReplicaAck();
        a.opId = opId;
        a.lsn = lsn;
        a.ok = false;
        a.error = msg;
        return a;
    }
}

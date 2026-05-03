package ru.hse.drip.systems.distributed.homework.i3.protocol;

import java.util.UUID;

public class ReplicaAppend {
    public UUID opId;
    public long lsn;
    public Mutation mutation;

    public ReplicaAppend() {}

    public static ReplicaAppend of(UUID opId, long lsn, Mutation mutation) {
        ReplicaAppend a = new ReplicaAppend();
        a.opId = opId;
        a.lsn = lsn;
        a.mutation = mutation;
        return a;
    }
}

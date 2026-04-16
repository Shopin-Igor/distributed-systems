package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class MerkleRecordsAck {
    public UUID requestId;
    public String fromNodeId;
    public boolean ok;
    public int appliedRecords;
    public String error;

    public MerkleRecordsAck() {}
}

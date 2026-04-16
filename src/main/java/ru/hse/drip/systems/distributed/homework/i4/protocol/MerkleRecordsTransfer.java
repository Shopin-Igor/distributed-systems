package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.List;
import java.util.UUID;

public class MerkleRecordsTransfer {
    public UUID requestId;
    public String sourceNodeId;
    public List<MerkleRecord> records;

    public MerkleRecordsTransfer() {}
}

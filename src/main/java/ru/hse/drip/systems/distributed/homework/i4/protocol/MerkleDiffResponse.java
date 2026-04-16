package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MerkleDiffResponse {
    public UUID requestId;
    public String fromNodeId;
    public boolean ok;
    public String error;
    public String rootHash;
    public Map<Integer, String> bucketHashes;
    public List<MerkleRecord> records;

    public MerkleDiffResponse() {}
}

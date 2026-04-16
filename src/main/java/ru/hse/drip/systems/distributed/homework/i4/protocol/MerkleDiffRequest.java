package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.List;
import java.util.UUID;

public class MerkleDiffRequest {
    public UUID requestId;
    public List<Integer> bucketIndexes;
    public boolean includeRecords;

    public MerkleDiffRequest() {}
}

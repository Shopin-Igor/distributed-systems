package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class MerkleRootResponse {
    public UUID requestId;
    public String fromNodeId;
    public String rootHash;
    public boolean ok;
    public String error;

    public MerkleRootResponse() {}
}

package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class ReadQuery {
    public UUID requestId;
    public String key;

    public ReadQuery() {}
}

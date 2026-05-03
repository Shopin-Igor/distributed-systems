package ru.hse.drip.systems.distributed.homework.i2.protocol;

public enum ErrorCode {
    NOT_LEADER,
    NOT_ENOUGH_REPLICAS,
    TIMEOUT,
    BAD_REQUEST,
    UNKNOWN_NODE
}

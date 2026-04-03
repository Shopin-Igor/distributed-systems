package ru.hse.drip.systems.distributed.homework.i3.protocol;

public enum ErrorCode {
    NOT_LEADER,
    NOT_LEADER_FOR_WRITE,
    NOT_ENOUGH_REPLICAS,
    TIMEOUT,
    BAD_REQUEST,
    UNKNOWN_NODE,
    INVALID_MODE,
    INVALID_TOPOLOGY,
    INVALID_CONFIG
}

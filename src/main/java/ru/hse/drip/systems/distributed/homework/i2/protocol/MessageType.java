package ru.hse.drip.systems.distributed.homework.i2.protocol;

public enum MessageType {
    CLIENT_REQUEST,
    CLIENT_RESPONSE,
    REPLICA_APPEND,
    REPLICA_ACK,
    JOIN,
    LEAVE,
    SET_LEADER,
    MEMBERS,
    ERROR
}

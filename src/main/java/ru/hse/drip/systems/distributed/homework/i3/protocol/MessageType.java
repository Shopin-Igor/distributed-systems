package ru.hse.drip.systems.distributed.homework.i3.protocol;

public enum MessageType {
    CLIENT_PUT,
    CLIENT_GET,
    CLIENT_DUMP,
    CLIENT_DELETE,

    REPL_PUT,
    REPL_ACK,

    CLUSTER_UPDATE
}

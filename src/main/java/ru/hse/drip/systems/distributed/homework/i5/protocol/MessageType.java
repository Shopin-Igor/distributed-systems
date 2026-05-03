package ru.hse.drip.systems.distributed.homework.i5.protocol;

public enum MessageType {
    CLIENT_PUT,
    CLIENT_GET,
    CLIENT_DELETE,
    CLIENT_GET_RANGE,

    SHARD_PUT,
    SHARD_GET,
    SHARD_DELETE,
    SHARD_GET_KEYS,
    SHARD_DUMP,
    SHARD_GET_RANGE,
    SHARD_CLEAR,

    ADD_NODE,
    REMOVE_NODE,
    SET_STRATEGY,
    SET_VNODES,
    SET_RANGES,
    SET_WEIGHT,
    MIGRATE_DATA,
    CLUSTER_INFO,
    LIST_NODES,
    DUMP,
    CLUSTER_DUMP,
    STATS,
    RING_INFO,

    RESPONSE
}

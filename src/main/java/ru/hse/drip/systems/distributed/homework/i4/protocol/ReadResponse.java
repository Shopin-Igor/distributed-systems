package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class ReadResponse {
    public UUID requestId;
    public String fromNodeId;
    public String key;
    public VersionedValue value;
    public boolean ok;
    public String error;

    public ReadResponse() {}

    public static ReadResponse ok(UUID requestId, String fromNodeId, String key, VersionedValue value) {
        ReadResponse response = new ReadResponse();
        response.requestId = requestId;
        response.fromNodeId = fromNodeId;
        response.key = key;
        response.value = value;
        response.ok = true;
        return response;
    }

    public static ReadResponse error(UUID requestId, String fromNodeId, String key, String error) {
        ReadResponse response = new ReadResponse();
        response.requestId = requestId;
        response.fromNodeId = fromNodeId;
        response.key = key;
        response.ok = false;
        response.error = error;
        return response;
    }
}

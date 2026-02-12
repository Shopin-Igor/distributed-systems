package ru.hse.drip.systems.distributed.homework.i2.protocol;

import java.util.UUID;

public class ClientRequest {
    public UUID requestId;
    public String command;
    public String key;
    public String value;

    public ClientRequest() {}

    public static ClientRequest put(String key, String value) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.command = "put";
        r.key = key;
        r.value = value;
        return r;
    }

    public static ClientRequest get(String key) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.command = "get";
        r.key = key;
        return r;
    }

    public static ClientRequest delete(String key) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.command = "delete";
        r.key = key;
        return r;
    }
}

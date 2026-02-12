package ru.hse.drip.systems.distributed.homework.i2.protocol;

import java.util.UUID;

public class ClientResponse {
    public UUID requestId;
    public Status status;
    public String value;
    public String error;  //  ERROR/TIMEOUT

    public ClientResponse() {}

    public static ClientResponse ok(UUID requestId) {
        ClientResponse r = new ClientResponse();
        r.requestId = requestId;
        r.status = Status.OK;
        return r;
    }

    public static ClientResponse okValue(UUID requestId, String value) {
        ClientResponse r = ok(requestId);
        r.value = value;
        return r;
    }

    public static ClientResponse notFound(UUID requestId) {
        ClientResponse r = new ClientResponse();
        r.requestId = requestId;
        r.status = Status.NOT_FOUND;
        return r;
    }

    public static ClientResponse timeout(UUID requestId, String msg) {
        ClientResponse r = new ClientResponse();
        r.requestId = requestId;
        r.status = Status.TIMEOUT;
        r.error = msg;
        return r;
    }

    public static ClientResponse error(UUID requestId, String msg) {
        ClientResponse r = new ClientResponse();
        r.requestId = requestId;
        r.status = Status.ERROR;
        r.error = msg;
        return r;
    }
}

package ru.hse.drip.systems.distributed.homework.i2.protocol;

import java.util.Map;
import java.util.UUID;

public class ClientResponse {
    public UUID requestId;
    public ResponseStatus status;

    public ErrorCode errorCode;
    public String errorMessage;

    public String leaderNodeId;
    public String leaderHost;
    public Integer leaderPort;

    public String value;
    public Map<String, String> dump;

    public ClientResponse() {}

    public static ClientResponse ok(UUID requestId) {
        ClientResponse r = new ClientResponse();
        r.requestId = requestId;
        r.status = ResponseStatus.OK;
        return r;
    }

    public static ClientResponse okValue(UUID requestId, String value) {
        ClientResponse r = ok(requestId);
        r.value = value;
        return r;
    }

    public static ClientResponse okDump(UUID requestId, Map<String,String> dump) {
        ClientResponse r = ok(requestId);
        r.dump = dump;
        return r;
    }

    public static ClientResponse error(UUID requestId, ErrorCode code, String msg) {
        ClientResponse r = new ClientResponse();
        r.requestId = requestId;
        r.status = ResponseStatus.ERROR;
        r.errorCode = code;
        r.errorMessage = msg;
        return r;
    }

    public static ClientResponse notLeader(UUID requestId, String leaderNodeId, String host, Integer port) {
        ClientResponse r = error(requestId, ErrorCode.NOT_LEADER, "запись разрешена только на лидере");
        r.leaderNodeId = leaderNodeId;
        r.leaderHost = host;
        r.leaderPort = port;
        return r;
    }
}

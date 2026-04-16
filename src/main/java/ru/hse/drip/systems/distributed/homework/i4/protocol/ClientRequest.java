package ru.hse.drip.systems.distributed.homework.i4.protocol;

import java.util.UUID;

public class ClientRequest {
    public UUID requestId;
    public String clientId;
    public MessageType type;
    public String key;
    public String value;

    public ClientRequest() {}

    public static ClientRequest put(String clientId, String key, String value) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_PUT;
        r.key = key;
        r.value = value;
        return r;
    }

    public static ClientRequest get(String clientId, String key) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_GET;
        r.key = key;
        return r;
    }

    public static ClientRequest dump(String clientId) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_DUMP;
        return r;
    }

    public static ClientRequest dumpHints(String clientId) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_DUMP_HINTS;
        return r;
    }

    public static ClientRequest nodeStats(String clientId) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_NODE_STATS;
        return r;
    }

    public static ClientRequest showMerkleRoot(String clientId) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_SHOW_MERKLE_ROOT;
        return r;
    }

    public static ClientRequest runHintedHandoff(String clientId) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_RUN_HINTED_HANDOFF;
        return r;
    }

    public static ClientRequest wipeNodeData(String clientId) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_WIPE_NODE_DATA;
        return r;
    }

    public static ClientRequest runAntiEntropy(String clientId) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_RUN_ANTI_ENTROPY;
        return r;
    }


    public static ClientRequest delete(String clientId, String key) {
        ClientRequest r = new ClientRequest();
        r.requestId = UUID.randomUUID();
        r.clientId = clientId;
        r.type = MessageType.CLIENT_DELETE;
        r.key = key;
        return r;
    }
}

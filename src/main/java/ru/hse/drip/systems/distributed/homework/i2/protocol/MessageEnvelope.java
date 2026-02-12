package ru.hse.drip.systems.distributed.homework.i2.protocol;

import com.fasterxml.jackson.databind.JsonNode;

public class MessageEnvelope {
    public MessageType type;
    public JsonNode payload;

    public MessageEnvelope() {}

    public static MessageEnvelope of(MessageType type, JsonNode payload) {
        MessageEnvelope e = new MessageEnvelope();
        e.type = type;
        e.payload = payload;
        return e;
    }
}

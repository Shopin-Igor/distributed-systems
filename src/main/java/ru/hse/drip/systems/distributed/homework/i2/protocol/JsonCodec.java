package ru.hse.drip.systems.distributed.homework.i2.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonCodec {
    private final ObjectMapper mapper = new ObjectMapper();

    public ObjectMapper mapper() {
        return mapper;
    }

    public String toLine(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("JSON serialize failed", e);
        }
    }

    public <T> T fromLine(String line, Class<T> clazz) {
        try {
            return mapper.readValue(line, clazz);
        } catch (Exception e) {
            throw new RuntimeException("JSON parse failed: " + line, e);
        }
    }

    public JsonNode toTree(Object obj) {
        return mapper.valueToTree(obj);
    }

    public <T> T treeToValue(JsonNode node, Class<T> clazz) {
        try {
            return mapper.treeToValue(node, clazz);
        } catch (Exception e) {
            throw new RuntimeException("JSON treeToValue failed", e);
        }
    }
}

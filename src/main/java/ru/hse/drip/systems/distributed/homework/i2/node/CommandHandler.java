package ru.hse.drip.systems.distributed.homework.i2.node;

import ru.hse.drip.systems.distributed.homework.i2.store.InMemoryKeyValueStore;

import java.util.Map;

public class CommandHandler {
    private final InMemoryKeyValueStore store;

    public CommandHandler(InMemoryKeyValueStore store) {
        this.store = store;
    }

    public void put(String key, String value) {
        store.put(key, value);
    }

    public void put(String key, String value, long version) {
        store.put(key, value, version);
    }

    public String get(String key) {
        return store.get(key);
    }

    public boolean delete(String key) {
        return store.delete(key);
    }

    public boolean delete(String key, long version) {
        return store.delete(key, version);
    }

    public Map<String, String> dump() {
        return store.snapshot();
    }
}

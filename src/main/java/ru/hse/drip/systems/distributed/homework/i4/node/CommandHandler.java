package ru.hse.drip.systems.distributed.homework.i4.node;

import ru.hse.drip.systems.distributed.homework.i4.protocol.VersionedValue;
import ru.hse.drip.systems.distributed.homework.i4.store.InMemoryKeyValueStore;

import java.util.Map;

public class CommandHandler {
    private final InMemoryKeyValueStore store;

    public CommandHandler(InMemoryKeyValueStore store) {
        this.store = store;
    }

    public void put(String key, String value) {
        store.put(key, value);
    }

    public String get(String key) {
        return store.get(key);
    }

    public VersionedValue getVersioned(String key) {
        return store.getVersioned(key);
    }

    public boolean delete(String key) {
        return store.delete(key);
    }

    public Map<String, String> dump() {
        return store.snapshot();
    }

    public void putVersioned(String key, VersionedValue value) {
        store.putVersioned(key, value);
    }

    public Map<String, VersionedValue> dumpVersioned() {
        return store.versionedSnapshot();
    }
}

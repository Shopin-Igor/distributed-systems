package ru.hse.drip.systems.distributed.homework.i3.store;

import ru.hse.drip.systems.distributed.homework.i3.protocol.VersionedValue;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryKeyValueStore implements KeyValueStore {
    private final ConcurrentMap<String, String> data = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VersionedValue> versionedData = new ConcurrentHashMap<>();

    @Override
    public void put(String key, String value) {
        data.put(key, value);
        versionedData.remove(key);
    }

    @Override
    public String get(String key) {
        return data.get(key);
    }

    public VersionedValue getVersioned(String key) {
        VersionedValue value = versionedData.get(key);
        if (value == null) {
            return null;
        }
        return new VersionedValue(value.value, value.version);
    }

    @Override
    public boolean delete(String key) {
        versionedData.remove(key);
        return data.remove(key) != null;
    }

    public void putVersioned(String key, VersionedValue value) {
        versionedData.put(key, value);
        data.put(key, value == null ? null : value.value);
    }

    public java.util.Map<String, String> snapshot() {
        return new HashMap<>(data);
    }

    public java.util.Map<String, VersionedValue> versionedSnapshot() {
        return new HashMap<>(versionedData);
    }

}

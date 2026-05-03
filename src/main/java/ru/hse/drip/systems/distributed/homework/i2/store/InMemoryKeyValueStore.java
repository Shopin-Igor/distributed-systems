package ru.hse.drip.systems.distributed.homework.i2.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryKeyValueStore implements KeyValueStore {
    private final ConcurrentMap<String, VersionedValue> data = new ConcurrentHashMap<>();

    @Override
    public void put(String key, String value) {
        put(key, value, Long.MAX_VALUE);
    }

    @Override
    public void put(String key, String value, long version) {
        data.compute(key, (ignored, current) -> {
            if (current != null && version < current.version) {
                return current;
            }
            return new VersionedValue(value, version, false);
        });
    }

    @Override
    public String get(String key) {
        VersionedValue value = data.get(key);
        if (value == null || value.tombstone) {
            return null;
        }
        return value.value;
    }

    @Override
    public boolean delete(String key) {
        return delete(key, Long.MAX_VALUE);
    }

    @Override
    public boolean delete(String key, long version) {
        final boolean[] changed = {false};
        data.compute(key, (ignored, current) -> {
            if (current != null && version < current.version) {
                return current;
            }
            changed[0] = current == null || !current.tombstone || version > current.version;
            return new VersionedValue(null, version, true);
        });
        return changed[0];
    }

    public java.util.Map<String, String> snapshot() {
        java.util.Map<String, String> snapshot = new java.util.HashMap<>();
        data.forEach((key, value) -> {
            if (!value.tombstone) {
                snapshot.put(key, value.value);
            }
        });
        return snapshot;
    }

    private record VersionedValue(String value, long version, boolean tombstone) {}
}

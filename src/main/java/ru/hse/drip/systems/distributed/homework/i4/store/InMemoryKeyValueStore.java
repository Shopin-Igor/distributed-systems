package ru.hse.drip.systems.distributed.homework.i4.store;

import ru.hse.drip.systems.distributed.homework.i4.protocol.HintRecord;
import ru.hse.drip.systems.distributed.homework.i4.protocol.VersionedValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryKeyValueStore implements KeyValueStore {
    private final ConcurrentMap<String, String> data = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VersionedValue> versionedData = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, HintRecord> hints = new ConcurrentHashMap<>();

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
        return copy(value);
    }

    @Override
    public boolean delete(String key) {
        versionedData.remove(key);
        return data.remove(key) != null;
    }

    public void putVersioned(String key, VersionedValue value) {
        versionedData.put(key, value);
        if (value != null && value.value != null) {
            data.put(key, value.value);
        } else {
            data.remove(key);
        }
    }

    public Map<String, String> snapshot() {
        return new HashMap<>(data);
    }

    public Map<String, VersionedValue> versionedSnapshot() {
        Map<String, VersionedValue> copy = new HashMap<>();
        for (var entry : versionedData.entrySet()) {
            copy.put(entry.getKey(), copy(entry.getValue()));
        }
        return copy;
    }

    public void storeHint(HintRecord hint) {
        hints.put(hint.hintId(), hint.copy());
    }

    public List<HintRecord> hintSnapshot() {
        return hints.values().stream().map(HintRecord::copy).toList();
    }

    public void removeHint(String hintId) {
        hints.remove(hintId);
    }

    public void clearAll() {
        data.clear();
        versionedData.clear();
        hints.clear();
    }

    private VersionedValue copy(VersionedValue value) {
        return new VersionedValue(value.value, value.version, value.operationId);
    }
}

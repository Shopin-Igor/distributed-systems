package ru.hse.drip.systems.distributed.homework.i5.store;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryKeyValueStore implements KeyValueStore {
    private final ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();

    @Override
    public void put(String key, String value) {
        values.put(key, value);
    }

    @Override
    public Optional<String> get(String key) {
        return Optional.ofNullable(values.get(key));
    }

    @Override
    public boolean delete(String key) {
        return values.remove(key) != null;
    }

    @Override
    public Set<String> keys() {
        return new TreeSet<>(values.keySet());
    }

    @Override
    public Map<String, String> snapshot() {
        return new TreeMap<>(values);
    }

    @Override
    public Map<String, String> range(String fromKey, String toKey) {
        Map<String, String> sorted = new TreeMap<>(values);
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            if (entry.getKey().compareTo(fromKey) >= 0 && entry.getKey().compareTo(toKey) < 0) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    @Override
    public void clear() {
        values.clear();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }
}

package ru.hse.drip.systems.distributed.homework.i5.store;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface KeyValueStore {
    void put(String key, String value);

    Optional<String> get(String key);

    boolean delete(String key);

    Set<String> keys();

    Map<String, String> snapshot();

    Map<String, String> range(String fromKey, String toKey);

    void clear();

    boolean isEmpty();
}

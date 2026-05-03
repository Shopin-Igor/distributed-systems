package ru.hse.drip.systems.distributed.homework.i3.store;

public interface KeyValueStore {
    void put(String key, String value);
    String get(String key);
    boolean delete(String key);
}

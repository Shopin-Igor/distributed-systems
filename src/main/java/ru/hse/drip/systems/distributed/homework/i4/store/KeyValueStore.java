package ru.hse.drip.systems.distributed.homework.i4.store;

public interface KeyValueStore {
    void put(String key, String value);
    String get(String key);
    boolean delete(String key);
}

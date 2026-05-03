package ru.hse.drip.systems.distributed.homework.i2.store;

public interface KeyValueStore {
    void put(String key, String value);
    default void put(String key, String value, long version) {
        put(key, value);
    }
    String get(String key);
    boolean delete(String key);
    default boolean delete(String key, long version) {
        return delete(key);
    }
}

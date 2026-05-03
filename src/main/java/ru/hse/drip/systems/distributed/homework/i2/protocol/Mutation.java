package ru.hse.drip.systems.distributed.homework.i2.protocol;

public class Mutation {
    public enum Kind { PUT, DELETE }

    public Kind kind;
    public String key;
    public String value;
    public long version;

    public Mutation() {}

    public static Mutation put(String key, String value, long version) {
        Mutation m = new Mutation();
        m.kind = Kind.PUT;
        m.key = key;
        m.value = value;
        m.version = version;
        return m;
    }

    public static Mutation delete(String key, long version) {
        Mutation m = new Mutation();
        m.kind = Kind.DELETE;
        m.key = key;
        m.value = null;
        m.version = version;
        return m;
    }
}

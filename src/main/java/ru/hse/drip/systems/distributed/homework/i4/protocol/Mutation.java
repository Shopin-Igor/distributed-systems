package ru.hse.drip.systems.distributed.homework.i4.protocol;

public class Mutation {
    public enum Kind { PUT, DELETE }

    public Kind kind;
    public String key;
    public String value;

    public Mutation() {}

    public static Mutation put(String key, String value) {
        Mutation m = new Mutation();
        m.kind = Kind.PUT;
        m.key = key;
        m.value = value;
        return m;
    }

    public static Mutation delete(String key) {
        Mutation m = new Mutation();
        m.kind = Kind.DELETE;
        m.key = key;
        m.value = null;
        return m;
    }
}

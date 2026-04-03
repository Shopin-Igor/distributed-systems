package ru.hse.drip.systems.distributed.homework.i3.store;

import ru.hse.drip.systems.distributed.homework.i3.protocol.Mutation;

public class StoreApplier {
    private final KeyValueStore store;

    public StoreApplier(KeyValueStore store) {
        this.store = store;
    }

    public void apply(Mutation m) {
        if (m == null || m.kind == null) throw new IllegalArgumentException("mutation не задана");
        switch (m.kind) {
            case PUT -> store.put(m.key, m.value);
            case DELETE -> store.delete(m.key);
        }
    }
}

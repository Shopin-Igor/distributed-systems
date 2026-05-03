package ru.hse.drip.systems.distributed.homework.i2.store;

import ru.hse.drip.systems.distributed.homework.i2.protocol.Mutation;

public class StoreApplier {
    private final KeyValueStore store;

    public StoreApplier(KeyValueStore store) {
        this.store = store;
    }

    public void apply(Mutation m) {
        if (m == null || m.kind == null) throw new IllegalArgumentException("мутация не задана");
        switch (m.kind) {
            case PUT -> store.put(m.key, m.value, m.version);
            case DELETE -> store.delete(m.key, m.version);
        }
    }
}

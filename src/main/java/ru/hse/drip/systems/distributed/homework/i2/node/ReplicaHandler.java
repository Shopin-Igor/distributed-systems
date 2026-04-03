package ru.hse.drip.systems.distributed.homework.i2.node;

import ru.hse.drip.systems.distributed.homework.i2.protocol.ReplAck;
import ru.hse.drip.systems.distributed.homework.i2.protocol.ReplPut;
import ru.hse.drip.systems.distributed.homework.i2.store.StoreApplier;

import java.util.Random;

public class ReplicaHandler {
    private final StoreApplier applier;
    private final NodeState state;
    private final SeenOpCache seen;
    private final Random rnd = new Random();

    public ReplicaHandler(StoreApplier applier, NodeState state) {
        this.applier = applier;
        this.state = state;
        this.seen = new SeenOpCache(5 * 60_000L);
    }

    public ReplAck onReplPut(ReplPut put) {
        try {
            if (put == null || put.operationId == null) return ReplAck.error(null, "некорректное сообщение репликации");

            boolean isNew = seen.markIfNew(put.operationId);
            if (!isNew) {
                return ReplAck.ok(put.operationId);
            }

            int min = Math.max(0, state.delayMinMs);
            int max = Math.max(min, state.delayMaxMs);
            if (max > 0) {
                int d = (min == max) ? min : (min + rnd.nextInt(max - min + 1));
                Thread.sleep(d);
            }

            if ("DELETE".equals(put.operationType)) {
                applier.apply(ru.hse.drip.systems.distributed.homework.i2.protocol.Mutation.delete(put.key, put.version));
            } else {
                applier.apply(ru.hse.drip.systems.distributed.homework.i2.protocol.Mutation.put(put.key, put.value, put.version));
            }
            return ReplAck.ok(put.operationId);
        } catch (Exception e) {
            return ReplAck.error(put.operationId, e.getMessage());
        }
    }
}

package ru.hse.drip.systems.distributed.homework.i4.node;

import ru.hse.drip.systems.distributed.homework.i4.protocol.ReplAck;
import ru.hse.drip.systems.distributed.homework.i4.protocol.ReplPut;
import ru.hse.drip.systems.distributed.homework.i4.protocol.Version;
import ru.hse.drip.systems.distributed.homework.i4.protocol.VersionedValue;
import ru.hse.drip.systems.distributed.homework.i4.store.StoreApplier;

public class ReplicaHandler {
    private final String nodeId;
    private final StoreApplier applier;
    private final NodeState state;
    private final SeenOpCache seen;
    private final CommandHandler command;

    public ReplicaHandler(String nodeId, StoreApplier applier, NodeState state, CommandHandler command) {
        this.nodeId = nodeId;
        this.applier = applier;
        this.state = state;
        this.command = command;
        this.seen = new SeenOpCache(5 * 60_000L);
    }

    public boolean markLocalOperation(java.util.UUID operationId) {
        return seen.markIfNew(operationId);
    }

    public ReplicaApplyResult onReplPut(ReplPut put) {
        try {
            if (put == null || put.operationId == null) {
                return ReplicaApplyResult.error(ReplAck.error(null, nodeId, "некорректное сообщение репликации"));
            }

            boolean isNew = seen.markIfNew(put.operationId);
            if (!isNew) {
                return ReplicaApplyResult.duplicate(ReplAck.ok(put.operationId, nodeId));
            }

            if (put.version != null) {
                updateLamport(put.version);
                VersionedValue local = command.getVersioned(put.key);
                VersionedValue incoming = new VersionedValue(put.value, put.version, put.operationId == null ? null : put.operationId.toString());
                if (local == null || compare(incoming.version, local.version) > 0) {
                    command.putVersioned(put.key, incoming);
                }
            } else {
                if ("DELETE".equalsIgnoreCase(put.operationType)) {
                    applier.apply(ru.hse.drip.systems.distributed.homework.i4.protocol.Mutation.delete(put.key));
                } else {
                    applier.apply(ru.hse.drip.systems.distributed.homework.i4.protocol.Mutation.put(put.key, put.value));
                }
            }
            return ReplicaApplyResult.applied(ReplAck.ok(put.operationId, nodeId));
        } catch (Exception e) {
            return ReplicaApplyResult.error(ReplAck.error(put.operationId, nodeId, e.getMessage()));
        }
    }

    private void updateLamport(Version incoming) {
        state.lamportCounter.updateAndGet(current -> Math.max(current, incoming.lamport) + 1);
    }

    private int compare(Version left, Version right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        return left.compareTo(right);
    }

    public static class ReplicaApplyResult {
        public final ReplAck ack;
        public final boolean isNew;

        private ReplicaApplyResult(ReplAck ack, boolean isNew) {
            this.ack = ack;
            this.isNew = isNew;
        }

        public static ReplicaApplyResult applied(ReplAck ack) {
            return new ReplicaApplyResult(ack, true);
        }

        public static ReplicaApplyResult duplicate(ReplAck ack) {
            return new ReplicaApplyResult(ack, false);
        }

        public static ReplicaApplyResult error(ReplAck ack) {
            return new ReplicaApplyResult(ack, false);
        }
    }
}

package ru.hse.drip.systems.distributed.homework.i3.protocol;

public class Version implements Comparable<Version> {
    public long lamport;
    public String nodeId;

    public Version() {}

    public Version(long lamport, String nodeId) {
        this.lamport = lamport;
        this.nodeId = nodeId;
    }

    @Override
    public int compareTo(Version other) {
        if (other == null) {
            return 1;
        }
        int byLamport = Long.compare(this.lamport, other.lamport);
        if (byLamport != 0) {
            return byLamport;
        }
        if (this.nodeId == null && other.nodeId == null) {
            return 0;
        }
        if (this.nodeId == null) {
            return -1;
        }
        if (other.nodeId == null) {
            return 1;
        }
        return this.nodeId.compareTo(other.nodeId);
    }
}

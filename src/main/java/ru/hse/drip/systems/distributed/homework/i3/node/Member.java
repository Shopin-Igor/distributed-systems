package ru.hse.drip.systems.distributed.homework.i3.node;

public class Member {
    public String host;
    public int port;

    public Member() {}

    public Member(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}

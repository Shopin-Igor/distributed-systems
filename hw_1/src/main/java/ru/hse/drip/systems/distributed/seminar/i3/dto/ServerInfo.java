package ru.hse.drip.systems.distributed.seminar.i3.dto;

public class ServerInfo {
    public String serverId;
    public String host;
    public int port;

    @Override
    public String toString() {
        return serverId + "(" + host + ":" + port + ")";
    }
}

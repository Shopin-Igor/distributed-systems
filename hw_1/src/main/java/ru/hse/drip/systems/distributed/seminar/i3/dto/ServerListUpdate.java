package ru.hse.drip.systems.distributed.seminar.i3.dto;

import java.util.List;

public class ServerListUpdate {
    public String type = "SERVER_LIST_UPDATE";
    public List<ServerInfo> servers;

    public ServerListUpdate() {
    }

    public ServerListUpdate(List<ServerInfo> servers) {
        this.servers = servers;
    }
}

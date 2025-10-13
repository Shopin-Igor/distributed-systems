package ru.hse.drip.systems.distributed.seminar.i3.peer;

import ru.hse.drip.systems.distributed.seminar.i3.dto.ServerListUpdate;

public class ServerListUpdateHandler {
    private final PeerManager peerManager;

    public ServerListUpdateHandler(PeerManager peerManager) {
        this.peerManager = peerManager;
    }

    public void handle(ServerListUpdate upd) {
        peerManager.rebuildPeers(upd.servers);
    }
}

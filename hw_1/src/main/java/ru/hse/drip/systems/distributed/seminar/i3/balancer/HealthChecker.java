package ru.hse.drip.systems.distributed.seminar.i3.balancer;

import ru.hse.drip.systems.distributed.seminar.i3.control.BalancerControlServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class HealthChecker implements Runnable {
    private final ConfigService cfg;
    private final AtomicReference<List<InetSocketAddress>> aliveRef;
    private final BalancerControlServer controlServer;
    private List<ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo> lastKnownServers = new ArrayList<>();

    public HealthChecker(ConfigService cfg, AtomicReference<List<InetSocketAddress>> aliveRef, BalancerControlServer controlServer) {
        this.cfg = cfg;
        this.aliveRef = aliveRef;
        this.controlServer = controlServer;
    }

    @Override public void run() {
        Config c = cfg.get();
        if (c == null || c.servers == null) return;

        List<InetSocketAddress> alive = new ArrayList<>();
        List<ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo> liveServers = new ArrayList<>();
        
        for (ru.hse.drip.systems.distributed.seminar.i3.balancer.ServerInfo s : c.servers) {
            try (Socket sock = new Socket()) {
                sock.connect(new InetSocketAddress(s.host, s.port), c.connectTimeoutMs);
                // Читаем ответ от сервера (должен быть "OK")
                try {
                    byte[] buffer = new byte[1024];
                    int bytesRead = sock.getInputStream().read(buffer);
                    if (bytesRead > 0) {
                        String response = new String(buffer, 0, bytesRead).trim();
                        if ("OK".equals(response)) {
                            alive.add(new InetSocketAddress(s.host, s.port));
                            ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo dtoServer = new ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo();
                            dtoServer.serverId = s.serverId;
                            dtoServer.host = s.host;
                            dtoServer.port = s.port;
                            liveServers.add(dtoServer);
                            System.out.println("[health] сервер " + s.serverId + " (" + s.host + ":" + s.port + ") - OK");
                        }
                    }
                } catch (IOException e) {
                    System.out.println("[health] сервер " + s.serverId + " не ответил на health-check: " + e.getMessage());
                }
            } catch (Exception e) {
                System.out.println("[health] сервер " + s.serverId + " недоступен: " + e.getMessage());
            }
        }
        aliveRef.set(alive);
        
        // Отправляем обновления только если список серверов изменился
        if (!serversEqual(lastKnownServers, liveServers)) {
            onHealthResult(liveServers);
            lastKnownServers = new ArrayList<>(liveServers);
        }
        
        System.out.println("[health] живых=" + alive.size());
    }

    private void onHealthResult(List<ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo> liveServers) {
        controlServer.broadcastServerListUpdate(liveServers);
    }
    
    private boolean serversEqual(List<ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo> list1, 
                                 List<ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }
        
        for (int i = 0; i < list1.size(); i++) {
            ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo s1 = list1.get(i);
            ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo s2 = list2.get(i);
            
            if (!s1.serverId.equals(s2.serverId) || 
                !s1.host.equals(s2.host) || 
                s1.port != s2.port) {
                return false;
            }
        }
        
        return true;
    }
}

package ru.hse.drip.systems.distributed.seminar.i3.peer;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ChatMessage;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PeerManager {
    private final ObjectMapper mapper = new ObjectMapper();
    private final String selfId;
    private final Map<String, PeerConnection> peers = new ConcurrentHashMap<>();

    public PeerManager(String selfId) {
        this.selfId = selfId;
    }

    public void rebuildPeers(List<ServerInfo> live) {
        Set<String> target = new HashSet<>();
        for (ServerInfo s : live) {
            if (s.serverId != null && s.serverId.equals(selfId)) {
                continue;
            }
            String key = s.host + ":" + s.port;
            target.add(key);
        }
        
        // Проверяем, изменился ли список серверов
        Set<String> current = new HashSet<>(peers.keySet());
        if (target.equals(current)) {
            // Список не изменился, ничего не делаем
            return;
        }
        
        System.out.println("[peer-manager] обновляем список серверов: " + live.size() + " серверов");
        for (ServerInfo s : live) {
            System.out.println("[peer-manager] сервер: " + s.serverId + " " + s.host + ":" + s.port);
        }
        
        for (ServerInfo s : live) {
            if (s.serverId != null && s.serverId.equals(selfId)) {
                System.out.println("[peer-manager] пропускаем себя: " + s.serverId);
                continue;
            }
            String key = s.host + ":" + s.port;
            System.out.println("[peer-manager] добавляем peer: " + key);
            peers.computeIfAbsent(key, k -> PeerConnection.connect(s));
        }
        for (String k : new ArrayList<>(peers.keySet())) {
            if (!target.contains(k)) {
                System.out.println("[peer-manager] удаляем peer: " + k);
                PeerConnection pc = peers.remove(k);
                if (pc != null) pc.close();
            }
        }
        System.out.println("[peer-manager] итого peer соединений: " + peers.size());
    }

    public void broadcastChat(ChatMessage msg) {
        System.out.println("[peer] отправляем сообщение: username='" + msg.username + "', content='" + msg.content + "', serverId=" + msg.serverId);
        String line;
        try {
            line = mapper.writeValueAsString(msg) + "\n";
            System.out.println("[peer] сериализовано: " + line.trim());
        } catch (Exception e) {
            System.err.println("[peer] ошибка сериализации: " + e.getMessage());
            e.printStackTrace();
            return;
        }
        for (PeerConnection pc : peers.values()) {
            pc.sendLine(line);
        }
    }

    public Collection<PeerConnection> all() { return peers.values(); }
}

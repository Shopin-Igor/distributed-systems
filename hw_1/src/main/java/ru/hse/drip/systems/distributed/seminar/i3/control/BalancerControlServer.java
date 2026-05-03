package ru.hse.drip.systems.distributed.seminar.i3.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ServerListUpdate;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BalancerControlServer {
    private final int controlPort;
    private final ObjectMapper mapper = new ObjectMapper();

    private final Set<Writer> subscribers = ConcurrentHashMap.newKeySet();

    public BalancerControlServer(int controlPort) {
        this.controlPort = controlPort;
    }

    public void startAsync() {
        Thread t = new Thread(this::acceptLoop, "control-accept");
        t.setDaemon(true);
        t.start();
    }

    private void acceptLoop() {
        try (ServerSocket ss = new ServerSocket(controlPort)) {
            System.out.println("[балансировщик] слушает на " + controlPort);
            while (true) {
                Socket s = ss.accept();
                System.out.println("[балансировщик] сервер подключился: " + s.getRemoteSocketAddress());
                Writer w = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
                subscribers.add(w);

                Thread reader = new Thread(() -> {
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
                        while (br.readLine() != null) {}
                    } catch (IOException ignore) {}
                    subscribers.remove(w);
                    try { s.close(); } catch (IOException ignore) {}
                    System.out.println("[балансировщик] сервер отключился: " + s.getRemoteSocketAddress());
                }, "control-peer-" + s.getPort());
                reader.setDaemon(true);
                reader.start();
            }
        } catch (IOException e) {
            System.err.println("[балансировщик] ошибка accept: " + e.getMessage());
        }
    }

    public void broadcastServerListUpdate(List<ServerInfo> liveServers) {
        String json;
        try {
            json = mapper.writeValueAsString(new ServerListUpdate(liveServers)) + "\n";
        } catch (Exception e) {
            System.err.println("[балансировщик] ошибка сериализации: " + e.getMessage());
            return;
        }

        subscribers.removeIf(w -> {
            try {
                w.write(json);
                w.flush();
                return false;
            } catch (IOException e) {
                try { w.close(); } catch (IOException ignore) {}
                return true;
            }
        });
        System.out.println("[балансировщик] разослано обновление " + subscribers.size() + " серверам");
    }
}

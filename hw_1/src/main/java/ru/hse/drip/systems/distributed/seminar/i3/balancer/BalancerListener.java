package ru.hse.drip.systems.distributed.seminar.i3.balancer;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ServerListUpdate;
import ru.hse.drip.systems.distributed.seminar.i3.peer.ServerListUpdateHandler;

import java.io.*;
import java.net.Socket;

public class BalancerListener implements Runnable {
    private final Socket socket;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ServerListUpdateHandler updateHandler;

    public BalancerListener(Socket socket, ServerListUpdateHandler updateHandler) {
        this.socket = socket;
        this.updateHandler = updateHandler;
    }

    @Override
    public void run() {
        System.out.println("[listener] подключен к балансировщику " + socket.getRemoteSocketAddress());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("[listener] получено сообщение от балансера: " + line);
                handleMessage(line);
            }
        } catch (IOException e) {
            System.err.println("[listener] соединение потеряно: " + e.getMessage());
        }
    }

    private void handleMessage(String json) {
        try {
            ServerListUpdate update = mapper.readValue(json, ServerListUpdate.class);
            if ("SERVER_LIST_UPDATE".equals(update.type)) {
                handleServerListUpdate(update);
            } else {
                System.out.println("[listener] неизвестный тип сообщения: " + update.type);
            }
        } catch (Exception e) {
            System.err.println("[listener] ошибка парсинга сообщения: " + json + " (" + e.getMessage() + ")");
        }
    }

    private void handleServerListUpdate(ServerListUpdate update) {
        System.out.println("[listener] получен новый список серверов:");
        for (ServerInfo info : update.servers) {
            System.out.println(" → " + info);
        }
        updateHandler.handle(update);
    }
}

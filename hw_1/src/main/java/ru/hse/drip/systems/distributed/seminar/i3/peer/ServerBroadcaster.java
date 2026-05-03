package ru.hse.drip.systems.distributed.seminar.i3.peer;

import ru.hse.drip.systems.distributed.seminar.i3.dto.ChatMessage;
import ru.hse.drip.systems.distributed.seminar.i3.ClientHandler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ServerBroadcaster {
    private final List<ClientHandler> clients = new CopyOnWriteArrayList<>();

    public void addClient(ClientHandler client) {
        clients.add(client);
    }

    public void removeClient(ClientHandler client) {
        clients.remove(client);
    }

    public void broadcastToLocalClients(ChatMessage msg) {
        System.out.println("[broadcaster] отправляем клиентам: username='" + msg.username + "', content='" + msg.content + "'");
        
        // Отправляем просто содержимое сообщения
        String message = msg.content != null ? msg.content : "";
        
        System.out.println("[broadcaster] сформированное сообщение: " + message);
        for (ClientHandler client : clients) {
            client.sendMessage(message);
        }
    }
}

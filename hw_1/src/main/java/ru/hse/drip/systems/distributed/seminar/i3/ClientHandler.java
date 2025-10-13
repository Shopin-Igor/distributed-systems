package ru.hse.drip.systems.distributed.seminar.i3;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ChatMessage;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ClientMessage;
import ru.hse.drip.systems.distributed.seminar.i3.peer.PeerManager;
import ru.hse.drip.systems.distributed.seminar.i3.peer.ServerBroadcaster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.UUID;

public class ClientHandler implements Runnable {

    private final Socket socket;
    private BufferedReader in;
    private BufferedWriter out;
    private final String serverId;
    private final PeerManager peers;
    private final ServerBroadcaster broadcaster;

    public ClientHandler(Socket socket, String serverId, PeerManager peers, ServerBroadcaster broadcaster) {
        this.socket = socket;
        this.serverId = serverId;
        this.peers = peers;
        this.broadcaster = broadcaster;
    }

    @Override
    public void run() {
        try {
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            broadcaster.addClient(this);

            String msg;
            while ((msg = in.readLine()) != null) {
                System.out.println("Сервер получил: " + msg);
                handleClientMessage(msg);
            }
        } catch (IOException e) {
            System.out.println("клиент отключился");
        } finally {
            broadcaster.removeClient(this);
        }
    }

    private void handleClientMessage(String message) {
        try {
            System.out.println("[handler] получено сообщение от клиента: '" + message + "'");
            
            // Вся строка - это сообщение, username генерируем автоматически
            String username = "user_" + System.currentTimeMillis() % 10000;
            String content = message.trim();
            
            ClientMessage cm = new ClientMessage();
            cm.type = "CLIENT_MESSAGE";
            cm.username = username;
            cm.content = content;
            cm.timestamp = System.currentTimeMillis();
            
            onClientMessage(cm);
        } catch (Exception e) {
            System.err.println("Ошибка обработки сообщения клиента: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void onClientMessage(ClientMessage cm) {
        System.out.println("[handler] получено сообщение от клиента: username='" + cm.username + "', content='" + cm.content + "'");
        
        ChatMessage msg = new ChatMessage();
        msg.type = "CHAT_MESSAGE";
        msg.messageId = UUID.randomUUID().toString();
        msg.serverId = serverId;
        msg.username = cm.username;
        msg.content = cm.content;
        msg.timestamp = cm.timestamp;

        System.out.println("[handler] создано ChatMessage: username='" + msg.username + "', content='" + msg.content + "', serverId=" + msg.serverId);
        
        broadcaster.broadcastToLocalClients(msg);
        peers.broadcastChat(msg);
    }

    public void sendMessage(String message) {
        try {
            out.write(message + "\n");
            out.flush();
        } catch (IOException e) {
        }
    }

}

package ru.hse.drip.systems.distributed.seminar.i3.peer;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.hse.drip.systems.distributed.seminar.i3.dto.ChatMessage;

public class PeerMessageBus {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DedupStore dedup = new DedupStore();
    static { DedupStore.startCleaner(dedup); }

    public static ServerBroadcaster localBroadcaster;
    public static PeerManager peerManager;

    public static void onIncomingLine(String line) {
        try {
            System.out.println("[bus] получена строка: " + line);
            
            // Игнорируем health-check ответы
            if ("OK".equals(line.trim())) {
                System.out.println("[bus] получен health-check ответ, пропускаем");
                return;
            }
            
            ChatMessage msg = mapper.readValue(line, ChatMessage.class);
            System.out.println("[bus] распарсено: type=" + msg.type + ", username='" + msg.username + "', content='" + msg.content + "', serverId=" + msg.serverId);
            
            if (!"CHAT_MESSAGE".equals(msg.type)) {
                System.out.println("[bus] не CHAT_MESSAGE, пропускаем");
                return;
            }

            if (dedup.firstTimeSeen(msg.messageId)) {
                System.out.println("[bus] новое сообщение, отправляем локальным клиентам");
                localBroadcaster.broadcastToLocalClients(msg);
            } else {
                System.out.println("[bus] дубликат, пропускаем");
            }
        } catch (Exception e) {
            System.err.println("[bus] ошибка парсинга: " + e.getMessage());
            System.err.println("[bus] проблемная строка: " + line);
            e.printStackTrace();
        }
    }
}

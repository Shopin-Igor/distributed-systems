package ru.hse.drip.systems.distributed.seminar.i3;

import ru.hse.drip.systems.distributed.seminar.i3.balancer.BalancerListener;
import ru.hse.drip.systems.distributed.seminar.i3.peer.PeerManager;
import ru.hse.drip.systems.distributed.seminar.i3.peer.PeerMessageBus;
import ru.hse.drip.systems.distributed.seminar.i3.peer.ServerBroadcaster;
import ru.hse.drip.systems.distributed.seminar.i3.peer.ServerListUpdateHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Set;
import java.util.HashSet;

public class ChatServerMain {
    private static final int SERVER_PORT = 9001;
    private static final String SERVER_ID = "A";
    
    // Множество известных peer адресов
    private static final Set<String> knownPeerAddresses = new HashSet<>();

    public static void main(String[] args) throws Exception {
        String serverId = args.length > 0 ? args[0] : SERVER_ID;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : SERVER_PORT;

        PeerManager peerManager = new PeerManager(serverId);
        ServerBroadcaster broadcaster = new ServerBroadcaster();
        ServerListUpdateHandler updateHandler = new ServerListUpdateHandler(peerManager);

        PeerMessageBus.localBroadcaster = broadcaster;
        PeerMessageBus.peerManager = peerManager;

        // Подключаемся к балансеру с retry логикой
        Socket controlSocket = null;
        int retries = 0;
        while (controlSocket == null && retries < 20) {
            try {
                controlSocket = new Socket("localhost", 9090);
                System.out.println("Подключились к балансеру на порту 9090");
            } catch (IOException e) {
                retries++;
                System.out.println("Не удалось подключиться к балансеру, попытка " + retries + "/20");
                try {
                    Thread.sleep(2000); // Увеличиваем время ожидания
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        if (controlSocket != null) {
            new Thread(new BalancerListener(controlSocket, updateHandler), "balancer-listener").start();
        } else {
            System.err.println("Не удалось подключиться к балансеру после 20 попыток");
        }

        System.out.println("Сервер запущен на порту " + port);
        ServerSocket serverSocket = new ServerSocket(port);
        ExecutorService executor = Executors.newFixedThreadPool(4);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            String remoteAddr = clientSocket.getRemoteSocketAddress().toString();
            
            // Проверяем, является ли это health-check от балансировщика
            if (isHealthCheck(clientSocket)) {
                System.out.println("Health-check от балансировщика: " + remoteAddr);
                // Отвечаем на health-check и закрываем соединение
                try {
                    clientSocket.getOutputStream().write("OK\n".getBytes());
                    clientSocket.getOutputStream().flush();
                } catch (IOException e) {
                    // Игнорируем ошибки
                }
                clientSocket.close();
            } else if (isKnownPeer(remoteAddr)) {
                System.out.println("Peer соединение от: " + remoteAddr);
                // Peer соединения обрабатываются через PeerConnection, не создаем ClientHandler
                // Просто закрываем соединение, так как peer уже подключен через PeerConnection
                clientSocket.close();
            } else {
                System.out.println("Подключился клиент: " + remoteAddr);
                var handler = new ClientHandler(clientSocket, serverId, peerManager, broadcaster);
                executor.submit(handler);
            }
        }
    }
    
    // Проверяем, является ли это health-check от балансировщика
    private static boolean isHealthCheck(Socket clientSocket) {
        try {
            // Health-check соединения приходят от балансировщика и сразу закрываются
            // Проверяем, что соединение от localhost и нет данных для чтения
            String remoteAddr = clientSocket.getRemoteSocketAddress().toString();
            boolean isLocalhost = remoteAddr.startsWith("/127.0.0.1:") || remoteAddr.startsWith("/localhost:");
            
            if (isLocalhost) {
                // Проверяем, есть ли данные для чтения
                clientSocket.setSoTimeout(100); // 100ms timeout
                try {
                    int available = clientSocket.getInputStream().available();
                    if (available == 0) {
                        // Нет данных - это health-check
                        return true;
                    }
                } catch (IOException e) {
                    // Если не можем прочитать - считаем health-check
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }
    
    // Проверяем, является ли адрес известным peer'ом
    private static boolean isKnownPeer(String remoteAddr) {
        // Известные порты серверов
        return remoteAddr.contains(":9001") || 
               remoteAddr.contains(":9002") || 
               remoteAddr.contains(":9003");
    }
    
    // Метод для добавления peer адресов (может быть вызван из PeerManager)
    public static void addPeerAddress(String address) {
        knownPeerAddresses.add(address);
    }
}

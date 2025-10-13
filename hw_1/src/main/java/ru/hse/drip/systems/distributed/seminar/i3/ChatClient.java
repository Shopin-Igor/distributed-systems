package ru.hse.drip.systems.distributed.seminar.i3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ChatClient {

    private static final int SERVER_PORT = 9999;
    private static final int RECONNECT_DELAY_MS = 10000;
    
    private static final ConcurrentLinkedQueue<String> pendingMessages = new ConcurrentLinkedQueue<>();
    private static final AtomicBoolean connected = new AtomicBoolean(false);
    private static final AtomicBoolean shouldReconnect = new AtomicBoolean(true);
    private static final AtomicReference<BufferedWriter> currentWriter = new AtomicReference<>();

    public static void main(String[] args) {
        Thread connectionThread = new Thread(ChatClient::connectionLoop, "connection-thread");
        connectionThread.setDaemon(true);
        connectionThread.start();

        Thread inputThread = new Thread(ChatClient::handleUserInput, "input-thread");
        inputThread.setDaemon(true);
        inputThread.start();

        try {
            inputThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            shouldReconnect.set(false);
        }
    }

    private static void connectionLoop() {
        while (shouldReconnect.get()) {
            try {
                connectAndRead();
            } catch (Exception e) {
                System.out.println("Соединение потеряно. Переподключение через 10 секунд...");
                try {
                    Thread.sleep(RECONNECT_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void connectAndRead() throws IOException {
        Socket socket = new Socket("localhost", SERVER_PORT);
        System.out.println("Подключен к серверу на порту: " + SERVER_PORT);

        var out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        var in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        currentWriter.set(out);
        connected.set(true);
        sendPendingMessages(out);

        String msg;
        while ((msg = in.readLine()) != null && shouldReconnect.get()) {
            if (msg.contains("Нет доступных серверов")) {
                System.out.println("Нет доступных серверов. Повтор через 10 секунд.");
                break;
            }
            // Игнорируем health-check ответы
            if ("OK".equals(msg.trim())) {
                continue;
            }
            System.out.println("<< " + msg);
        }

        connected.set(false);
        currentWriter.set(null);
        socket.close();
    }

    private static void sendPendingMessages(BufferedWriter out) throws IOException {
        while (!pendingMessages.isEmpty()) {
            String message = pendingMessages.poll();
            out.write(message + "\n");
            out.flush();
        }
    }

    private static void handleUserInput() {
        var console = new BufferedReader(new InputStreamReader(System.in));
        String userMsg;
        try {
            while ((userMsg = console.readLine()) != null && shouldReconnect.get()) {
                if (connected.get()) {
                    sendMessageImmediately(userMsg);
                } else {
                    pendingMessages.offer(userMsg);
                    System.out.println("Сообщение сохранено. Будет отправлено после восстановления соединения.");
                }
            }
        } catch (IOException e) {
            System.err.println("Ошибка чтения ввода: " + e.getMessage());
        }
    }

    private static void sendMessageImmediately(String message) {
        BufferedWriter writer = currentWriter.get();
        if (writer != null) {
            try {
                writer.write(message + "\n");
                writer.flush();
            } catch (IOException e) {
                pendingMessages.offer(message);
            }
        } else {
            pendingMessages.offer(message);
        }
    }

}

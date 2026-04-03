package ru.hse.drip.systems.distributed.homework.i3.transport;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class TcpServer implements AutoCloseable {
    private final int port;
    private final ExecutorService pool;
    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public TcpServer(int port, int threads) {
        this.port = port;
        this.pool = Executors.newFixedThreadPool(Math.max(1, threads));
    }

    public void start(Consumer<LineConnection> handler) throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;

        System.out.println("[TcpServer] запущен на порту " + port);

        pool.submit(() -> {
            while (running) {
                try {
                    Socket s = serverSocket.accept();
                    System.out.println("[TcpServer] принято соединение " + s.getRemoteSocketAddress());

                    pool.submit(() -> {
                        try (LineConnection c = new LineConnection(s)) {
                            handler.accept(c);
                            System.out.println("[TcpServer] обработка завершена " + s.getRemoteSocketAddress());
                        } catch (Exception e) {
                            System.out.println("[TcpServer] ошибка обработчика для " + s.getRemoteSocketAddress()
                                    + " : " + e.getClass().getSimpleName() + " - " + e.getMessage());
                            e.printStackTrace(System.out);
                        }
                    });

                } catch (IOException e) {
                    if (running) {
                        System.out.println("[TcpServer] ошибка accept на порту " + port
                                + " : " + e.getClass().getSimpleName() + " - " + e.getMessage());
                        e.printStackTrace(System.out);
                    } else {
                        System.out.println("[TcpServer] остановлен");
                    }
                }
            }
        });
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (serverSocket != null) serverSocket.close();
        pool.shutdownNow();
    }
}

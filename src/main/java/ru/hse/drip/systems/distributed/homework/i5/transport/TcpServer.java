package ru.hse.drip.systems.distributed.homework.i5.transport;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class TcpServer implements AutoCloseable {
    private final int port;
    private final ExecutorService pool;
    private volatile boolean running;
    private ServerSocket serverSocket;

    public TcpServer(int port, int threads) {
        this.port = port;
        this.pool = Executors.newFixedThreadPool(Math.max(1, threads));
    }

    public void start(Consumer<LineConnection> handler) throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;

        pool.submit(() -> acceptLoop(handler));
    }

    private void acceptLoop(Consumer<LineConnection> handler) {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                pool.submit(() -> handle(socket, handler));
            } catch (IOException e) {
                if (running) {
                    System.out.println("[TcpServer] accept failed on port " + port + ": " + e.getMessage());
                }
            }
        }
    }

    private void handle(Socket socket, Consumer<LineConnection> handler) {
        try (LineConnection connection = new LineConnection(socket)) {
            handler.accept(connection);
        } catch (Exception e) {
            System.out.println("[TcpServer] connection failed: " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        pool.shutdownNow();
    }
}

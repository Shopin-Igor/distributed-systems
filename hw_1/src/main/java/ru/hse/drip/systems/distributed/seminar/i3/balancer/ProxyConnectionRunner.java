package ru.hse.drip.systems.distributed.seminar.i3.balancer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

class ProxyConnectionRunner implements Runnable {
    private final Socket clientSocket;
    private final InetSocketAddress backend;

    public ProxyConnectionRunner(Socket clientSocket, InetSocketAddress backend) {
        this.clientSocket = clientSocket;
        this.backend = backend;
    }

    @Override
    public void run() {
        System.out.println("[proxy] Проксирование соединения к " + backend);
        try (Socket backendSocket = new Socket()) {
            backendSocket.connect(backend);
            System.out.println("[proxy] Подключились к серверу " + backend);

            Thread forwardToBackend = new Thread(() -> forward(clientSocket, backendSocket, "client->server"));
            Thread forwardToClient = new Thread(() -> forward(backendSocket, clientSocket, "server->client"));

            forwardToBackend.start();
            forwardToClient.start();

            forwardToBackend.join();
            forwardToClient.join();
            
            System.out.println("[proxy] Соединение с " + backend + " завершено");

        } catch (Exception e) {
            System.out.println("[proxy] Ошибка проксирования к " + backend + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void forward(Socket from, Socket to, String direction) {
        try (InputStream in = from.getInputStream();
             OutputStream out = to.getOutputStream()) {

            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                out.flush();
                System.out.println("[proxy] " + direction + " передано " + bytesRead + " байт");
            }
        } catch (IOException e) {
            System.out.println("[proxy] Ошибка передачи данных " + direction + ": " + e.getMessage());
        }
    }
}

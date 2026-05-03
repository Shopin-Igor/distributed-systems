package ru.hse.drip.systems.distributed.homework.i5.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class TcpClient {
    public String callLine(String host, int port, String line, int timeoutMs) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), timeoutMs);
        socket.setSoTimeout(timeoutMs);

        try (LineConnection connection = new LineConnection(socket)) {
            connection.writeLine(line);
            return connection.readLine();
        } finally {
            socket.close();
        }
    }

    public boolean canConnect(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }
}

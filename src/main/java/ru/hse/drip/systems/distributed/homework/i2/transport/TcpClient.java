package ru.hse.drip.systems.distributed.homework.i2.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class TcpClient {
    public String callLine(String host, int port, String line, int timeoutMs) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port), timeoutMs);
        s.setSoTimeout(timeoutMs);

        try (LineConnection c = new LineConnection(s)) {
            c.writeLine(line);
            return c.readLine();
        } finally {
            s.close();
        }
    }
}

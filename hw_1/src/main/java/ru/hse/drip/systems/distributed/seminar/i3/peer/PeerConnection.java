package ru.hse.drip.systems.distributed.seminar.i3.peer;

import ru.hse.drip.systems.distributed.seminar.i3.dto.ServerInfo;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

class PeerConnection {
    private final Socket socket;
    private final BufferedWriter out;
    private final Thread readerThread;

    private PeerConnection(Socket socket, BufferedWriter out, Thread readerThread) {
        this.socket = socket;
        this.out = out;
        this.readerThread = readerThread;
    }

    public static PeerConnection connect(ServerInfo s) {
        try {
            System.out.println("[peer] пытаемся подключиться к " + s.serverId + " (" + s.host + ":" + s.port + ")");
            Socket sock = new Socket();
            sock.connect(new InetSocketAddress(s.host, s.port), 1000);
            sock.setTcpNoDelay(true);
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
            BufferedReader in  = new BufferedReader(new InputStreamReader(sock.getInputStream()));

            Thread reader = new Thread(() -> readLoop(in), "peer-read-" + s.serverId);
            reader.setDaemon(true);
            reader.start();

            System.out.println("[peer] успешно подключен к " + s.serverId + " (" + s.host + ":" + s.port + ")");
            return new PeerConnection(sock, out, reader);
        } catch (IOException e) {
            System.err.println("[peer] ошибка подключения " + s.serverId + ": " + e.getMessage());
            return new PeerConnection(null, null, null);
        }
    }

    private static void readLoop(BufferedReader in) {
        String line;
        try {
            while ((line = in.readLine()) != null) {
                System.out.println("[peer-connection] получена строка: " + line);
                PeerMessageBus.onIncomingLine(line);
            }
        } catch (IOException ignore) {
        }
    }

    public void sendLine(String line) {
        if (out == null) return;
        try {
            out.write(line);
            out.flush();
        } catch (IOException e) {
        }
    }

    public void close() {
        try { if (socket != null) socket.close(); } catch (IOException ignore) {}
    }
}

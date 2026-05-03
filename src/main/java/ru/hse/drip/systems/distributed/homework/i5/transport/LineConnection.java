package ru.hse.drip.systems.distributed.homework.i5.transport;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class LineConnection implements Closeable {
    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;

    public LineConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
    }

    public String readLine() throws IOException {
        return in.readLine();
    }

    public void writeLine(String line) {
        out.println(line);
    }

    public Socket socket() {
        return socket;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}

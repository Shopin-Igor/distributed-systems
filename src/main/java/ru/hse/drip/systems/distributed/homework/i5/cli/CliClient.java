package ru.hse.drip.systems.distributed.homework.i5.cli;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ErrorCode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpClient;

import java.io.IOException;

public class CliClient {
    private final String routerHost;
    private final int routerPort;
    private final JsonCodec codec;
    private final TcpClient tcpClient;
    private final int timeoutMs;

    public CliClient(String routerHost, int routerPort, JsonCodec codec, TcpClient tcpClient) {
        this(routerHost, routerPort, codec, tcpClient, 120_000);
    }

    public CliClient(String routerHost, int routerPort, JsonCodec codec, TcpClient tcpClient, int timeoutMs) {
        this.routerHost = routerHost;
        this.routerPort = routerPort;
        this.codec = codec;
        this.tcpClient = tcpClient;
        this.timeoutMs = timeoutMs;
    }

    public WireMessage send(WireMessage request) {
        try {
            String line = tcpClient.callLine(routerHost, routerPort, codec.toLine(request), timeoutMs);
            return codec.fromLine(line, WireMessage.class);
        } catch (IOException e) {
            return WireMessage.error(request.requestId, ErrorCode.NODE_UNAVAILABLE,
                    "router unavailable: " + routerHost + ":" + routerPort);
        }
    }
}

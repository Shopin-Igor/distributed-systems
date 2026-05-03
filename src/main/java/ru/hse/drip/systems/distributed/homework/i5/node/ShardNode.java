package ru.hse.drip.systems.distributed.homework.i5.node;

import ru.hse.drip.systems.distributed.homework.i5.protocol.ErrorCode;
import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;
import ru.hse.drip.systems.distributed.homework.i5.transport.LineConnection;

import java.io.IOException;

public class ShardNode {
    private final JsonCodec codec;
    private final ShardCommandHandler commandHandler;

    public ShardNode(JsonCodec codec, ShardCommandHandler commandHandler) {
        this.codec = codec;
        this.commandHandler = commandHandler;
    }

    public void handleConnection(LineConnection connection) {
        try {
            String line = connection.readLine();
            if (line == null) {
                return;
            }

            WireMessage request = codec.fromLine(line, WireMessage.class);
            WireMessage response = commandHandler.handle(request);
            connection.writeLine(codec.toLine(response));
        } catch (Exception e) {
            WireMessage response = WireMessage.error(null, ErrorCode.BAD_REQUEST, e.getMessage());
            connection.writeLine(codec.toLine(response));
        }
    }
}

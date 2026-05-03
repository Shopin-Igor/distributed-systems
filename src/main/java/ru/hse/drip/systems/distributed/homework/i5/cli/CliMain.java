package ru.hse.drip.systems.distributed.homework.i5.cli;

import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpClient;

public final class CliMain {
    private CliMain() {
    }

    public static void main(String[] args) {
        run(args);
    }

    public static void run(String[] args) {
        int offset = args.length > 0 && "cli".equalsIgnoreCase(args[0]) ? 1 : 0;
        String host = args.length > offset ? args[offset] : "127.0.0.1";
        int port = args.length > offset + 1 ? Integer.parseInt(args[offset + 1]) : 9200;

        JsonCodec codec = new JsonCodec();
        CliClient client = new CliClient(host, port, codec, new TcpClient());
        InteractiveShell shell = new InteractiveShell(new CliParser(), client, codec);

        System.out.println("[CLI] connected to router " + host + ":" + port);
        shell.run();
    }
}

package ru.hse.drip.systems.distributed.homework.i5.tools;

import ru.hse.drip.systems.distributed.homework.i5.cli.CliClient;
import ru.hse.drip.systems.distributed.homework.i5.protocol.JsonCodec;
import ru.hse.drip.systems.distributed.homework.i5.protocol.MessageType;
import ru.hse.drip.systems.distributed.homework.i5.protocol.ResponseStatus;
import ru.hse.drip.systems.distributed.homework.i5.protocol.WireMessage;
import ru.hse.drip.systems.distributed.homework.i5.transport.TcpClient;

import java.util.Locale;

public final class LoadKeysMain {
    private LoadKeysMain() {
    }

    public static void main(String[] args) {
        run(args);
    }

    public static void run(String[] args) {
        LoadKeysOptions options = LoadKeysOptions.parse(args);
        CliClient client = new CliClient(
                options.routerHost,
                options.routerPort,
                new JsonCodec(),
                new TcpClient(),
                options.timeoutMs
        );

        long startedAt = System.nanoTime();
        for (int offset = 0; offset < options.count; offset++) {
            int number = options.start + offset;
            WireMessage response = client.send(put(options.key(number), options.value(number)));
            if (response.status != ResponseStatus.OK) {
                throw new IllegalStateException("PUT failed for " + options.key(number)
                        + ": " + response.errorCode + " " + response.error);
            }

            int done = offset + 1;
            if (options.reportEvery > 0 && (done % options.reportEvery == 0 || done == options.count)) {
                System.out.println("[LoadKeys] " + done + "/" + options.count + " keys written");
            }
        }

        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        double throughput = options.count / Math.max(elapsedSeconds, 0.001);
        System.out.printf(Locale.ROOT,
                "[LoadKeys] done: count=%d, start=%d, keyPrefix=%s, throughput=%.2f keys/sec%n",
                options.count,
                options.start,
                options.keyPrefix,
                throughput
        );
    }

    private static WireMessage put(String key, String value) {
        WireMessage request = WireMessage.request(MessageType.CLIENT_PUT);
        request.key = key;
        request.value = value;
        return request;
    }

    private static class LoadKeysOptions {
        private String routerHost = "127.0.0.1";
        private int routerPort = 9200;
        private int count = 1_000;
        private int start = 0;
        private int width = 5;
        private String keyPrefix = "key:";
        private String valuePrefix = "value-";
        private int timeoutMs = 5_000;
        private int reportEvery = 1_000;

        private static LoadKeysOptions parse(String[] args) {
            LoadKeysOptions options = new LoadKeysOptions();
            int offset = args.length > 0 && "loadkeys".equalsIgnoreCase(args[0]) ? 1 : 0;
            for (int i = offset; i < args.length; i++) {
                String arg = args[i].toLowerCase(Locale.ROOT);
                switch (arg) {
                    case "--router-host", "--host" -> options.routerHost = requireValue(args, ++i, arg);
                    case "--router-port", "--port" -> options.routerPort = parseInt(requireValue(args, ++i, arg), arg);
                    case "--count" -> options.count = parseInt(requireValue(args, ++i, arg), arg);
                    case "--start" -> options.start = parseInt(requireValue(args, ++i, arg), arg);
                    case "--width" -> options.width = parseInt(requireValue(args, ++i, arg), arg);
                    case "--key-prefix", "--prefix" -> options.keyPrefix = requireValue(args, ++i, arg);
                    case "--value-prefix" -> options.valuePrefix = requireValue(args, ++i, arg);
                    case "--timeout-ms" -> options.timeoutMs = parseInt(requireValue(args, ++i, arg), arg);
                    case "--report-every" -> options.reportEvery = parseInt(requireValue(args, ++i, arg), arg);
                    case "--help", "-h" -> {
                        printUsage();
                        System.exit(0);
                    }
                    default -> throw new IllegalArgumentException("unknown option: " + args[i]);
                }
            }
            options.validate();
            return options;
        }

        private void validate() {
            if (count < 1) {
                throw new IllegalArgumentException("--count must be >= 1");
            }
            if (start < 0) {
                throw new IllegalArgumentException("--start must be >= 0");
            }
            if (width < 1) {
                throw new IllegalArgumentException("--width must be >= 1");
            }
            if (timeoutMs < 1) {
                throw new IllegalArgumentException("--timeout-ms must be >= 1");
            }
            if (reportEvery < 0) {
                throw new IllegalArgumentException("--report-every must be >= 0");
            }
        }

        private String key(int number) {
            return keyPrefix + padded(number);
        }

        private String value(int number) {
            return valuePrefix + padded(number);
        }

        private String padded(int number) {
            return String.format(Locale.ROOT, "%0" + width + "d", number);
        }

        private static String requireValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException(option + " requires a value");
            }
            return args[index];
        }

        private static int parseInt(String raw, String option) {
            try {
                return Integer.parseInt(raw);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(option + " must be an integer: " + raw);
            }
        }

        private static void printUsage() {
            System.out.println("""
                    Usage:
                      LoadKeysMain [options]

                    Options:
                      --router-host <host>      default: 127.0.0.1
                      --router-port <port>      default: 9200
                      --count <n>               default: 1000
                      --start <n>               default: 0
                      --key-prefix <prefix>     default: key:
                      --value-prefix <prefix>   default: value-
                      --width <n>               default: 5
                      --timeout-ms <n>          default: 5000
                      --report-every <n>        default: 1000, 0 disables progress

                    Example:
                      --count 100 --start 1 --key-prefix user: --width 3
                    """);
        }
    }
}

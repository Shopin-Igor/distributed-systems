package ru.hse.drip.systems.distributed.homework.i5.router;

public class RouterConfig {
    public final String host;
    public final int port;
    public final int requestTimeoutMs;
    public final int healthCheckIntervalMs;

    public RouterConfig(String host, int port) {
        this(host, port, 1_500, 5_000);
    }

    public RouterConfig(String host, int port, int requestTimeoutMs, int healthCheckIntervalMs) {
        this.host = host;
        this.port = port;
        this.requestTimeoutMs = requestTimeoutMs;
        this.healthCheckIntervalMs = healthCheckIntervalMs;
    }
}

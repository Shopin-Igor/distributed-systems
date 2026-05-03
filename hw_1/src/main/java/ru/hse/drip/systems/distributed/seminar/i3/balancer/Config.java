package ru.hse.drip.systems.distributed.seminar.i3.balancer;

import java.util.List;

public class Config {
    public int balancerPort;
    public int controlPort;
    public int healthCheckIntervalSec;
    public int configReloadIntervalSec;
    public int connectTimeoutMs;
    public List<ServerInfo> servers;
}

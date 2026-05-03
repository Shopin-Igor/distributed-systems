package ru.hse.drip.systems.distributed.seminar.i3.balancer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

public class ConfigService {
    private final File file;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicReference<Config> ref = new AtomicReference<>();

    public ConfigService(String path) {
        this.file = new File(path);
        reload();
    }

    public void reload() {
        try {
            Config cfg = mapper.readValue(file, Config.class);
            if (cfg.connectTimeoutMs <= 0) cfg.connectTimeoutMs = 800;
            if (cfg.healthCheckIntervalSec <= 0) cfg.healthCheckIntervalSec = 10;
            if (cfg.configReloadIntervalSec <= 0) cfg.configReloadIntervalSec = 30;
            ref.set(cfg);
            System.out.println("[config] перезагружен, серверов=" + cfg.servers.size());
        } catch (Exception e) {
            System.err.println("[config] ошибка перезагрузки: " + e.getMessage());
        }
    }

    public Config get() { return ref.get(); }
}

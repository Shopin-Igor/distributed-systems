package ru.hse.drip.systems.distributed.seminar.i3.balancer;

import ru.hse.drip.systems.distributed.seminar.i3.control.BalancerControlServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class SimpleLoadBalancer {

    public static void main(String[] args) throws Exception {
        String configPath = args.length > 0 ? args[0] : "balancer-config.json";
        ConfigService configService = new ConfigService(configPath);

        AtomicReference<List<InetSocketAddress>> aliveRef =
                new AtomicReference<>(Collections.emptyList());

        RoundRobinPicker picker = new RoundRobinPicker(aliveRef);

        BalancerControlServer controlServer = new BalancerControlServer(configService.get().controlPort);
        controlServer.startAsync();

        ScheduledExecutorService sch = Executors.newScheduledThreadPool(
                2, r -> { Thread t = new Thread(r, "scheduler"); t.setDaemon(true); return t; });

        sch.scheduleAtFixedRate(configService::reload, 0,
                configService.get().configReloadIntervalSec, TimeUnit.SECONDS);

        sch.scheduleAtFixedRate(new HealthChecker(configService, aliveRef, controlServer), 0,
                configService.get().healthCheckIntervalSec, TimeUnit.SECONDS);

        int port = configService.get().balancerPort;
        try (ServerSocket balancer = new ServerSocket(port)) {
            System.out.println("[балансировщик] запущен на " + port);

            while (true) {
                Socket client = balancer.accept();
                InetSocketAddress backend = picker.nextOrNull();

                if (backend == null) {
                    try (OutputStream os = client.getOutputStream()) {
                        os.write("Нет доступных серверов. Повтор через 10 секунд.\n".getBytes());
                    } catch (IOException ignore) {}
                    try { client.close(); } catch (IOException ignore) {}
                    continue;
                }

                new Thread(new ProxyConnectionRunner(client, backend), "proxy-" + backend).start();
            }
        }
    }
}
    
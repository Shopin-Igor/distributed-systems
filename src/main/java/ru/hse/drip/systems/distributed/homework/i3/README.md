# Домашнее задание 03: Multi-leader replication

## Сборка

```bash
./gradlew classes
```

## Self-test

```bash
./gradlew runI3SelfTest
```

## Запуск 10 нод

Открой 10 терминалов.

Терминал 1:

```bash
./gradlew runI3Node1
```

Терминал 2:

```bash
./gradlew runI3Node2
```

Терминал 3:

```bash
./gradlew runI3Node3
```

Терминал 4:

```bash
./gradlew runI3Node4
```

Терминал 5:

```bash
./gradlew runI3Node5
```

Терминал 6:

```bash
./gradlew runI3Node6
```

Терминал 7:

```bash
./gradlew runI3Node7
```

Терминал 8:

```bash
./gradlew runI3Node8
```

Терминал 9:

```bash
./gradlew runI3Node9
```

Терминал 10:

```bash
./gradlew runI3Node10
```

## Запуск CLI

Терминал 11:

```bash
./gradlew runI3Cli
```

## Регистрация нод

Выполни в CLI:

```text
addNode 127.0.0.1:9101 127.0.0.1 9101
```

```text
addNode 127.0.0.1:9102 127.0.0.1 9102
```

```text
addNode 127.0.0.1:9103 127.0.0.1 9103
```

```text
addNode 127.0.0.1:9104 127.0.0.1 9104
```

```text
addNode 127.0.0.1:9105 127.0.0.1 9105
```

```text
addNode 127.0.0.1:9106 127.0.0.1 9106
```

```text
addNode 127.0.0.1:9107 127.0.0.1 9107
```

```text
addNode 127.0.0.1:9108 127.0.0.1 9108
```

```text
addNode 127.0.0.1:9109 127.0.0.1 9109
```

```text
addNode 127.0.0.1:9110 127.0.0.1 9110
```

```text
setMode multi
```

```text
listNodes
```

После `setMode multi` первые 4 `nodeId` в отсортированном порядке становятся лидерами.

## Проверки работоспособности

Self-test:

```bash
./gradlew runI3SelfTest
```

Базовая проверка кластера:

```text
listNodes
```

```text
clusterDump
```

Ожидаемый результат:

- после `setMode multi` есть 4 лидера и 6 follower-узлов;
- `clusterDump` отвечает с каждой ноды.

Smoke test для single-mode:

```text
setMode single
```

```text
setLeader 127.0.0.1:9101
```

```text
setReplication async
```

```text
setRF 3
```

```text
put s1 v1
```

```text
get s1
```

```text
get s1 --target 127.0.0.1:9102
```

```text
get s1 --target 127.0.0.1:9103
```

Проверка, что follower отклоняет запись:

```text
setMode multi
```

```text
put wrong target --target 127.0.0.1:9105
```

Ожидаемый результат: `NOT_LEADER_FOR_WRITE`.

Проверка записи и сходимости в multi-mode:

```text
setMode multi
```

```text
setTopology mesh
```

```text
put k1 v1 --target 127.0.0.1:9101
```

```text
getAll k1
```

Проверка stale read:

```text
setMode multi
```

```text
setTopology mesh
```

```text
setReplicationDelayMs 5000 5000
```

```text
put lagged v1 --target 127.0.0.1:9101
```

```text
get lagged --target 127.0.0.1:9110
```

```text
getAll lagged
```

Ожидаемый результат: немедленное чтение с другой ноды может быть устаревшим, затем значения сходятся.

Проверка разрешения конфликтов:

```text
setMode multi
```

```text
setTopology mesh
```

```text
setReplicationDelayMs 3000 3000
```

```text
put conflict valueA --target 127.0.0.1:9101
```

```text
put conflict valueB --target 127.0.0.1:9102
```

```text
getAll conflict
```

Ожидаемый результат: все ноды в итоге сходятся к одному детерминированному значению и версии.

Проверка топологий:

```text
setMode multi
```

```text
setTopology ring
```

```text
put topo r1 --target 127.0.0.1:9101
```

```text
getAll topo
```

```text
setTopology star
```

```text
setStarCenter 127.0.0.1:9101
```

```text
put topo s1 --target 127.0.0.1:9102
```

```text
getAll topo
```

Проверка delete:

```text
setMode single
```

```text
setLeader 127.0.0.1:9101
```

```text
put doomed value
```

```text
delete doomed
```

```text
get doomed
```

```text
get doomed --target 127.0.0.1:9102
```

Проверка чтения через leader:

```text
setMode single
```

```text
setLeader 127.0.0.1:9101
```

```text
setReplication async
```

```text
setRF 3
```

```text
setReplicationDelayMs 5000 5000
```

```text
put fresh value
```

```text
get fresh --target 127.0.0.1:9102
```

```text
get fresh --target 127.0.0.1:9102 --read leader
```

## Основные команды

Управление кластером:

```text
addNode <nodeId> <host> <port>
```

```text
removeNode <nodeId>
```

```text
listNodes
```

```text
setMode single|multi
```

```text
setLeader <nodeId>
```

```text
setTopology mesh|ring|star
```

```text
setStarCenter <nodeId>
```

```text
setReplication async|sync|semi-sync
```

```text
setRF <n>
```

```text
setSemiSyncAcks <k>
```

```text
setReplicationDelayMs <min> <max>
```

Пользовательские команды:

```text
put <key> <value> [--target <nodeId>] [--client <clientId>]
```

```text
get <key> [--target <nodeId>] [--client <clientId>]
```

```text
delete <key> [--target <nodeId>] [--client <clientId>]
```

```text
dump [--target <nodeId>] [--client <clientId>]
```

```text
getAll <key>
```

```text
clusterDump
```

## Заметки

- `single` сохраняет поведение из `i2`.
- `multi` использует до 4 лидеров, Lamport timestamps и LWW conflict resolution.
- Поддерживаются топологии `mesh`, `ring` и `star`.
- `setReplicationDelayMs` полезен для демонстрации stale-read и сходимости.

## Вспомогательные скрипты

Альтернативный фоновый запуск:

```bash
bash src/main/java/ru/hse/drip/systems/distributed/homework/i3/scripts/start-cluster-10.sh
```

```bash
bash src/main/java/ru/hse/drip/systems/distributed/homework/i3/scripts/stop-cluster-10.sh
```

Скрипт для benchmark:

```bash
bash src/main/java/ru/hse/drip/systems/distributed/homework/i3/scripts/run-benchmark.sh --nodes 127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105,127.0.0.1:9106,127.0.0.1:9107,127.0.0.1:9108,127.0.0.1:9109,127.0.0.1:9110
```

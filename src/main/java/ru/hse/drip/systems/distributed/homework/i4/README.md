# Домашнее задание 04: Leaderless replication
В `i4` сохранены старые режимы:
- `single`
- `multi`
- `leaderless`

`leaderless` добавлен поверх существующего проекта и использует тот же `Node`, тот же CLI, тот же транспорт `JSON Lines` поверх TCP.

## Что реализовано

- `leaderless`-режим с fixed cluster model `5 home replicas + 2 spare nodes`
- настраиваемые `W` и `R` с валидацией `W + R > 5`
- `strict` и `sloppy` write quorum
- `hinted handoff`
- `async read-repair`
- `Merkle Tree` по основному store
- `runAntiEntropyCluster`
- `wipeNodeData`
- smoke-check старых режимов `single` и `multi`

## Заметка

В режиме `leaderless` команда `getAll <key>` отправляет обычный клиентский `GET` на каждый target-узел.
Это означает, что каждый узел выступает координатором quorum-read по home-репликам.

Если нужен локальный снимок состояния узла, используй:
- `clusterDump`
- `dumpHints [--target <nodeId>]`
- `showMerkleRoot <nodeId>`

## Сборка

```bash
./gradlew classes
```


## Запуск leaderless-кластера

Нужны 7 нод: `9101..9107`.

Терминал 1:

```bash
./gradlew runI4Node1
```

Терминал 2:

```bash
./gradlew runI4Node2
```

Терминал 3:

```bash
./gradlew runI4Node3
```

Терминал 4:

```bash
./gradlew runI4Node4
```

Терминал 5:

```bash
./gradlew runI4Node5
```

Терминал 6:

```bash
./gradlew runI4Node6
```

Терминал 7:

```bash
./gradlew runI4Node7
```

Отдельно запусти CLI:

```bash
./gradlew runI4Cli
```

## Инициализация leaderless-конфига

В CLI:

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
setMode leaderless
```

```text
setHomeReplicas 127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103 127.0.0.1:9104 127.0.0.1:9105
```

```text
setSpareNodes 127.0.0.1:9106 127.0.0.1:9107
```

```text
setQuorum 3 3
```

```text
setWriteQuorumMode strict
```

```text
listNodes
```

Ожидаемый результат:

- `9101..9105` имеют роль `home`
- `9106..9107` имеют роль `spare`
- quorum отображается как `W=3 R=3 writeMode=strict`

## Архитектура leaderless-режима

### Координатор

Любой узел может принять клиентский `PUT` или `GET`.
Узел, принявший запрос, становится координатором именно этого запроса.

### Версионирование

Для записи используется `Version = (lamport, nodeId)`.
При сравнении версий побеждает:

1. большая `lamport`
2. при равной `lamport` лексикографически больший `nodeId`

### Write path

- в `strict` запись считается успешной только после `W` ACK от home-реплик
- в `sloppy`, если home-реплики недоступны, координатор пытается добрать ACK на spare-узлах
- spare-узел хранит такую запись как `hint`, а не как обычную home-реплику

### Read path

- чтение идёт только по home-репликам
- координатор ждёт `R` успешных ответов
- клиенту возвращается запись с максимальной версией
- stale home-реплики исправляются через `async read-repair`

### Hinted handoff

Hints хранятся отдельно от основного store.
Команда:

```text
runHintedHandoff [--target <nodeId>]
```

пытается доставить hints с выбранного узла на их intended home replicas.

### Merkle Tree и anti-entropy

Merkle Tree строится только по основному `store`.
Hints в дерево не входят.

Команды:

```text
showMerkleRoot <nodeId>
```

```text
runAntiEntropyCluster
```

`runAntiEntropyCluster` сравнивает home-реплики, локализует отличающиеся бакеты и переносит записи только из этих бакетов.

## Benchmark-прогоны (обязательные для сдачи)

Требуется выполнить все обязательные прогоны (`6` из части A, `2` из части B, `1` из части C), всего `9` прогона.

```bash
./gradlew runI4RequiredBenchmarkSuite
```

Или через явный запуск раннера:

```bash
./gradlew runI4RequiredBenchmarks
```

```bash
./gradlew runI4RequiredBenchmarks --nodes "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105,127.0.0.1:9106,127.0.0.1:9107" --output "src/main/java/ru/hse/drip/systems/distributed/homework/i4/benchmarks/results.csv" --report "src/main/java/ru/hse/drip/systems/distributed/homework/i4/report.md"
```

По умолчанию для результатов используются:

```bash
sed -n '1,20p' src/main/java/ru/hse/drip/systems/distributed/homework/i4/benchmarks/results.csv
```

```bash
sed -n '1,80p' src/main/java/ru/hse/drip/systems/distributed/homework/i4/report.md
```

В `report.md` должны быть:
- таблица всех прогонов
- минимум 4 графика
- краткие выводы по trade-off, sloppy/hints/read-repair/anti-entropy

## Основные команды

### Управление кластером

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
setMode single|multi|leaderless
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

```text
setHomeReplicas <id1> <id2> <id3> <id4> <id5>
```

```text
setSpareNodes <id1> <id2>
```

```text
setQuorum <W> <R>
```

```text
setWriteQuorumMode strict|sloppy
```

### Пользовательские

```text
put <key> <value> [--target <nodeId>] [--client <clientId>]
```

```text
get <key> [--target <nodeId>] [--client <clientId>]
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

### Отладочные для leaderless

```text
dumpHints [--target <nodeId>]
```

```text
runHintedHandoff [--target <nodeId>]
```

```text
showMerkleRoot <nodeId>
```

```text
wipeNodeData <nodeId>
```

```text
runAntiEntropyCluster
```

## Быстрые smoke-check'и

### Fast smoke-check single/multi

Нужны 3 узла в `single/multi`:

Терминал 1:

```bash
./gradlew runI4Node1
```

Терминал 2:

```bash
./gradlew runI4Node2
```

Терминал 3:

```bash
./gradlew runI4Node3
```

В отдельном терминале открой CLI и вставь команды ниже:

```bash
./gradlew runI4Cli
```

```bash
addNode 127.0.0.1:9101 127.0.0.1 9101
```

```bash
addNode 127.0.0.1:9102 127.0.0.1 9102
```

```bash
addNode 127.0.0.1:9103 127.0.0.1 9103
```

### Single

```bash
setMode single
```

```bash
setLeader 127.0.0.1:9101
```

```bash
setReplication sync
```

```bash
setRF 2
```

```bash
put s1 v1
```

```bash
get s1
```

```bash
get s1 --target 127.0.0.1:9102
```

### Multi

```bash
setMode multi
```

```bash
setTopology mesh
```

```bash
setReplicationDelayMs 0 0
```

```bash
setRF 3
```

```bash
put m1 valueA --target 127.0.0.1:9101
```

```bash
put m1 valueB --target 127.0.0.1:9102
```

```bash
getAll m1
```

### Leaderless

```text
setMode leaderless
```

```text
setQuorum 3 3
```

```text
setWriteQuorumMode strict
```

```text
put l1 v1
```

```text
get l1
```

```text
showMerkleRoot 127.0.0.1:9101
```

## Демонстрационные сценарии

Подробные пошаговые сценарии вынесены в `demo-scenario.md`.

В этом файле сейчас есть:
- leaderless-сценарии для quorum, sloppy quorum, hinted handoff и anti-entropy
- отдельный сценарий для `single`
- отдельный сценарий для `multi`

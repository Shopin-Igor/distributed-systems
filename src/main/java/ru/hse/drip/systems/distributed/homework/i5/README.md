# Домашнее задание 05: Sharding

Учебный прототип распределённого in-memory key-value storage с Router, ShardNode и CLI.
Все сетевые сообщения идут поверх TCP в формате JSON Lines.

## Архитектура

- `ShardNode` хранит локальный `Map<String, String>` и не знает о других узлах.
- `Router` хранит карту кластера, выбирает shard по активной стратегии, выполняет миграции и health-check.
- `CLI` отправляет клиентские и управляющие команды в Router.

Код разложен по пакетам:

```text
i5.protocol    JSON Lines модели и коды ошибок
i5.transport   TCP server/client
i5.store       in-memory key-value store
i5.node        ShardNode process
i5.router      Router, ClusterState, migration, health-check, stats
i5.sharding    range, hash_mod_n, consistent_hashing + vnodes/weights
i5.cli         interactive CLI
i5.benchmarks  обязательные benchmark-прогоны
```

## Сборка

```bash
./gradlew classes
```

Быстрая проверка:

```bash
./gradlew runI5SelfTest
```

## Запуск

В отдельных терминалах:

```bash
./gradlew runI5Router
```

```bash
./gradlew runI5Node1
```

```bash
./gradlew runI5Node2
```

```bash
./gradlew runI5Node3
```

```bash
./gradlew runI5Cli
```

В CLI:

```text
addNode A 127.0.0.1 9201
```

```text
addNode B 127.0.0.1 9202
```

```text
addNode C 127.0.0.1 9203
```

```text
setStrategy consistent_hashing
```

```text
setVnodes 150
```

```text
put user:1 Alice
```

```text
get user:1
```

```text
stats
```

```text
ringInfo
```

## Стратегии

- `range`: лексикографические диапазоны задаются вручную через `setRanges`.
- `hash_mod_n`: `SHA-256(key)` -> первые 8 байт -> `long`, затем modulo числа узлов.
- `consistent_hashing`: `TreeMap<Long, nodeId>`, `ceilingEntry`, wrap-around, обратная мапа `nodeId -> tokens`.

`setWeight <nodeId> <weight>` реализует бонус weighted consistent hashing: effective vnodes = `vnodes * weight`.

## CLI-команды

```text
addNode <nodeId> <host> <port> [weight]
```

```text
removeNode <nodeId>
```

```text
deleteNode <nodeId>
```

`deleteNode` является алиасом для `removeNode`.

```text
listNodes
```

```text
setStrategy range|hash_mod_n|consistent_hashing [--force]
```

`--force` пишется в той же команде. Например:

```text
setStrategy consistent_hashing --force
```

Этот флаг очищает данные на shard-узлах перед сменой стратегии.

```text
setRanges <boundary1> <boundary2> ...
```

```text
setVnodes <count>
```

```text
setWeight <nodeId> <weight>
```

```text
migrateData
```

`migrateData` нужен для ручной миграции после изменения range boundaries. Для `hash_mod_n` и `consistent_hashing` добавление и удаление узлов уже запускают миграцию автоматически.

```text
put <key> <value>
```

```text
get <key>
```

```text
delete <key>
```

```text
getRange <fromKey> <toKey>
```

```text
dump [--target <nodeId>]
```

```text
clusterDump
```

```text
stats
```

```text
ringInfo
```

`getRange` работает для `range` strategy и возвращает ключи из полуинтервала `[fromKey, toKey)`.

## Протокол

Все сообщения имеют поле `type`. Примеры:

```json
{"requestId":"uuid","type":"CLIENT_PUT","key":"user:1","value":"Alice"}
```

```json
{"requestId":"uuid","type":"CLIENT_GET","key":"user:1"}
```

```json
{"requestId":"uuid","type":"ADD_NODE","nodeId":"A","host":"127.0.0.1","port":9201}
```

```json
{"requestId":"uuid","type":"SET_STRATEGY","strategy":"consistent_hashing"}
```

Ошибки:

```text
KEY_NOT_FOUND
NO_AVAILABLE_NODES
INVALID_CONFIG
BAD_REQUEST
NODE_UNAVAILABLE
```

## Benchmarks

Полный набор:

```bash
./gradlew runI5RequiredBenchmarks
```

Быстрый локальный прогон:

```bash
./gradlew runI5RequiredBenchmarks --args='benchmark --quick'
```

Результаты пишутся в:

```text
src/main/java/ru/hse/drip/systems/distributed/homework/i5/benchmarks/results.csv
```

## Массовая загрузка ключей

Когда Router и ShardNode уже запущены, можно загрузить много ключей отдельной утилитой:

```bash
./gradlew runI5LoadKeys --args='--count 1000'
```

То же через shell wrapper:

```bash
src/main/java/ru/hse/drip/systems/distributed/homework/i5/scripts/load-keys.sh --count 1000
```

Для сценария `user:001..user:100`:

```bash
./gradlew runI5LoadKeys --args='--count 100 --start 1 --key-prefix user: --value-prefix value- --width 3'
```

Параметры:

```text
--router-host <host>      default: 127.0.0.1
--router-port <port>      default: 9200
--count <n>               default: 1000
--start <n>               default: 0
--key-prefix <prefix>     default: key:
--value-prefix <prefix>   default: value-
--width <n>               default: 5
--timeout-ms <n>          default: 5000
--report-every <n>        default: 1000, 0 disables progress
```

## Stop all

Проверить, свободны ли порты:
```text
ss -ltnp '( sport = :9200 or sport = :9201 or sport = :9202 or sport = :9203 )'
```

```text
ps -ef | grep 'homework.i5.Main'
```

```text
kill <pid>
```

```text
kill -9 <pid>
```

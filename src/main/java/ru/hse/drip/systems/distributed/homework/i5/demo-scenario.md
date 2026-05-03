# Demo Scenario

Перед сценариями запустить Router и нужные ShardNode:

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

Для сценариев, где добавляется узел `D`, заранее запустить процесс ноды `D`, но не добавлять её в Router до команды `addNode D ...`:

```bash
./gradlew runI5Node4
```

Для сценария с 5 узлами дополнительно запустить `E`:

```bash
./gradlew runI5Node5
```

```bash
./gradlew runI5Cli
```

Сценарии можно показывать подряд в одном Router. Переходные блоки ниже возвращают кластер к нужному числу нод. Команда `deleteNode` является алиасом для `removeNode`; в сценариях используется `removeNode`, потому что так она названа в задании.

## 1. Range-based: плохие границы

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
setStrategy range
```

```text
setRanges m z
```

Загрузить ключи `user:001` ... `user:100`:

```bash
./gradlew runI5LoadKeys --args='--count 100 --start 1 --key-prefix user: --value-prefix value- --width 3'
```

Затем:

```text
stats
```

```text
setRanges user:034 user:067
```

```text
migrateData
```

```text
stats
```

```text
getRange user:010 user:020
```

## 2. Hash-mod-N: катастрофическая ребалансировка

```text
setStrategy hash_mod_n --force
```

```text
put key:00001 value-1
```

```text
put key:00002 value-2
```

```text
stats
```

После загрузки 1000 ключей:

```bash
./gradlew runI5LoadKeys --args='--count 1000 --key-prefix key: --value-prefix value- --width 5'
```

```text
addNode D 127.0.0.1 9204
```

`addNode` для `hash_mod_n` уже запускает ребалансировку автоматически и выводит процент мигрированных ключей.

```text
stats
```

```text
get key:00001
```

```text
get key:00002
```

В ответе `migration` будет точный процент moved keys.

### Переход к сценарию 3: вернуться с 4 нод на 3

```text
removeNode D
```

```text
stats
```

После этого в Router снова должны остаться только `A`, `B`, `C`. Процесс `runI5Node4` можно не останавливать: он понадобится в следующем сценарии при повторном `addNode D ...`.

## 3. Consistent hashing: минимальная миграция

```text
setStrategy consistent_hashing --force
```

```text
setVnodes 150
```

```text
put key:00001 value-1
```

```text
put key:00002 value-2
```

```text
stats
```

Загрузить 1000 ключей:

```bash
./gradlew runI5LoadKeys --args='--count 1000 --key-prefix key: --value-prefix value- --width 5'
```

```text
addNode D 127.0.0.1 9204
```

```text
stats
```

```text
get key:00001
```

```text
get key:00002
```

При добавлении четвёртого узла ожидается миграция около `1 / 4`.

### Переход к сценарию 4: оставить 4 ноды и добавить пятую

Если сценарии идут подряд, после сценария 3 в Router уже есть `A`, `B`, `C`, `D`. Для сценария 4 нужно добавить только `E`:

```text
addNode E 127.0.0.1 9205
```

## 4. Влияние vnodes

Если запускаешь этот сценарий отдельно, добавить 5 узлов в Router:

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
addNode D 127.0.0.1 9204
```

```text
addNode E 127.0.0.1 9205
```

```text
setStrategy consistent_hashing --force
```

```text
setVnodes 1
```

Загрузить 10000 ключей:

```bash
./gradlew runI5LoadKeys --args='--count 10000 --key-prefix key: --value-prefix value- --width 5'
```

```text
stats
```

```text
setVnodes 5
```

```text
stats
```

```text
setVnodes 150
```

```text
stats
```

```text
ringInfo
```

Чем больше виртуальных узлов, тем ниже `stddevOverMean` и `maxOverAverage`.

### Переход к сценарию 5: вернуться с 5 нод на 3

```text
removeNode E
```

```text
removeNode D
```

```text
stats
```

После этого в Router снова должны остаться `A`, `B`, `C`.

## 5. Health-check

```text
setStrategy consistent_hashing --force
```

```text
setVnodes 150
```

```text
put key:00001 value-1
```

```text
put key:00002 value-2
```

```text
stats
```

Загрузить 1000 ключей:

```bash
./gradlew runI5LoadKeys --args='--count 1000 --key-prefix key: --value-prefix value- --width 5'
```

Перед остановкой ноды можно посмотреть, какие ключи лежат на каждом shard:

```text
clusterDump
```

Остановить один ShardNode, подождать до 5 секунд:

```text
listNodes
```

Взять любой ключ из секции остановленной ноды в выводе `clusterDump` и выполнить `get` для него. Например, если этот ключ `key:00042`:

```text
get key:00042
```

Для ключей, которые принадлежат живым shard, запросы продолжают работать:

```text
put alive-key value
```

Ключи, принадлежащие упавшему owner shard, вернут `NODE_UNAVAILABLE`; ключи живых shard продолжат работать.

### Переход к бонусному сценарию после health-check

Если ты останавливал ShardNode через `Ctrl+C`, снова запусти остановленную ноду в её терминале. Например, если останавливал `B`:

```bash
./gradlew runI5Node2
```

Подождать до 5 секунд и проверить, что Router снова видит ноду живой:

```text
listNodes
```

## Бонус: weighted consistent hashing

```text
setStrategy consistent_hashing --force
```

```text
setVnodes 50
```

```text
setWeight A 3
```

```text
setWeight B 1
```

```text
setWeight C 1
```

```text
ringInfo
```

```text
stats
```

Узел `A` получает в 3 раза больше virtual nodes.

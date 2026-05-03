# Сценарий демонстрации

## Подготовка
```bash
./gradlew classes
```
10 нод на портах `9101..9110`:

```bash
./gradlew runI3Node1
```

```bash
./gradlew runI3Node2
```

```bash
./gradlew runI3Node3
```

```bash
./gradlew runI3Node4
```

```bash
./gradlew runI3Node5
```

```bash
./gradlew runI3Node6
```

```bash
./gradlew runI3Node7
```

```bash
./gradlew runI3Node8
```

```bash
./gradlew runI3Node9
```

```bash
./gradlew runI3Node10
```

CLI:

```bash
./gradlew runI3Cli
```

Регаем все ноды:

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

Ожидаемый результат: 4 лидера и 6 follower-узлов.

## Сценарий 1: Конфликт по одному ключу

```text
setTopology mesh
```

```text
setReplicationDelayMs 15000 15000
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

Сразу после конкурентных записей разные ноды могут временно показывать разные значения и версии.

Ждём несколько секунды, затем:

```text
getAll conflict
```

```text
clusterDump
```

Ожидаемый результат: все ноды сходятся к одному значению согласно LWW по `(lamport, nodeId)`.

## Сценарий 2: Задержка репликации и stale read

```text
setTopology mesh
```

```text
setReplicationDelayMs 15000 15000
```

```text
put lagged v1 --target 127.0.0.1:9101
```

```text
get lagged --target 127.0.0.1:9108
```

Ожидаемый результат: первое чтение с другой ноды может вернуть устаревшие данные или `null`.

После паузы:

```text
get lagged --target 127.0.0.1:9108
```

```text
getAll lagged
```

Ожидаемый результат: после завершения репликации данные сходятся.

## Сценарий 3: Mesh против Ring

Прогон для mesh:

```text
setTopology mesh
```

```text
setReplicationDelayMs 10000 10000
```

```text
put topo x1 --target 127.0.0.1:9101
```

```text
getAll topo
```

Прогон для ring:

```text
setTopology ring
```

```text
setReplicationDelayMs 10000 10000
```

```text
put topo x2 --target 127.0.0.1:9101
```

```text
getAll topo
```

Ожидаемый результат: `mesh` сходится быстрее, а `ring` распространяет обновление по одному hop за раз.

## Сценарий 4: Bottleneck в Star и отказ центра

```text
setTopology star
```

```text
setStarCenter 127.0.0.1:9101
```

```text
setReplicationDelayMs 10000 10000
```

```text
put starKey v1 --target 127.0.0.1:9102
```

```text
getAll starKey
```

Останови ноду `127.0.0.1:9101` снаружи, затем:

```text
put starKey v2 --target 127.0.0.1:9102
```

```text
getAll starKey
```

Ожидаемый результат: распространение в `star` деградирует или ломается, если центр недоступен.

## Сценарий 5: Ring ломается без removeNode

```text
setTopology ring
```

```text
setReplicationDelayMs 10000 10000
```

Останови одну промежуточную ноду снаружи, например `127.0.0.1:9104`, не вызывая `removeNode`.

Затем:

```text
put brokenRing v1 --target 127.0.0.1:9101
```

```text
getAll brokenRing
```

Ожидаемый результат: часть нод не получит обновление, потому что логическое кольцо всё ещё включает упавшую ноду.

Теперь почини membership:

```text
removeNode 127.0.0.1:9104
```

```text
put brokenRing v2 --target 127.0.0.1:9101
```

```text
getAll brokenRing
```

Ожидаемый результат: кольцо перестраивается, и распространение снова работает.

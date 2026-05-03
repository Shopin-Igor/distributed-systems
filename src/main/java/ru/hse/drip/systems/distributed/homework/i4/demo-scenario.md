# Сценарий демонстрации i4

## Подготовка

```bash
./gradlew classes
```

Подними 7 нод:

```bash
./gradlew runI4Node1
```

```bash
./gradlew runI4Node2
```

```bash
./gradlew runI4Node3
```

```bash
./gradlew runI4Node4
```

```bash
./gradlew runI4Node5
```

```bash
./gradlew runI4Node6
```

```bash
./gradlew runI4Node7
```

Запусти CLI:

```bash
./gradlew runI4Cli
```

## Базовая настройка кластера

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
- quorum показан как `W=3 R=3 writeMode=strict`

## Сценарий 1: strict quorum и чтение

```text
setQuorum 3 3
```

```text
setWriteQuorumMode strict
```

```text
put k1 v1
```

```text
get k1
```

```text
clusterDump
```

Ожидаемый результат:

- `put` возвращает `status=OK`
- `get` возвращает `value=v1`
- в `clusterDump` ключ `k1` виден на home-репликах

## Сценарий 2: strict quorum падает при недоступной home-реплике

Снаружи останови одну home-реплику, например `127.0.0.1:9105`.

Затем в CLI:

```text
setQuorum 5 1
```

```text
setWriteQuorumMode strict
```

```text
put strictFail value1
```

Ожидаемый результат:

- `put` возвращает `status=ERROR`
- `errorCode=NOT_ENOUGH_REPLICAS`

## Сценарий 3: sloppy quorum + hinted handoff

При той же остановленной `127.0.0.1:9105`:

```text
setWriteQuorumMode sloppy
```

```text
put sloppyKey value2
```

```text
dumpHints --target 127.0.0.1:9106
```

```text
dumpHints --target 127.0.0.1:9107
```

Ожидаемый результат:

- запись проходит успешно
- на одном из spare-узлов появляется hint c `intendedHomeNodeId=127.0.0.1:9105`

Теперь верни `127.0.0.1:9105` обратно и выполни:

```text
setMode leaderless
```

```text
runHintedHandoff --target 127.0.0.1:9106
```

```text
dumpHints --target 127.0.0.1:9106
```

```text
clusterDump
```

Ожидаемый результат:

- `runHintedHandoff` показывает `affectedRecords > 0`
- hint исчезает из `dumpHints`
- в `clusterDump` home-реплика `9105` уже содержит `sloppyKey`

## Сценарий 4: Merkle root и anti-entropy recovery

```text
setQuorum 3 3
```

```text
setWriteQuorumMode strict
```

```text
put recoverKey recoverValue
```

```text
showMerkleRoot 127.0.0.1:9101
```

```text
showMerkleRoot 127.0.0.1:9103
```

Ожидаемый результат: root на `9101` и `9103` совпадают.

Теперь wipe одной home-реплики:

```text
wipeNodeData 127.0.0.1:9103
```

```text
showMerkleRoot 127.0.0.1:9103
```

```text
runAntiEntropyCluster
```

```text
showMerkleRoot 127.0.0.1:9103
```

```text
clusterDump
```

Ожидаемый результат:

- после `wipeNodeData` root у `9103` меняется
- `runAntiEntropyCluster` возвращает `affectedRecords > 0`
- после anti-entropy root у `9103` снова совпадает с остальными home-репликами
- `recoverKey` снова появляется в `clusterDump` на `9103`

## Сценарий 5: single leader replication

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
put singleKey singleValue
```

```text
get singleKey
```

```text
get singleKey --target 127.0.0.1:9102
```

```text
get singleKey --target 127.0.0.1:9103
```

```text
clusterDump
```

Ожидаемый результат:

- запись проходит через лидера `127.0.0.1:9101`
- значение читается с лидера
- после репликации значение видно на follower-узлах `9102` и `9103`

## Сценарий 6: multi leader replication

```text
setMode multi
```

```text
setTopology mesh
```

```text
setReplicationDelayMs 0 0
```

```text
put multiKey valueA --target 127.0.0.1:9101
```

```text
put multiKey valueB --target 127.0.0.1:9102
```

```text
get multiKey --target 127.0.0.1:9101
```

```text
get multiKey --target 127.0.0.1:9102
```

```text
clusterDump
```

Ожидаемый результат:

- запись принимают leader-узлы `9101` и `9102`
- конфликт разрешается по Lamport/LWW
- после сходимости `clusterDump` показывает одно итоговое значение на всех узлах

## Полезные замечания

- `getAll <key>` в `leaderless` показывает результат quorum-read с каждого узла-координатора, а не raw local store.
- Для локального состояния узлов используй `clusterDump`.
- Для spare-узлов и hints используй `dumpHints`.
- Для Merkle-проверки используй `showMerkleRoot`.

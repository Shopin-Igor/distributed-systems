# Homework-02: Single-leader replication

## Build

```bash
./gradlew classes
```

## Run self-test

```bash
./gradlew runSelfTest
```

## Run nodes
Terminal 1:

```bash
cd ~/IdeaProjects/distributed-systems
./gradlew runNode1
```

Terminal 2:

```bash
cd ~/IdeaProjects/distributed-systems
./gradlew runNode2
```

Terminal 3:

```bash
cd ~/IdeaProjects/distributed-systems
./gradlew runNode3
```

## Run CLI

Terminal 4:

```bash
cd ~/IdeaProjects/distributed-systems
./gradlew runCli
```

Then register nodes in CLI:

```text
addNode 127.0.0.1:9001 127.0.0.1 9001
addNode 127.0.0.1:9002 127.0.0.1 9002
addNode 127.0.0.1:9003 127.0.0.1 9003
listNodes
```

Expected leader by default:

```text
leader=127.0.0.1:9001
```

## Scenarios

### Scenario 1: basic replication (`async`, `RF=3`)

```text
setReplication async
setRF 3
put k1 v1
get k1
get k1 --target 127.0.0.1:9002
get k1 --target 127.0.0.1:9003
```

### Scenario 2: stale reads in `async`

```text
setReplication async
setRF 3
setReplicationDelayMs 300 300
put k2 v2
get k2 --target 127.0.0.1:9002
```

The first follower read may return `null`. After `400-500ms`, retry:

```text
get k2 --target 127.0.0.1:9002
```

### Scenario 3: `sync` timeout / waiting for ACKs

```text
setReplication sync
setRF 3
setReplicationDelayMs 600 600
put s1 a
```

With default `hotPathTimeoutMs=500`, the write should return `NOT_ENOUGH_REPLICAS`, but followers can still catch up later.

### Scenario 4: `semi-sync` (`K=1`, `RF=3`)

```text
setReplication semi-sync
setRF 3
setSemiSyncAcks 1
setReplicationDelayMs 300 300
put m1 x
get m1 --target 127.0.0.1:9002
get m1 --target 127.0.0.1:9003
```

### Scenario 5: write to follower

```text
put wrong target --target 127.0.0.1:9002
```

Expected result: `NOT_LEADER` with leader address in the response.

### Scenario 6: replicated delete
(bon)
```text
put doomed value
delete doomed
get doomed
get doomed --target 127.0.0.1:9002
```

Expected result: all reads eventually return `null`.

### Scenario 7: read-your-writes from leader
(bon)
```text
setReplication async
setRF 3
setReplicationDelayMs 300 300
put fresh value
get fresh --target 127.0.0.1:9002
get fresh --target 127.0.0.1:9002 --read leader
```

Expected result: the plain follower read may be stale, while `--read leader` forces the read through the leader and returns the fresh value.

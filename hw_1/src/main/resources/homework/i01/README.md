

1. **Балансировщик нагрузки** (`SimpleLoadBalancer`)
   - TCP-прокси на порту 9999
   - Round-Robin распределение клиентов
   - Health-check каждые 10 секунд
   - Автоматическое обнаружение серверов из конфига

2. **Серверные узлы** (`ChatServerMain`)
   - Обработка клиентских подключений
   - Межсерверная синхронизация сообщений
   - Дедупликация сообщений
   - Автоматическое перестроение peer-соединений

3. **Клиент** (`ChatClient`)
   - Автоматическое переподключение при сбоях
   - Сохранение сообщений в очереди при недоступности
   - Graceful degradation

### Схема взаимодействия

```
[Клиент] → [Балансировщик:9999] → [Сервер A:9001]
                              → [Сервер B:9002]
                              → [Сервер C:9003]

[Сервер A] ←→ [Сервер B] ←→ [Сервер C]
    ↑              ↑              ↑
[Контрольный канал:9090] ← [Балансировщик]
```

## Форматы сообщений

### 1. Обновление списка серверов (от балансировщика к серверам)

```json
{
  "type": "SERVER_LIST_UPDATE",
  "servers": [
    {"serverId":"A","host":"localhost","port":9001},
    {"serverId":"B","host":"localhost","port":9002}
  ]
}
```

### 2. Сообщение от клиента к серверу

**Формат ввода клиента:**
```
message content
```

**Пример:**
```
hello
hi everyone
test message
```

**Внутренняя обработка сервером (JSON):**
```json
{
  "type": "CLIENT_MESSAGE",
  "username": "user_1234",
  "content": "hello",
  "timestamp": 1634567890
}
```

### 3. Сообщение между серверами

```json
{
  "type": "CHAT_MESSAGE",
  "messageId": "uuid",
  "serverId": "A",
  "username": "alice",
  "content": "hello",
  "timestamp": 1634567890
}
```

## Конфигурация

### Файл `balancer-config.json`

```json
{
  "balancerPort": 9999,
  "controlPort": 9090,
  "healthCheckIntervalSec": 10,
  "configReloadIntervalSec": 30,
  "connectTimeoutMs": 800,
  "servers": [
    { "serverId": "A", "host": "localhost", "port": 9001 },
    { "serverId": "B", "host": "localhost", "port": 9002 },
    { "serverId": "C", "host": "localhost", "port": 9003 }
  ]
}
```

## Инструкции по запуску

### Предварительные требования

- Java 11 или выше
- Gradle (для сборки проекта)

### Сборка проекта

```bash
./gradlew build
```

### Запуск системы

#### 1. Запуск балансировщика

```bash
java -cp build/libs/* ru.hse.drip.systems.distributed.seminar.i3.balancer.SimpleLoadBalancer
```

#### 2. Запуск серверных узлов

В отдельных терминалах:

```bash
# Сервер A
java -cp build/libs/* ru.hse.drip.systems.distributed.seminar.i3.ChatServerMain A 9001

# Сервер B  
java -cp build/libs/* ru.hse.drip.systems.distributed.seminar.i3.ChatServerMain B 9002

# Сервер C
java -cp build/libs/* ru.hse.drip.systems.distributed.seminar.i3.ChatServerMain C 9003
```

#### 3. Запуск клиентов

В отдельных терминалах:

```bash
java -cp build/libs/* ru.hse.drip.systems.distributed.seminar.i3.ChatClient
```

### Формат ввода клиента

Клиент принимает простые текстовые сообщения:
```
message content
```

Пример:
```
Hello everyone!
Hi there!
Test message
```

**Важно:** Клиенты отправляют простые текстовые строки, а не JSON. JSON используется только для внутреннего обмена между серверами. Username генерируется автоматически.

## Особенности реализации

### Отказоустойчивость

- **Health-check**: Балансировщик каждые 10 секунд проверяет доступность серверов
- **Автоматическое переподключение**: Клиенты автоматически переподключаются при сбоях
- **Graceful degradation**: При недоступности всех серверов клиент выводит сообщение и повторяет попытки

### Дедупликация сообщений

- In-memory хранилище `messageId` с TTL = 5 минут
- Периодическая очистка устаревших ID каждую минуту
- Предотвращение дублирования сообщений между серверами

### Масштабируемость

- Автоматическое обнаружение новых серверов из конфига
- Динамическое перестроение peer-соединений
- Round-Robin балансировка нагрузки

## Структура проекта

```
src/main/java/ru/hse/drip/systems/distributed/seminar/i3/
├── balancer/           # Компоненты балансировщика
│   ├── SimpleLoadBalancer.java
│   ├── HealthChecker.java
│   ├── RoundRobinPicker.java
│   ├── ConfigService.java
│   ├── BalancerListener.java
│   └── ProxyConnectionRunner.java
├── peer/              # Межсерверное взаимодействие
│   ├── PeerManager.java
│   ├── PeerConnection.java
│   ├── PeerMessageBus.java
│   ├── DedupStore.java
│   ├── ServerBroadcaster.java
│   └── ServerListUpdateHandler.java
├── dto/               # Объекты передачи данных
│   ├── ChatMessage.java
│   ├── ClientMessage.java
│   ├── ServerInfo.java
│   └── ServerListUpdate.java
├── control/           # Контрольный канал
│   └── BalancerControlServer.java
├── ChatClient.java    # Клиентское приложение
├── ChatServerMain.java # Основной класс сервера
└── ClientHandler.java # Обработчик клиентских соединений
```

## Тестирование

### Базовый сценарий

1. Запустите балансировщик
2. Запустите 2-3 серверных узла
3. Подключите несколько клиентов
4. Отправьте сообщения от разных клиентов
5. Проверьте, что все клиенты получают все сообщения

### Тестирование отказоустойчивости

1. Остановите один из серверов
2. Проверьте, что клиенты автоматически переподключаются
3. Проверьте, что сообщения продолжают доставляться
4. Запустите сервер обратно и проверьте восстановление

### Тестирование масштабируемости

1. Добавьте новый сервер в `balancer-config.json`
2. Запустите новый сервер
3. Проверьте, что балансировщик автоматически его обнаружил
4. Проверьте, что клиенты могут подключаться к новому серверу

## Приложение промптов LLM

**🔗 [Чат с LLM](https://chatgpt.com/c/68ec008f-d088-8329-a74b-25cd4656c96a)**

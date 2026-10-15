# Kafka 3.7.0 生产者发送消息流程深度解析（源码级）

> 基于 Kafka **3.7.0**，源码均在 `clients` 模块：
> - `clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`（门面 + send）
> - `clients/src/main/java/org/apache/kafka/clients/producer/internals/`：
>   `RecordAccumulator.java`、`ProducerBatch.java`、`BufferPool.java`、`Sender.java`、
>   `ProducerMetadata.java`、`TransactionManager.java`、`BuiltInPartitioner.java`、`StickyPartitionCache.java`
> - `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`、`clients/.../network/Selector.java`（KSelector）
>
> 示例沿用 `ProducerTest`：`send(new ProducerRecord<>("test-topic01", "k1", "hello kafka")).get()`

---

## 目录

1. [总体架构：双线程模型](#1-总体架构双线程模型)
2. [send() 主流程（应用线程）](#2-send-主流程应用线程)
3. [RecordAccumulator：生产者缓冲区](#3-recordaccumulator生产者缓冲区)
4. [BufferPool：内存池](#4-bufferpool内存池)
5. [分区选择与粘性分区](#5-分区选择与粘性分区)
6. [Sender 线程主循环](#6-sender-线程主循环)
7. [元数据（ProducerMetadata）](#7-元数据producermetadata)
8. [NetworkClient：请求收发与连接管理](#8-networkclient请求收发与连接管理)
9. [响应处理、重试与幂等](#9-响应处理重试与幂等)
10. [close() 与优雅关闭](#10-close-与优雅关闭)
11. [端到端时序总图](#11-端到端时序总图)
12. [关键配置](#12-关键配置)
13. [调试断点指引](#13-调试断点指引)

---

## 1. 总体架构：双线程模型

KafkaProducer 是**双线程**设计：业务线程只做"攒批"（微秒级），网络 I/O 全部交给一个后台 `Sender` 线程，两者通过 `RecordAccumulator`（并发队列）解耦。

```mermaid
flowchart TB
    subgraph UserThread["应用线程（调用 KafkaProducer.send 的线程）"]
        SEND["KafkaProducer.send(record)"]
        INTER["ProducerInterceptors.onSend<br/>（拦截器链，可改写/丢弃消息）"]
        SER["keySerializer / valueSerializer<br/>（StringSerializer→UTF-8字节）"]
        PART["partition 计算（见第5节）"]
        APP["RecordAccumulator.append(...)"]
        FUT["返回 FutureRecordMetadata<br/>（此时消息还没发出）"]
    end

    subgraph Accu["RecordAccumulator（生产者缓冲区，buffer.memory=32MB）"]
        direction TB
        BP["BufferPool<br/>batch.size(16KB) ByteBuffer 池化复用"]
        DQ["ConcurrentMap&lt;TopicPartition, Deque&lt;ProducerBatch&gt;&gt;<br/>每个分区一个双端队列"]
        B0["ProducerBatch(tp0, 16KB)"]
        B1["ProducerBatch(tp1, 16KB)"]
    end

    subgraph SenderThread["Sender 线程（kafka-producer-network-thread-*)"]
        RUN["runOnce() 每轮："]
        READY["① accumulator.ready()<br/>哪些分区攒够了/过期了"]
        DRAIN["② accumulator.drain()<br/>按broker取走batch"]
        PRQ["③ sendProduceRequests<br/>组装ProduceRequest"]
        NCPOLL["④ client.poll()<br/>网络收发"]
        RESP["⑤ handleProduceResponse<br/>完成Future/触发重试"]
    end

    subgraph Net["NetworkClient + KSelector"]
        NC["NetworkClient<br/>inFlightRequests（每连接≤5）<br/>节点连接状态机"]
        SEL["Selector（KSelector）<br/>NIO，与broker端同款实现"]
    end

    subgraph Meta["ProducerMetadata（生产者元数据缓存）"]
        MCL["Cluster：topic→partition→leader/replicas<br/>后台定时刷新 metadata.max.age.ms=5min"]
    end

    SEND --> INTER --> SER --> PART --> APP --> FUT
    APP --> BP
    APP --> DQ
    DQ --> B0 & B1
    RUN --> READY --> DRAIN --> PRQ --> NCPOLL --> RESP
    READY -.->|"leader未知→触发"| MCL
    DRAIN -.->|"按leader所在的node分组"| MCL
    NCPOLL --> NC --> SEL
    SEL <==>|"TCP 9092<br/>ProduceRequest/Response"| BROKER["Broker SocketServer"]
```

**各组件一句话职责：**

| 组件 | 职责 |
|---|---|
| `KafkaProducer` | 门面：校验、拦截器、序列化、分区、入队 |
| `RecordAccumulator` | 缓冲区核心：分区→批次队列，攒批（batch.size / linger.ms） |
| `ProducerBatch` | 一批消息的封装（内部 `MemoryRecordsBuilder` 写 RecordBatch v2 格式） |
| `BufferPool` | 16KB batch 缓冲池化，避免 GC 抖动 |
| `Sender` | 后台 I/O 线程：drain→组请求→poll→处理响应 |
| `ProducerMetadata` | 集群元数据缓存，分区→broker 路由的依据 |
| `NetworkClient` + `Selector` | 与 broker 的连接管理、请求收发、响应配对（correlationId） |
| `TransactionManager` | 事务/幂等状态机（enable.idempotence 默认开启，3.x） |

## 2. send() 主流程（应用线程）

```mermaid
flowchart TD
    A["KafkaProducer.send(record, callback)"] --> B["interceptors.onSend(record)<br/>（拦截器可能改写record）"]
    B --> C["waitOnMetadata(record.topic, maxWaitMs)"]
    C -->|"metadata无该topic"| C1["metadata.add(topic)<br/>requestUpdate → awaitUpdate<br/>（最多max.block.ms=60s）"]
    C1 --> C2["Sender线程发出的Metadata请求<br/>拿到topic的分区/leader"]
    C2 --> D
    C -->|"已有"| D["keySerializer.serialize / valueSerializer.serialize"]
    D --> E["partition(topic, key, keyBytes, value, valueBytes, cluster)"]
    E --> F["validSize校验（≤max.request.size）"]
    F --> G["accumulator.append(tp, timestamp, key, value, ...)"]
    G --> H{"append结果？"}
    H -->|"OK/已满新建batch"| I["返回 FutureRecordMetadata<br/>（消息仅入队，未发送）"]
    H -->|"BufferPool满且超max.block.ms"| J["抛 BufferExhaustedException<br/>（buffer.memory耗尽）"]
    I --> K["send().get()：应用线程阻塞<br/>等 Sender 线程完成该batch"]
```

**关键认知：`send()` 返回 ≠ 消息已发送**——只是进了缓冲区。真正的网络发送由 Sender 异步完成，`Future.get()` 才能确认结果。这就是 KafkaProducer 高吞吐的根基：业务线程的 `send()` 成本极低（序列化 + 入队，微秒级）。

## 3. RecordAccumulator：生产者缓冲区

### 3.1 数据结构

```java
// RecordAccumulator 的核心字段
private final ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches;
private final BufferPool free;      // buffer.memory 池
private final IncompleteBatches incomplete;
```

```mermaid
flowchart LR
    subgraph batches["ConcurrentMap&lt;TopicPartition, Deque&gt;"]
        subgraph TP0["test-topic01-0 → Deque"]
            S00["ProducerBatch<br/>baseOffset未定<br/>16KB ByteBuffer"]
        end
        subgraph TP1["test-topic01-1 → Deque"]
            S10["ProducerBatch（已满）"]
            S11["ProducerBatch（active）"]
        end
    end
    BP["BufferPool<br/>free队列：16KB×N<br/>（非压缩batch用池化buffer）"]
    BP -- "allocate(batch.size)" --> S00
    S10 -. "drain后deallocation归还" .-> BP
```

### 3.2 append 的逻辑（RecordAccumulator.append）

```mermaid
flowchart TD
    A["append(tp, timestamp, key, value, callback...)"] --> B["getOrCreateDeque(tp)<br/>每个分区懒创建一个Deque"]
    B --> C["dq加锁（synchronized on deque）"]
    C --> D["取队尾batch last"]
    D --> E{"last != null 且<br/>tryAppend成功？<br/>（批还没满）"}
    E -->|是| F["消息写入last<br/>返回该batch的future"]
    E -->|"否（满了/无batch）"| G["从BufferPool分配16KB<br/>new MemoryRecordsBuilder<br/>写入RecordBatch头+该条消息"]
    G --> H["deque.addLast(batch)"]
    H --> I["记录incomplete"]
    E2["异常路径：batch被关闭（正在drain）<br/>→ 也走G新建"]
    F --> J["batchIsFull / newBatchCreated？<br/>→ 唤醒Sender线程（sender.wakeup）"]
    I --> J
```

细节：

- **一个分区的消息永远追加到队尾 batch**（tryAppend），队头只被 Sender drain——双端各司其职，天然无锁冲突（分区内的 deque 锁粒度）；
- `RecordAppendResult.abortIfNewBatch`、`batchIsFull` 等标志决定是否唤醒 Sender；
- **压缩场景不使用 BufferPool**（压缩 buffer 大小不定，直接 allocate，batch 关闭后 GC 回收）。

### 3.3 batch 何时"就绪"（ready 的判定）

Sender 每轮调 `accumulator.ready(metadata, now)`，一个**分区**就绪的条件（满足其一）：

1. 队头 batch **写满**（`batch.size` = 16KB）；
2. 队头 batch **滞留超过 `linger.ms`**（默认 0，即立刻发送——你本地测试就是这个行为）；
3. batch 已被重试标记（立即重发）；
4. 有 batch 内存不足被 abort 过（`flush` 场景）。

结果 `ReadyCheckResult` 含：就绪分区集合 + **unknownLeaderTopics**（元数据缺失的 topic）+ nextReadyCheckDelayMs（ Sender 的 poll 超时依据）。

## 4. BufferPool：内存池

`buffer.memory`（默认 32MB）中，**只有等于 `batch.size` 的请求才走池化**：

| 操作 | 行为 |
|---|---|
| `allocate(size)` | size == batch.size → 从 `free` 队列取一块复用；否则堆上新建 |
| 池耗尽 | 调用线程**阻塞等待**（`max.block.ms` = 60s 超时抛 `BufferExhaustedException`） |
| `deallocate(buffer, size)` | batch 被 Sender 完成处理后归还（压缩 batch 直接丢弃给 GC） |
| `nonBlockingAvailableSize` | 池内可用量监控 |

设计动机：高频分配/释放 16KB 堆外/堆内 buffer 会带来 GC 压力，池化后稳态下**零分配**。

## 5. 分区选择与粘性分区

3.7 的 `KafkaProducer.partition`（`BuiltInPartitioner`）逻辑：

```mermaid
flowchart TD
    A{"record.partition() 已指定？"} -->|是| Z["直接用"]
    A -->|否| B{"partitioner.class 配置了？"}
    B -->|是| C["自定义partitioner.partition(...)"]
    B -->|"否（默认）"| D{"key 存在？"}
    D -->|"是（你的'k1'）"| E["murmur2(keyBytes) % 分区数<br/>（sticky分区缓存避开满分区）"]
    D -->|否| F["StickyPartitionCache.partition(topic)<br/>粘住一个分区持续写"]
    E --> Z2["返回分区号"]
    F --> Z2
    F -.->|"KIP-794：该分区攒满一个batch后<br/>切换到下一个分区（均匀打散）"| F
```

- **有 key**：`Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions`——同 key 恒定同分区（保序的前提）；
- **无 key**：粘性分区（3.3 起的 KIP-794 增强版）——不再每条消息轮换分区，而是粘住一个分区写满一个 batch 再切换，**减少小 batch、提高压缩率**，同时仍保证长期均匀。

## 6. Sender 线程主循环

```mermaid
flowchart TD
    A["Sender.run()（kafka-producer-network-thread）"] --> B["while (running) runOnce()"]
    B --> C["runOnce()"]
    C --> D{"transactionManager != null？<br/>（幂等/事务状态机优先处理）"}
    D -->|是| D1["maybeResolveSequences /<br/>InitProducerId等事务请求<br/>（发出后本轮结束）"]
    D -->|否| E["sendProducerData(now)"]
    D1 --> E2["client.poll(pollTimeout)"]
    E --> F1["① accumulator.ready()<br/>就绪分区→就绪broker节点"]
    F1 --> F2{"unknownLeaderTopics非空？"}
    F2 -->|是| F3["metadata.requestUpdate()<br/>（元数据缺失触发刷新）"]
    F2 -->|否| F4
    F3 --> F4["② 过滤未就绪节点<br/>client.ready(node)：<br/>连接未建立/正在连接的剔除"]
    F4 --> F5["③ accumulator.drain()<br/>按node取走batch（每节点≤max.request.size）<br/>关batch（禁止再append）"]
    F5 --> F6["max.in.flight=1? → mute被drain的分区<br/>（保序：同一分区同时只允许一个在途请求）"]
    F6 --> F7["④ expiredBatches：<br/>超过delivery.timeout.ms的批<br/>failBatch（TimeoutException）"]
    F7 --> F8["⑤ sendProduceRequests：<br/>按node组装ProduceRequest<br/>client.send()"]
    F8 --> F9["⑥ 返回pollTimeout<br/>（=min(下一批就绪时间, 批过期时间, 节点退避时间)）"]
    F9 --> E2
    E2 --> G["NetworkClient.poll内部处理响应：<br/>handleProduceResponse（第9节）"]
    G --> B
```

**drain 的细节**（`RecordAccumulator.drain`）：

- 遍历每个就绪 node 的其 leader 所在的所有分区，从各自 deque **队头**取 batch；
- 取出的 batch 被 `close()`（应用线程此后无法 append，只能新建后继 batch）；
- `max.in.flight.requests.per.connection=5`（默认）时，同 node 最多 5 个请求在途；配置为 1 且 `enable.idempotence=true` 时通过 **mute 分区**保证严格顺序；
- 单节点一次 drain 的总大小 ≤ `max.request.size`。

## 7. 元数据（ProducerMetadata）

`ProducerMetadata` 继承 `Metadata`，是"哪些分区、leader 在哪个 broker"的客户端缓存：

| 场景 | 行为 |
|---|---|
| `send()` 遇到未知 topic | `metadata.add(topic)` + `requestUpdate`，`awaitUpdate` 阻塞应用线程（≤ max.block.ms） |
| Sender 发现 unknownLeaderTopics | `requestUpdate(false)`，本轮先发 Metadata 请求 |
| 定时刷新 | `metadata.max.age.ms`（5min）后台更新 |
| 分区 leader 变更 | 收到 `NOT_LEADER_OR_FOLLOWER` 等错误 → `requestUpdate`，下一轮按新 leader 重路由 |
| 批量失效 | producer 侧响应处理发现 topic 元数据过期 → 相关 in-flight batch 重排入队 |

**`ProducerMetadata` 特有的"批次时间窗"优化**：记录每个 topic 的 `lastBatchCreatedTime`——topic 有活跃批次时（`metadataIdleMs` 内发过数据）,元数据更新可以更快满足，避免长时间等待。

```mermaid
sequenceDiagram
    participant UT as 应用线程
    participant MD as ProducerMetadata
    participant ST as Sender线程
    participant BK as Broker

    Note over UT: send("test-topic01"...) 首次
    UT->>MD: add("test-topic01") + requestUpdate
    UT->>MD: awaitUpdate(lastVersion, maxBlockMs)
    Note over UT: 应用线程阻塞
    ST->>BK: MetadataRequest(topics=["test-topic01"])
    BK-->>ST: MetadataResponse（分区/leader列表）
    ST->>MD: update(response) → version+1，notifyAll
    MD-->>UT: awaitUpdate返回（拿到分区数）
    Note over UT: 继续序列化→分区→append
    ST->>ST: 后续ready()即知leader=node0<br/>直接对该节点建连+发送
```

## 8. NetworkClient：请求收发与连接管理

`NetworkClient` 在 Sender 线程上下文里执行，包装 `KSelector`（与 broker 端 Processor 复用同一个 `Selector` 实现）：

```mermaid
flowchart LR
    subgraph NetworkClient["NetworkClient.poll(timeout, now)"]
        direction TB
        P1["① 处理超时/断连检测<br/>（metadata超时/请求超时）"]
        P2["② selector.poll()<br/>NIO读写（对应broker端KSelector）"]
        P3["③ handleCompletedSends<br/>（发出的inFlight登记）"]
        P4["④ handleCompletedReceives<br/>correlationId配对→处理响应"]
        P5["⑤ handleDisconnections<br/>该node所有inflight请求失败→batch重排"]
        P1 --> P2 --> P3 --> P4 --> P5
    end
```

- **连接状态机**：`node.ready()` 内部 `initiateConnect` 异步建连，`isReady` 才允许 send；连接失败按 `reconnect.backoff.ms`（指数退避到 `reconnect.backoff.max.ms`）重试；
- **请求配对**：`correlationId` 严格递增，响应按序到达（broker 端顺序保证），`inFlightRequests` 按 node 维护 ≤ `max.in.flight.requests.per.connection`；
- **DNS/多节点**：bootstrap.servers 只是"入口"——拉到元数据后，producer 直连各分区 leader 所在节点，与 bootstrap 列表无关。

## 9. 响应处理、重试与幂等

### 9.1 响应处理（Sender → ProducerBatch → Future）

```mermaid
sequenceDiagram
    participant BK as Broker
    participant NC as NetworkClient<br/>(Sender线程)
    participant SN as Sender
    participant PB as ProducerBatch
    participant FT as FutureRecordMetadata
    participant UT as 应用线程

    BK-->>NC: ProduceResponse（correlationId配对）
    NC-->>SN: handleCompletedReceives → completeProduceResponses
    SN->>SN: handleProduceResponse(batch, response)
    alt 成功（error=NONE）
        SN->>PB: batch.done(baseOffset, logAppendTime)<br/>produceFuture.set(baseOffset)
        PB->>FT: 逐条future.complete<br/>（thunk执行用户callback）
        FT-->>UT: send().get()返回RecordMetadata
        SN->>SN: completeBatch →<br/>accumulator.deallocate(batch)（归还BufferPool）
    else 可重试错误（NOT_LEADER/<br/>UNKNOWN_TOPIC/CORRUPT...）
        SN->>SN: canRetry(batch, response)?
        SN->>PB: 重新入队 accumulator.reenqueue<br/>（batch.retry=true，回队头）
        Note over PB: 下一轮ready视为就绪<br/>（可能等元数据刷新后发新leader）
    else 不可重试（INVALID_REQUIRED_ACKS/<br/>RECORD_TOO_LARGE...）
        SN->>PB: batch.done(exception)<br/>future.completeExceptionally
        FT-->>UT: send().get()抛ExecutionException
    else 超时（delivery.timeout.ms累计）
        SN->>PB: failBatch(TimeoutException)
    end
```

### 9.2 重试的关键细节

- **重试次数**：`retries`（默认 `Integer.MAX_VALUE`，3.0 起），由 `delivery.timeout.ms`（120s，含攒批+首次发送+所有重试）统一兜底；
- **重排入队**：失败的 batch `reenqueue` 回**队头**，保持分区顺序；
- **批拆分**：若 batch 重试时超过 `max.request.size`（元数据变更/压缩率变化），`splitAndReenqueue` 拆成多个小批——3.0 后对用户透明；
- **退避**：可重试错误按 `retry.backoff.ms`（100ms）延迟。

### 9.3 幂等（enable.idempotence=true，3.x 默认）

| 机制 | 实现 |
|---|---|
| broker 去重窗口 | 每个分区维护 `(producerId, baseSequence)`，`ProducerStateManager` 记录每个 pid 最近 5 个 batch 的元数据，重复 seq 拒收（`DUPLICATE_SEQUENCE_NUMBER` 静默成功） |
| producer 侧 | `TransactionManager` 分配递增 sequence，重试时**复用原 sequence**（不是新批），broker 才能识别为同一批 |
| 严格顺序 | `max.in.flight ≤ 5` 且 broker 按序响应，配合重试重排，保证**单分区不乱序不重复** |
| InitProducerId | 幂等模式启动时 Sender 自动向 broker 申请 producerId（broker 侧持久化到 `.snapshot`） |

## 10. close() 与优雅关闭

```mermaid
sequenceDiagram
    participant UT as 应用线程
    participant KP as KafkaProducer
    participant SN as Sender线程

    UT->>KP: close(timeout)
    Note over UT: flush阻塞？默认close()等Sender跑完
    UT->>SN: running=false + wakeup
    loop 直到 accumulator无未drain批 且 inflight=0
        SN->>SN: 继续runOnce()发完存量
    end
    SN->>SN: abortIncompleteBatches（仅forceClose时）
    SN->>KP: 关闭NetworkClient/指标
    UT->>KP: close()返回（所有Future已完成或失败）
    Note over UT: close(timeout)限时版<br/>超时后forceClose：残余batch全部异常完成
```

`close()` 默认无限等待发完存量（`Long.MAX_VALUE`）；`close(timeout)` 超时走 forceClose 路径，`abortIncompleteBatches` 让所有未完成 Future 抛异常——**保证 Future 终态化**，不会出现"程序退出了 send().get() 还挂着"。

## 11. 端到端时序总图

```mermaid
sequenceDiagram
    autonumber
    participant UT as 应用线程
    participant AC as RecordAccumulator
    participant MD as ProducerMetadata
    participant SN as Sender线程
    participant NC as NetworkClient/KSelector
    participant BK as Broker

    Note over UT,BK: ═══ 首次send：元数据准备 ═══
    UT->>UT: send(record)
    UT->>MD: add(topic)+requestUpdate→awaitUpdate
    SN->>BK: MetadataRequest
    BK-->>SN: 分区/leader信息
    SN->>MD: update → 唤醒UT

    Note over UT,AC: ═══ 序列化+分区+攒批（应用线程） ═══
    UT->>UT: serialize("k1"/"hello kafka")<br/>partition: murmur2("k1")%N
    UT->>AC: append→BufferPool取16KB<br/>写入RecordBatch
    AC-->>UT: FutureRecordMetadata（未发送）
    UT->>UT: .get()阻塞

    Note over SN,BK: ═══ Sender轮次（linger.ms=0立即触发） ═══
    SN->>AC: ready() → test-topic01-0就绪
    SN->>MD: leader→broker0
    SN->>NC: ready(node0)：首次需建TCP
    NC->>BK: TCP连接
    SN->>AC: drain() → 取走batch并close
    SN->>SN: 组装ProduceRequest(v10)<br/>size=132+4
    SN->>NC: send(request)→selector
    NC->>BK: ProduceRequest
    Note over BK: Acceptor→Processor→<br/>RequestChannel→Handler→<br/>KafkaApis→写日志
    BK-->>NC: ProduceResponse(base_offset)
    NC-->>SN: correlationId配对
    SN->>AC: batch.done+deallocate
    AC-->>UT: future.complete(RecordMetadata)
    Note over UT: .get()返回<br/>topic/分区/offset
```

## 12. 关键配置

| 配置 | 默认值 | 作用 | 阶段 |
|---|---|---|---|
| `bootstrap.servers` | — | 入口地址（仅用于首次元数据） | 建连 |
| `key/value.serializer` | — | 序列化器 | send |
| `buffer.memory` | 32MB | 缓冲区总大小，耗尽+超时抛 `BufferExhaustedException` | append |
| `batch.size` | 16KB | 单批大小上限；**也是池化buffer规格** | append/ready |
| `linger.ms` | 0 | 批滞留时间（0=立即发，调大提高攒批率） | ready |
| `partitioner.class` | 内置（3.7中DefaultPartitioner已标记弃用） | 分区策略（key murmur2 / 粘性） | partition |
| `max.block.ms` | 60s | 元数据获取+BufferPool等待的总阻塞上限 | send |
| `max.request.size` | 1MB | 单请求上限（批组总大小） | drain |
| `retries` / `delivery.timeout.ms` | MAX / 120s | 重试次数 / 总时限兜底 | 响应 |
| `retry.backoff.ms` | 100ms | 重试退避 | 响应 |
| `acks` | all（3.0起） | 0/1/all 可靠性等级 | 协议 |
| `enable.idempotence` | true（3.0起） | 幂等：pid+sequence | 协议 |
| `max.in.flight.requests.per.connection` | 5 | 单连接在途请求数（=1+幂等→严格保序） | NetworkClient |
| `compression.type` | none | none/gzip/snappy/lz4/zstd（批级压缩） | append |
| `metadata.max.age.ms` | 5min | 元数据强制刷新周期 | metadata |
| `request.timeout.ms` | 30s | 单请求超时 | NetworkClient |

**性能/可靠性调优的常见杠杆**：吞吐 → 增大 `batch.size` + `linger.ms` + 压缩；时延 → `linger.ms=0`（默认）+ `acks=1`；可靠 → `acks=all` + `retries` + 幂等（3.x 默认即最可靠配置）。

## 13. 调试断点指引

| 观察目标 | 断点位置 | 看什么 |
|---|---|---|
| send 主流程 | `KafkaProducer.send` / `doSend` | 拦截器、序列化、分区计算 |
| 元数据等待 | `ProducerMetadata.awaitUpdate` / `Sender` 发 MetadataRequest | 首次 send 的阻塞唤醒 |
| 攒批 | `RecordAccumulator.append` → `tryAppend` | 队尾批复用 vs 新建 |
| 内存池 | `BufferPool.allocate` / `deallocate` | free 队列、availableMemory |
| 分区选择 | `BuiltInPartitioner.partition` | murmur2、粘性缓存 |
| 就绪判定 | `RecordAccumulator.ready` | linger/满批逻辑 |
| 取批组包 | `RecordAccumulator.drain` → `Sender.sendProduceRequests` | 队头取出、ProduceRequest 组装 |
| 网络收发 | `NetworkClient.poll` / `Selector.poll` | 与 broker 端 KSelector 对称 |
| 响应回调 | `Sender.completeBatch` / `ProducerBatch.done` | 成功路径 + 重试分支 |
| 幂等申请 | `TransactionManager` 的 InitProducerId 相关 | producerId/epoch |
| 优雅关闭 | `KafkaProducer.close` / `Sender.run` 尾部 | 存量发完 + Future 终态化 |

**与你的 ProducerTest 对应的观察点**：

1. 首次 `send()` 在 `awaitUpdate` 断点停下——能看到应用线程等元数据；Resume 后 `drain` 断点命中——linger.ms=0 使批立即就绪；
2. `handleProduceResponse` 断点看 `baseOffset`（即 `recordMetadata.offset()` 的来源）；
3. 把 `linger.ms` 改成 100 再连续 send 5 条，在 `drain` 处观察一个 batch 里 5 条消息（RecordBatch 的 records>1）；
4. `enable.idempotence=false` 与 `true` 各跑一次，对比 `Sender.runOnce` 里 TransactionManager 分支是否进入。

---

## 附：一句话总结

**生产者发送 = 业务线程"序列化→分区→攒批进缓冲区"（微秒级返回 Future）+ Sender 线程"就绪判定→按 leader 取批→组装请求→NIO 收发→响应完成 Future / 失败重排"**——`send()` 与实际发送彻底解耦，`buffer.memory` 是反压边界，元数据是路由依据，幂等序列号是"至少一次升级到精确一次（单分区）"的关键。

# Kafka 副本同步机制深度源码解析（基于 3.7.0）

> 涉及核心源码：
> - `core/src/main/scala/kafka/server/AbstractFetcherThread.scala`
> - `core/src/main/scala/kafka/server/ReplicaFetcherThread.scala` / `ReplicaFetcherManager.scala`
> - `core/src/main/scala/kafka/server/LocalLeaderEndPoint.scala` / `LeaderEndPoint.scala`
> - `core/src/main/scala/kafka/cluster/Partition.scala` / `Replica.scala`
> - `core/src/main/scala/kafka/server/ReplicaManager.scala`
> - `core/src/main/scala/kafka/server/DelayedProduce.scala` / `DelayedFetch.scala`
> - `storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java`

---

## 一、整体架构概览

Kafka 副本同步采用 **Pull（拉取）模型**：每个 Follower 副本向 Leader 副本发送 Fetch 请求拉取消息，而不是 Leader 主动推送。

```mermaid
graph TB
    subgraph LB["Leader Broker"]
        LR["LocalLog<br/>(LEO=120, HW=100)"]
        EPOCH["LeaderEpochCache<br/>{0→0, 5→80, 8→95}"]
        PART["Partition<br/>维护 ISR={leader, follower1, follower2}"]
        DELAY["Purgatory<br/>DelayedProduce / DelayedFetch"]
    end

    subgraph FB1["Follower Broker 1"]
        F1["ReplicaFetcherThread<br/>LEO=100, HW=100"]
    end

    subgraph FB2["Follower Broker 2"]
        F2["ReplicaFetcherThread<br/>LEO=95, HW=95"]
    end

    F1 -- "Fetch(offset=100, epoch=8)" --> LR
    F2 -- "Fetch(offset=95, epoch=8)" --> LR
    LR -- "records + LeaderHW" --> F1
    LR -- "records + LeaderHW" --> F2
    PART --> LR
    EPOCH --> LR
    PART -. "ISR 变更提交" .-> DELAY
```

```mermaid
graph LR
    subgraph FollowerBroker["Follower Broker"]
        RFM["ReplicaFetcherManager"]
        RF1["ReplicaFetcherThread-1"]
        RF2["ReplicaFetcherThread-2"]
        RFM --> RF1
        RFM --> RF2
    end
    subgraph LeaderBroker["Leader Broker"]
        RA["RequestChannel / KafkaApis"]
        RM["ReplicaManager"]
        P1["Partition-A (Leader)"]
        P2["Partition-B (Leader)"]
        RA --> RM
        RM --> P1
        RM --> P2
    end
    RF1 -- "FetchRequest (分区按 Leader 归组)" --> RA
    RF2 -- "FetchRequest" --> RA
    P1 -- "FetchResponse(records, HW)" --> RF1
```

关键角色：

| 组件 | 职责 |
|---|---|
| `ReplicaFetcherManager` | 为每个 Broker 创建若干 `ReplicaFetcherThread`，管理分区到线程的映射 |
| `ReplicaFetcherThread` | 继承 `AbstractFetcherThread`，实现从 Leader 拉取、截断、写入本地日志 |
| `AbstractFetcherThread` | 通用拉取循环框架：截断检查 → 构建 Fetch → 处理响应（也用于 consumer/事务等） |
| `Partition` / `Replica` | 维护 LEO、HW、ISR、Leader Epoch 等状态 |
| `DelayedProduce` / `DelayedFetch` | purgatory（炼狱）延迟操作，实现 `acks=all` 等待与消费者长轮询 |

---

## 二、副本角色切换：becomeLeaderOrFollower

### 2.1 入口

Leader 变更（controller 通过 `LeaderAndIsr` 请求或 KRaft 的 `AlterPartition` 提交）后，Broker 端 `ReplicaManager.becomeLeaderOrFollower()` 被触发（`ReplicaManager.scala:1995` 附近），核心分两步：

```mermaid
flowchart TD
    A["收到 LeaderAndIsr 请求<br/>(KRaft: AlterPartition 提交)"] --> B["ReplicaManager.becomeLeaderOrFollower()"]
    B --> C{"对每个分区：<br/>本地副本是新 Leader?"}
    C -- 是 --> D["makeLeaders()"]
    C -- 否(变Follower) --> E["makeFollowers()"]
    D --> D1["removeFetcherForPartitions()<br/>停掉本分区拉取器"]
    D1 --> D2["partition.makeLeader()<br/>校验 epoch 更新"]
    D2 --> D3["LeaderEpochFileCache 追加<br/>(新epoch, 当前LEO)"]
    D3 --> D4["初始化 ISR 为 controller 下发集合"]
    D4 --> D5["完成被延迟的 Produce/Fetch"]
    E --> E1["partition.makeFollower()<br/>更新 leader id / epoch"]
    E1 --> E2["removeFetcherForPartitions()<br/>停掉旧拉取器"]
    E2 --> E3["完成延迟的 Produce/Fetch"]
    E3 --> E4["addFetcherForPartitions()<br/>initialFetchOffset=本地LEO"]
```

### 2.2 makeLeaders 流程（ReplicaManager.scala:2285 附近）

1. **停止该分区的拉取器**：`replicaFetcherManager.removeFetcherForPartitions(partitions)`——自己是 Leader，不再拉取；
2. 调用 `partition.makeLeader()`：
   - 校验请求中的 leader epoch 比本地新（防止过期请求）；
   - 更新 partition 的 leader epoch，并记录 `leaderEpochStartOffset`（该 epoch 开始时的 LEO）；
   - 在 `LeaderEpochFileCache` 中追加 `(新epoch, 当前LEO)` 条目；
   - 重新初始化 ISR 为 controller 下发的集合。
3. 尝试完成被延迟的 Produce/Fetch 请求。

### 2.3 makeFollowers 流程（ReplicaManager.scala:2367 附近）

1. `partition.makeFollower()`：更新 leader id、epoch，本地副本转为 Follower；
2. `removeFetcherForPartitions()` 停掉旧拉取器；
3. 完成所有延迟的 Produce/Fetch 请求（角色变了，之前等待的请求需重新判定）；
4. **为新的 Follower 分配/启动拉取线程**：`replicaFetcherManager.addFetcherForPartitions()`，初始拉取偏移由 `initialFetchOffset(log)` 决定（基于本地 LEO）。

> 拉取线程数由 `num.replica.fetchers`（默认 5... 实际默认 1，生产常调大）控制，分区按 Leader Broker 维度分配到不同线程，避免单线程瓶颈。

---

## 三、Follower 拉取主循环（AbstractFetcherThread）

### 3.1 doWork：截断优先于拉取

```scala
// AbstractFetcherThread.scala:111 附近
override def doWork(): Unit = {
  maybeTruncate()   // 1. 先做日志截断检查
  maybeFetch()      // 2. 再拉取数据
}
```

**为什么截断优先**：Leader 可能已经切换，Follower 本地日志末尾可能包含"孤儿数据"（旧 Leader 未同步到新 ISR 的消息，即 unclean 场景残留）。必须先对齐，否则后续拉取会出现数据分叉。

Follower 拉取主循环整体流程图：

```mermaid
flowchart TD
    START(["ReplicaFetcherThread 运行"]) --> TR["maybeTruncate()<br/>截断检查"]
    TR --> T1{"有分区需要<br/>leader epoch 对齐?"}
    T1 -- 是 --> T2["发 OffsetsForLeaderEpoch<br/>获取 epoch 边界 offset"]
    T2 --> T3["计算截断点<br/>getOffsetTruncationState()"]
    T3 --> T4["partition.truncateTo(offset)<br/>删除分叉数据"]
    T4 --> FE["maybeFetch()"]
    T1 -- 否 --> FE
    FE --> F1["leader.buildFetch()<br/>构建 FetchRequest<br/>(fetchOffset=LEO, currentLeaderEpoch)"]
    F1 --> F2{"所有分区<br/>都被限流?"}
    F2 -- 是 --> WAIT["backoff 等待"] --> FE
    F2 -- 否 --> F3["发送 Fetch 到 Leader"]
    F3 --> F4["processFetchRequest()<br/>处理响应"]
    F4 --> F5{"响应类型"}
    F5 -- "正常数据" --> F6["processPartitionData()<br/>appendAsFollower + 更新HW"]
    F5 -- "divergingEpoch" --> TR
    F5 -- "OFFSET_OUT_OF_RANGE" --> F7["handleOutOfRangeError()<br/>重新定位 offset"]
    F7 --> TR
    F5 -- "FENCED/UNKNOWN_LEADER_EPOCH" --> F8["等待 / 移出分区"]
    F5 -- "NOT_LEADER_OR_FOLLOWER" --> F9["分区移出拉取集合"]
    F6 --> LOOP["completeDelayedFetchRequests()<br/>回到下一轮 doWork"]
    F8 --> LOOP
    F9 --> LOOP
    LOOP --> TR
```

### 3.2 截断流程与 Leader Epoch（KIP-101 / KIP-279）

`maybeTruncate()` → `truncateToEpochEndOffsets()`：

1. Follower 从本地日志取**最新 leader epoch**（`latestEpoch()`，读取 `LeaderEpochFileCache`）；
2. 向 Leader 发送 **OffsetsForLeaderEpoch 请求**（`fetchEpochEndOffsets()`），询问"我这个 epoch 结束时的 offset 是多少"；
3. Leader 端（`LocalLeaderEndPoint.fetchEpochEndOffsets()` → `partition.lastOffsetForLeaderEpoch()`）查自己的 epoch cache 返回 `(epoch, endOffset)`；
4. Follower 根据 `getOffsetTruncationState()`（`AbstractFetcherThread.scala:565` 附近）计算截断点：

| Leader 返回情况 | Follower 动作 |
|---|---|
| `UNDEFINED_EPOCH_OFFSET` | 截断到本地 HW（follower 全新/无缓存） |
| follower 未知该 epoch（落后多个 epoch） | 用自己已知的最大 epoch 的 endOffset，截断到 `min(followerLEO, leaderOffset)` |
| 正常返回 | 截断到 `min(followerLEO, leaderEndOffset)` |

5. `truncate()` → `partition.truncateTo(offset)` 实际执行日志删除（超出 offset 的 segment 部分被丢弃）。

**Leader Epoch 解决了什么问题**（对比纯 HW 截断）：
- 旧的 HW 机制下，Follower 重启后只能截断到 HW，可能丢失已写入但未过 HW 的合法数据（造成数据丢失的 "replication 场景 bug"）；
- Epoch 机制下，Follower 精确询问 Leader "每个 epoch 边界处的 offset"，只截断真正分叉的数据，并且解决**副本永久不一致**与**重启后 Leader 选举数据丢失**两类问题。

截断 + 拉取同步的完整时序图：

```mermaid
sequenceDiagram
    autonumber
    participant F as Follower<br/>ReplicaFetcherThread
    participant L as Leader<br/>(LocalLeaderEndPoint)
    participant P as Leader端 Partition
    participant FL as Follower 本地日志

    Note over F: doWork(): maybeTruncate() 先行
    F->>L: OffsetsForLeaderEpochRequest<br/>(我最新 epoch=8, 询问结束 offset)
    L->>P: partition.lastOffsetForLeaderEpoch(8)
    P-->>L: (epoch=8, endOffset=110)
    L-->>F: OffsetsForLeaderEpochResponse

    alt Follower LEO=118 (unclean 残留)
        F->>FL: truncateTo(110) 丢弃 110~118
    else Follower LEO=95 (正常落后)
        Note over F: 无需截断
    end

    F->>L: Fetch(fetchOffset=95, currentLeaderEpoch=8, maxBytes)
    L->>P: 读日志 (可读到 LEO, 不受 HW 限制)
    P-->>L: records [95, 110) + leaderHW
    L-->>F: FetchResponse

    F->>FL: appendAsFollower(records)
    F->>FL: maybeUpdateHighWatermark(leaderHW)
    Note over F: 下轮 Fetch(fetchOffset=110) 继续
    F->>L: Fetch(110, ...) 循环直到追平
```

### 3.3 构建与发送 Fetch 请求

`maybeFetch()` → `leader.buildFetch()`（`LocalLeaderEndPoint.scala:164`），构建 `FetchRequest.PartitionData`，关键字段：

- `fetchOffset`：上次拉取结束位置（即本地 LEO）；
- `currentLeaderEpoch`：**Follower 已知的 Leader epoch**（用于 Leader 端校验，若过期返回 `FENCED_LEADER_EPOCH`，防止从旧 Leader 拉到脏数据）；
- `logStartOffset`：用于日志保留判断；
- `maxBytes`：受 `replica.fetch.max.bytes` 限制。

同时受**复制配额**（`replication.quota.window.num` 等，Leader 端 `QuotaFactory` 违约检测）约束：`buildFetch` 中若分区处于限流中，会跳过本次拉取。

### 3.4 processFetchRequest：处理响应（AbstractFetcherThread.scala:309 附近）

对每个分区的响应分支处理：

| 错误/状态 | 处理 |
|---|---|
| 正常数据 | 交给 `processPartitionData()` 写入本地日志 |
| `divergingEpoch`（KIP-279） | Leader 发现 Follower 的 epoch 落后，返回分叉点；Follower 标记分区需截断，下轮循环执行 |
| `OFFSET_OUT_OF_RANGE` | `handleOutOfRangeError()`（:718 附近）重新定位 offset（见 3.6） |
| `UNKNOWN_LEADER_EPOCH` / `FENCED_LEADER_EPOCH` | Follower 的 epoch 比 Leader 还新/过期 → 等待重新同步 |
| `NOT_LEADER_OR_FOLLOWER` | 自己已不是 Leader → 分区移出拉取集合 |

### 3.5 processPartitionData：写入本地日志（ReplicaFetcherThread.scala:103 附近）

```scala
// 关键步骤（简化）
if (fetchOffset != log.logEndOffset) throw IllegalStateException  // 严格校验
partition.appendRecordsToFollowerOrFutureReplica(records, ...)    // 追加到本地日志
log.maybeUpdateHighWatermark(partitionData.highWatermark)         // 用 Leader 的 HW 更新本地 HW
log.maybeIncrementLogStartOffset(leaderLogStartOffset)            // 对齐日志起始偏移
```

要点：
1. **Follower 的 HW 来自 Leader 响应中携带的 HW 值**（Leader 会把 ISR 最小 LEO 作为 HW 随 Fetch 响应返回），Follower 自己从不"计算" HW，只是接收并单调递增更新（`maybeUpdateHighWatermark` 保证不回退）；
2. 写入走 `UnifiedLog.appendAsFollower()`，与生产者写入（`appendAsLeader`）不同：**不分配 offset、不更新 producer epoch 相关分配逻辑**，直接按 Leader 已分配好的 offset 追加。

### 3.6 handleOutOfRangeError：偏移越界的两种场景

Follower 请求的 `fetchOffset` 超出 Leader 日志范围时：

1. **Unclean Leader 选举场景**：Follower（旧 Leader 的同步者）LEO > 新 Leader LEO → 拿 Leader 的 `logEndOffset`，**截断到 Leader 的 LEO**，丢弃多出来的消息；
2. **数据被删除场景**：Follower 落后太多，本地日志起始位点比 Leader 的 log start offset 还旧（或 fetchOffset < Leader logStartOffset）→ 直接从 Leader 的 `logStartOffset` 重新拉取（必要时截断本地日志）。

---

## 四、Leader 端：ISR 维护与 HW 推进（Partition.scala）

### 4.1 LEO 与 HW 的关系

- **LEO（Log End Offset）**：每个副本本地日志下一条待写入的 offset；
- **HW（High Watermark）**：**ISR 中所有副本（含 Leader）LEO 的最小值**，是"已成功复制到所有同步副本"的分界线；消费者最多只能读到 HW。

```mermaid
graph LR
    subgraph Leader["Leader (LEO=120)"]
        direction LR
        A0["0"] --- A1["..."] --- A2["HW?"] --- A3["LEO=120"]
    end
    subgraph F1["Follower1 (LEO=100)"]
        direction LR
        B0["0"] --- B1["..."] --- B2["LEO=100"]
    end
    subgraph F2["Follower2 (LEO=95)"]
        direction LR
        C0["0"] --- C1["..."] --- C2["LEO=95"]
    end
    F2 -.->|"ISR 中 LEO 最小<br/>HW = 95"| Leader
```

HW 的推进与消费边界示意：

```mermaid
flowchart LR
    W["Producer 写入<br/>Leader LEO 95→120"] --> HW["HW 推进条件:<br/>所有 ISR 副本 LEO ≥ 120"]
    HW --> UP["HW: 95 → 120"]
    UP --> C["消费者可读范围扩展到 120<br/>(READ_COMMITTED 则看 LSO)"]
```

### 4.2 updateFollowerFetchState：Follower 拉取时 Leader 记录状态

Leader 处理 Follower 的 Fetch 请求时（`ReplicaManager.fetchMessages` → `Partition.updateFollowerFetchState`）：

1. 校验 `fetchOffset`：
   - `fetchOffset > leaderLEO` → 返回 `OFFSET_OUT_OF_RANGE`；
   - `fetchOffset + fetchedBytes == leaderLEO` → 更新该副本的 **`lastCaughtUpTimeMs`**（追上 Leader 了）；
   - `fetchOffset >= leaderHW` → 该 Follower 追上了 HW；
2. 记录 `lastFetchTimeMs`；
3. 调用 **`maybeExpandIsr()`** 尝试把该 Follower 加入 ISR。

### 4.3 ISR 扩容：maybeExpandIsr（Partition.scala:1006 附近）

Follower 满足以下条件即加入 ISR（`isFollowerInSync()`，:1037 附近）：

```
followerLEO >= leaderHW                      // 追上了高水位
&& followerLEO >= leaderEpochStartOffset     // 确保拉过了当前 epoch 的起点（KIP-797）
&& isReplicaIsrEligible()                    // broker 存活/未被 fencing（KRaft 校验 broker epoch）
```

满足则进入 `PendingExpandIsr` 状态，并通过 `AlterPartitionManager` 向 controller（KRaft）提交新的 ISR，controller 持久化到 metadata log 后返回，状态变为 `CommittedPartitionState`（ZK 模式下写入 `/brokers/topics/.../state` znode）。**提交是异步的**，因此存在 pending 状态锁保护。

ISR 扩容时序图（KRaft 模式）：

```mermaid
sequenceDiagram
    autonumber
    participant F as Follower
    participant L as Leader Partition
    participant AP as AlterPartitionManager
    participant C as Controller (KRaft)

    F->>L: Fetch(fetchOffset=110) 追上 leaderHW
    L->>L: updateFollowerFetchState()<br/>更新 lastCaughtUpTimeMs
    L->>L: maybeExpandIsr() 判定:
    Note right of L: followerLEO(110) >= leaderHW(100) ✓<br/>followerLEO >= leaderEpochStartOffset ✓<br/>broker 未被 fencing ✓
    L->>AP: PendingExpandIsr (ISR 加入 F)
    AP->>C: AlterPartition 请求 (新 ISR)
    C->>C: 持久化到 metadata log
    C-->>AP: 新 ISR 生效
    AP-->>L: CommittedPartitionState
    Note over L: inSyncReplicaIds 更新完成
```

### 4.4 ISR 收缩：maybeShrinkIsr（Partition.scala:1227 附近）

由 `ReplicaManager` 上的定时任务驱动，周期为 `replica.lag.time.max.ms / 2`（默认 30000/2 = 15s），对每个分区调用 `maybeShrinkIsr()` → `getOutOfSyncReplicas()`：

判定某副本落后（`isFollowerOutOfSync()`，:1271 附近）的依据是 **`isCaughtUp`**：

```scala
def isCaughtUp(leaderEndOffset, currentTimeMs, replicaMaxLagMs): Boolean =
  leaderEndOffset == logEndOffset ||              // 已完全追上
  currentTimeMs - lastCaughtUpTimeMs <= replicaMaxLagMs  // 最近 replicaLagTimeMaxMs 内追上过一次
```

区分两种落后者：
- **Stuck follower**：长时间没有发 Fetch（网络/进程故障）；
- **Slow follower**：一直在 Fetch 但始终追不上 Leader 的 LEO。

任一情况超过 `replica.lag.time.max.ms`（默认 30s）→ 移出 ISR（同样走 `PendingShrinkIsr` → controller 提交流程）。

ISR 收缩流程图：

```mermaid
flowchart TD
    T["ReplicaManager 定时任务<br/>周期 = replica.lag.time.max.ms / 2 (默认15s)"] --> S["maybeShrinkIsr()"]
    S --> G["getOutOfSyncReplicas()<br/>遍历 ISR 中每个 follower"]
    G --> Q{"isCaughtUp?<br/>LEO追平 或 <br/>now - lastCaughtUpTimeMs <= 30s"}
    Q -- 否 --> SHRINK["PendingShrinkIsr<br/>从 ISR 移除"]
    Q -- 是 --> KEEP["保留在 ISR"]
    SHRINK --> CT["AlterPartitionManager<br/>→ Controller 提交新 ISR"]
    CT --> HW["maybeIncrementLeaderHW()<br/>重算 HW = min(剩余ISR LEO)"]
    HW --> DP["触发 DelayedProduce 完成检查<br/>(min.insync.replicas 校验)"]
```

PartitionState 状态机（ISR 变更的生命周期）：

```mermaid
stateDiagram-v2
    [*] --> Committed: 初始/角色切换
    Committed --> PendingExpand: Follower 追上 HW<br/>maybeExpandIsr
    Committed --> PendingShrink: 副本超时落后<br/>maybeShrinkIsr
    PendingExpand --> Committed: Controller 提交成功
    PendingExpand --> Committed: 提交失败回滚
    PendingShrink --> Committed: Controller 提交成功
    PendingExpand --> PendingShrink: 追赶中的副本又落后
    note right of Committed
        CommittedPartitionState:
        isr: Set[Int]
        leaderRecoveryState
    end note
```

> 注意：判断依据是**时间**而非消息数（旧版本 0.9 之前用 `replica.lag.max.messages`，因流量突增易误判已废弃）。

### 4.5 HW 推进：maybeIncrementLeaderHW（Partition.scala:1150 附近）

在**Leader 本地写入完成**和**收到 Follower Fetch（更新其 LEO）**时都会尝试推进 HW：

```
newHW = min(所有"同步副本"的 LEO)
```

细节：
1. 只有 ISR 收缩后仍满足条件才会推进（若 ISR 过小，HW 停滞，配合 `min.insync.replicas` 起保护作用）；
2. "同步副本"采用 **maximal ISR**：ISR ∪ 正在追赶（满足 `isCaughtUp`）的副本（KIP-497，避免 ISR 收缩瞬间 HW 回退、导致消费者读到的数据"消失"）；
3. HW 推进后会触发 `DelayedFetch` / `DelayedProduce` 的完成检查。

---

## 五、acks=all 的写入等待：DelayedProduce

### 5.1 写入路径

```mermaid
flowchart TD
    A["KafkaApis.handleProduceRequest"] --> B["ReplicaManager.appendRecords()<br/>(ReplicaManager.scala:765)"]
    B --> C["appendToLocalLog()"]
    C --> D["Partition.appendRecordsToLeader()<br/>(Partition.scala:1349)"]
    D --> E{"acks == -1 且<br/>ISR.size < min.insync.replicas?"}
    E -- 是 --> ERR["抛 NotEnoughReplicasException<br/>直接拒绝写入"]
    E -- 否 --> F["log.appendAsLeader(records, leaderEpoch)"]
    F --> G["maybeIncrementLeaderHW()"]
    G --> H{"requiredAcks?"}
    H -- "1" --> R1["本地写成功即返回"]
    H -- "0" --> R0["不等本地日志直接返回"]
    H -- "-1 (all)" --> I["创建 DelayedProduce<br/>放入 Purgatory 等待"]
```

### 5.2 DelayedProduce 的完成条件（DelayedProduce.scala:91 附近 `tryComplete`）

对请求中每个分区，满足任一即完成：
1. 分区不再是 Leader / 副本下线 → 返回 `NOT_LEADER_OR_FOLLOWER` 等错误；
2. **`partition.checkEnoughReplicasReachOffset(requiredOffset)`**：**ISR 中所有副本的 LEO 都 ≥ 该批消息的结束 offset**——即消息已被整个 ISR 复制；
3. 超时（`request.timeout.ms`）→ 视 acks 配置返回成功或失败。

事件驱动：每个 Follower 的 Fetch 到来导致其 LEO 增长 → 触发 `DelayedProduce` 的完成检查，无需轮询。

```
Producer(acks=all) ──produce──▶ Leader 写入 (LEO 95→120)
                                  │
                                  ├── Follower1 fetch → LEO=120 ✅
                                  └── Follower2 fetch → LEO=120 ✅
                                         │
                          HW: 95 ──▶ 120 ──▶ DelayedProduce 完成，响应 Producer
```

用完整时序图呈现 acks=all 的端到端等待过程：

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer (acks=all)
    participant L as Leader
    participant F1 as Follower1
    participant F2 as Follower2
    participant PU as Purgatory<br/>(DelayedProduce)

    P->>L: Produce(records, acks=-1)
    L->>L: appendAsLeader (LEO 95→120)
    L->>PU: DelayedProduce(requiredOffset=120)

    par Follower1 复制
        F1->>L: Fetch(offset=95)
        L-->>F1: records [95,120)
        F1->>F1: appendAsFollower (LEO=120)
        F1->>L: Fetch(offset=120)
        L->>L: 更新 F1 LEO=120
    and Follower2 复制
        F2->>L: Fetch(offset=95)
        L-->>F2: records [95,120)
        F2->>F2: appendAsFollower (LEO=120)
        F2->>L: Fetch(offset=120)
        L->>L: 更新 F2 LEO=120
    end

    L->>L: checkEnoughReplicasReachOffset(120)<br/>ISR 所有副本 LEO ≥ 120 ✓
    L->>L: HW 95 → 120
    L->>PU: tryComplete → 强制完成
    PU-->>P: ProduceResponse (成功)
    Note over P: 只有此刻消息才对消费者可见
```

---

## 六、消费者读请求与 HW：DelayedFetch

`KafkaApis.handleFetchRequest` → `ReplicaManager.fetchMessages()`：

1. 根据 `isolation.level` 决定可读上界（`DelayedFetch.scala:85` 附近）：

| 隔离级别 | 可读上界 |
|---|---|
| `READ_UNCOMMITTED`（默认） | **highWatermark** |
| `READ_COMMITTED` | **lastStableOffset**（HW 与最早未完成事务 offset 的较小者） |
| 副本拉取（内部） | logEndOffset（Follower 可以读到 LEO，方便快速追上并计入 ISR 判定） |

2. 若当前数据不足（`endOffset <= fetchOffset` 且未满足 `minBytes`/`maxWaitMs`），将请求封装为 `DelayedFetch` 挂入 purgatory，等待 Follower 拉取推进 HW 后被唤醒。

```mermaid
flowchart TD
    A["Consumer Fetch 请求"] --> B["ReplicaManager.fetchMessages()"]
    B --> C{"isolation.level?"}
    C -- READ_UNCOMMITTED --> D["上界 = highWatermark"]
    C -- READ_COMMITTED --> E["上界 = lastStableOffset<br/>= min(HW, 最早未完成事务offset)"]
    C -- 副本内部拉取 --> F["上界 = logEndOffset<br/>(Follower 可读到 LEO)"]
    D --> G{"fetchOffset + minBytes<br/>是否已有足够数据?"}
    E --> G
    G -- 否 --> H["DelayedFetch 挂入 Purgatory<br/>(最长 maxWaitMs=500ms)"]
    H --> I["Follower Fetch 推进 HW<br/>→ 唤醒 DelayedFetch"]
    I --> J["返回数据给 Consumer"]
    G -- 是 --> J
    F --> K["直接返回到 LEO 的数据<br/>给 Follower (不计入消费)"]
```

这就是**消费者永远读不到未同步（超过 HW）消息**的底层原因——保证在 acks=all 语义下，消费者读到的消息不会因 Leader 切换而丢失。

---

## 七、Leader Epoch 文件与故障恢复细节

### 7.1 LeaderEpochFileCache

位置：`storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java`，每个分区一个 `.log` 目录旁的 `leader-epoch-checkpoint` 文件，内容为 `(epoch, startOffset)` 序列：

```
0 0
5 80
8 95
```

- `makeLeader` 时追加 `(newEpoch, 当前LEO)`；
- 每次日志截断/删除时同步裁剪 epoch cache（保证 cache 与日志一致）。

### 7.2 一次完整的故障切换时间线

```
T0: Leader A(epoch=8) LEO=120, HW=100
    Follower B LEO=110 (ISR), Follower C LEO=95 (ISR)
T1: A 宕机。C 险些落后，但 lastCaughtUpTimeMs 在 30s 内，仍在 ISR
T2: Controller 选举 B 为 Leader(epoch=9)，ISR={B,C}
T3: C 的 ReplicaFetcherThread 向 B 发 Fetch(带 currentLeaderEpoch=8)
    → B 返回 FENCED_LEADER_EPOCH / divergingEpoch
    → C 发送 OffsetsForLeaderEpoch(epoch=8)
    → B 回答 "epoch 8 结束于 110"（B 视角 epoch9 起点是110）
    → C 发现自己 LEO=95 < 110，无需截断，直接从 95 继续拉取
    → 若 C 的 LEO 曾是 118（unclean 场景残留），则截断到 110，丢弃 110~118
T4: C 追上 B 的 HW → maybeExpandIsr → ISR={B,C} 恢复
```

对应的故障切换时序图：

```mermaid
sequenceDiagram
    autonumber
    participant A as Broker A<br/>(旧Leader, epoch=8)
    participant CT as Controller
    participant B as Broker B<br/>(新Leader, epoch=9)
    participant C as Broker C<br/>(Follower, LEO=95)

    Note over A: T0: A LEO=120, HW=100<br/>B LEO=110 (ISR), C LEO=95 (ISR)
    A--xC: T1: A 宕机
    CT->>CT: 检测 A 下线<br/>(KRaft: 收到 UnregisterBroker)
    CT->>B: T2: LeaderAndIsr<br/>B 当选 Leader (epoch=9), ISR={B,C}
    B->>B: makeLeader(): epoch cache 追加 (9, LEO=110)

    Note over C: T3: C 发现 Leader 变更
    C->>B: Fetch(fetchOffset=95, currentLeaderEpoch=8)
    B-->>C: divergingEpoch / FENCED_LEADER_EPOCH
    C->>B: OffsetsForLeaderEpoch(epoch=8)
    B->>B: 查 epoch cache: epoch8 结束于 110
    B-->>C: (epoch=8, endOffset=110)
    alt C LEO=95 (正常)
        Note over C: 无需截断, 从 95 继续拉取
    else C LEO=118 (unclean 残留)
        C->>C: truncateTo(110) 丢弃 110~118
    end
    C->>B: Fetch(95, epoch=9)
    B-->>C: records + HW
    Note over C: T4: C LEO >= B 的 HW
    B->>B: maybeExpandIsr()
    B->>CT: AlterPartition: ISR={B,C}
    CT-->>B: 提交成功, ISR 恢复
```

---

## 八、关键配置汇总

| 参数 | 默认值 | 作用 |
|---|---|---|
| `replica.lag.time.max.ms` | 30000 | 副本未在此时间内追上 Leader 即被移出 ISR；也是 ISR 收缩任务周期的一半基准 |
| `min.insync.replicas` | 1 | `acks=all` 写入要求的最小 ISR 数，不满足则抛 `NotEnoughReplicasException` |
| `num.replica.fetchers` | 1 | 每个 Broker 的 Follower 拉取线程数 |
| `replica.fetch.max.bytes` | 1MB | 单分区单次拉取上限（软限制） |
| `replica.fetch.response.max.bytes` | 10MB | 整个 Fetch 响应大小上限 |
| `replica.fetch.wait.max.ms` | 500 | Leader 端 Fetch 请求等待数据的最大时间（长轮询） |
| `unclean.leader.election.enable` | false | 是否允许非 ISR 副本当 Leader（允许则可能丢数据） |
| `unclean.leader.election.auto.enable`（3.7 新增 KIP-951） | false | 是否允许 controller 自动执行 unclean 选举 |

---

## 九、总结：一图串联全流程

```mermaid
flowchart TD
    subgraph 写入路径
        PR["Producer (acks=all)"] -->|"ProduceRequest"| AL["Leader: appendAsLeader"]
        AL --> IH["maybeIncrementLeaderHW"]
        AL --> DP["DelayedProduce<br/>等待 ISR 全部 LEO ≥ offset"]
        DP -->|"ISR 到齐 / 超时"| PR
    end

    subgraph 复制路径
        FT["Follower<br/>AbstractFetcherThread"] -->|"Fetch(offset, epoch)"| UR["Leader: updateFollowerFetchState"]
        UR --> EX["maybeExpandIsr<br/>(LEO >= HW → 加入 ISR)"]
        EX -->|"AlterPartition"| CTRL["Controller (KRaft)"]
        CTRL -->|"提交新 ISR"| EX
        SH["定时任务 maybeShrinkIsr<br/>(30s 未追上 → 移出 ISR)"] -->|"AlterPartition"| CTRL
        UR --> IH2["HW = min(同步副本 LEO)"]
        IH2 --> DP2["唤醒 DelayedProduce / DelayedFetch"]
    end

    subgraph 恢复路径
        TR["maybeTruncate<br/>(OffsetsForLeaderEpoch 对齐)"] --> AF["appendAsFollower"]
        UR -->|"records + LeaderHW"| FT
        FT --> TR
    end

    subgraph 消费路径
        CU["Consumer"] -->|"FetchRequest"| DF["DelayedFetch<br/>(可读上界 = HW / LSO)"]
        IH2 --> DF
        DF -->|"数据就绪"| CU
    end
```

**核心设计思想**：
1. **Pull 模型**让 Follower 自己控制节奏，天然支持限流与背压；
2. **Leader Epoch + checkpoint** 替代纯 HW 截断，消除数据不一致与丢失；
3. **ISR 动态伸缩**在一致性与可用性间取得平衡（对比 Quorum/多数派：Kafka 只需 ISR=1 也可写入，配合 `min.insync.replicas` 可调）；
4. **purgatory 延迟操作**将写等待/读等待事件化，避免轮询开销。

# RocketMQ 4.9.8 生产者启动与消息发送源码深度分析

> 基于 rocketmq-4.9.8 源码（client 模块），所有分析均对应到具体类与方法。

---

## 目录

1. [整体架构](#1-整体架构)
2. [核心类与配置默认值](#2-核心类与配置默认值)
3. [生产者启动流程](#3-生产者启动流程)
4. [MQClientInstance 启动详解](#4-mqclientinstance-启动详解)
5. [消息发送总体流程（sendDefaultImpl）](#5-消息发送总体流程)
6. [主题路由获取（tryToFindTopicPublishInfo）](#6-主题路由获取)
7. [队列选择与容错策略（MQFaultStrategy）](#7-队列选择与容错策略)
8. [消息发送核心（sendKernelImpl）](#8-消息发送核心)
9. [底层网络通信（MQClientAPIImpl.sendMessage）](#9-底层网络通信)
10. [异步发送与 Oneway 发送](#10-异步发送与-oneway-发送)
11. [批量消息发送](#11-批量消息发送)
12. [总结](#12-总结)

---

## 1. 整体架构

### 1.1 生产者客户端架构图

```mermaid
flowchart TB
    subgraph 用户层
        A[DefaultMQProducer<br/>用户直接使用的门面类]
        B[TransactionMQProducer<br/>事务生产者]
    end

    subgraph 实现层
        C[DefaultMQProducerImpl<br/>生产者核心实现<br/>发送/重试/选择队列]
        D[MQFaultStrategy<br/>故障规避策略]
        E[TopicPublishInfo<br/>主题路由缓存 + 轮询索引]
        F[LatencyFaultToleranceImpl<br/>broker 延迟熔断表]
    end

    subgraph 共享实例层
        G[MQClientManager<br/>单例, 缓存 MQClientInstance]
        H[MQClientInstance<br/>同一 clientId 共享<br/>网络/定时任务/服务]
        I[MQClientAPIImpl<br/>网络 API 门面]
        J[NettyRemotingClient<br/>Netty 网络层]
        K[pullMessageService<br/>rebalanceService<br/>scheduledExecutorService]
    end

    subgraph 服务端
        L[NameServer 集群<br/>路由注册与发现]
        M[Broker Master]
        N[Broker Slave]
    end

    A --> C
    B --> A
    C --> D
    D --> F
    C --> E
    A -->|start| G
    G --> H
    H --> I
    I --> J
    H --> K
    C -->|sendMessage| I
    J -->|获取路由| L
    J -->|SEND_MESSAGE| M
    M -.->|主从同步| N
```

### 1.2 关键设计思想

| 设计 | 说明 |
|---|---|
| 门面模式 | `DefaultMQProducer` 只是 API 门面，真正的逻辑在 `DefaultMQProducerImpl` |
| 实例共享 | 同一 `clientId`（IP@instanceName）的 Producer/Consumer **共享同一个 `MQClientInstance`**，由 `MQClientManager` 缓存 |
| 读写分离 | Producer 只与 Master 通信（`findBrokerAddressInPublish` 只取 `MASTER_ID`） |
| 路由本地缓存 | 路由信息从 NameServer 拉取后缓存在 `topicPublishInfoTable`，定时刷新 + 发送时兜底刷新 |

---

## 2. 核心类与配置默认值

### 2.1 DefaultMQProducer 关键默认配置

`client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java`

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `sendMsgTimeout` | 3000ms | 同步发送超时时间 |
| `compressMsgBodyOverHowmuch` | 4096 (4KB) | 消息体超过此值才压缩 |
| `retryTimesWhenSendFailed` | 2 | 同步发送失败重试次数（总尝试 = 1 + 2 = 3） |
| `retryTimesWhenSendAsyncFailed` | 2 | 异步发送失败重试次数 |
| `retryAnotherBrokerWhenNotStoreOK` | false | 非 SEND_OK 时是否换 broker 重发 |
| `maxMessageSize` | 4MB | 消息最大限制 |
| `sendMessageWithVIPChannel` | true(4.x) | — | VIP 通道（端口 -2，即 10911→10909） |
| `createTopicKey` | TBW102 | — | 自动创建 topic 时使用的默认主题 |

### 2.2 涉及的核心类

| 类 | 路径 | 职责 |
|---|---|---|
| `DefaultMQProducer` | `client/producer/DefaultMQProducer.java` | 门面 API |
| `DefaultMQProducerImpl` | `client/impl/producer/DefaultMQProducerImpl.java` | 发送核心逻辑 |
| `TopicPublishInfo` | `client/impl/producer/TopicPublishInfo.java` | 路由缓存 + 队列轮询 |
| `MQFaultStrategy` | `client/latency/MQFaultStrategy.java` | 延迟故障规避 |
| `LatencyFaultToleranceImpl` | `client/latency/LatencyFaultToleranceImpl.java` | broker 熔断状态表 |
| `MQClientInstance` | `client/impl/factory/MQClientInstance.java` | 客户端共享实例 |
| `MQClientAPIImpl` | `client/impl/MQClientAPIImpl.java` | 协议封装层 |
| `NettyRemotingClient` | `remoting/.../NettyRemotingClient.java` | Netty 网络层 |

---

## 3. 生产者启动流程

### 3.1 启动流程图

```mermaid
flowchart TD
    S1[用户调用 producer.start] --> S2[DefaultMQProducer.start<br/>setProducerGroup 校验后委托]
    S2 --> S3[DefaultMQProducerImpl.start startFactory=true]
    S3 --> S4{serviceState?}
    S4 -- CREATE_JUST --> S5[serviceState = START_FAILED<br/>防止并发重复启动]
    S4 -- RUNNING/START_FAILED/SHUTDOWN_ALREADY --> ERR[抛 MQClientException<br/>不能重复启动]
    S5 --> S6[checkConfig<br/>校验 group 合法性且 != DEFAULT_PRODUCER_GROUP]
    S6 --> S7[changeInstanceNameToPID<br/>非内部producer时 instanceName = 进程PID]
    S7 --> S8["MQClientManager.getInstance()<br/>.getOrCreateMQClientInstance()"]
    S8 --> S9{registerProducer 成功?}
    S9 -- 否(同group已存在) --> S10[回滚状态为 CREATE_JUST<br/>抛 group 重复异常]
    S9 -- 是 --> S11["topicPublishInfoTable.put<br/>(createTopicKey=TBW102, 空TopicPublishInfo)"]
    S11 --> S12[startFactory=true → mQClientFactory.start]
    S12 --> S13[serviceState = RUNNING<br/>日志: producer start OK]
    S13 --> S14[sendHeartbeatToAllBrokerWithLock<br/>立即发一次心跳]
    S14 --> S15[RequestFutureHolder<br/>启动超时请求清理任务]
```

### 3.2 启动时序图

```mermaid
sequenceDiagram
    participant U as 用户
    participant P as DefaultMQProducer
    participant PI as DefaultMQProducerImpl
    participant MGR as MQClientManager
    participant CI as MQClientInstance
    participant API as MQClientAPIImpl
    participant NS as NameServer

    U->>P: start()
    P->>PI: start(true)
    PI->>PI: 状态机校验 CREATE_JUST → START_FAILED
    PI->>PI: checkConfig() 校验 producerGroup
    PI->>PI: changeInstanceNameToPID()
    PI->>MGR: getOrCreateMQClientInstance(clientConfig, rpcHook)
    Note over MGR: clientId = IP@instanceName<br/>相同 clientId 复用同一实例
    MGR->>CI: new MQClientInstance()
    Note over CI: 构造 NettyRemotingClient<br/>初始化 pullMessageService 等
    MGR-->>PI: 返回 MQClientInstance
    PI->>CI: registerProducer(group, this)
    alt group 已注册
        CI-->>PI: false → 抛 GROUP_NAME_DUPLICATE 异常
    end
    PI->>CI: topicPublishInfoTable.put(TBW102, 空)
    PI->>CI: start()
    CI->>CI: fetchNameServerAddr() (若未配置地址)
    CI->>API: start() (启动 Netty client)
    CI->>CI: startScheduledTask() (5个定时任务)
    CI->>CI: pullMessageService.start()
    CI->>CI: rebalanceService.start()
    CI->>CI: 内部 defaultProducer.start(false)
    CI-->>PI: RUNNING
    PI->>CI: sendHeartbeatToAllBrokerWithLock()
    CI->>NS: 心跳(此时路由为空,实际无broker可发)
    PI->>PI: RequestFutureHolder 定时任务
    PI-->>U: 启动完成
```

### 3.3 关键源码：DefaultMQProducerImpl.start()

```java
// DefaultMQProducerImpl.java
public void start(final boolean startFactory) throws MQClientException {
    switch (this.serviceState) {
        case CREATE_JUST:
            this.serviceState = ServiceState.START_FAILED;   // 先置失败，防止启动过程中被再次调用

            this.checkConfig();                               // 校验 group

            if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                this.defaultMQProducer.changeInstanceNameToPID(); // instanceName → PID，保证同机多进程 clientId 不冲突
            }

            // 获取/创建共享的 MQClientInstance
            this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

            // 按 producerGroup 注册到实例
            boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;   // 回滚状态
                throw new MQClientException("The producer group[...] has been created before...");
            }

            // 预置默认主题 TBW102 的空路由占位
            this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

            if (startFactory) {
                mQClientFactory.start();                       // 启动共享客户端实例
            }

            this.serviceState = ServiceState.RUNNING;
            break;
        ...
    }
    this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();    // 立即心跳
    RequestFutureHolder.getInstance().startScheduledTask(this); // 请求超时清理
}
```

### 3.4 ServiceState 状态机

```mermaid
stateDiagram-v2
    [*] --> CREATE_JUST: new DefaultMQProducer()
    CREATE_JUST --> START_FAILED: start()进入,防重入
    START_FAILED --> RUNNING: 一切注册/启动成功
    START_FAILED --> CREATE_JUST: registerProducer失败回滚
    RUNNING --> SHUTDOWN_ALREADY: shutdown()
    RUNNING --> RUNNING: 再次start() → 抛异常(不允许)
```

### 3.5 MQClientManager 的实例复用

```java
// MQClientManager.getOrCreateMQClientInstance()
String clientId = clientConfig.buildMQClientId();   // IP@instanceName[@unitName]
MQClientInstance instance = this.factoryTable.get(clientId);
if (null != instance) {
    return instance;                                  // 复用
}
instance = new MQClientInstance(clientConfig, this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
```

- **复用意义**：同一个 JVM 中，多个 Producer/Consumer 只要 `clientId` 相同，就共享一个 `MQClientInstance`，即共享 Netty 连接、定时任务、拉取/平衡线程。
- `changeInstanceNameToPID()` 保证同一机器上不同进程的 instanceName 不同（用 PID），避免误复用。

---

## 4. MQClientInstance 启动详解

### 4.1 start() 源码（MQClientInstance.java）

```java
public void start() throws MQClientException {
    synchronized (this) {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                // 1. 未配置 NameServer 地址时，从地址服务器(http)拉取
                if (null == this.clientConfig.getNamesrvAddr()) {
                    this.mQClientAPIImpl.fetchNameServerAddr();
                }
                // 2. 启动 Netty 网络客户端
                this.mQClientAPIImpl.start();
                // 3. 启动各种定时任务
                this.startScheduledTask();
                // 4. 启动消息拉取服务(消费者用)
                this.pullMessageService.start();
                // 5. 启动重平衡服务(消费者用)
                this.rebalanceService.start();
                // 6. 启动内部默认生产者(startFactory=false, 避免递归)
                this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                this.serviceState = ServiceState.RUNNING;
                break;
            ...
        }
    }
}
```

> 注意第 6 步：`MQClientInstance` 内部持有一个 `defaultMQProducer`（`CLIENT_INNER_PRODUCER_GROUP` 组），用于客户端内部的消息回发（如事务消息回查、消费者回发消息）。它调用 `start(false)` 防止递归启动 factory。

### 4.2 五大定时任务（startScheduledTask）

| # | 任务 | 初始延迟 | 周期 | 作用 |
|---|---|---|---|---|
| 1 | `fetchNameServerAddr` | 10s | 2min | 仅当未静态配置 NameServer 时，定期从 HTTP 地址服务拉取 |
| 2 | `updateTopicRouteInfoFromNameServer` | 10ms | `pollNameServerInterval`(默认30s) | 刷新所有订阅/发布 topic 的路由 |
| 3 | `cleanOfflineBroker` + `sendHeartbeatToAllBrokerWithLock` | 1s | `heartbeatBrokerInterval`(默认30s) | 清理下线 broker、发心跳 |
| 4 | `persistAllConsumerOffset` | 10s | `persistConsumerOffsetInterval`(默认5s) | 持久化消费者位点(生产者不涉及) |
| 5 | `adjustThreadPool` | 1min | 1min | 动态调整消费线程池(生产者不涉及) |

```java
// 定时刷新路由(核心)
this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
    public void run() {
        MQClientInstance.this.updateTopicRouteInfoFromNameServer();
    }
}, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

// 心跳 + 清理下线 broker
this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
    public void run() {
        MQClientInstance.this.cleanOfflineBroker();
        MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
    }
}, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);
```

### 4.3 Netty 客户端配置（MQClientInstance 构造函数）

`MQClientInstance` 构造时创建 `MQClientAPIImpl`，内部创建 `NettyRemotingClient`，默认参数（`NettyClientConfig`）：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `clientWorkerThreads` | 4 | Netty work 线程数 |
| `connectTimeoutMillis` | 3000 | 连接超时 |
| `clientCallbackExecutorThreads` | Runtime可用核数 | 回调线程池 |
| `clientOnewaySemaphoreValue` | 65535 | oneway 信号量 |
| `clientAsyncSemaphoreValue` | 65535 | 异步信号量 |
| `socketSndBufSize / socketRcvBufSize` | 65535 | 收发缓冲区 |

---

## 5. 消息发送总体流程

### 5.1 三种发送模式

| 模式 | API | 特点 | 重试 |
|---|---|---|---|
| 同步 | `send(msg)` | 阻塞等待结果，返回 `SendResult` | 1 + retryTimesWhenSendFailed = 3 次 |
| 异步 | `send(msg, callback)` | 立即返回，回调通知 | 1 + retryTimesWhenSendAsyncFailed 次(内部线程重试) |
| 单向 | `sendOneway(msg)` | 不关心结果，无响应 | 无重试 |

### 5.2 sendDefaultImpl 主流程图（DefaultMQProducerImpl.java）

```mermaid
flowchart TD
    T0[send / sendDefaultImpl] --> T1[makeSureStateOK<br/>校验 RUNNING 状态]
    T1 --> T2[Validators.checkMessage<br/>topic非空/body非空/大小<=4M]
    T2 --> T3[tryToFindTopicPublishInfo<br/>本地缓存→NameServer拉取]
    T3 --> T4{路由存在且 ok?}
    T4 -- 否 --> TERR[抛 MQClientException<br/>No route info of this topic]
    T4 -- 是 --> T5["timesTotal = SYNC ? 1+retryTimes : 1<br/>默认同步=3次"]
    T5 --> T6[times 循环 0..timesTotal-1]
    T6 --> T7["selectOneMessageQueue(tpInfo, lastBrokerName)<br/>轮询+规避上次失败broker"]
    T7 --> T8{选到队列?}
    T8 -- 否 --> T6N[continue 下一次循环]
    T8 -- 是 --> T9[sendKernelImpl<br/>真正的网络发送]
    T9 --> T10{发送结果}
    T10 -- 成功 --> T11[updateFaultItem 记录本次耗时<br/>isolation=false]
    T11 --> T12{communicationMode}
    T12 -- ASYNC/ONEWAY --> T13[return null 直接返回]
    T12 -- SYNC --> T14{sendStatus == SEND_OK?}
    T14 -- 是 --> T15[return sendResult]
    T14 -- 否且retryAnotherBrokerWhenNotStoreOK --> T6
    T14 -- 否 --> T15
    T10 -- 抛异常 --> T16[updateFaultItem<br/>isolation=true 视为30000ms延迟熔断]
    T16 --> T17[log.warn + continue 重试]
    T6N --> T6
    T6 -->|次数耗尽仍失败| T18[构造 MQClientException<br/>Send N times still failed<br/>附带 brokersSent 轨迹]
    T18 --> T19["按异常类型转换:<br/>RemotingTooMuchRequestException(超时)<br/>MQBrokerException / MQClientException"]
```

### 5.3 核心源码

```java
// DefaultMQProducerImpl.java sendDefaultImpl（精简）
long beginTimestampFirst = System.currentTimeMillis();
long beginTimestampPrev = beginTimestampFirst;
this.makeSureStateOK();                                     // 校验 RUNNING
Validators.checkMessage(msg, this.defaultMQProducer);       // 校验消息
TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());

int timesTotal = communicationMode == CommunicationMode.SYNC
    ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;   // 同步默认 3 次
int times = 0;
String[] brokersSent = new String[timesTotal];

for (; times < timesTotal; times++) {
    String lastBrokerName = null == mq ? null : mq.getBrokerName();
    MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
    if (mqSelected != null) {
        mq = mqSelected;
        brokersSent[times] = mq.getBrokerName();
        beginTimestampPrev = System.currentTimeMillis();
        try {
            // 核心发送，timeout 减去已消耗时间（保证整体超时）
            sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback,
                topicPublishInfo, timeout - (beginTimestampPrev - beginTimestampFirst));
            endTimestamp = System.currentTimeMillis();
            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);

            switch (communicationMode) {
                case ASYNC: return null;   // 异步由回调处理结果
                case ONEWAY: return null;
                case SYNC:
                    if (sendResult.getSendStatus() != SendStatus.SEND_OK
                        && this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                        continue;   // 非 SEND_OK(刷盘/同步超时) 且配置允许 → 换 broker 重试
                    }
                    return sendResult;
            }
        } catch (RemotingException | MQClientException | MQBrokerException | InterruptedException e) {
            endTimestamp = System.currentTimeMillis();
            // isolation=true：异常直接按 30000ms 延迟计算熔断时长
            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
            exception = e;
            continue;   // 立即重试(可能换 broker)
        }
    } else {
        break;
    }
}

// 全部失败 → 抛出带 brokersSent 轨迹的异常
throw new MQClientException("Send [" + times + "] times, still failed, ... BrokersSent: "
    + Arrays.toString(brokersSent), exception);
```

### 5.4 同步发送完整时序图

```mermaid
sequenceDiagram
    autonumber
    participant U as 业务线程
    participant PI as DefaultMQProducerImpl
    participant TP as TopicPublishInfo
    participant FS as MQFaultStrategy
    participant API as MQClientAPIImpl
    participant NC as NettyRemotingClient
    participant BK as Broker(Master)

    U->>PI: send(msg, timeout=3000ms)
    PI->>PI: makeSureStateOK + Validators.checkMessage
    PI->>PI: tryToFindTopicPublishInfo(topic)
    alt 本地无路由
        PI->>NC: getTopicRouteInfoFromNameServer(topic)
        NC-->>PI: TopicRouteData(queueDatas/brokerDatas)
        PI->>PI: topicRouteData2TopicPublishInfo 转换并缓存
    end
    loop 重试最多 3 次(同步)
        PI->>FS: selectOneMessageQueue(tpInfo, lastBrokerName)
        FS->>TP: selectOneMessageQueue / isAvailable 检查
        TP-->>FS: MessageQueue(brokerName, queueId)
        FS-->>PI: MessageQueue
        PI->>PI: sendKernelImpl: findBrokerAddr + VIP(10909) + 压缩 + uniqID
        PI->>API: sendMessage(brokerAddr, msg, header)
        API->>API: 构造 RemotingCommand(SEND_MESSAGE / V2)
        API->>NC: invokeSync(addr, request, timeout)
        NC->>BK: 网络发送(同步等待)
        BK-->>NC: RemotingCommand response
        NC-->>API: response
        API->>API: processSendResponse 解析响应码
        API-->>PI: SendResult
        PI->>FS: updateFaultItem(broker, 耗时, false)
        alt SEND_OK
            PI-->>U: SendResult
        else 异常
            PI->>FS: updateFaultItem(broker, 耗时, true→熔断30s)
            Note over PI: continue 换队列/broker 重试
        end
    end
```

---

## 6. 主题路由获取

### 6.1 tryToFindTopicPublishInfo（DefaultMQProducerImpl.java）

```java
private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
    TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
    if (null == topicPublishInfo || !topicPublishInfo.ok()) {
        // 1. 放入空占位，然后主动从 NameServer 拉一次
        this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
        this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
        topicPublishInfo = this.topicPublishInfoTable.get(topic);
    }
    if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
        return topicPublishInfo;
    } else {
        // 2. topic 不存在 → 用默认主题 TBW102 再试一次(支持 broker 自动创建 topic)
        this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
        topicPublishInfo = this.topicPublishInfoTable.get(topic);
        return topicPublishInfo;
    }
}
```

### 6.2 路由转换：topicRouteData2TopicPublishInfo（MQClientInstance.java）

```mermaid
flowchart LR
    A[TopicRouteData] --> B{orderTopicConf 非空?}
    B -- 是 --> C["解析 orderTopicConf<br/>brokerName:queueNums;...<br/>逐个生成 MessageQueue"]
    C --> D[orderTopic = true]
    B -- 否 --> E[按 QueueData 排序遍历]
    E --> F{PermName.isWriteable<br/>且 broker 有 MASTER?}
    F -- 是 --> G["for i in 0..writeQueueNums<br/>new MessageQueue(topic, brokerName, i)"]
    F -- 否 --> H[跳过该 broker]
    G --> I[TopicPublishInfo<br/>messageQueueList]
    D --> I
```

要点：
- **只保留可写（`isWriteable`）且存在 Master 地址的 broker** 的写队列 —— 印证生产者只发 Master。
- `haveTopicRouterInfo=true` 标记真实路由（区别于空占位对象）。

---

## 7. 队列选择与容错策略

### 7.1 两层选择逻辑总览

```mermaid
flowchart TD
    A[selectOneMessageQueue tpInfo, lastBrokerName] --> B{sendLatencyFaultEnable?}
    B -- "false(默认)" --> C["TopicPublishInfo<br/>.selectOneMessageQueue(lastBrokerName)"]
    C --> D{lastBrokerName == null?}
    D -- 是 --> E[sendWhichQueue.incrementAndGet<br/>简单轮询取模]
    D -- 否 --> F[轮询找一个<br/>broker != lastBrokerName 的队列<br/>找不到则退化为普通轮询]
    B -- true --> G[MQFaultStrategy 逻辑]
    G --> H[递增 index,遍历队列列表]
    H --> I{"latencyFaultTolerance<br/>.isAvailable(brokerName)?"}
    I -- 可用 --> J[返回该队列]
    I -- 全部不可用 --> K["pickOneAtLeast()<br/>挑排序后半段中的'最差'broker"]
    K --> L{该 broker 有写队列?}
    L -- 是 --> M[用该 broker + 随机 queueId 组合队列返回]
    L -- 否 --> N[remove 该 broker<br/>兜底普通轮询]
```

### 7.2 普通轮询（TopicPublishInfo.java）

```java
// 简单轮询：ThreadLocalIndex 每线程独立计数
public MessageQueue selectOneMessageQueue() {
    int index = this.sendWhichQueue.incrementAndGet();     // 线程本地递增
    int pos = Math.abs(index) % this.messageQueueList.size();
    if (pos < 0) pos = 0;
    return this.messageQueueList.get(pos);
}

// 规避上次失败的 broker
public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
    if (lastBrokerName == null) return selectOneMessageQueue();
    for (int i = 0; i < this.messageQueueList().size(); i++) {
        int index = this.sendWhichQueue.incrementAndGet();
        int pos = Math.abs(index) % this.messageQueueList.size();
        MessageQueue mq = this.messageQueueList.get(pos);
        if (!mq.getBrokerName().equals(lastBrokerName)) return mq;  // 规避
    }
    return selectOneMessageQueue();   // 全是同一 broker 时退化
}
```

`ThreadLocalIndex`（client/common/ThreadLocalIndex.java）：每个线程一个独立计数器，避免线程竞争；初值随机，使不同线程负载分散。

### 7.3 MQFaultStrategy（延迟故障规避）

```java
// MQFaultStrategy.java
private boolean sendLatencyFaultEnable = false;   // 默认关闭
private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

// 异常/发送完成后更新
public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
    if (this.sendLatencyFaultEnable) {
        // isolation=true(异常) 按 30000ms 档计算 → 熔断 10 分钟
        long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
        this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
    }
}

// 根据延迟落在哪一档，返回对应熔断时长
private long computeNotAvailableDuration(final long currentLatency) {
    for (int i = latencyMax.length - 1; i >= 0; i--) {
        if (currentLatency >= latencyMax[i]) return this.notAvailableDuration[i];
    }
    return 0;
}
```

延迟与熔断时间映射表：

| 本次发送耗时 | 落入档位 | broker 规避时长 |
|---|---|---|
| < 100ms | 0/1档 | 不规避(0ms) |
| ≥ 550ms | 2档 | 30s |
| ≥ 1000ms | 3档 | 60s |
| ≥ 2000ms | 4档 | 120s |
| ≥ 3000ms | 5档 | 180s |
| ≥ 15000ms / 发送异常 | 6档 | 600s(10分钟) |

### 7.4 LatencyFaultToleranceImpl.FaultItem

```java
class FaultItem implements Comparable<FaultItem> {
    private final String name;
    private volatile long currentLatency;    // 最近一次耗时
    private volatile long startTimestamp;    // now + notAvailableDuration，"解禁"时间

    public boolean isAvailable() {
        return (System.currentTimeMillis() - startTimestamp) >= 0;   // 到达解禁时间即可用
    }

    public int compareTo(FaultItem other) {  // 可用 > 不可用；延迟小者优先
        if (this.isAvailable() != other.isAvailable()) return this.isAvailable() ? -1 : 1;
        if (this.currentLatency < other.currentLatency) return -1;
        ...
    }
}
```

`pickOneAtLeast()`：将 faultItemTable 全部 item 排序后取**后半段（较差的一半）**轮询挑一个 —— 含义是当所有 broker 都处于熔断期时，仍要挑一个"次差"的继续发，保证可用性优先于规避。

### 7.5 容错机制全景

```mermaid
flowchart LR
    subgraph 发送过程
        A[sendKernelImpl] --> B{结果}
        B -- 成功 --> C["updateFaultItem(broker, 真实耗时, false)"]
        B -- 异常 --> D["updateFaultItem(broker, 耗时, true)<br/>按30000ms档=熔断10分钟"]
    end
    C --> E[computeNotAvailableDuration<br/>查 latencyMax 表]
    D --> E
    E --> F["FaultItem.startTimestamp =<br/>now + notAvailableDuration"]
    F --> G[后续 selectOneMessageQueue 时<br/>isAvailable 过滤掉熔断中的 broker]
```

---

## 8. 消息发送核心

### 8.1 sendKernelImpl 流程图（DefaultMQProducerImpl.java）

```mermaid
flowchart TD
    K0[sendKernelImpl msg, mq, mode] --> K1["findBrokerAddressInPublish(brokerName)<br/>找不到→再tryToFindTopicPublishInfo一次"]
    K1 --> K2["MixAll.brokerVIPChannel<br/>VIP开启则端口10911-2=10909"]
    K2 --> K3{msg 是 MessageBatch?}
    K3 -- 否 --> K4[MessageClientIDSetter.setUniqID<br/>生成全局唯一ID]
    K3 -- 是 --> K5["跳过(批量统一处理)"]
    K4 --> K6["tryToCompressMessage<br/>body>=4KB → zlib压缩<br/>sysFlag |= COMPRESSED_FLAG"]
    K5 --> K6
    K6 --> K7["PROPERTY_TRANSACTION_PREPARED?<br/>是则 sysFlag |= TRANSACTION_PREPARED_TYPE"]
    K7 --> K8{有 SendMessageHook?}
    K8 -- 是 --> K9[构造 SendMessageContext<br/>executeSendMessageHookBefore]
    K8 -- 否 --> K10
    K9 --> K10[构造 SendMessageRequestHeader<br/>group/topic/queueId/sysFlag/bornTimestamp/...]
    K10 --> K11{communicationMode}
    K11 -- ASYNC --> K12["MQClientAPIImpl.sendMessage(...)<br/>带 callback + retryTimesWhenSendAsyncFailed"]
    K11 -- SYNC/ONEWAY --> K13["MQClientAPIImpl.sendMessage(...)<br/>同步/oneway 版本"]
    K12 --> K14{有 hook?}
    K13 --> K14
    K14 -- 是 --> K15[executeSendMessageHookAfter]
    K14 -- 否 --> K16
    K15 --> K16["恢复现场: msg.setBody(prevBody)<br/>topic 去掉 namespace 前缀"]
    K16 --> K17[return sendResult]
```

### 8.2 关键源码片段

```java
// 获取 broker 地址（只找 Master）
String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
if (null == brokerAddr) {
    tryToFindTopicPublishInfo(mq.getTopic());   // 兜底再刷路由
    brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
}

// VIP 通道：高优先级端口 = 原端口 - 2 (10911 → 10909)
brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

// 唯一 ID
if (!(msg instanceof MessageBatch)) {
    MessageClientIDSetter.setUniqID(msg);
}

// 压缩（tryToCompressMessage）
// body >= compressMsgBodyOverHowmuch(4KB) 时压缩；MessageBatch 不压缩

// 构造请求头
SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
requestHeader.setTopic(msg.getTopic());
requestHeader.setQueueId(mq.getQueueId());
requestHeader.setSysFlag(sysFlag);
requestHeader.setBornTimestamp(System.currentTimeMillis());
requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
requestHeader.setBatch(msg instanceof MessageBatch);
```

### 8.3 VIP 通道说明

生产者与 Broker 的网络通信默认走 VIP 通道（`brokerVIPChannel`）：把 broker 端口减 2（10911→10909）。目的：读写端口分离，避免发送与拉取互相争抢带宽。Broker 配置 `brokerVIPChannel` 可关闭，客户端也要相应设置 `setSendMessageWithVIPChannel(false)`。

---

## 9. 底层网络通信

### 9.1 MQClientAPIImpl.sendMessage（MQClientAPIImpl.java）

```mermaid
flowchart TD
    M0[MQClientAPIImpl.sendMessage] --> M1{sendSmartMsg 或<br/>MessageBatch?}
    M1 -- 是 --> M2["SendMessageRequestHeaderV2<br/>(精简版字段)<br/>RequestCode.SEND_MESSAGE_V2<br/>或 SEND_BATCH_MESSAGE"]
    M1 -- 否 --> M3["RequestCode.SEND_MESSAGE<br/>+ 完整 SendMessageRequestHeader"]
    M2 --> M4["request.setBody msg.getBody"]
    M3 --> M4
    M4 --> M5{communicationMode}
    M5 -- ONEWAY --> M6["remotingClient.invokeOneway<br/>return null"]
    M5 -- ASYNC --> M7["sendMessageAsync<br/>invokeAsync + InvokeCallback"]
    M5 -- SYNC --> M8["sendMessageSync<br/>invokeSync 阻塞等待"]
    M8 --> M9[processSendResponse]
    M7 --> M9
    M9 --> M10{response.code}
    M10 -- SUCCESS --> M11[SEND_OK]
    M10 -- FLUSH_DISK_TIMEOUT --> M12[FLUSH_DISK_TIMEOUT]
    M10 -- FLUSH_SLAVE_TIMEOUT --> M13[FLUSH_SLAVE_TIMEOUT]
    M10 -- SLAVE_NOT_AVAILABLE --> M14[SLAVE_NOT_AVAILABLE]
    M10 -- 其他 --> M15[抛 MQBrokerException]
    M11 --> M16["new SendResult(status, uniqID,<br/>msgId, MessageQueue, queueOffset)"]
```

### 9.2 processSendResponse 响应码语义

| ResponseCode | SendStatus | 含义 |
|---|---|---|
| SUCCESS | SEND_OK | 完全成功 |
| FLUSH_DISK_TIMEOUT | FLUSH_DISK_TIMEOUT | 同步刷盘超时（消息已在内存，可能丢失） |
| FLUSH_SLAVE_TIMEOUT | FLUSH_SLAVE_TIMEOUT | 同步复制到 slave 超时（master 已落盘） |
| SLAVE_NOT_AVAILABLE | SLAVE_NOT_AVAILABLE | slave 不可用，未完成同步复制 |
| 其他 | — | 抛 `MQBrokerException` |

### 9.3 SendMessageRequestHeader 主要字段

`producerGroup`、`topic``defaultTopic`(TBW102)、`defaultTopicQueueNums`、`queueId`、`sysFlag`(压缩/事务标记)、`bornTimestamp`、`flag`、`properties`(消息属性字符串)、`reconsumeTimes`、`batch`、`transactionId` 等。V2 版本对字段做了紧凑编码，减少网络开销。

### 9.4 通信层调用链

```mermaid
sequenceDiagram
    participant PI as DefaultMQProducerImpl
    participant API as MQClientAPIImpl
    participant RC as NettyRemotingClient
    participant C as Netty Channel
    participant BK as Broker Master:10909/10911

    PI->>API: sendMessage(addr, brokerName, msg, header, SYNC)
    API->>API: RemotingCommand.createRequestCommand(SEND_MESSAGE, header)
    API->>RC: invokeSync(addr, request, timeout)
    RC->>C: channel.writeAndFlush(request)（编码器序列化）
    C->>BK: TCP 发送
    BK-->>C: response
    C->>RC: processResponseCommand → ResponseFuture.complete
    RC-->>API: RemotingCommand response（同步等待 via latch）
    API->>API: processSendResponse → SendResult
    API-->>PI: SendResult
```

---

## 10. 异步发送与 Oneway 发送

### 10.1 异步发送链路

```mermaid
sequenceDiagram
    autonumber
    participant U as 业务线程
    participant PI as DefaultMQProducerImpl
    participant API as MQClientAPIImpl
    participant RC as NettyRemotingClient
    participant CB as 回调线程(InvokeCallback)
    participant BK as Broker

    U->>PI: send(msg, sendCallback)
    PI->>PI: sendDefaultImpl(ASYNC) 选中队列
    PI->>API: sendMessage(..., callback, timesTotal=1+retryAsync)
    API->>RC: invokeAsync(addr, request, timeout, InvokeCallback)
    RC-->>U: 立即返回（不等响应）
    RC->>BK: 异步发送请求
    BK-->>RC: 响应到达
    RC->>CB: operationComplete(ResponseFuture)
    alt 响应正常
        CB->>CB: processSendResponse → sendCallback.onSuccess(sendResult)
    else 无响应/异常
        CB->>CB: onExceptionImpl(...)
        opt 未超重试次数(默认3次)
            CB->>PI: selectOneMessageQueue 规避 broker
            CB->>API: sendMessageAsync 再发（内部线程重试）
        end
        CB->>CB: 重试耗尽 → sendCallback.onException(e)
    end
```

### 10.2 onExceptionImpl（MQClientAPIImpl.java）

```java
private void onExceptionImpl(...) {
    int tmp = curTimes.incrementAndGet();
    if (needRetry && tmp <= timesTotal) {
        // 规避失败 broker，重新选队列，继续异步发送
        MessageQueue mqChosen = producer.selectOneMessageQueue(topicPublishInfo, brokerName);
        String addr = instance.findBrokerAddressInPublish(mqChosen.getBrokerName());
        sendMessageAsync(addr, mqChosen.getBrokerName(), msg, timeoutMillis, request, ...);
    } else {
        if (context != null) {                     // 触发 after hook
            context.setException(e);
            context.getProducer().executeSendMessageHookAfter(context);
        }
        sendCallback.onException(e);               // 最终通知用户失败
    }
}
```

与同步重试的区别：**异步重试在回调线程中执行**，不阻塞业务线程；`needRetry` 在超时(`timeoutMillis < 0`)时为 false，不再重试。

### 10.3 Oneway

`sendOneway` → `sendDefaultImpl(ONEWAY)` → `sendKernelImpl` → `MQClientAPIImpl` → `remotingClient.invokeOneway`（受 `clientOnewaySemaphoreValue` 信号量限流），**不等待、不解析响应、无重试**。适合日志类可容忍丢失的场景。

---

## 11. 批量消息发送

`common/message/MessageBatch.java`

```mermaid
flowchart LR
    A[DefaultMQProducer.send List&lt;Message&gt;] --> B["MessageBatch<br/>.generateFromList(messages)"]
    B --> C{"校验: 所有消息 topic 相同?<br/>waitStoreMsgOK 相同?"}
    C -- 否 --> D[抛 UnsupportedOperationException]
    C -- 是 --> E["MessageBatch(topic, messages)<br/>setTopic/setWaitStoreMsgOK"]
    E --> F["走普通发送流程<br/>但: 不压缩/不设uniqID"]
    F --> G["RequestCode.SEND_BATCH_MESSAGE<br/>body = MessageDecoder.encodeMessages<br/>(多条消息拼字节流)"]
    G --> H[Broker 批量写入,返回拼接的 msgId 列表]
```

要点：
- 批量消息要求**同一 topic 且 waitStoreMsgOK 一致**，否则抛异常。
- `MessageBatch` 本身是一条"大消息"，body 是多条消息的编码拼接，`encode()` 使用 `MessageDecoder.encodeMessages`。
- 批量消息**不压缩、不逐条设置 uniqID**，请求码用 `SEND_BATCH_MESSAGE`。
- 响应中 msgId 为多条消息 ID 的拼接串，`SendResult` 一并返回。
- 单批总量同样受 `maxMessageSize`(4M) 限制。

---

## 12. 总结

### 12.1 一图总览：启动 + 发送全景

```mermaid
flowchart TB
    subgraph 启动阶段
        S1[producer.start] --> S2[校验+状态机<br/>CREATE_JUST→RUNNING]
        S2 --> S3[获取/复用 MQClientInstance<br/>clientId=IP@PID]
        S3 --> S4[registerProducer 到组表]
        S4 --> S5[MQClientInstance.start<br/>Netty client/定时任务/拉取/平衡服务]
    end

    subgraph 发送阶段
        T1[send msg] --> T2[状态/消息校验]
        T2 --> T3[tryToFindTopicPublishInfo<br/>缓存→NameServer→TBW102兜底]
        T3 --> T4[selectOneMessageQueue<br/>轮询+规避+容错熔断]
        T4 --> T5[sendKernelImpl<br/>寻址/VIP/uniqID/压缩/事务标记/钩子]
        T5 --> T6[MQClientAPIImpl.sendMessage<br/>RemotingCommand 封装]
        T6 --> T7[Netty invokeSync/Async/Oneway]
        T7 --> T8[processSendResponse<br/>解析 SEND_OK 等]
        T8 --> T9[updateFaultItem<br/>记录耗时/熔断 broker]
        T9 -- 失败且未超次数 --> T4
        T9 -- 成功 --> R[返回 SendResult]
    end

    S5 --> T1
```

### 12.2 关键设计要点

1. **状态机防重**：`CREATE_JUST → START_FAILED → RUNNING` 的三段式转换防止并发/重复启动；注册失败回滚到 `CREATE_JUST`。
2. **实例共享**：`MQClientManager` 以 clientId 为 key 缓存 `MQClientInstance`，多 Producer/Consumer 共享网络与后台任务，是客户端的"连接池"式设计。
3. **路由三级获取**：本地缓存 → 主动查 NameServer → 默认主题 TBW102 兜底（支持 broker 端自动建 topic）。
4. **只写 Master**：`topicRouteData2TopicPublishInfo` 只保留可写且有 Master 的队列；`findBrokerAddressInPublish` 只取 `MASTER_ID`。
5. **两级队列选择**：默认轮询 + 规避上次失败 broker；开启 `sendLatencyFaultEnable` 后叠加基于延迟分级的 broker 熔断（50ms~15000ms → 0~600s 规避时长）。
6. **同步重试 3 次且换 broker**：`brokersSent` 数组记录每次尝试的 broker，异常按 `isolation=true` 直接按 30s 档熔断 10 分钟。
7. **超时扣减**：重试时 `timeout - costTime`，保证总耗时不超过用户设置的超时。
8. **VIP 通道**：默认端口 -2（10909），读写流量分离。
9. **发送钩子**：`SendMessageHook` before/after 环绕（trace 场景使用），发送后恢复消息原始 body/topic（namespace 剥离）。
10. **三种通信模式**：SYNC（invokeSync 阻塞）、ASYNC（invokeAsync + 回调线程重试）、ONEWAY（信号量限流、无响应无重试）。

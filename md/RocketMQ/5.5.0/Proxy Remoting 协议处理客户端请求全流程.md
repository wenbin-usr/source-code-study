# RocketMQ 5.4.0 Proxy Remoting 协议处理客户端请求全流程源码分析

> 源码版本：RocketMQ 5.4.0（`proxy` 模块）
> 核心结论：Proxy 的 Remoting 接入层**完全复用**了 `remoting` 模块的 `NettyRemotingServer`（通过子类 `MultiProtocolRemotingServer` 扩展多协议识别），请求经过 **Netty IO 线程解码 → 业务线程池分发 → Activity（NettyRequestProcessor）→ RequestPipeline（鉴权/上下文）→ MessagingProcessor（异步转发到 Broker）** 的完整链路。

---

## 目录

1. [总体架构](#1-总体架构)
2. [启动与组件装配](#2-启动与组件装配)
3. [网络层：连接建立与协议识别](#3-网络层连接建立与协议识别)
4. [请求分发：NettyServerHandler 与线程池](#4-请求分发nettyserverhandler-与线程池)
5. [业务层：RequestPipeline 与 Activity](#5-业务层requestpipeline-与-activity)
6. [以 SendMessage 为例的完整时序](#6-以-sendmessage-为例的完整时序)
7. [客户端连接管理（Channel/心跳/断连清理）](#7-客户端连接管理channel心跳断连清理)
8. [Proxy → Client 的反向调用](#8-proxy--client-的反向调用)
9. [过载保护：慢请求监控与过期请求清理](#9-过载保护慢请求监控与过期请求清理)
10. [关键设计总结](#10-关键设计总结)

---

## 1. 总体架构

### 1.1 架构图

```mermaid
flowchart TB
    subgraph Client["4.x Remoting 客户端 (Producer/Consumer)"]
        C1["NettyRemotingClient"]
    end

    subgraph Proxy["Proxy 进程"]
        subgraph NettyLayer["网络接入层 (remoting 模块复用)"]
            NS["MultiProtocolRemotingServer<br/>(extends NettyRemotingServer)<br/>监听 remotingListenPort:8080"]
            HSK["HandshakeHandler<br/>(TLS 握手)"]
            PN["ProtocolNegotiationHandler<br/>(4字节协议嗅探)"]
            RPH["RemotingProtocolHandler<br/>match()=true 兜底"]
            DEC["NettyDecoder / NettyEncoder"]
            DIST["RemotingCodeDistributionHandler"]
            CMH["NettyConnectManageHandler"]
            SH["NettyServerHandler"]
        end

        subgraph ThreadPool["业务线程池 (隔离)"]
            E1["sendMessageExecutor"]
            E2["pullMessageExecutor"]
            E3["heartbeatExecutor"]
            E4["updateOffsetExecutor"]
            E5["topicRouteExecutor"]
            E6["defaultExecutor"]
        end

        subgraph Activity["Activity 层 (NettyRequestProcessor)"]
            A1["SendMessageActivity"]
            A2["PullMessageActivity / PopMessageActivity"]
            A3["ClientManagerActivity"]
            A4["ConsumerManagerActivity"]
            A5["GetTopicRouteActivity"]
            A6["TransactionActivity / RecallMessageActivity<br/>AckMessageActivity / ChangeInvisibleTimeActivity"]
        end

        subgraph Pipeline["RequestPipeline (洋葱模型)"]
            P1["ContextInitPipeline"]
            P2["AuthenticationPipeline"]
            P3["AuthorizationPipeline"]
        end

        subgraph Core["核心处理层"]
            MP["MessagingProcessor<br/>(DefaultMessagingProcessor)"]
            RCM["RemotingChannelManager"]
            SVCS["MetadataService / SendMessageService /<br/>ConsumerProcessor / ReceiptHandleProcessor ..."]
        end
    end

    subgraph Broker["Broker 集群"]
        B1["Broker NettyRemotingServer"]
    end

    C1 -- "RemotingCommand (TCP)" --> NS
    NS --> HSK --> PN
    PN -- "remoting 协议" --> RPH
    RPH --> DEC --> DIST --> CMH --> SH
    SH -- "按 RequestCode 提交" --> ThreadPool
    ThreadPool --> Activity
    Activity --> Pipeline
    Pipeline --> MP
    MP -- "异步 invokeAsync" --> B1
    MP -- "连接登记" --> RCM
    B1 -- "response" --> MP
```

### 1.2 核心组件一览

| 组件 | 源码位置 | 职责 |
|---|---|---|
| `RemotingProtocolServer` | `proxy/remoting/RemotingProtocolServer.java` | Remoting 协议服务器门面：组装 Activity、线程池、注册 Processor |
| `MultiProtocolRemotingServer` | `proxy/remoting/MultiProtocolRemotingServer.java` | 继承 `NettyRemotingServer`，重写 `configChannel()` 加入协议嗅探 |
| `ProtocolNegotiationHandler` | `proxy/remoting/protocol/ProtocolNegotiationHandler.java` | 读取前 4 字节做协议分流 |
| `RemotingProtocolHandler` | `proxy/remoting/protocol/remoting/RemotingProtocolHandler.java` | remoting 协议命中时动态装配标准 pipeline |
| `Http2ProtocolProxyHandler` | `proxy/remoting/protocol/http2proxy/Http2ProtocolProxyHandler.java` | HTTP/2(gRPC) 命中时本机代理到 grpcServerPort |
| `AbstractRemotingActivity` | `proxy/remoting/activity/AbstractRemotingActivity.java` | 所有 Activity 的模板基类 |
| `RequestPipeline` | `proxy/remoting/pipeline/RequestPipeline.java` | 函数式洋葱管道（上下文初始化 + 认证 + 授权） |
| `RemotingChannelManager` | `proxy/remoting/channel/RemotingChannelManager.java` | group → channel → RemotingChannel 的连接登记簿 |
| `RemotingChannel` | `proxy/remoting/channel/RemotingChannel.java` | 原始 Netty Channel 的包装，支持 Proxy→Client 反向调用 |
| `ClientHousekeepingService` | `proxy/remoting/ClientHousekeepingService.java` | 连接断开/异常/空闲事件 → 注销客户端 |

---

## 2. 启动与组件装配

`ProxyStartup` 中创建（ProxyStartup.java:95-96）：

```java
RemotingProtocolServer remotingServer = new RemotingProtocolServer(messagingProcessor, tlsCertificateManager);
PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(remotingServer);
```

### 2.1 `RemotingProtocolServer` 构造函数（RemotingProtocolServer.java:96-198）做了 5 件事

```mermaid
flowchart TD
    A["new RemotingProtocolServer(processor, tlsMgr)"] --> B["1. 创建 RemotingChannelManager<br/>（客户端连接登记簿）"]
    B --> C["2. createRequestPipeline()<br/>ContextInit → Authentication → Authorization"]
    C --> D["3. 创建 10 个 Activity<br/>每个 Activity 持有 pipeline + messagingProcessor"]
    D --> E["4. 创建 NettyServerConfig<br/>listenPort = remotingListenPort(8080)<br/>+ TLS 系统属性"]
    E --> F{"enableRemotingLocalProxyGrpc?"}
    F -- "true" --> G["MultiProtocolRemotingServer<br/>（支持 remoting+gRPC 共端口）"]
    F -- "false" --> H["原生 NettyRemotingServer"]
    G --> I["5. 创建 6 个隔离线程池<br/>+ registerRemotingServer()<br/>按 RequestCode 注册 Processor"]
    H --> I
```

**线程池隔离**（RemotingProtocolServer.java:132-190）——这是 Proxy 直接借鉴 Broker 的做法：

| 线程池 | 处理的请求 | 关键配置 |
|---|---|---|
| `RemotingSendMessageThread` | SEND_MESSAGE / SEND_MESSAGE_V2 / SEND_BATCH_MESSAGE / CONSUMER_SEND_MSG_BACK / END_TRANSACTION / RECALL_MESSAGE | `remotingSendMessageThreadPoolNums` / `remotingWaitTimeMillsInSendQueue` |
| `RemotingPullMessageThread` | PULL_MESSAGE / LITE_PULL_MESSAGE / POP_MESSAGE | `remotingPullMessageThreadPoolNums` |
| `RemotingHeartbeatThread` | HEART_BEAT | `remotingHeartbeatThreadPoolNums` |
| `RemotingUpdateOffsetThread` | UPDATE_CONSUMER_OFFSET / ACK_MESSAGE / CHANGE_MESSAGE_INVISIBLETIME / GET_CONSUMER_CONNECTION_LIST | `remotingUpdateOffsetThreadPoolNums` |
| `RemotingTopicRouteThread` | GET_ROUTEINFO_BY_TOPIC | `remotingTopicRouteThreadPoolNums` |
| `RemotingDefaultThread` | UNREGISTER_CLIENT / CHECK_CLIENT_CONFIG / 其余 ConsumerManager 类请求 | `remotingDefaultThreadPoolNums` |

每个线程池都挂了 `ThreadPoolHeadSlowTimeMillsMonitor`（队头任务滞留时间监控），超阈值时打印 jstack——用于发现慢请求。

### 2.2 Processor 注册（RemotingProtocolServer.java:210-241）

`registerProcessor(RequestCode, NettyRequestProcessor, ExecutorService)` 是 `NettyRemotingServer` 提供的标准接口，Proxy 的 Activity 实现了 `NettyRequestProcessor` 接口，**因此可以像 Broker 的 Processor 一样被注册到复用的 Netty 服务器里**。这是整个设计的精髓：*Proxy 用"换 Processor"的方式把 NettyRemotingServer 变成了代理服务器*。

---

## 3. 网络层：连接建立与协议识别

### 3.1 多协议共端口机制

`MultiProtocolRemotingServer` 重写 `configChannel()`（MultiProtocolRemotingServer.java:79-87）：

```java
protected ChannelPipeline configChannel(SocketChannel ch) {
    return ch.pipeline()
        .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, new HandshakeHandler())
        .addLast(defaultEventExecutorGroup,
            new IdleStateHandler(0, 0, serverChannelMaxIdleTimeSeconds),
            new ProtocolNegotiationHandler(remotingProtocolHandler)   // 兜底 handler
                .addProtocolHandler(http2ProtocolProxyHandler));       // HTTP/2 探测
}
```

### 3.2 协议识别流程

```mermaid
flowchart TD
    A["TCP 连接建立 :8080"] --> B["HandshakeHandler<br/>TLS 模式协商<br/>(PERMISSIVE 时嗅探是否加密)"]
    B --> C["ProtocolNegotiationHandler.decode()<br/>等待 ≥4 字节"]
    C --> D{"遍历 protocolHandlerList<br/>Http2ProtocolProxyHandler.match()<br/>前4字节 == 'PRI ' (0x50524920)?"}
    D -- "是(gRPC客户端)" --> E["Http2ProtocolProxyHandler.config()<br/>Netty Bootstrap 连 127.0.0.1:grpcServerPort<br/>Http2ProxyFrontend/BackendHandler 双向转发<br/>之后由 gRPC 侧处理"]
    D -- "否" --> F["fallback = RemotingProtocolHandler<br/>(match() 恒为 true)"]
    F --> G["RemotingProtocolHandler.config()<br/>动态挂载标准 remoting pipeline"]
    G --> H["移除 ProtocolNegotiationHandler<br/>后续字节流按 remoting 协议解码"]
```

`RemotingProtocolHandler.config()`（RemotingProtocolHandler.java:52-60）动态装配的 pipeline 与原生 Broker 完全一致：

```java
ctx.pipeline().addLast(
    encoderSupplier.get(),                        // NettyEncoder (RemotingCommand → 字节)
    new NettyDecoder(),                           // 字节 → RemotingCommand
    remotingCodeDistributionHandlerSupplier.get(),// requestCode 分布统计
    connectionManageHandlerSupplier.get(),        // 连接事件 → ClientHousekeepingService
    serverHandlerSupplier.get()                   // NettyServerHandler：请求入口
);
```

> 注意四个 handler 是通过 `Supplier` 从 `NettyRemotingServer` 实例获取的（MultiProtocolRemotingServer.java:55-59），保证与父类的连接管理、事件通知逻辑共享同一套状态。

---

## 4. 请求分发：NettyServerHandler 与线程池

`NettyServerHandler.channelRead()`（remoting 模块）收到解码后的 `RemotingCommand`：

```mermaid
sequenceDiagram
    participant IO as Netty IO线程
    participant SH as NettyServerHandler
    participant PR as processRequestCommand
    participant TP as 业务线程池
    participant RT as RequestTask(FutureTaskExt)

    IO->>SH: channelRead(RemotingCommand)
    SH->>SH: 根据 code 查找<br/>Pair<Processor, Executor>
    alt 未注册该 code
        SH-->>IO: 返回 REQUEST_CODE_NOT_SUPPORTED 响应
    else 已注册
        SH->>PR: processRequestCommand(ctx, request)
        PR->>PR: 构建 RequestTask<br/>(记录 createTimestamp)
        PR->>TP: submit(RequestTask)
        Note over TP: 异步执行，IO线程立即返回
        TP->>RT: run()
        RT->>RT: processor.processRequest(ctx, request)
        Note over RT: 即 Activity.processRequest()
        alt processor 抛异常 / rejectRequest
            RT-->>客户端: SYSTEM_ERROR 响应
        end
    end
```

关键点：

- **IO 线程与业务线程隔离**：解码在 IO 线程，业务处理在按 code 隔离的线程池，避免慢请求阻塞网络线程。
- **`RequestTask` 包装**：提交时记录 `createTimestamp`，供第 9 节的慢请求监控和过期清理使用。
- **同步返回值为 null**：Proxy 的 Activity `processRequest()` 一律返回 `null`（AbstractRemotingActivity.java:94-107），响应由 Activity 自己异步 `writeAndFlush`——因为 Proxy 需要先等 Broker 的响应，无法同步返回。

---

## 5. 业务层：RequestPipeline 与 Activity

### 5.1 `AbstractRemotingActivity.processRequest()` 模板方法

```java
public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
    ProxyContext context = createContext();
    try {
        this.requestPipeline.execute(ctx, request, context);      // ① 管道前置处理
        RemotingCommand response = this.processRequest0(ctx, request, context);  // ② 子类业务
        if (response != null) {
            writeResponse(ctx, context, request, response);       // ③ 同步响应
        }
        return null;   // 响应全部自行写出，不依赖 remoting 框架回写
    } catch (Throwable t) {
        writeErrResponse(ctx, context, request, t);               // ④ 异常统一转换
        return null;
    }
}
```

### 5.2 RequestPipeline：函数式洋葱管道

`RequestPipeline.pipe()`（RequestPipeline.java:28-33）：

```java
default RequestPipeline pipe(RequestPipeline source) {
    return (ctx, request, context) -> {
        source.execute(ctx, request, context);  // 先执行被包裹的
        execute(ctx, request, context);         // 再执行自己
    };
}
```

装配代码（RemotingProtocolServer.java:294-305）：

```java
pipeline = 空;
pipeline = pipeline.pipe(new AuthorizationPipeline(...))   // 最外层
                   .pipe(new AuthenticationPipeline(...));
pipeline = pipeline.pipe(new ContextInitPipeline());        // 最内层、最先执行
```

执行顺序：**ContextInit → Authentication → Authorization → Activity 业务逻辑**

| Pipeline | 职责 |
|---|---|
| `ContextInitPipeline` | 从 Channel attribute（心跳时写入的 clientId/language/version，见 ClientManagerActivity.java:108-118）填充 `ProxyContext`：action、protocolType=REMOTING、channel、local/remote address、clientID 等 |
| `AuthenticationPipeline` | 调用 `messagingProcessor` 的认证服务校验 AK/SK（继承 `AbstractTransactionPipeline`，复用 gRPC 侧同一套 auth 实现） |
| `AuthorizationPipeline` | 按 action/topic 做 ACL 授权检查 |

> 设计意图：remoting 侧与 gRPC 侧共享同一套 `ProxyContext` + 鉴权服务，差异只在协议解析层。

### 5.3 `AbstractRemotingActivity.request()`：转发到 Broker 的通用方法

大多数 Activity（Send/Pull/Pop/Transaction/Recall...）最终都走这个方法（AbstractRemotingActivity.java:64-91）：

```java
protected RemotingCommand request(ChannelHandlerContext ctx, RemotingCommand request,
    ProxyContext context, long timeoutMillis) throws Exception {
    // 1. 提取目标 broker 名称
    //    SEND_MESSAGE_V2/SEND_BATCH_MESSAGE 用短字段 "n"，其余用 "bname"
    String brokerName = request.getExtFields().get("bname" /* 或 "n" */);
    if (brokerName == null) {
        return buildErrorResponse(VERSION_NOT_SUPPORTED, "Request doesn't have field bname");
    }
    // 2. oneway 请求：fire-and-forget
    if (request.isOnewayRPC()) {
        messagingProcessor.requestOneway(context, brokerName, request, timeoutMillis);
        return null;
    }
    // 3. 异步转发到 broker，future 完成后写回客户端
    messagingProcessor.request(context, brokerName, request, timeoutMillis)
        .thenAccept(r -> writeResponse(ctx, context, request, r))
        .exceptionally(t -> { writeErrResponse(ctx, context, request, t); return null; });
    return null;
}
```

要点：

- **客户端在请求 extFields 中携带 `bname`**：4.x 客户端从 Proxy 返回的路由信息里拿到 broker 名称（而不是地址，因为客户端只认识 Proxy），Proxy 据此选择后端 Broker。
- **全程异步**：`CompletableFuture` 链，不占用业务线程等待 Broker 响应。
- **超时**：各 Activity 按语义传不同 timeout（如 SendMessage 是 3 秒，AbstractRemotingActivity + SendMessageActivity.java:84）。

### 5.4 Activity 与 RequestCode 映射

```mermaid
flowchart LR
    subgraph "请求分发"
        RC["RequestCode"]
    end
    RC -->|"SEND_MESSAGE / V2 / BATCH /<br/>CONSUMER_SEND_MSG_BACK"| SA["SendMessageActivity<br/>消息类型校验(TRANSACTION等)<br/>→ request() 转发"]
    RC -->|"END_TRANSACTION"| TA["TransactionActivity<br/>事务状态回查登记"]
    RC -->|"RECALL_MESSAGE"| RA["RecallMessageActivity"]
    RC -->|"PULL / LITE_PULL / POP_MESSAGE"| PA["PullMessageActivity / PopMessageActivity"]
    RC -->|"HEART_BEAT"| CA["ClientManagerActivity<br/>登记 producer/consumer 连接"]
    RC -->|"UNREGISTER_CLIENT /<br/>CHECK_CLIENT_CONFIG"| CA
    RC -->|"UPDATE_CONSUMER_OFFSET / ACK_MESSAGE /<br/>CHANGE_MESSAGE_INVISIBLETIME /<br/>QUERY_CONSUMER_OFFSET / LOCK_BATCH_MQ ..."| COA["ConsumerManagerActivity"]
    RC -->|"GET_ROUTEINFO_BY_TOPIC"| GA["GetTopicRouteActivity<br/>路由改写：broker地址→proxy地址"]
```

以 `SendMessageActivity.sendMessage()` 为例（SendMessageActivity.java:64-85），转发前还做了 Proxy 特有的校验：

1. 解析 `SendMessageRequestHeader`，从消息属性推断 `TopicMessageType`；
2. `enableTopicMessageTypeCheck=true` 时，从 MetadataService 查询 topic 的实际类型并校验一致性（如向普通 topic 发事务消息会被拒绝，retry/dlq topic 跳过）；
3. 事务消息则调用 `addTransactionSubscription()` 登记事务订阅（供后续回查路由）；
4. `request(ctx, request, context, 3s)` 转发 Broker。

### 5.5 响应写回：`writeResponse()`

（AbstractRemotingActivity.java:150-170）

```java
protected void writeResponse(ctx, context, request, response, t) {
    if (request.isOnewayRPC()) return;      // oneway 不回
    if (!ctx.channel().isWritable()) return; // 对端缓冲区满，丢弃（防止自身 OOM）
    response.setOpaque(request.getOpaque()); // 关联请求序号
    response.markResponseType();
    response.addExtField(PROPERTY_MSG_REGION, regionId);
    response.addExtField(PROPERTY_TRACE_SWITCH, traceOn);
    if (t != null) response.setRemark(t.getMessage());
    ctx.writeAndFlush(response);
}
```

异常→响应码映射（AbstractRemotingActivity.java:49-56, 121-143）：

| 异常 | ResponseCode |
|---|---|
| `ProxyException(FORBIDDEN)` | NO_PERMISSION |
| `ProxyException(MESSAGE_PROPERTY_CONFLICT_WITH_TYPE)` | MESSAGE_ILLEGAL |
| `ProxyException(INTERNAL_SERVER_ERROR)` | SYSTEM_ERROR |
| `ProxyException(TRANSACTION_DATA_NOT_FOUND)` | SUCCESS（特殊：按成功带回查数据缺失语义） |
| `MQClientException` / `MQBrokerException` | 保留原 code |
| `AclException` | NO_PERMISSION |
| 其他 | SYSTEM_ERROR |

---

## 6. 以 SendMessage 为例的完整时序

```mermaid
sequenceDiagram
    autonumber
    participant C as 4.x客户端
    participant N as NettyRemotingServer<br/>(MultiProtocol)
    participant TP as RemotingSendMessageThread
    participant A as SendMessageActivity
    participant P as RequestPipeline
    participant M as MessagingProcessor
    participant B as Broker

    C->>N: TCP连接 :8080
    N->>N: HandshakeHandler + ProtocolNegotiationHandler
    Note over N: 非"PRI "前缀 → RemotingProtocolHandler<br/>挂载 NettyDecoder/Encoder/ServerHandler

    C->>N: SEND_MESSAGE_V2 (bname=broker-a, opaque=10)
    N->>N: NettyDecoder 解码 RemotingCommand
    N->>N: NettyServerHandler 查表得<br/>(SendMessageActivity, sendMessageExecutor)
    N->>TP: submit(RequestTask)
    Note over N: IO线程立即返回

    TP->>A: processRequest(ctx, request)
    A->>P: pipeline.execute(ctx, request, context)
    P->>P: ContextInitPipeline<br/>填充 ProxyContext
    P->>P: AuthenticationPipeline<br/>AK/SK 认证
    P->>P: AuthorizationPipeline<br/>ACL 授权
    P-->>A: 通过(异常则抛出)

    A->>A: 解析 header / 校验 TopicMessageType<br/>事务消息登记 addTransactionSubscription
    A->>M: request(context, "broker-a", request, 3000ms)
    M->>M: ClusterService 选择 broker-a 的<br/>可写地址
    M->>B: invokeAsync(SEND_MESSAGE_V2)
    Note over A: Activity 返回 null，线程归还线程池

    B-->>M: response (SEND_MESSAGE_OK)
    M-->>A: future.complete(response)
    A->>A: writeResponse()<br/>opaque=10, markResponseType
    A->>C: ctx.writeAndFlush(response)
    Note over C: 客户端按 opaque 匹配挂起请求
```

补充：**GET_ROUTEINFO_BY_TOPIC 是 Proxy 模式的关键前置请求**。`GetTopicRouteActivity` 会把路由中 Broker 的真实地址替换为 Proxy 的 `remotingAccessAddr`，客户端因此"只看得见 Proxy"——这就是后续请求里必须携带 `bname` 的原因（Proxy 拿 bname 再解析真实 Broker 地址）。

---

## 7. 客户端连接管理（Channel/心跳/断连清理）

### 7.1 RemotingChannel 包装体系

```mermaid
classDiagram
    class Channel~Netty接口~ {
        <<interface>>
    }
    class ProxyChannel {
        <<abstract>>
        #ProxyRelayService proxyRelayService
        +writeAndFlush(Object)
        #processCheckTransaction(...) CompletableFuture
        #processGetConsumerRunningInfo(...) CompletableFuture
        #processConsumeMessageDirectly(...) CompletableFuture
    }
    class RemotingChannel {
        -String clientId
        -Set~SubscriptionData~ subscriptionData
        +toRemoteChannel() RemoteChannel
        +getChannelExtendAttribute() String
    }
    class RemotingChannelManager {
        -ConcurrentMap~group, Map~Channel,RemotingChannel~~ groupChannelMap
        +createProducerChannel(ctx, ch, group, clientId)
        +createConsumerChannel(ctx, ch, group, clientId, subscriptions)
        +removeChannel(channel) Set
    }
    Channel <|.. ProxyChannel : 代理原始channel
    ProxyChannel <|-- RemotingChannel
    RemotingChannelManager o-- RemotingChannel : 登记 p{group}/c{group}

    note for RemotingChannel "包装的意义：\n1. 让 Proxy→Client 的反向指令\n   (事务回查/回溯消费) 走 ProxyChannel\n   统一的 relay 通道\n2. toRemoteChannel() 序列化连接信息\n   供 Local/Cluster 模式跨进程传递"
```

### 7.2 心跳登记流程（ClientManagerActivity.java:78-106）

```mermaid
sequenceDiagram
    autonumber
    participant C as 客户端
    participant TP as RemotingHeartbeatThread
    participant CA as ClientManagerActivity
    participant RCM as RemotingChannelManager
    participant MP as MessagingProcessor
    participant B as Broker(Local模式) / 下游

    C->>TP: HEART_BEAT (HeartbeatData: producerGroup+consumerGroup+订阅关系)
    TP->>CA: processRequest0()
    CA->>CA: HeartbeatData.decode(body)

    loop 每个 ProducerData
        CA->>RCM: createProducerChannel(ctx, channel, group, clientId)
        RCM-->>CA: RemotingChannel (key="p"+group)
        CA->>MP: registerProducer(group, ClientChannelInfo)
    end
    loop 每个 ConsumerData
        CA->>RCM: createConsumerChannel(ctx, channel, group, clientId, subscriptions)
        RCM-->>CA: RemotingChannel (key="c"+group, 带订阅数据)
        CA->>MP: registerConsumer(group, ClientChannelInfo, consumeType, ...)
    end
    CA->>CA: clientId/language/version 写入原始 Channel attr<br/>(供 ContextInitPipeline 后续读取)
    CA-->>C: SUCCESS
    MP->>B: 转发注册/通知 ConsumerIdsChangeListener 等
```

细节：

- `groupChannelMap` 的 key 是 `"p"+group` / `"c"+group`（RemotingChannelManager.java:49-59），生产者与消费者分开登记；内层 Map 以**原始 Netty Channel** 为 key（同一个物理连接可以同时注册为某 group 的 producer 和另一 group 的 consumer）。
- `registerConsumer(..., notifyConsumerIdsChanged=true)`：消费者变更时会触发重平衡通知链。

### 7.3 断连清理（ClientHousekeepingService）

`MultiProtocolRemotingServer` 构造时把 `ClientHousekeepingService` 作为 `ChannelEventListener` 传入（RemotingProtocolServer.java:124,127）。Netty 的 `NettyConnectManageHandler` 在连接 close/exception/idle 事件时回调：

```mermaid
flowchart LR
    E["Netty 事件"] --> CHS["ClientHousekeepingService"]
    E2["onChannelClose / onChannelException /<br/>onChannelIdle (serverChannelMaxIdleTimeSeconds)"] --> CHS
    CHS --> DO["ClientManagerActivity.doChannelCloseEvent(addr, channel)"]
    DO --> RC2["RemotingChannelManager.removeChannel(channel)<br/>遍历所有 group 移除该 channel"]
    RC2 --> UN["对每个移除的 RemotingChannel：<br/>unRegisterProducer / unRegisterConsumer"]
    UN --> NTF["通知 Broker 侧注销<br/>+ ConsumerIdsChangeListener 重平衡"]
```

`UNREGISTER_CLIENT` 请求（客户端正常关闭时主动发送，ClientManagerActivity.java:120-150）走的是同样的 `removeProducerChannel/removeConsumerChannel` + 注销逻辑。

---

## 8. Proxy → Client 的反向调用

Proxy 有时需要**主动向客户端发请求**（Broker 发起的指令经 Proxy 中继）：

| 场景 | 实现 | 入口 |
|---|---|---|
| 事务消息回查 | `RemotingChannel.processCheckTransaction()`：构造 `CHECK_TRANSACTION_STATE` RemotingCommand，`parent().writeAndFlush()` 直接写给客户端 | Broker → Proxy TransactionProcessor → ProxyRelayService |
| 查询消费者运行信息 | `processGetConsumerRunningInfo()`：通过 `RemotingProxyOutClient.invokeToClient()`（即 `RemotingProtocolServer.invokeToClient()`，RemotingProtocolServer.java:268-292）调用 `NettyRemotingServer.invokeAsync()` 走标准异步调用框架 | Broker 管理指令 |
| 直接消费一条消息 | `processConsumeMessageDirectly()`：同上，`CONSUME_MESSAGE_DIRECTLY` | Broker 管理指令 |

`invokeToClient()` 的实现把 `NettyRemotingServer` 的回调式 API 适配成 `CompletableFuture`，与 Proxy 内部异步风格统一：

```java
this.defaultRemotingServer.invokeAsync(channel, request, timeoutMillis, new InvokeCallback() {
    public void operationSucceed(RemotingCommand response) { future.complete(response); }
    public void operationFail(Throwable t) { future.completeExceptionally(t); }
});
```

> 注意：**同一条 TCP 连接上双向复用**——客户端的请求和 Proxy 发起的请求都靠 `opaque` 序号区分，`NettyRemotingServer` 内部的 `ResponseTable` 完成挂起/匹配。

---

## 9. 过载保护：慢请求监控与过期请求清理

`RemotingProtocolServer` 启动了每 10 秒一次的定时任务（RemotingProtocolServer.java:192-195）：

```mermaid
flowchart TD
    T["timerExecutor 每10s<br/>cleanExpireRequest()"] --> L["遍历 6 个线程池"]
    L --> Q["cleanExpiredRequestInQueue(executor,<br/>maxWaitTimeMillsInQueue)"]
    Q --> P1["peek 队头 RequestTask"]
    P1 --> J{"now - createTimestamp<br/>≥ 阈值?"}
    J -- "是" --> RM["queue.remove(task) 成功后:<br/>stopRun=true<br/>returnResponse(SYSTEM_BUSY,<br/>'[TIMEOUT_CLEAN_QUEUE]broker busy,<br/>start flow control for a while')"]
    RM --> P1
    J -- "否" --> E["结束本轮"]
```

- `RequestTask.returnResponse()` 会直接向客户端回 `SYSTEM_BUSY`，并且 `stopRun=true` 使该任务即便随后被线程取到也不再执行（remoting 模块的 `RequestTask.run()` 检查该标志）。
- 阈值按线程池分别可配：`remotingWaitTimeMillsInSendQueue` / `...PullQueue` / `...HeartbeatQueue` / `...UpdateOffsetQueue` / `...TopicRouteQueue` / `...DefaultQueue`。
- 另有 `ThreadPoolHeadSlowTimeMillsMonitor`：`ThreadPoolMonitor` 周期采集队头滞留时间，超阈值自动打印 jstack，定位慢在哪个环节。

---

## 10. 关键设计总结

1. **最大化复用 remoting 框架**：Proxy 不重写网络层，而是继承 `NettyRemotingServer` 并只重写 `configChannel()`（协议嗅探）+ 注册自己的 `NettyRequestProcessor`（Activity）。编解码、opaque 匹配、连接管理、线程池分发、响应框架全部白嫖，客户端 SDK 零改动即可接入 Proxy。

2. **多协议共端口**：`ProtocolNegotiationHandler` 用前 4 字节区分 remoting（兜底）与 HTTP/2（`"PRI "` 前缀），后者经 `Http2ProtocolProxyHandler` 本机 TCP 代理到 gRPC 服务器——一个 8080 端口同时服务 4.x 和 5.x 客户端。

3. **线程池按业务隔离 + 双重过载保护**：send/pull/heartbeat/offset/route/default 六池隔离防止相互拖垮；慢队列监控（jstack）+ 过期请求清理（SYSTEM_BUSY 流控）保证雪崩时快速失败。

4. **统一异步模型**：从 `NettyServerHandler` 提交线程池开始，Activity → MessagingProcessor → Broker 全链路 `CompletableFuture`，业务线程从不阻塞等待 Broker；响应通过 `thenAccept` 回调在 IO 线程写出。

5. **RemotingChannel 双重身份**：对外是"这个客户端连接"的登记凭证（group→channel 映射，供管理指令路由），对内是反向调用通道（事务回查、运行信息查询直写 `parent()` channel），并可通过 `toRemoteChannel()` 序列化以支持 Cluster 模式下跨 Proxy 转发。

6. **路由改写 + bname 寻址**：Proxy 在 `GetTopicRouteActivity` 中把 Broker 地址改写为自身地址，客户端后续请求携带 `bname`，由 Proxy 的 ClusterService/MetadataService 解析真实 Broker——客户端拓扑被完全屏蔽。

---

## 附：核心源码文件索引

| 文件（相对 `proxy/src/main/java/org/apache/rocketmq/proxy/`） | 行号参考 |
|---|---|
| `ProxyStartup.java` | 85-96：grpc/remoting 服务器创建 |
| `remoting/RemotingProtocolServer.java` | 96-198 构造；210-241 注册；268-292 invokeToClient；351-392 清理 |
| `remoting/MultiProtocolRemotingServer.java` | 79-87 configChannel |
| `remoting/protocol/ProtocolNegotiationHandler.java` | 40-60 decode |
| `remoting/protocol/remoting/RemotingProtocolHandler.java` | 52-60 config |
| `remoting/protocol/http2proxy/Http2ProtocolProxyHandler.java` | 84-129 match/config |
| `remoting/activity/AbstractRemotingActivity.java` | 64-91 request；94-107 processRequest；121-170 响应 |
| `remoting/activity/SendMessageActivity.java` | 64-85 sendMessage |
| `remoting/activity/ClientManagerActivity.java` | 78-118 心跳；120-150 注销 |
| `remoting/channel/RemotingChannelManager.java` | 61-131 增删查 |
| `remoting/channel/RemotingChannel.java` | 115-209 反向调用 |
| `remoting/ClientHousekeepingService.java` | 38-50 事件回调 |
| `remoting/pipeline/RequestPipeline.java` | 28-33 pipe |
| `remoting/pipeline/ContextInitPipeline.java` | 33-52 execute |

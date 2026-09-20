# Elasticsearch 7.13.0 网络通信架构源码深度剖析

> 基于 Elasticsearch 7.13.0 源码（`server` 模块 + `modules/transport-netty4` 模块）
> 涉及包：`org.elasticsearch.transport`、`org.elasticsearch.http`、`org.elasticsearch.rest`、`org.elasticsearch.client.node`

---

## 目录

1. [总体架构鸟瞰](#1-总体架构鸟瞰)
2. [两条通信通道：HTTP vs Transport](#2-两条通信通道http-vs-transport)
3. [HTTP REST 通信链路（9200 端口）](#3-http-rest-通信链路9200-端口)
4. [节点间 Transport 通信链路（9300 端口）](#4-节点间-transport-通信链路9300-端口)
5. [传输协议与编解码](#5-传输协议与编解码)
6. [连接管理与握手](#6-连接管理与握手)
7. [线程模型](#7-线程模型)
8. [一次搜索请求的端到端时序](#8-一次搜索请求的端到端时序)
9. [可靠性机制：超时、重试、背压、熔断](#9-可靠性机制超时重试背压熔断)
10. [关键源码索引](#10-关键源码索引)

---

## 1. 总体架构鸟瞰

Elasticsearch 的网络层完全构建在 **Netty 4** 之上（通过 `transport-netty4` 插件模块），对外暴露两套独立的通信体系：

```mermaid
flowchart TB
    subgraph Client["客户端（curl / Kibana / Java Client）"]
        C1["HTTP REST 请求<br/>JSON DSL"]
    end

    subgraph ESNode["Elasticsearch 节点"]
        subgraph HTTPLayer["HTTP 层 :9200"]
            NETTY_HTTP["Netty4HttpServerTransport<br/>(modules/transport-netty4)"]
            REST["RestController<br/>PathTrie 路由"]
            RA["Rest*Action<br/>(RestSearchAction...)"]
            NC["NodeClient<br/>(本地 Action 分发)"]
        end

        subgraph ActionLayer["Action 层（协议无关）"]
            TA["TransportAction<br/>(TransportSearchAction...)"]
            TS["TransportService<br/>(RPC 核心：注册/发送/分发)"]
        end

        subgraph TransportLayer["Transport 层 :9300"]
            TT["TcpTransport / Netty4Transport"]
            HANDLER["RequestHandlers 注册表<br/>action 字符串 → handler"]
        end

        subgraph Core["索引引擎核心"]
            SHARD["IndexShard"]
            ENGINE["InternalEngine (Lucene)"]
        end
    end

    Peer["集群中的其他节点<br/>(数据节点/主节点)"]

    C1 -->|"HTTP/TCP"| NETTY_HTTP
    NETTY_HTTP --> REST --> RA --> NC --> TA
    TA --> TS
    TS -->|"本节点"| SHARD --> ENGINE
    TS -->|"Netty TCP 二进制协议"| TT
    TT <-->|"9300"| TT2["对端 Netty4Transport"]
    TT2 --> Peer
```

核心设计思想：

| 设计点 | 说明 |
|---|---|
| **协议无关的 Action 层** | `TransportAction` 不知道请求来自 HTTP 还是 Transport，统一通过 `TransportService` 的 `sendRequest` / `registerRequestHandler` 通信 |
| **NodeClient 本地短路** | 收到 REST 请求的协调节点通过 `NodeClient.executeLocally` 在本地发起 Action，而不是网络回环 |
| **插件化传输实现** | `TcpTransport`/`HttpServerTransport` 是抽象，Netty4 只是默认实现（理论上可替换为其他网络库） |

---

## 2. 两条通信通道：HTTP vs Transport

| 维度 | HTTP 通道（9200） | Transport 通道（9300） |
|---|---|---|
| 抽象接口 | `HttpServerTransport`（`server/.../http/HttpServerTransport.java`） | `Transport`（`server/.../transport/Transport.java`） |
| 默认实现 | `Netty4HttpServerTransport` | `Netty4Transport` |
| 协议 | HTTP/1.1 + JSON（支持 chunked、gzip） | ES 私有二进制协议（ES header + StreamOutput 序列化） |
| 服务对象 | 外部客户端 | 集群内部节点间 RPC（含跨集群 CCS） |
| 消息路由 | URL 路径 + HTTP Method（PathTrie） | action 字符串（`RequestHandlers` 注册表） |
| 压缩 | `http.compression`（默认开，级别 3） | `transport.compress`（默认关，DEFLATE 级别 3） |
| 连接复用 | HTTP Pipelining（`Netty4HttpPipeliningHandler`，默认上限 10000） | 每节点多连接池（ConnectionProfile 分级） |

```mermaid
flowchart LR
    subgraph 通道对比
        direction LR
        A["外部请求<br/>GET /index/_search"] --> H["HTTP :9200<br/>JSON 文本协议"]
        B["内部 RPC<br/>indices:data/read/search[phase_id]"] --> T["Transport :9300<br/>二进制协议"]
    end
```

> ⚠️ 注意：9300 端口的二进制协议**不保证跨大版本兼容**，Java TransportClient 在 7.x 已废弃（替换为走 HTTP 9200 的 RestHighLevelClient / Java HLRC），这是理解 ES 版本演进的重要背景。

---

## 3. HTTP REST 通信链路（9200 端口）

### 3.1 Netty Pipeline 组成

`Netty4HttpServerTransport` 启动时（`doStart()`）创建 `ServerBootstrap`，每个接入的连接由 `HttpChannelHandler.initChannel()` 装配如下 pipeline：

```mermaid
flowchart TB
    subgraph NettyPipeline["Netty ChannelPipeline（入站方向自上而下）"]
        H1["byte_buf_sizer<br/>NettyByteBufSizer（探测缓冲区）"]
        H2["read_timeout<br/>ReadTimeoutHandler（空闲超时断连）"]
        H3["decoder<br/>HttpRequestDecoder（HTTP 解码）"]
        H4["decoder_compress<br/>HttpContentDecompressor（gzip 请求解压）"]
        H5["aggregator<br/>HttpObjectAggregator（聚合为 FullHttpRequest）"]
        H6["encoder<br/>HttpResponseEncoder"]
        H7["encoder_compress<br/>HttpContentCompressor（可选，响应压缩）"]
        H8["request_creator / response_creator<br/>(Netty4HttpRequest / Response 转换)"]
        H9["pipelining<br/>Netty4HttpPipeliningHandler（HTTP 流水线化）"]
        H10["handler<br/>Netty4HttpRequestHandler（终点）"]
    end
    H1 --> H2 --> H3 --> H4 --> H5 --> H10
    H10 -.出站.-> H8 --> H7 --> H6
```

关键配置（`HttpTransportSettings`）：

- `http.port`：默认 `9200-9300`（范围绑定，取第一个可用端口）
- `http.max_content_length`：默认 100mb，聚合器上限
- `http.compression`：默认 `true`，`http.compression_level` 默认 3
- `http.pipelining.max_events`：默认 10000

### 3.2 请求分发全链路

```mermaid
sequenceDiagram
    participant C as 客户端
    participant N as Netty4HttpRequestHandler
    participant T as Netty4HttpServerTransport
    participant RC as RestController
    participant RH as RestSearchAction
    participant NClient as NodeClient
    participant TA as TransportSearchAction

    C->>N: channelRead0(HttpPipelinedRequest)
    N->>T: incomingRequest(request, nettyHttpChannel)
    Note over T: HttpPipelinedRequest → RestRequest<br/>(解析 method/path/params/headers)
    T->>RC: dispatcher.dispatchRequest(request, channel, threadContext)
    RC->>RC: getAllHandlers(params, rawPath)<br/>PathTrie 路由匹配 + Method 匹配
    Note over RC: 404/405 处理、请求体类型检查、<br/>IN_FLIGHT_REQUESTS 熔断器记账
    RC->>RH: handler.handleRequest(request, channel, client)
    RH->>RH: prepareRequest() 解析 DSL<br/>→ SearchRequest + SearchSourceBuilder
    RH->>NClient: cancelClient.execute(SearchAction.INSTANCE, req, listener)
    NClient->>NClient: executeLocally() — actions map 查找
    NClient->>TA: transportAction.execute(request, listener)
    Note over TA: 进入 Action 层，后续走 TransportService<br/>协调节点本地 + 远程分片并行执行
    TA--)RH: (异步) RestStatusToXContentListener.onResponse
    RH--)RC: channel.sendResponse(BytesRestResponse)
    RC--)T: Netty4HttpChannel 写回
    T--)C: HTTP 响应（可 gzip / chunked）
```

### 3.3 RestController 路由细节

`server/.../rest/RestController.java`：

- **`PathTrie`**：前缀树结构注册所有 REST handler，支持路径参数（如 `/{index}/_doc/{id}`）
- **`MethodHandlers`**：同一路径下按 GET/POST/PUT/DELETE 区分
- **`RestHandlerWrapper`**：全局包装器链（安全认证、`x-opaque-id` 追踪等都挂在这）
- 未匹配时：`handleBadRequest()` 返回 400；`GET /` 由 main action 处理返回节点信息
- **熔断**：请求体大小计入 `CircuitBreaker.IN_FLIGHT_REQUESTS` 断路器，响应完成后释放，防止并发大请求打爆堆内存

### 3.4 NodeClient —— REST 与 Action 的桥梁

```java
// server/.../client/node/NodeClient.java
public <Request extends ActionRequest, Response extends ActionResponse>
Task executeLocally(ActionType<Response> action, Request request, ActionListener<Response> listener) {
    return transportAction(action).execute(request, listener);  // actions map 本地查找，无网络
}
```

所有 `ActionPlugin` 注册的 Action 在节点启动时汇总到 `actions` map（`ActionModule` 完成），`NodeClient` 只是本地分发器。

---

## 4. 节点间 Transport 通信链路（9300 端口）

### 4.1 核心组件分层

```mermaid
flowchart TB
    subgraph TransportStack["Transport 栈（自上而下）"]
        TS["TransportService<br/>sendRequest / registerRequestHandler / inboundMessage"]
        CONN["TransportConnection (抽象)<br/>+ ConnectionManager"]
        TT["TcpTransport (抽象)<br/>inboundMessage / outboundMessage / 握手"]
        N4T["Netty4Transport<br/>Netty Bootstrap / Pipeline / 编解码"]
    end
    subgraph PerNode["每个远端节点：NodeChannels"]
        CH_G["ChannelProfile: 'high'<br/>集群状态发布等"]
        CH_M["ChannelProfile: 'med'（默认）<br/>常规搜索/索引请求"]
        CH_L["ChannelProfile: 'low'<br/>恢复/快照等吞吐型操作"]
        CH_P["ChannelProfile: 'ping'<br/>存活探测"]
    end
    TS --> CONN --> TT --> N4T
    CONN --> PerNode
```

`TcpTransport.NodeChannels`：**到每个远端节点不是一条连接，而是按 ConnectionProfile 建立一组连接**——高优先级消息（cluster state）不会被大块恢复数据（low 通道）阻塞。

### 4.2 出站（发送请求）流程

```mermaid
sequenceDiagram
    participant A as TransportAction (调用方)
    participant TS as TransportService
    participant CM as ConnectionManager
    participant CONN as TransportConnection
    participant OM as OutboundMessage
    participant CH as Netty4TcpChannel

    A->>TS: sendRequest(node, action, request, options, handler)
    TS->>TS: requestId = newRequestId()<br/>responseHandlers.add(requestId, handler)
    Note over TS: TimeoutTransportHandler 包装<br/>注册超时清理任务
    TS->>CM: getConnection(node)
    CM-->>TS: TransportConnection（已建立）
    TS->>CONN: sendRequest(requestId, action, request, options)
    CONN->>OM: new OutboundMessage.Request(threadContext, version, compress...)
    Note over OM: TransportRequest.writeTo(StreamOutput)<br/>可选 DEFLATE 压缩（级别3）<br/>写入 TcpHeader
    CONN->>CH: channel.writeAndFlush(BytesReference)
    Note over CH: Netty event loop 异步发送
```

### 4.3 入站（接收并分发）流程

```mermaid
sequenceDiagram
    participant NET as Netty EventLoop 线程
    participant MC as Netty4MessageChannelHandler
    participant IP as InboundPipeline
    participant TT as TcpTransport
    participant TS as TransportService
    participant REG as RequestHandlerRegistry
    participant TP as ThreadPool
    participant H as TransportRequestHandler

    NET->>MC: channelRead(ByteBuf)
    MC->>IP: handleBytes(channel, bytes)
    Note over IP: InboundDecoder 按帧拆分<br/>校验 magic "ES" / 长度 / 版本<br/>解压 → InboundMessage
    IP->>TT: inboundMessage(TcpChannel, InboundMessage)
    TT->>TS: inboundMessage(connection, message)
    alt 握手/PING 消息
        Note over TS: TransportHandshaker / PING-PONG<br/>在 IO 线程直接处理
    else 正常请求
        TS->>REG: requestHandlers.get(action)
        REG-->>TS: RequestHandlerRegistry
        Note over TS: 反序列化 request（NamedWriteable）
        alt executor == SAME
            TS->>H: messageReceived (IO 线程直接执行)
        else 其他 executor
            TS->>TP: executor.execute(() -> handler.messageReceived(...))
            TP->>H: 在 search/get/generic... 线程池执行
            H--)TS: TransportChannel.sendResponse(response)
        end
    end
```

### 4.4 请求注册机制（action 字符串路由）

```java
// TransportService.java
public <Request extends TransportRequest> void registerRequestHandler(
        String action, String executor, Writeable.Reader<Request> reader,
        TransportRequestHandler<Request> handler) {
    requestHandlers.register(new RequestHandlerRegistry<>(action, handler, reader, executor, ...));
}
```

- **action 字符串示例**：`indices:data/read/search[np]`（np=查询阶段编号）、`indices:data/write/bulk[s][r]`（分片/副本阶段）
- 每个 `TransportAction` 在构造时向 `TransportService` 注册自己的 handler
- `TransportInterceptor`（`AsyncSender` 装饰链）允许插件在发送前后插入逻辑（安全、追踪）

### 4.5 响应回传

handler 处理完后通过 `TransportChannel.sendResponse()` 写回（`TcpTransportChannel`），响应同样走 `OutboundMessage.Response` → header 中 status 标记 `isResponse`。发送方收到后：

1. `InboundDecoder` 解出 response，按 header 中的 `requestId` 在 `responseHandlers` map 中查找注册的 `TransportResponseHandler`
2. 回调 `handleResponse()` / `handleException()`（在 handler 指定的 executor 上执行，默认 `SAME`）

---

## 5. 传输协议与编解码

### 5.1 消息帧格式（`TcpHeader.java`）

```mermaid
flowchart LR
    subgraph Wire["ES Transport 二进制帧"]
        M["MAGIC<br/>2B: 0x45 0x53 'ES'"]
        S["messageSize<br/>4B"]
        RID["requestId<br/>8B"]
        ST["status<br/>1B 位标志"]
        V["version<br/>4B"]
        VH["variableHeaderSize<br/>4B (7.6+)"]
        BODY["消息体（可压缩）<br/>features + threadContext + payload"]
    end
    M --> S --> RID --> ST --> V --> VH --> BODY
```

| 字段 | 大小 | 说明 |
|---|---|---|
| `MAGIC` | 2B | 固定 `0x45 0x53`（"ES"），帧同步/协议识别 |
| `messageSize` | 4B | 帧总长（用于 TCP 流上切帧） |
| `requestId` | 8B | 请求-响应关联 ID（唯一，线程安全生成） |
| `status` | 1B | bit0: request/response；bit1: error response；bit2: compressed；bit3: handshake |
| `version` | 4B | 发送方节点版本 ID，接收方据此选择 BwC 编解码 |
| `variableHeaderSize` | 4B | 7.6.0 引入的可变头部长度（feature flags） |

**消息体**（`InboundMessage` / `OutboundMessage`，7.13 的新协议抽象）：
- features 集合（如 `x-pack` 特性协商）
- `ThreadContext`（上下文头，请求头/系统设置随请求传播——ES 的"隐式上下文传递"机制）
- 真实 payload：request 对象经 `writeTo(StreamOutput)` 序列化的字节

### 5.2 序列化体系

```mermaid
flowchart TB
    WR["Writeable 接口<br/>writeTo(StreamOutput)"]
    NW["NamedWriteable<br/>按注册名反序列化（Registry）"]
    TR["TransportRequest / TransportResponse<br/>(带 parentTask, headers)"]
    SI["StreamInput / StreamOutput<br/>版本感知的编解码"]
    V["Version 参数<br/>决定 BwC 序列化格式"]
    WR --> SI
    NW --> SI
    TR --> WR
    SI --> V
```

- `NamedWriteableRegistry`：类似序列化框架的类型注册表，查询 DSL（`QueryBuilder`）、聚合（`AggregatorBuilder`）等通过**名字字符串**跨节点重建对象
- `Version`：`Version.minCompatVersion()` 保证相邻主版本互操作（7.x ↔ 6.8），序列化时根据对端版本走旧格式分支

### 5.3 压缩

`CompressibleBytesOutputStream`（`server/.../transport/CompressibleBytesOutputStream.java`）：
- 算法：JDK `DeflaterOutputStream`（DEFLATE），级别固定 3
- 触发：`transport.compress: true` 时（默认关闭，节点间内网带宽通常足够）
- header `status` bit2 标记是否压缩，接收端据此解压

---

## 6. 连接管理与握手

### 6.1 握手流程（TransportHandshaker）

```mermaid
sequenceDiagram
    participant A as 节点A (发起方)
    participant B as 节点B (监听 :9300)
    Note over A,B: TCP 三次握手完成后
    A->>B: 握手请求 (header status bit3=handshake)<br/>携带 minCompatVersion
    B->>A: 握手响应<br/>节点ID + Version + clusterName
    A->>A: 校验 clusterName 一致<br/>校验版本互相兼容
    alt 校验失败
        A--xB: 关闭连接 (IllegalStateException)
    else 校验通过
        Note over A: ConnectionManager 记录 TransportConnection<br/>建立 NodeChannels (high/med/low/ping)
    end
```

- 集群名不一致 → 拒绝加入（防止误连集群）
- 版本不兼容 → 连接失败（滚动升级时保证主版本差 ≤1）
- TLS：若启用 x-pack security，握手前有 TLS 握手层（`TransportLayer` 接口，`Netty4NioServerSocketChannel` 包装 SSLEngine）。未启用 TLS 的节点收到 TLS ClientHello（首字节 `0x16 0x03`，见 `TcpTransport.appearsToBeTLS()`）会抛 `StreamCorruptedException` 并给出友好提示

### 6.2 连接生命周期（ConnectionManager）

- `ClusterConnectionManager`：管理集群内所有节点连接（master、数据节点间 mesh）
- `RemoteConnectionManager`：跨集群连接（CCS），种子节点 + 网关模式，失败种子节点会被暂时隔离避免反复重试
- 断连：`TransportService.onNodeDisconnected()` → **批量清理该节点上所有 pending 的 responseHandlers**（回调 `handleException(NodeDisconnectedException)`），这是副本请求失败快速反馈的来源

---

## 7. 线程模型

```mermaid
flowchart TB
    subgraph IO["Netty EventLoop 线程（http_server_worker / transport_worker）"]
        E1["编解码 / 拆帧 / 解压"]
        E2["握手、PING-PONG"]
        E3["SAME executor 的轻量 handler"]
    end
    subgraph TP["ThreadPool 业务线程池"]
        P1["generic<br/>通用/网络回调"]
        P2["management<br/>集群管理"]
        P3["get<br/>GET 实时读取"]
        P4["search / search_throttled<br/>查询/聚合"]
        P5["write / bulk<br/>写入"]
        P6["flush / refresh / force_merge"]
        P7["snapshot / recovery"]
    end
    E1 -->|"executor 名分发"| TP
    E2 -.-> E3
```

| 层 | 执行者 | 说明 |
|---|---|---|
| 帧解码、magic 校验 | Netty IO 线程 | `InboundDecoder`，纯字节操作，不阻塞 |
| 握手 / PING | Netty IO 线程 | 必须快，不能占用业务池 |
| `TransportRequestHandler` | 按 action 注册时声明的 executor | 如 search phase 交给 `search` 池 |
| `TransportResponseHandler` | 默认 `SAME`（IO 线程） | 协调节点聚合各分片响应时多为轻量计数，走 SAME 提高吞吐 |
| REST handler | `management`（HTTP 层统一入口） | 真正的重活交给后续 Action |

**设计要点**：action 字符串末尾的 `[np]`/`[s]`/`[r]` 后缀让同一 action 的不同阶段在不同 executor 上隔离执行，避免慢查询阻塞集群状态发布。

---

## 8. 一次搜索请求的端到端时序

以 `GET /myindex/_search` 为例，请求落在协调节点（coordinating node），跨 3 个数据节点：

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant Coord as 协调节点 :9200
    participant CS as TransportService(协调点)
    participant D1 as 数据节点1 :9300
    participant D2 as 数据节点2 :9300
    participant D3 as 数据节点3 :9300

    Client->>Coord: HTTP GET /myindex/_search
    Coord->>Coord: Netty pipeline → RestController → RestSearchAction<br/>→ NodeClient → TransportSearchAction
    Coord->>Coord: 按索引路由表计算目标分片<br/>shard = hash(routing) % num_primary_shards
    par 并行转发（med 通道）
        CS->>D1: sendRequest("indices:data/read/search[np]", SearchRequest)
        CS->>D2: sendRequest("indices:data/read/search[np]", SearchRequest)
        CS->>D3: sendRequest("indices:data/read/search[np]", SearchRequest)
    end
    D1->>D1: TransportService.inboundMessage → query 阶段<br/>QueryPhase: ContextIndexSearcher(Lucene)
    D1-->>CS: SearchPhaseController 合并 top-N docId
    D2-->>CS: (各分片响应)
    D3-->>CS: (各分片响应)
    Coord->>Coord: reduce 归并排序选出最终 top-N
    par fetch 阶段（第二轮 RPC）
        CS->>D1: "indices:data/read/search[qp]" 取 _source
        CS->>D2: 同上
    end
    D1-->>CS: FetchSearchResult
    D2-->>CS: FetchSearchResult
    Coord->>Coord: 组装 SearchResponse<br/>RestStatusToXContentListener → BytesRestResponse
    Coord-->>Client: HTTP 200 + JSON
```

> **两阶段搜索**（query then fetch）在网络上体现为：协调节点对每个目标数据节点发起**两轮** Transport RPC——第一轮只要排序后的 docId + score，第二轮按 docId 批量取 `_source`。这直接减少了跨节点传输量。

---

## 9. 可靠性机制：超时、重试、背压、熔断

```mermaid
flowchart TB
    subgraph Reliability["通信可靠性体系"]
        TO["TimeoutTransportHandler<br/>每个请求注册超时任务，<br/>超时后从 responseHandlers 摘除并回调异常"]
        RID["requestId 生命周期<br/>生成→注册→匹配响应→移除<br/>节点断连时批量清理"]
        FB["重连管理<br/>ConnectionManager + ping 探测<br/>失败节点临时隔离"]
        CB["CircuitBreaker<br/>HTTP: IN_FLIGHT_REQUESTS<br/>(并发请求体大小上限)"]
        BF["InboundBuffer / 请求体大小上限<br/>限制单帧大小防 OOM"]
    end
```

- **超时**：`TransportService.sendRequest` 用 `TimeoutTransportHandler` 包装用户 handler，调度到 `generic` 池的清理任务在超时后触发 `handleException(ReceiveTimeoutTransportException)`
- **请求丢失**：`ConnectionManager` 关闭连接时触发 `onNodeDisconnected`，`responseHandlers` 中所有指向该节点的 pending handler 收到 `NodeDisconnectedException`——上层 `TransportReplicationAction` 据此决定副本失效或重试到新主分片
- **背压**：
  - 入站：帧大小上限（`network.tcp.buffer`? 实际由 messageSize 校验）+ `ReadTimeoutHandler` 断空闲连接
  - HTTP：`http.max_content_length` + in-flight 断路器
- **熔断**：`CircuitBreakerService`，HTTP 层记账的是请求字节数（响应写回后释放），超出即拒绝请求（429 Too Many Requests）

---

## 10. 关键源码索引

| 组件 | 文件 |
|---|---|
| TCP 传输抽象 | `server/src/main/java/org/elasticsearch/transport/TcpTransport.java` |
| Netty 传输实现 | `modules/transport-netty4/src/main/java/org/elasticsearch/transport/netty4/Netty4Transport.java` |
| RPC 核心 | `server/src/main/java/org/elasticsearch/transport/TransportService.java` |
| 请求注册表 | `server/src/main/java/org/elasticsearch/transport/RequestHandlers.java` |
| 消息头定义 | `server/src/main/java/org/elasticsearch/transport/TcpHeader.java` |
| 入/出站消息 | `server/src/main/java/org/elasticsearch/transport/InboundMessage.java` / `OutboundMessage.java` |
| 入站处理管道 | `server/src/main/java/org/elasticsearch/transport/InboundPipeline.java` |
| 握手 | `server/src/main/java/org/elasticsearch/transport/TransportHandshaker.java` |
| 连接管理 | `server/src/main/java/org/elasticsearch/transport/ConnectionManager.java` |
| 压缩 | `server/src/main/java/org/elasticsearch/transport/CompressibleBytesOutputStream.java` |
| HTTP 抽象 | `server/src/main/java/org/elasticsearch/http/HttpServerTransport.java` |
| HTTP 实现 | `modules/transport-netty4/src/main/java/org/elasticsearch/http/netty4/Netty4HttpServerTransport.java` |
| HTTP 请求处理 | `modules/transport-netty4/src/main/java/org/elasticsearch/http/netty4/Netty4HttpRequestHandler.java` |
| REST 路由 | `server/src/main/java/org/elasticsearch/rest/RestController.java` |
| REST→Action 桥 | `server/src/main/java/org/elasticsearch/client/node/NodeClient.java` |
| 搜索 REST 入口 | `server/src/main/java/org/elasticsearch/rest/action/search/RestSearchAction.java` |
| 复制类 Action 范式 | `server/src/main/java/org/elasticsearch/action/support/replication/TransportReplicationAction.java` |

---

## 总结

ES 网络通信的核心可以浓缩为三句话：

1. **对外（9200）**：Netty HTTP → `RestController`(PathTrie 路由) → `Rest*Action` 解析 DSL → `NodeClient` 本地转交 Action 层，异步 listener 链一路写回响应。
2. **对内（9300）**：`TransportService` 提供以 **action 字符串** 为路由键的异步 RPC，`"ES" magic + requestId + version` 的二进制帧 + `StreamOutput` 序列化 + `ThreadContext` 随请求传播；每节点按 high/med/low/ping 分级建连，断连时 pending 请求批量失败。
3. **分层解耦**：Netty IO 线程只做编解码和握手，业务逻辑按 action 声明的 executor 落到 `ThreadPool` 的专属线程池——这是 ES 能同时扛住慢查询和集群状态发布而互不干扰的根本原因。

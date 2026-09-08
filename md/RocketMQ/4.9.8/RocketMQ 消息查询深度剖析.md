# RocketMQ 消息查询深度剖析

> 基于 RocketMQ 4.9.8 源码分析:消息查询的原理与流程、Dashboard 控制台查询与 Admin Tool 查询的对比、`tools.cmd` 与 `mqadmin.cmd` 脚本的区别、tools/client 模块与脚本的映射关系。

---

## 目录

1. [总体架构](#1-总体架构)
2. [消息查询的底层原理](#2-消息查询的底层原理)
3. [Admin Tool 查询流程(mqadmin)](#3-admin-tool-查询流程mqadmin)
4. [Dashboard 控制台查询流程](#4-dashboard-控制台查询流程)
5. [两种查询方式的对比](#5-两种查询方式的对比)
6. [tools.cmd 与 mqadmin.cmd 的区别](#6-toolscmd-与-mqadmincmd-的区别)
7. [模块与脚本的映射关系](#7-模块与脚本的映射关系)
8. [关键源码索引](#8-关键源码索引)

---

## 1. 总体架构

无论是 Dashboard 控制台还是 Admin Tool,本质上都是 **RocketMQ 客户端**,都通过同一套客户端内核(`MQClientInstance` / `MQClientAPIImpl`)与 NameServer、Broker 通信。Broker 端由 `QueryMessageProcessor` 统一处理查询请求,底层依赖 **IndexFile 索引文件** 与 **CommitLog**。

```mermaid
graph TB
    subgraph UserLayer["用户入口层"]
        DASH["RocketMQ Dashboard<br/>(Spring Boot Web 应用)"]
        CLI["mqadmin.cmd / mqadmin<br/>(命令行工具)"]
        EXAMPLE["tools.cmd + 任意主类<br/>(通用启动器,可跑 example)"]
    end

    subgraph ToolsModule["tools 模块 (rocketmq-tools.jar)"]
        STARTUP["MQAdminStartup<br/>命令分发器"]
        SUBCMD["各 SubCommand<br/>QueryMsgById / QueryMsgByKey<br/>QueryMsgByUniqueKey ..."]
        ADMINEXT["DefaultMQAdminExt / DefaultMQAdminExtImpl<br/>Admin 外观 API"]
    end

    subgraph ClientModule["client 模块 (rocketmq-client.jar)"]
        ADMINIMPL["MQAdminImpl<br/>查询核心逻辑"]
        API["MQClientAPIImpl<br/>Remoting 协议封装"]
        INSTANCE["MQClientInstance<br/>客户端实例(路由缓存/Netty)"]
    end

    subgraph DashboardModule["Dashboard 应用内(非 RocketMQ 源码)"]
        CTRL["MessageController 等"]
    end

    subgraph Server["服务端"]
        NS["NameServer<br/>路由信息"]
        subgraph BrokerProc["Broker"]
            QMP["QueryMessageProcessor<br/>QUERY_MESSAGE / VIEW_MESSAGE_BY_ID"]
            STORE["DefaultMessageStore"]
            INDEX["IndexService / IndexFile<br/>哈希索引"]
            CLOG["CommitLog<br/>消息存储"]
        end
    end

    DASH --> CTRL
    CLI --> STARTUP
    STARTUP --> SUBCMD
    CTRL -->|直接依赖| ADMINEXT
    SUBCMD --> ADMINEXT
    ADMINEXT --> ADMINIMPL
    ADMINIMPL --> API
    INSTANCE --> API
    API -->|Netty Remoting| QMP
    API -->|获取路由| NS
    QMP --> STORE
    STORE --> INDEX
    STORE --> CLOG
```

**核心结论:Dashboard 与 mqadmin 在客户端底层走的是完全相同的代码路径**(`DefaultMQAdminExtImpl` → `MQAdminImpl` → `MQClientAPIImpl`),区别只在最外层的入口形态(Web 页面 vs 命令行)。

---

## 2. 消息查询的底层原理

### 2.1 两种查询维度

| 查询方式 | 输入 | 依赖的存储结构 | 复杂度 | RequestCode |
|---|---|---|---|---|
| **按 MessageId 查(View)** | `ip@port@phyOffset` | CommitLog 直接定位 | O(1) | `VIEW_MESSAGE_BY_ID (33)` |
| **按 Key/UniqueKey 查(Query)** | topic + key + 时间范围 | IndexFile 哈希索引 + CommitLog | hash 查找 + 冲突链遍历 | `QUERY_MESSAGE (14)` |

### 2.2 MessageId 的结构

MessageId 由客户端在发送时生成(`MessageClientIDSetter`),格式为 `IP@PORT@物理偏移量`。**最后一段就是消息写入 CommitLog 后的全局物理偏移量**,因此按 Id 查询根本不需要索引:

```
7F000001@1F90@000000000000000000C8
   │      │            │
   │      │            └── phyOffset (CommitLog 全局偏移量)
   │      └── broker 端口
   └── broker IP(发送该消息时所在的 broker 地址)
```

客户端解析逻辑(`MQAdminImpl.viewMessage`, MQAdminImpl.java:258):

```java
messageId = MessageDecoder.decodeMessageId(msgId);          // 解析出 address + offset
api.viewMessage(socketAddress2String(messageId.getAddress()),
                messageId.getOffset(), timeoutMillis);       // 直连该 broker 按 offset 读取
```

### 2.3 IndexFile 哈希索引结构

按 Key 查询依赖 broker 端构建的索引文件。消息写入 CommitLog 后,`ReputMessageService`(异步 reput 线程)会为每条消息的 `UNIQ_KEY` 和用户 keys 各建一条索引:

```mermaid
graph LR
    subgraph IndexFile["IndexFile (单个文件默认 400MB)"]
        HEADER["IndexHeader<br/>beginTimestamp / endTimestamp<br/>beginPhyOffset / endPhyOffset<br/>hashSlotCount / indexCount"]
        subgraph Slots["Hash Slot Table (默认 500w 个槽)"]
            S0["slot[0]<br/>最新 index 下标"]
            S1["slot[1]"]
            SN["slot[n] ..."]
        end
        subgraph Entries["Index 条目表 (默认 2000w 条,单向链表)"]
            E1["keyHash(4B)<br/>phyOffset(8B)<br/>timeDiff(4B)<br/>prevIndex(4B)"]
            E2["..."]
        end
    end

    K["key 字符串"] -->|"hash(key) % slotNum"| SN
    SN -->|"slot 存最新条目下标"| E1
    E1 -->|"prevIndex 冲突链"| E2
    E2 -->|"phyOffset"| CLOG["CommitLog"]
```

查询流程(`IndexService.queryOffset`):

1. 对 key 计算 `hash(key) % slotCount` 定位到槽;
2. 取槽中最新 index 条目下标,沿 `prevIndex` 指针**逆向遍历链表**(新消息在前);
3. 校验 `keyHash` 一致 + `storeTime` 在 `[begin, end]` 范围内,收集对应的 `phyOffset`;
4. 槽中记录的 `indexLastUpdateTimestamp` 若早于查询起始时间,说明**索引构建滞后**,需重试。

### 2.4 Broker 端消息回传:PageCache + 零拷贝

Broker 查到消息后不反序列化,而是把 CommitLog 的 MappedFile 缓冲区包装成 Netty `FileRegion`,通过 **sendfile 零拷贝**直接写回网络(QueryMessageProcessor.java:101):

```java
FileRegion fileRegion = new QueryMessageTransfer(
    response.encodeHeader(queryMessageResult.getBufferTotalSize()),
    queryMessageResult);
ctx.channel().writeAndFlush(fileRegion);
```

---

## 3. Admin Tool 查询流程(mqadmin)

### 3.1 命令入口链

```
mqadmin.cmd queryMsgById -i <msgId>
        │
        └─ call tools.cmd org.apache.rocketmq.tools.command.MQAdminStartup queryMsgById -i <msgId>
                │
                └─ MQAdminStartup.main()
                     ├─ initCommand(): 注册全部 SubCommand
                     ├─ 解析第一个参数 → 找到 QueryMsgByIdSubCommand
                     └─ subCommand.execute(commandLine, options, rpcHook)
                          └─ DefaultMQAdminExt.start() → 发起查询 → shutdown()
```

`QueryMsgByKeySubCommand.execute`(QueryMsgByKeySubCommand.java:56)的典型模式:

```java
DefaultMQAdminExt admin = new DefaultMQAdminExt(rpcHook);
admin.setInstanceName(Long.toString(System.currentTimeMillis())); // 避免实例名冲突
admin.start();
QueryResult queryResult = admin.queryMessage(topic, key, 64, 0, Long.MAX_VALUE);
admin.shutdown();
```

### 3.2 时序图:queryMsgById(按 MessageId 查询)

```mermaid
sequenceDiagram
    autonumber
    participant U as 运维人员
    participant CMD as mqadmin.cmd
    participant TOOLS as tools.cmd (JVM)
    participant START as MQAdminStartup
    participant SUB as QueryMsgByIdSubCommand
    participant EXT as DefaultMQAdminExtImpl
    participant ADMIN as MQAdminImpl (client模块)
    participant API as MQClientAPIImpl
    participant B as Broker<br/>QueryMessageProcessor
    participant S as DefaultMessageStore

    U->>CMD: mqadmin.cmd queryMsgById -i msgId
    CMD->>TOOLS: call tools.cmd MQAdminStartup queryMsgById -i msgId
    TOOLS->>START: java ... MQAdminStartup queryMsgById -i msgId
    START->>SUB: dispatch("queryMsgById")
    SUB->>EXT: new DefaultMQAdminExt(rpcHook).start()
    SUB->>EXT: viewMessage(msgId)
    EXT->>ADMIN: viewMessage(msgId)
    ADMIN->>ADMIN: MessageDecoder.decodeMessageId(msgId)<br/>解析出 broker 地址 + phyOffset
    ADMIN->>API: viewMessage(brokerAddr, offset)
    API->>B: VIEW_MESSAGE_BY_ID (RequestCode 33)<br/>header: offset
    B->>S: selectOneMessageByOffset(offset)
    S-->>B: SelectMappedBufferResult<br/>(CommitLog MappedFile 切片)
    B-->>API: response header + FileRegion<br/>(零拷贝传输消息体)
    API-->>ADMIN: MessageExt(客户端解码)
    ADMIN-->>SUB: MessageExt
    SUB->>SUB: 打印 Message ID / QID / Offset / 消息体
    SUB->>EXT: admin.shutdown()
```

### 3.3 时序图:queryMsgByKey / queryMsgByUniqueKey(按 Key 查询)

```mermaid
sequenceDiagram
    autonumber
    participant SUB as QueryMsgByKeySubCommand
    participant EXT as DefaultMQAdminExtImpl
    participant ADMIN as MQAdminImpl (client模块)
    participant NS as NameServer
    participant API as MQClientAPIImpl
    participant B as Broker<br/>QueryMessageProcessor
    participant S as DefaultMessageStore
    participant IDX as IndexService / IndexFile

    SUB->>EXT: queryMessage(topic, key, maxNum, begin, end)
    EXT->>ADMIN: queryMessage(topic, key, maxNum, begin, end, isUniqKey)
    ADMIN->>NS: 获取 Topic 路由信息
    NS-->>ADMIN: TopicRouteData (BrokerData 列表)
    ADMIN->>ADMIN: 收集所有 broker 地址<br/>(selectBrokerAddr 优先 master)

    par 并行查询每个 Broker
        ADMIN->>API: queryMessage(brokerAddr1, header)
        API->>B: QUERY_MESSAGE (RequestCode 14)<br/>topic/key/maxNum/begin/end<br/>+ UNIQUE_MSG_QUERY_FLAG
        B->>S: queryMessage(topic, key, maxNum, begin, end)
        S->>IDX: queryOffset(topic, key, maxNum, begin, end)
        IDX->>IDX: hash(key)%slotNum 定位槽<br/>沿 prevIndex 遍历冲突链<br/>过滤时间范围
        IDX-->>S: phyOffsets 列表<br/>+ indexLastUpdateTimestamp
        loop 最多重试 3 次(处理索引滞后)
            S->>S: lookMessageByOffset(phyOffset)
            S->>S: commitLog.getData(offset, false)<br/>读取完整消息
        end
        S-->>B: QueryMessageResult
        B-->>API: FileRegion 零拷贝回传
        API-->>ADMIN: 回调收集 MessageExt 列表
    and
        ADMIN->>API: queryMessage(brokerAddr2, ...) (同上)
    end

    ADMIN->>ADMIN: CountDownLatch.await() 等待全部返回<br/>合并所有 broker 结果
    ADMIN-->>EXT: QueryResult(messageList)
    EXT-->>SUB: QueryResult
    SUB->>SUB: 打印 #Message ID / #QID / #Offset
```

### 3.4 关键源码逻辑:DefaultMessageStore.queryMessage

`DefaultMessageStore.queryMessage`(DefaultMessageStore.java:978):

```java
long lastQueryMsgTime = end;
for (int i = 0; i < 3; i++) {   // 最多 3 次,处理索引构建滞后
    QueryOffsetResult r = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
    if (r.getPhyOffsets().isEmpty()) break;

    for (int m = 0; m < r.getPhyOffsets().size(); m++) {
        long offset = r.getPhyOffsets().get(m);
        MessageExt msg = this.lookMessageByOffset(offset);
        if (0 == m) lastQueryMsgTime = msg.getStoreTimestamp();  // 逐步收缩时间上界
        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
        queryMessageResult.addMessage(result);
    }
    if (queryMessageResult.getBufferTotalSize() > 0) break;
    if (lastQueryMsgTime < begin) break;
}
```

**为什么要重试?** IndexFile 由 reput 线程异步构建,若消息刚写入还没建索引,第一次查不到;通过 `indexLastUpdateTimestamp` 判断索引落后程度,收缩 `lastQueryMsgTime` 后再查。

### 3.5 queryMsgByUniqueKey 的特殊之处

UNIQ_KEY 本质也是走 IndexFile 查询,但有两个增强(DefaultMQAdminExtImpl.java:1126、MQAdminImpl.java:283):

- 客户端通过 `MessageClientIDSetter.getNearlyTimeFromID(uniqKey)` 从 uniqKey 中**反推出消息的大致生成时间**(uniqKey 内嵌时间戳),从而自动推算 begin 时间,用户无需手动指定时间范围;
- Broker 端检测到 `UNIQUE_MSG_QUERY_FLAG=true` 时,将 maxNum 强制改为 `defaultQueryMaxNum`(QueryMessageProcessor.java:82),因为 uniqKey 逻辑上只对应一条消息。

---

## 4. Dashboard 控制台查询流程

RocketMQ Dashboard(原 rocketmq-console,独立的 Spring Boot 项目)是**长期运行的 Web 服务**,内部直接引入 `rocketmq-tools` 依赖,通过 `DefaultMQAdminExt` 与集群交互。

### 4.1 与 Admin Tool 的架构差异

```mermaid
graph TB
    subgraph DashboardApp["Dashboard 进程(常驻)"]
        BRW["浏览器"]
        MVC["MessageController<br/>/message/viewMessage.query<br/>/message/queryMessageByTopic.query"]
        SVC["MessageServiceImpl"]
        EXT2["DefaultMQAdminExt<br/>(应用启动时创建、常驻复用)"]
    end
    subgraph AdminToolCLI["Admin Tool 进程(按需创建)"]
        SH["mqadmin.cmd"]
        SUB2["SubCommand<br/>(每次命令 new + start + shutdown)"]
        EXT1["DefaultMQAdminExt<br/>(生命周期与命令一致)"]
    end

    BRW -->|HTTP| MVC --> SVC --> EXT2
    SH --> SUB2 --> EXT1

    EXT1 --> COMMON["MQAdminImpl → MQClientAPIImpl<br/>→ Netty → Broker/NameServer"]
    EXT2 --> COMMON
```

### 4.2 Dashboard 查询消息的时序图

```mermaid
sequenceDiagram
    autonumber
    participant B as 浏览器
    participant C as MessageController
    participant S as MessageServiceImpl
    participant EXT as DefaultMQAdminExt<br/>(Dashboard 常驻实例)
    participant ADMIN as MQAdminImpl
    participant NS as NameServer
    participant BK as Broker

    Note over EXT: Dashboard 启动时已创建并 start()<br/>长期持有,复用连接

    B->>C: GET /message/viewMessage.query?msgId=xxx
    C->>S: viewMessage(topic, msgId)
    S->>EXT: viewMessage(msgId) 或 queryMessageByUniqKey(...)
    EXT->>ADMIN: viewMessage(msgId)

    alt msgId 是标准 offset 型 ID
        ADMIN->>ADMIN: decodeMessageId → address + phyOffset
        ADMIN->>BK: VIEW_MESSAGE_BY_ID
        BK-->>ADMIN: 消息体(零拷贝)
    else 是 uniqKey(纯 UNIQ_KEY 查询)
        ADMIN->>NS: 获取 topic 路由
        ADMIN->>BK: QUERY_MESSAGE + UNIQUE_MSG_QUERY_FLAG
        BK-->>ADMIN: QueryResult
    end

    ADMIN-->>S: MessageExt / MessageExt 列表
    S-->>C: MessageView(含消息体、重试次数、轨迹等)
    C-->>B: JSON 渲染到页面
```

> 注:Dashboard 按 Topic + 时间范围浏览消息(`queryMessageByTopic`)走的是另一条路径——先 `searchOffset(mq, timestamp)` 按时间定位每个队列的起始 offset,再调用 `QUERY_CONSUMER_TIME_SPAN` / `pullMessage` 拉取,而不是走 IndexFile。这是"按 Topic 浏览"与"按 Key 查询"的本质区别。

### 4.3 Dashboard 与 Admin Tool 的本质对比

| 维度 | Dashboard | Admin Tool (mqadmin) |
|---|---|---|
| 形态 | 常驻 Web 服务 | 按需执行的一次性进程 |
| 依赖方式 | Maven 依赖 `rocketmq-tools` jar | 发行包 `lib/*` 全量 jar |
| AdminExt 生命周期 | 启动时创建、全局复用(连接池复用) | 每条命令 `new → start → 用完 shutdown` |
| 查询入口 API | `MessageService.viewMessage / queryMessageByTopic` | `QueryMsgByIdSubCommand` 等 SubCommand |
| 客户端底层 | **完全相同**:`DefaultMQAdminExtImpl → MQAdminImpl → MQClientAPIImpl` | **完全相同** |
| 认证 | 集中配置 ACL(rpcHook 全局注入) | 每次命令 `-u/-p` 或 `tools.yml`/`admin.properties` |
| 输出 | JSON → 页面渲染,可看轨迹、重试次数 | stdout 表格/文本 |

---

## 5. 两种查询方式的对比

从**查询语义**维度(与入口无关,Dashboard 和 mqadmin 都支持这两种语义):

| | 按 MessageId | 按 Key / UniqueKey |
|---|---|---|
| 底层协议 | `VIEW_MESSAGE_BY_ID` | `QUERY_MESSAGE` |
| 目标 broker | 由 msgId 内嵌地址直接决定 | 从 NameServer 路由获取,并行查所有 broker |
| 是否需要索引 | 否,CommitLog O(1) 定位 | 是,依赖 IndexFile(`messageIndexEnable=true`) |
| 时间范围 | 不需要 | 需要(默认 0 ~ Long.MAX) |
| 索引滞后处理 | 不存在 | 最多重试 3 次,逐步收缩 lastQueryMsgTime |
| 查不到的常见原因 | CommitLog 文件已被删除(过期清理) | 索引文件过期删除 / keys 未设置 / 时间范围错误 / broker 未建索引 |

```mermaid
flowchart TD
    Q["发起消息查询"] --> T{查询类型?}

    T -->|"按 MessageId<br/>(queryMsgById)"| A1["decodeMessageId(msgId)<br/>解析 IP@PORT@OFFSET"]
    A1 --> A2["直连对应 Broker<br/>发 VIEW_MESSAGE_BY_ID"]
    A2 --> A3["Broker: commitLog.getData(offset)<br/>从 MappedFile 直接读取"]
    A3 --> Z["零拷贝 FileRegion 回传"]

    T -->|"按 Key<br/>(queryMsgByKey / ByUniqueKey)"| B1["从 NameServer 获取 topic 路由"]
    B1 --> B2["收集全部 broker 地址"]
    B2 --> B3["并行发送 QUERY_MESSAGE<br/>CountDownLatch 等待"]
    B3 --> B4["Broker: indexService.queryOffset<br/>hash 定位槽 → 遍历冲突链<br/>过滤时间范围 → phyOffsets"]
    B4 --> B5{"查到消息?"}
    B5 -->|"否,索引滞后?"| B6["收缩 lastQueryMsgTime<br/>重试(最多3次)"]
    B6 --> B4
    B5 -->|"是"| B7["按每个 phyOffset<br/>commitLog.getData(offset)"]
    B7 --> Z
    B5 -->|"否,索引也未更新"| B8["返回 QUERY_NOT_FOUND"]

    Z --> MERGE["客户端合并各 broker 结果<br/>MessageDecoder 解码"]
    MERGE --> OUT["输出: CLI 表格 / Dashboard JSON"]
```

---

## 6. tools.cmd 与 mqadmin.cmd 的区别

### 6.1 脚本内容对照

**tools.cmd(distribution/bin/tools.cmd)— 通用 Java 启动器:**

```bat
set CLASSPATH=.;%BASE_DIR%conf;%BASE_DIR%lib\*;%CLASSPATH%     REM 加载所有 jar
set "JAVA_OPT=%JAVA_OPT% -server -Xms1g -Xmx1g -Xmn256m ..."    REM JVM 参数在这里
"%JAVA%" %JAVA_OPT% %*                                          REM 第一个参数 = 主类名
```

**mqadmin.cmd(distribution/bin/mqadmin.cmd)— mqadmin 专用包装:**

```bat
if not exist "%ROCKETMQ_HOME%\bin\tools.cmd" echo Please set the ROCKETMQ_HOME ... & EXIT /B 1
call "%ROCKETMQ_HOME%\bin\tools.cmd" org.apache.rocketmq.tools.command.MQAdminStartup %*
```

### 6.2 关系图

```mermaid
graph TD
    USER1["运维: mqadmin.cmd queryMsgById ..."] --> MQA["mqadmin.cmd<br/>(薄包装: 固定主类)"]
    USER2["开发者: tools.cmd org.apache...example.Producer"] --> TLS["tools.cmd<br/>(通用启动器)"]

    MQA -->|"call + 追加主类"| TLS
    TLS --> JVM["java -cp conf;lib/* <主类> <args>"]

    subgraph Lib["lib/* 下的 jar"]
        T1["rocketmq-tools.jar<br/>MQAdminStartup / SubCommands"]
        E1["rocketmq-example.jar<br/>Producer / Consumer 示例"]
        C1["rocketmq-client.jar<br/>(tools 依赖它)"]
        O["其他模块 jar ..."]
    end
    JVM --> T1
    JVM --> E1
    JVM --> C1
    JVM --> O
```

### 6.3 对比表

| | tools.cmd | mqadmin.cmd |
|---|---|---|
| 定位 | 通用启动器 | mqadmin CLI 专用入口 |
| 是否指定主类 | 否,由调用者第一个参数传入 | 固定 `org.apache.rocketmq.tools.command.MQAdminStartup` |
| JVM/Classpath 配置 | 在本脚本(`-Xms1g -Xmx1g -Xmn256m`) | 无,复用 tools.cmd |
| 前置检查 | 检查 `JAVA_HOME` | 检查 `ROCKETMQ_HOME` |
| 可运行的类 | lib 下所有 jar 的任意 main 类(含 example、tools) | 仅 mqadmin 子命令 |
| 典型用法 | `tools.cmd org.apache.rocketmq.example.quickstart.Producer` | `mqadmin.cmd queryMsgById -i xxx` |

**实践要点**:

- 修改 mqadmin 的堆内存,要改 **tools.cmd**(Linux 下为 `tools.sh`),改 mqadmin.cmd/mqadmin 无效;
- Linux 下结构完全对应:`mqadmin` shell 脚本内部 `sh ${ROCKETMQ_HOME}/bin/tools.sh org.apache.rocketmq.tools.command.MQAdminStartup "$@"`;
- 等价关系:`mqadmin.cmd queryMsgById -i x` ≡ `tools.cmd org.apache.rocketmq.tools.command.MQAdminStartup queryMsgById -i x`。

---

## 7. 模块与脚本的映射关系

### 7.1 Maven 模块依赖图

```mermaid
graph TD
    subgraph Scripts["发行包 bin/ 脚本"]
        MQA["mqadmin.cmd / mqadmin"]
        TLS["tools.cmd / tools.sh"]
    end

    subgraph Modules["Maven 模块"]
        TOOLS["tools 模块<br/>rocketmq-tools.jar<br/>MQAdminStartup, SubCommands,<br/>DefaultMQAdminExt(Impl)"]
        EX["example 模块<br/>rocketmq-example.jar"]
        CLIENT["client 模块<br/>rocketmq-client.jar<br/>MQAdminImpl, MQClientAPIImpl,<br/>MQClientInstance, DefaultMQProducer..."]
        STORE["store 模块<br/>DefaultMessageStore, IndexService"]
        BROKER["broker 模块<br/>QueryMessageProcessor"]
        COMMON["common / remoting 等基础模块"]
    end

    MQA -->|"固定主类"| TOOLS
    TLS -->|"lib/* 全量 classpath"| TOOLS
    TLS --> EX
    TOOLS -->|依赖| CLIENT
    TOOLS -->|"依赖(部分工具直连 broker,如查询索引)"| STORE
    CLIENT -->|依赖| COMMON
    STORE --> COMMON
    BROKER --> STORE
```

### 7.2 职责划分

| 模块 | jar | 职责 | 对应脚本 |
|---|---|---|---|
| **tools** | rocketmq-tools.jar | mqadmin 命令解析与分发(`MQAdminStartup`)、Admin 外观 API(`DefaultMQAdminExt`,基于 `@Deprecated` 内部实现类包装) | **mqadmin.cmd 的直接载体**;也被 tools.cmd 加载 |
| **client** | rocketmq-client.jar | 客户端内核:路由获取与缓存、Netty 连接管理、`MQAdminImpl`(查询核心逻辑:decodeMessageId / queryMessage 并行扇出)、生产者/消费者 | 无直接脚本,是 tools 的传递依赖 |
| **example** | rocketmq-example.jar | 官方示例(Producer/Consumer 等) | 通过 tools.cmd 通用入口运行 |
| **broker / store** | — | 服务端:QueryMessageProcessor、IndexFile、CommitLog | 由 mqbroker.cmd 启动,不在客户端脚本范围 |

### 7.3 概念澄清

- "tools.cmd 对应 tools 模块" **不完全准确**——tools.cmd 的 classpath 是 `lib/*` 全量 jar,tools 模块只是它能加载的模块之一(名字确实源自最初为 tools 模块服务,但实际是**所有客户端工具类的公共入口**);
- tools 模块**依赖 client 模块**,消息查询的真正逻辑(`MQAdminImpl`)在 client 模块中,tools 模块只做"命令行解析 + Admin API 门面";
- Dashboard 同样依赖 tools 模块(`rocketmq-tools` 作为 Maven 依赖),所以 Dashboard、mqadmin、example 三者在客户端层面共享同一套 `DefaultMQAdminExt → MQAdminImpl → MQClientAPIImpl` 代码。

---

## 8. 关键源码索引

| 功能 | 文件:行号 |
|---|---|
| mqadmin 脚本包装 | `distribution/bin/mqadmin.cmd:18` |
| 通用启动器(JVM 参数/Classpath) | `distribution/bin/tools.cmd:26-34` |
| mqadmin 命令分发 | `tools/.../command/MQAdminStartup.java` |
| queryMsgByKey 子命令 | `tools/.../command/message/QueryMsgByKeySubCommand.java:56-85` |
| Admin API 门面 | `tools/.../admin/DefaultMQAdminExtImpl.java:1114-1130` |
| viewMessage(解析 msgId) | `client/.../impl/MQAdminImpl.java:258-269` |
| queryMessage(并行扇出所有 broker) | `client/.../impl/MQAdminImpl.java:295-347` |
| uniqKey 反推时间 | `client/.../impl/MQAdminImpl.java:283-293` |
| Broker 请求路由(QUERY_MESSAGE / VIEW_MESSAGE_BY_ID) | `broker/.../processor/QueryMessageProcessor.java:50-124` |
| UNIQUE_MSG_QUERY_FLAG 处理 | `broker/.../processor/QueryMessageProcessor.java:82-85` |
| 索引查询 + 3 次重试 | `store/.../DefaultMessageStore.java:978-1022` |
| IndexFile 哈希索引结构 | `store/.../index/IndexFile.java` / `IndexService.java` |
| 零拷贝回传(FileRegion) | `broker/.../pagecache/QueryMessageTransfer.java` |

# Nginx 底层实现原理清单（基于 nginx-1.27.0 源码）

> 本文所有结论均对应你手头这份 nginx-1.27.0 主线源码，给出的链接可直接点开对应文件对照阅读。
> 建议用法：先通读本文建立"地图"，再按第 22 节的四阶段路线逐文件精读，每读完一章回看第 24 节自测清单。
>
> **逐模块深度剖析版**（29 篇独立文档，含 mermaid 架构图/流程图/时序图，约 2.4 万行）已在本仓库 [docs-深入剖析/00-总览与阅读指南.md](docs-深入剖析/00-总览与阅读指南.md)。

---

## 0. 一张图看懂 Nginx 整体架构

```
                      ┌────────────────────────────────┐
                      │          master 进程            │
                      │  解析配置/绑定端口/ fork worker   │
                      │  信号: SIGHUP reload, SIGUSR2   │
                      │        热升级, SIGQUIT 优雅退出   │
                      └───────┬────────────────────────┘
                              │ socketpair 管道(ngx_channel)
                              │ 传递新 worker fd / 退出指令
          ┌───────────────────┼───────────────────┬──────────────────┐
          │                   │                   │                  │
   ┌──────▼─────┐      ┌──────▼─────┐      ┌──────▼─────┐     ┌──────▼──────┐
   │  worker 0  │      │  worker 1  │ ...  │  worker N  │     │ cache loader│
   │  单线程     │      │  单线程     │      │  单线程     │     │ cache manager│
   └──────┬─────┘      └────────────┘      └──────┬─────┘     └─────────────┘
          │ epoll/kqueue 事件循环                   │
          │ accept: SO_REUSEPORT 或 accept_mutex    │
          ▼                                        ▼
   ngx_connection_t 连接池                 共享内存 ngx_shm_zone_t
          │                              (slab 分配器 + 自旋锁)
          ▼
   ngx_http_request_t 请求
          │  手写 DFA 解析请求行/头部
          ▼
   11 阶段 phase engine → content handler(静态/proxy/fastcgi...)
          ▼
   header filter 链 → body filter 链 (chunked/gzip/range...)
          ▼
   ngx_output_chain → writev / sendfile / TCP_NOPUSH → 内核
```

三大设计支柱：
1. **多进程，单 worker 线程**：worker 内没有每连接一线程，一切皆事件回调；线程池只用于卸载阻塞的文件 I/O（第 17 节）。
2. **一切皆模块**：编译期静态注册或运行期 dlopen，核心只是一个"模块宿主 + 事件框架"。
3. **内存池化 + 零拷贝链表**：请求级内存池一次性分配释放；数据用 `ngx_buf_t`/`ngx_chain_t` 链描述，尽量避免拷贝。

---

## 1. 源码目录地图

| 目录 | 职责 | 必读文件 |
|---|---|---|
| `src/core/` | 与协议无关的底层基建：内存池、slab、红黑树、hash、配置解析、模块框架、共享内存锁、resolver、线程池 | [src/core/nginx.c](src/core/nginx.c) [src/core/ngx_cycle.c](src/core/ngx_cycle.c) [src/core/ngx_palloc.c](src/core/ngx_palloc.c) |
| `src/event/` | 事件驱动框架与定时器、accept 管理、管道(upstream body 复用) | [src/event/ngx_event.c](src/event/ngx_event.c) [src/event/ngx_event_accept.c](src/event/ngx_event_accept.c) |
| `src/event/modules/` | 各平台多路复用器适配：epoll/kqueue/select/poll/devpoll/eventport/IOCP | [src/event/modules/ngx_epoll_module.c](src/event/modules/ngx_epoll_module.c) |
| `src/event/quic/` | QUIC 传输层实现（HTTP/3 的底座） | 该目录下全部 |
| `src/http/` | HTTP 框架：请求状态机、解析器、11 阶段引擎、upstream、缓存、变量 | [src/http/ngx_http_request.c](src/http/ngx_http_request.c) [src/http/ngx_http_parse.c](src/http/ngx_http_parse.c) |
| `src/http/v2/` | HTTP/2：帧解析、流管理、HPACK | [src/http/v2/ngx_http_v2.c](src/http/v2/ngx_http_v2.c) |
| `src/http/v3/` | HTTP/3：QPACK、HTTP/3 帧在 QUIC 流上的会话层 | [src/http/v3/ngx_http_v3_parse.c](src/http/v3/ngx_http_v3_parse.c) |
| `src/http/modules/` | 全部官方 HTTP 模块（handler/filter 两类） | [src/http/modules/ngx_http_proxy_module.c](src/http/modules/ngx_http_proxy_module.c) |
| `src/stream/` | 四层 TCP/UDP 代理框架 + 模块 | [src/stream/ngx_stream_proxy_module.c](src/stream/ngx_stream_proxy_module.c) |
| `src/mail/` | 邮件代理（POP3/IMAP/SMTP），认证后转发 | [src/mail/ngx_mail_parse.c](src/mail/ngx_mail_parse.c) |
| `src/os/unix/` | 进程管理、信号、channel、各平台 sendfile/atomic/setproctitle | [src/os/unix/ngx_process_cycle.c](src/os/unix/ngx_process_cycle.c) |
| `src/os/win32/` | Windows 适配（IOCP、select） | — |
| `auto/` | configure 全套 shell 脚本：探测特性、生成 `ngx_auto_config.h`、`objs/ngx_modules.c`、Makefile | [auto/configure](auto/configure) |
| `conf/` | 默认 nginx.conf | — |

---

## 2. 进程模型：master/worker 多进程架构

**核心原理**
- master 以 root 启动：读配置 → `ngx_init_cycle` 绑定监听端口 → 按 `worker_processes` fork 出 worker；worker 以低权限用户运行，收发连接。master 不碰任何客户端流量。
- worker 数默认 = CPU 核数；`worker_processes auto` 即 `ngx_ncpu`。
- 端口/监听 socket 由 master 创建并 fork 继承，reload 时新 worker 直接继承旧 fd，全程不关端口。
- 四类进程：master、worker、cache loader（启动后 60s 内扫一遍缓存目录然后退出）、cache manager（常驻，LRU 淘汰缓存）。

**精读源码**
| 主题 | 文件 | 要点 |
|---|---|---|
| master 主循环与全部信号 | [src/os/unix/ngx_process_cycle.c:74](src/os/unix/ngx_process_cycle.c#L74) `ngx_master_process_cycle` | SIGHUP=reload、SIGUSR1=重开日志、SIGUSR2=热升级(exec 新二进制)、SIGWINCH=让旧 worker 优雅退出、SIGTERM=快速退出 |
| fork 封装、信号注册表 | [src/os/unix/ngx_process.c](src/os/unix/ngx_process.c) | `ngx_spawn_process`；`signals[]` 数组把 Unix 信号映射为全局标志位 |
| worker 主循环 | [src/os/unix/ngx_process_cycle.c:699](src/os/unix/ngx_process_cycle.c#L699) `ngx_worker_process_cycle` | 死循环只做一件事：`ngx_process_events_and_timers`；SIGQUIT 优雅退出=关监听+关空闲连接+等在途请求 |
| worker 初始化 | 同文件 `ngx_worker_process_init` | 降权 setuid、`setrlimit`(worker_rlimit_nofile)、CPU 亲和性、逐模块调用 `init_process` 回调 |
| master↔worker 通道 | [src/os/unix/ngx_channel.c](src/os/unix/ngx_channel.c) | socketpair 上的私有协议 `ngx_channel_t`：OPEN_CHANNEL/QUIT/TERMINATE/REOPEN 等命令 |
| 守护化 | [src/os/unix/ngx_daemon.c](src/os/unix/ngx_daemon.c) | fork→setsid→chdir("/")→重定向 stdio 到 /dev/null |
| 热升级 | [src/core/nginx.c](src/core/nginx.c) + ngx_process_cycle.c | `ngx_exec_new_binary`：监听 fd 通过环境变量传给新 master，新旧 master 共存，新 worker 起来后旧 worker 收 SIGQUIT |

**自测**：reload 期间旧请求会不会断？热升级时新老 master 如何共享端口？cache loader 为什么跑一次就退出？

---

## 3. 事件驱动框架（重中之重）

**核心原理**
- 抽象层：`ngx_event_actions_t` 是一组函数指针（add/del/process_events…），多路复用器模块在初始化时填充它。上层代码只见 `ngx_add_event/ngx_process_events`，不感知平台。
- 每个 fd 对应一个 `ngx_connection_t`，其 `read`/`write` 各挂一个 `ngx_event_t`；事件回调指针 `handler` 是整个 nginx 的"线程调度单元"——状态机全部靠换 handler 实现。
- **epoll 是边缘触发(ET)**：[src/event/ngx_event.h:353](src/event/ngx_event.h#L353) 定义 `NGX_CLEAR_EVENT = EPOLLET`，`ngx_handle_read_event` 对 epoll/kqueue 走 `NGX_USE_CLEAR_EVENT` 分支；而 select/poll/devpoll 是水平触发，走 `NGX_USE_LEVEL_EVENT` 分支（读不完要反复 add/del）。正因 ET，nginx 所有读 handler 必须循环读到 `EAGAIN`。
- `ngx_epoll_add_connection` 一次性以 `EPOLLIN|EPOLLOUT|EPOLLET` 注册连接（[src/event/modules/ngx_epoll_module.c:705](src/event/modules/ngx_epoll_module.c#L705)），此后基本零 `epoll_ctl`；kqueue 同理。
- 主循环固定套路（[src/event/ngx_event.c:219-264](src/event/ngx_event.c#L219-L264)）：
  1. `ngx_event_find_timer()` 从红黑树取最近超时作为 `epoll_wait` 的 timeout；
  2. 若启用 accept_mutex：先 `ngx_trylock_accept_mutex`，持锁期间 `flags |= NGX_POST_EVENTS`——本轮 epoll 拿到的事件**只入队不执行**；
  3. `ngx_process_events` 执行 `epoll_wait` 并分发（非 accept 事件直接执行，accept 相关进 posted 队列）;
  4. 先跑 `ngx_posted_accept_events` → 立刻释放 accept_mutex（缩短持锁时间）→ 过期定时器 → 跑 `ngx_posted_events`。
- accept 惊群的三种解法（1.27 全部支持）：`accept_mutex`（共享内存锁，旧模式，默认关）、`EPOLLEXCLUSIVE`（内核层面只唤醒一个）、`reuseport`（每个 worker 独立 socket，内核按四元组分流，最优）。
- worker 间负载均衡：[src/event/ngx_event_accept.c:139](src/event/ngx_event_accept.c#L139) `ngx_accept_disabled = 连接总数/8 - 空闲连接数`，>0 时本 worker 让出锁轮空一轮——按连接数自调节。

**精读源码**
| 主题 | 文件 |
|---|---|
| 事件框架全局初始化、事件模型探测 | [src/event/ngx_event.c](src/event/ngx_event.c) `ngx_event_init_conf`（优先级 epoll > kqueue > …） |
| epoll 模块 | [src/event/modules/ngx_epoll_module.c](src/event/modules/ngx_epoll_module.c)（注意 `notify_fd` eventfd：`ngx_notify` 唤醒机制，线程池/动态重配靠它） |
| accept 与连接建立 | [src/event/ngx_event_accept.c](src/event/ngx_event_accept.c)（一次醒来循环 accept 直到 EAGAIN；设置 `TCP_DEFER_ACCEPT`/`accept_filter`） |
| 主动连接（upstream） | [src/event/ngx_event_connect.c](src/event/ngx_event_connect.c)（nonblock connect + `ngx_add_event` 写事件） |
| posted 事件队列 | [src/event/ngx_event_posted.c](src/event/ngx_event_posted.c) |
| UDP 事件 | [src/event/ngx_event_udp.c](src/event/ngx_event_udp.c)（`recvmsg` + `MSG_TRUNC` 判断丢包，多包复用地址） |
| 事件管道 ngx_event_pipe（upstream→client 中转） | [src/event/ngx_event_pipe.c](src/event/ngx_event_pipe.c) |

**自测**：为什么 ET 模式必须把 handler 写成"循环读到 EAGAIN"？posted 事件为什么要把 accept 和普通事件分成两个队列？`instance` 标志位（epoll `data.ptr` 低位）如何防陈旧事件？

---

## 4. 定时器管理

**核心原理**
- 所有超时事件挂在**一棵红黑树**上，key 是绝对到期时间；`ngx_event_find_timer()` 返回最左节点与当前时间的差值 = `epoll_wait` timeout，`ngx_event_expire_timers()` 从左往右摘除到期节点。查找 O(1)（直接取 root-leftmost），增删 O(log n)。
- `timer_resolution` 模式：改用 `setitimer/SIGALRM` 毫秒中断推进时间，`epoll_wait` 可以无限期阻塞，代价是时钟中断开销；默认不用。
- 时间缓存：[src/core/ngx_times.c](src/core/ngx_times.c) 每 1s 在 signal handler/时钟更新里缓存好 `ngx_cached_err_log_time` 等多种格式字符串 + `ngx_current_msec`，避免日志路径反复 `localtime()`。
- 惰性事件删除：`ngx_event_t` 的 `timer_set`/`active`/`ready`/`expired` 状态位 + 删除时只打标志不真删（`ngx_del_timer`、rbtree 的惰性标记在 `ngx_rbtree` 使用者层面）。

**精读源码**：[src/event/ngx_event_timer.c](src/event/ngx_event_timer.c)、[src/core/ngx_rbtree.c](src/core/ngx_rbtree.c)、[src/os/unix/ngx_time.c](src/os/unix/ngx_time.c)。

---

## 5. 内存管理体系

**核心原理**
- **ngx_pool_t 一页式内存池**（[src/core/ngx_palloc.h:57-65](src/core/ngx_palloc.h#L57-L65)）：
  - 小块（≤ `max`，默认页大小-1）从链上各块顺序 bump 分配，对齐用 `ngx_palloc`、不对齐用 `ngx_pnalloc`；块分配失败会在 `failed` 计数上累积，`current` 指针跳过失败块加速后续分配。
  - 大块走 `malloc` 单独挂 `large` 链，`ngx_pfree` 只释放大块。
  - `cleanup` 链挂资源回调（关 fd、删临时文件），`ngx_destroy_pool` 顺序：先 cleanup → 大块 → 块本身。**请求结束整池销毁，永不 free 单个对象**——这是 nginx 无内存碎片、代码无脑 `palloc` 的根基。
- **slab 分配器管理共享内存**（[src/core/ngx_slab.c](src/core/ngx_slab.c)）：每个 `ngx_shm_zone_t`（ssl_session_cache、limit_req、proxy_cache key、upstream zone）一块 `mmap/shm` 共享内存，内部按页管理：小对象走 size 槽位链，大对象整页/连续多页，带 best-fit 与页内位图。
- **共享锁**：[src/core/ngx_shmtx.c](src/core/ngx_shmtx.c) 基于 `ngx_atomic_cmp_lock` 的自旋尝试 + sem/futex 后备（`NGX_HAVE_SCHED_YIELD`）；[src/core/ngx_spinlock.c](src/core/ngx_spinlock.c)、[src/core/ngx_rwlock.c](src/core/ngx_rwlock.c)（读多写少场景）。
- 原子操作：平台内联汇编（[src/os/unix/ngx_gcc_atomic_x86.h](src/os/unix/ngx_gcc_atomic_x86.h) 系列），提供 CAS/加法，支撑 shmtx、连接计数、统计。
- buffer 复用：`ngx_buf_tag_t` 标记归属，模块间用 free chain 传递回收（upstream/proxy 的 `free->busy` 复用算法）。

**精读源码**：[src/core/ngx_palloc.c](src/core/ngx_palloc.c)、[src/core/ngx_slab.c](src/core/ngx_slab.c)、[src/core/ngx_buf.c](src/core/ngx_buf.c)、[src/os/unix/ngx_shmem.c](src/os/unix/ngx_shmem.c)（mmap/shm_open 两种实现）。

**自测**：为什么 `ngx_palloc` 对齐分配、`ngx_pnalloc` 不对齐？slab 槽位页和整页如何互不侵占？内存池 reset 与 destroy 的区别？

---

## 6. 核心数据结构工具箱（全部手写、侵入式、零依赖）

| 结构 | 文件 | 原理与用途 |
|---|---|---|
| `ngx_str_t` | [src/core/ngx_string.h](src/core/ngx_string.h) | `{len, data}` 非 0 结尾字符串，全库通用；配套 `ngx_snprintf`、`ngx_escape_uri`、`ngx_atoi` 在 [src/core/ngx_string.c](src/core/ngx_string.c) |
| `ngx_array_t` | [src/core/ngx_array.c](src/core/ngx_array.c) | 池上连续数组，自动 2 倍扩容（新块直接链在池上）；配置结构到处用它 |
| `ngx_list_t` | [src/core/ngx_list.c](src/core/ngx_list.c) | 定长元素块的链表，**HTTP 头部就存这里**（不删头，只标 hash=0） |
| `ngx_queue_t` | [src/core/ngx_queue.c](src/core/ngx_queue.c) | 侵入式双向链表（结构体内嵌成员），零内存分配；posted 队列、lru 队列、rr 对端全用它 |
| `ngx_hash_t` | [src/core/ngx_hash.c](src/core/ngx_hash.c) | 开放寻址 + 三级桶（dns 前缀/后缀通配符预编译），`ngx_hash_find_combined` 组合多张表；虚拟主机名、变量、头部查找全靠它 |
| `ngx_rbtree_t` | [src/core/ngx_rbtree.c](src/core/ngx_rbtree.c) | 可定制 insert 函数的红黑树（支持同 key），timer/缓存节点/resolver/文件缓存全用它 |
| `ngx_radix_tree_t` | [src/core/ngx_radix_tree.c](src/core/ngx_radix_tree.c) | 基数树（32bit 按位分叉），geo 模块、CIDR 匹配 |
| `ngx_buf_t` / `ngx_chain_t` | [src/core/ngx_buf.h](src/core/ngx_buf.h) | 缓冲区 + 链：`pos/last`（内存）与 `file_pos/file_last`（文件）二态，`temp_file/in_memory/last_buf` 标志位是 filter 链的"协议" |
| `ngx_hash_wildcard` | 同 hash | `*.example.com` / `www.example.*` 通配符表的三层嵌套查找 |

---

## 7. 配置系统：从 nginx.conf 到内存结构

**核心原理**
- 词法/语法：[src/core/ngx_conf_file.c](src/core/ngx_conf_file.c) `ngx_conf_read_token` 手写分词器，块用 `{}`，指令=名+参数；`include` 递归展开。
- 两阶段：
  1. **解析阶段**：递归下降，每条指令在所属模块的 `ngx_command_t[]` 里查到 `set` 回调（如 `ngx_conf_set_str_slot`），三层 context（main/server/location）各自 create_conf 分配结构体；
  2. **合并阶段**：解析完自底向上 `merge_srv_conf`/`merge_loc_conf` 逐级合并，`NGX_CONF_UNSET` 哨兵值实现"没配就用上一级默认"。
- `conf_ctx` 是树：`cycle->conf_ctx` → 各 core 模块 ctx → http 的 `ngx_http_conf_ctx_t`（三个数组：main_conf/srv_conf/loc_conf，一个 server 有多份 loc_conf，location 用 `ngx_http_core_loc_conf_t` 树）。
- `nginx -t` / `nginx -s reload` 都走 `ngx_init_cycle`：新配置全部解析成功、新端口全部 bind 成功才切换，失败则旧 cycle 原地不动——**配置热更新的原子性由此保证**。
- 指令参数支持变量：`ngx_http_script` 编译期把 `"...$var..."` 编译成指令序列，运行期拼装（见第 14 节）。

**精读源码**：[src/core/nginx.c](src/core/nginx.c)（main → 参数解析 → `ngx_init_cycle`）、[src/core/ngx_cycle.c](src/core/ngx_cycle.c)（cycle 生命周期：打开文件/共享内存/监听 socket 的 `ngx_open_listening_sockets`、`ngx_configure_listening_sockets` 设置 backlog/deferred/reuseport）、[src/core/ngx_conf_file.c](src/core/ngx_conf_file.c)。

---

## 8. 模块化体系

**核心原理**
- 统一骨架 `ngx_module_t`（[src/core/ngx_module.h](src/core/ngx_module.h)）：`ctx_index/index/version/commands/ctx/init_master/init_module/init_process/exit_process…`。
- 两类模块接口：
  - **事件/核心类** ctx 直接是一组生命周期回调；
  - **HTTP 类** ctx = `ngx_http_module_t`（8 个配置期回调）+ 可选 `ngx_http_core_module_t`（含 `ngx_http_handler_pt` 的 content phase 注册）。
- **编译期注册**：`auto/modules` 脚本按顺序排列模块 → 生成 `objs/ngx_modules.c` 的 `ngx_modules[]` 数组；`ngx_cycle_modules`/`ngx_count_modules`（[src/core/ngx_module.c:43](src/core/ngx_module.c#L43)）运行期编号。顺序即 filter 链顺序（在 `auto/modules` 里手工排定）。
- **动态模块**：`load_module xx.so`，实现是 [src/core/nginx.c:1582](src/core/nginx.c#L1582) `ngx_load_module`（dlopen + dlsym 找 `ngx_module_t` 符号，校验签名/版本，`--with-compat` 放宽 ABI 检查）。
- HTTP 模块挂载的三条总线：
  1. `postconfiguration` → 把 handler 塞进某个 phase 的 handlers 数组（11 阶段引擎，见第 9 节）；
  2. content 阶段指令注册 `ngx_str_t location` 内容处理器；
  3. **filter 链**：`ngx_http_top_header_filter` / `ngx_http_top_body_filter` 两个全局函数指针，后注册的模块把旧入口存进自己的 `next` 再覆盖头部——形成编译期固定顺序的单向链。

**自测**：为什么 filter 模块必须静态排进 `auto/modules`？动态 filter 模块能做到什么、不能做什么？

---

## 9. HTTP 请求生命周期（状态机全流程）

**核心原理（按时间线）**
1. **连接接入**：accept → `ngx_http_init_connection` → `client_header_timeout` 内等第一字节（`ngx_http_wait_request_handler`，[src/http/ngx_http_request.c:366](src/http/ngx_http_request.c#L366)）；keepalive 空闲连接超时直接回收。
2. **解析请求行**：`ngx_http_process_request_line`（[src/http/ngx_http_request.c:1083](src/http/ngx_http_request.c#L1083)）循环调 `ngx_http_parse_request_line`，不够数据就 `ngx_realloc` 请求行缓冲（`large_client_header_buffers`）。
3. **解析头部**：`ngx_http_process_request_headers`（[src/http/ngx_http_request.c:1367](src/http/ngx_http_request.c#L1367)）逐行 `ngx_http_parse_header_line` 存入 `ngx_list_t`，特殊头（Host/Content-Length/Connection/Range…）展开成 `ngx_http_headers_in_t` 字段。
4. **查找虚拟主机**：`ngx_http_find_server_conf` → server_name 的组合 hash（精确→`*.前缀`→`后缀*`→默认）。
5. **11 阶段引擎**（[src/http/ngx_http_core_module.h:109-128](src/http/ngx_http_core_module.h#L109-L128)）：
   `POST_READ(realip) → SERVER_REWRITE → FIND_CONFIG(内核找 location) → REWRITE → POST_REWRITE(防循环) → PREACCESS → ACCESS(auth/limit) → POST_ACCESS → PRECONTENT(try_files 等) → CONTENT(静态/proxy/…) → LOG`。
   每个 handler 前有 **checker**（generic/access/content 三种），checker 决定 handler 返回后是继续同阶段下一个 handler 还是跳到下一阶段——`ngx_http_phase_engine_t` 在配置期把全部 handler 压平成带 `next` 索引的数组，运行期零查找。
6. **收请求体**：`ngx_http_request_body.c`——`Content-Length`/chunked 两种，超 `client_body_buffer_size` 落临时文件；`ngx_http_read_client_request_body` 用 `r->main->count` 引用计数实现"回调式完成"。
7. **输出**：`ngx_http_send_header` 走 header filter 链 → `ngx_http_output_filter` 走 body filter 链（见第 11 节）。
8. **结束与保活**：`ngx_http_finalize_request` 递减 `count`；keepalive（默认单连接 1000 个请求，[src/http/ngx_http_core_module.c:3847](src/http/ngx_http_core_module.c#L3847)）→ `ngx_http_keepalive_handler`（[src/http/ngx_http_request.c:3315](src/http/ngx_http_request.c#L3315)，可解析 pipeline 的下一请求）；异常关闭走 **lingering close**（[src/http/ngx_http_request.c:3517](src/http/ngx_http_request.c#L3517)：先把客户端没发完的请求体读掉再关，避免 RST 导致客户端丢响应）。
9. **子请求**：`ngx_http_subrequest`（SSI/addition/auth_request 用）与 `internal redirect`（rewrite → 重新走 FIND_CONFIG），靠 `r->main->count` 与 `posted_requests` 队列调度。

**精读源码**：[src/http/ngx_http.c](src/http/ngx_http.c)（http 块配置与 phase 引擎组装）、[src/http/ngx_http_core_module.c](src/http/ngx_http_core_module.c)（location 树、静态文件、internal 跳转）、[src/http/ngx_http_request.c](src/http/ngx_http_request.c)（3000+ 行的状态机本体）。

**自测**：`r->main->count` 引用计数在哪些地方 +1/-1？为什么 CONTENT 阶段 checker 会"注册即执行"？`internal` location 为什么外部不可达？

---

## 10. HTTP 协议解析器（手写 DFA，性能的决定性细节）

**核心原理**
- [src/http/ngx_http_parse.c](src/http/ngx_http_parse.c) 纯状态机：无正则、无 `strtok`、逐字节 switch，**状态存进 `r->state` 可跨包续跑**——一行头部跨多次 recv 也能继续。
- 四大入口：`ngx_http_parse_request_line`（method/uri/version）、`ngx_http_parse_header_line`（含 header 值里 `\r\n` 折行）、`ngx_http_parse_complex_uri`（合并 `../`、解码 `%xx`、识别 `//`，防路径穿越）、`ngx_http_parse_chunked`（chunked body 状态机）。
- `sw_start…sw_…` 状态枚举即协议图；URI 三段解析出 `schema_start/host_start/port_start…` 指针区间，全程零拷贝。
- 配套协议解析：[src/core/ngx_proxy_protocol.c](src/core/ngx_proxy_protocol.c)（PROXY protocol v1/v2）、[src/mail/ngx_mail_parse.c](src/mail/ngx_mail_parse.c)。

**自测**：`ngx_http_parse_complex_uri` 如何挡住 `/a/../..//etc/passwd`？为什么 header 名要存小写 hash？

---

## 11. 输出过滤链与 I/O 发送优化

**核心原理**
- 两条链：header filter（改状态行/头部，`ngx_http_header_filter_module.c` 是最终编码者）与 body filter（传 `ngx_chain_t` 数据）。典型链序（编译期固定）：
  `write → postpone → copy → range → chunked → gzip → … → header_filter`。
- `last_buf` 标志在链上传递标记响应结束；subrequest 的输出被 [src/http/ngx_http_postpone_filter_module.c](src/http/ngx_http_postpone_filter_module.c) 按序重排。
- 发送主干 [src/core/ngx_output_chain.c](src/core/ngx_output_chain.c)：
  - 文件 buf + sendfile 可用 → 直接透传给 `ngx_sendfile`；
  - 不可用（如 TLS、directio 场景）→ 复制进内存 buf（` NGX_SENDFILE_LIMIT` 阈值切换）；
  - [src/os/unix/ngx_writev_chain.c](src/os/unix/ngx_writev_chain.c) 聚集写，partial write 剩余部分进 `ngx_write_queue` 等写事件再续传。
- Linux sendfile 特化（[src/os/unix/ngx_linux_sendfile_chain.c](src/os/unix/ngx_linux_sendfile_chain.c)）：配合 `tcp_nopush`（TCP_CORK）攒包、发送前开 CORK、`EAGAIN` 后关闭；大文件超过 `output_buffers` 直接走 `NGX_SENDFILE_MORE`。
- 流量整形：`limit_rate` 在 write filter 里按 `ngx_write_limit` 延后写事件。
- 输入侧对应物：[src/http/ngx_http_copy_filter_module.c](src/http/ngx_http_copy_filter_module.c)（把 sendfile 输入侧拷到内存，AIO/directio 时必要）。

**自测**：为什么开了 gzip 就自动用不了 sendfile？`busy` buf 机制如何防止上游数据撑爆内存？

---

## 12. 上游机制与负载均衡

**核心原理**
- `ngx_http_upstream_t` 是一个通用"客户端=反向"状态机：`create_request → connect/process_header → input_filter → finalize` 回调由具体协议模块（[src/http/modules/ngx_http_proxy_module.c](src/http/modules/ngx_http_proxy_module.c)、fastcgi/grpc/uwsgi/scgi/memcached）填充。
- 连接复用：`ngx_http_upstream_keepalive_module.c` 缓存到 upstream 的空闲连接。
- 平滑加权轮询：[src/http/ngx_http_upstream_round_robin.c](src/http/ngx_http_upstream_round_robin.c)——每轮 `current_weight += effective_weight`，选最大者，选完 `current_weight -= total_weight`（连续高分回落，避免低权重节点被雪崩）。
- 其它策略：hash（一致性hash）、least_conn、random（two-choices）、ip_hash；`zone` 指令把 peer 状态放共享内存跨 worker 共享（[src/core/ngx_slab.c](src/core/ngx_slab.c) 的实战样本）。
- 容错：`max_fails/fail_timeout` + retry 语义（`proxy_next_upstream`），重试需回滚已发出的部分（幂等约束）。
- 响应缓冲：`proxy_buffering on` 时 upstream buf 链 + `busy` 上限 + 溢出落 `proxy_temp_path` 临时文件；`off` 时近似透传（buf 复用）。

**自测**：平滑加权轮询为什么比朴素"按权重连续发"好？`proxy_buffering` 开关分别影响哪些时间参数（`proxy_read_timeout` vs `proxy_send_timeout`）？

---

## 13. 代理缓存

**核心原理**
- 键 = `proxy_cache_key` 的 md5 → 文件两级目录（md5 末位做目录名），文件头 `ngx_http_file_cache_header_t` 存元数据（有效期、Vary、大小）。
- 索引：共享内存里一棵 rbtree（`ngx_http_file_cache_node_t`），key 命中即得到缓存节点（含 `count` 引用/`busy` 并发回源锁——同一 key 回源只放一个请求，其余等）。
- 两个辅助进程：cache loader（启动时冷扫描目录建索引，跑完退出）、cache manager（周期 LRU 清理 `inactive` 与超配额项）。
- 细节：`proxy_cache_use_stale`（过期旧内容兜底）、`proxy_cache_background_update`（后台刷新）、`slice` 模块分片缓存大文件、`Cache-Control/Vary` 语义在 [src/http/ngx_http_file_cache.c](src/http/ngx_http_file_cache.c)。

**精读源码**：[src/http/ngx_http_file_cache.c](src/http/ngx_http_file_cache.c)、[src/http/ngx_http_cache.h](src/http/ngx_http_cache.h)。

---

## 14. 变量系统与脚本引擎

**核心原理**
- 内建变量在 `ngx_http_variables.c` 注册为 `{name, get_handler, set_handler?, data}`；模块可 `ngx_http_get_variable_index` 加索引变量（快）或按名查（慢路径 hash）。
- 正则命名捕获 `(?<name>)` 写入 `r->variables`，供 rewrite/`map` 使用。
- **复杂值编译**：`$var` 混合字符串在配置期被 `ngx_http_compile_complex_value` 编译成 `ngx_http_script_code_pt` 指令序列（copy_len/copy/flush/value…），运行期 `ngx_http_script_run` 执行——等价于一个微型字节码 VM。
- 同一套思想移植到 stream：[src/stream/ngx_stream_script.c](src/stream/ngx_stream_script.c)、[src/stream/ngx_stream_variables.c](src/stream/ngx_stream_variables.c)。

---

## 15. DNS 解析器（自带、无依赖）

**核心原理**
- [src/core/ngx_resolver.c](src/core/ngx_resolver.c)：worker 自己的 UDP socket 发标准 DNS 报文；查询节点挂 rbtree（key=case-insensitive 名字+类型），支持 A/AAAA/CNAME/SRV、CNAME 链跟随、超时重发、多 nameserver 轮转。
- 与 upstream 联动：`resolver` + `set $upstream ...` / 静态 `server` 域名在配置期一次性解析，运行期解析用于变量转发场景。
- `resolver_timeout`、`valid` TTL 覆盖；可放共享内存（`resolver_zone`）。

---

## 16. 线程池与异步文件 I/O

**核心原理**
- **thread pool**（[src/core/ngx_thread_pool.c](src/core/ngx_thread_pool.c)）：任务结构 `ngx_thread_task_t{handler, event}` 入任务队列（互斥锁+条件变量），N 个 pthread 消费；完成时把 completion 以 eventfd/管道 `ngx_notify` 投回 worker 的 epoll 循环——**worker 线程永远不阻塞**是铁律。
- 用途：`aio threads` 时文件读/写、sendfile offload、`ssl` 私钥操作等；也是「为什么 nginx 只有 master/worker 还能有并发磁盘 IO」的答案。
- 平台异步：Linux 原生 AIO（io_setup，[src/os/unix/ngx_linux_aio_read.c](src/os/unix/ngx_linux_aio_read.c) + [src/os/unix/ngx_file_aio_read.c](src/os/unix/ngx_file_aio_read.c)），完成事件经 eventfd 进入 epoll（[src/event/modules/ngx_epoll_module.c:255-300](src/event/modules/ngx_epoll_module.c#L255-L300)）；directio 用于绕 page cache 读大文件。
- 注意：nginx 至今未支持 io_uring（1.27 无此代码）。

---

## 17. SSL/TLS 与 HTTP/2、HTTP/3

**SSL/TLS**（[src/event/ngx_event_openssl.c](src/event/ngx_event_openssl.c)）
- 握手全异步：BIO 换成 nginx 自制 BIO，读写挂 read/write 事件，`SSL_ERROR_WANT_READ/WRITE` 即让出；
- session 复用：内建缓存用 slab 共享内存跨 worker，或外部 memcached；session ticket 密钥轮转 `ssl_session_ticket_key`；
- OCSP stapling：[src/event/ngx_event_openssl_stapling.c](src/event/ngx_event_openssl_stapling.c)（后台 resolver + 异步 OCSP fetch）；
- SNI 回调实现单端口多证书。

**HTTP/2**（[src/http/v2/](src/http/v2/ngx_http_v2.c)）
- 帧状态机（HEADERS/DATA/WINDOW_UPDATE/SETTINGS/PING/RST/GOAWAY…），多路流复用在一条 `ngx_connection_t` 上；
- HPACK：静态表+动态表（[src/http/v2/ngx_http_v2_table.c](src/http/v2/ngx_http_v2_table.c)）+ Huffman（[src/http/ngx_http_huff_decode.c](src/http/ngx_http_huff_decode.c)、[src/http/ngx_http_huff_encode.c](src/http/ngx_http_huff_encode.c) 手写编码表）；
- 流控窗口（连接级+流级）决定 `ngx_http_v2_filter_module.c` 的发送调度；每个流最终映射为一个伪 `ngx_http_request_t` 进入第 9 节的普通阶段引擎——**上层框架完全复用**。

**HTTP/3/QUIC**（[src/event/quic/](src/event/quic) + [src/http/v3/](src/http/v3/ngx_http_v3.c)）
- QUIC 传输层：握手(与 TLS1.3 共用 ngx_event_openssl)、包级加密、丢包重传、拥塞控制、0-RTT；
- HTTP/3 会话层：QPACK（[src/http/v3/ngx_http_v3_table.c](src/http/v3/ngx_http_v3_table.c)）、控制流/单向流（[src/http/v3/ngx_http_v3_uni.c](src/http/v3/ngx_http_v3_uni.c)）、请求流→同样是伪请求进入 HTTP 框架。

**自测**：HTTP/2 的一个"流"为什么能无缝复用 HTTP/1.1 的 11 阶段引擎？（答案：`ngx_http_request_t` 抽象了协议来源，`r->http_version` 分支在框架里极少出现。）

---

## 18. stream 四层代理 与 mail 邮件代理

- stream：与 http 平行的完整框架（[src/stream/ngx_stream.c](src/stream/ngx_stream.c)），同款 phase 引擎（preread/ssl/…）+ upstream RR；`ssl_preread` 不解密就按 SNI 分流；UDP 会话模型在 [src/stream/ngx_stream_proxy_module.c](src/stream/ngx_stream_proxy_module.c)（`proxy_responses` 控制会话边界）。
- mail：先代理到 auth_http 完成认证，再按用户协议状态机（[src/mail/ngx_mail_pop3_handler.c](src/mail/ngx_mail_pop3_handler.c) 等）逐行解析后转发真实邮件服务器——一个"认证前置 + 协议透传"的样板工程。

---

## 19. 日志系统

- error log：`ngx_log_t` 双层（`log->next` 链共享 file）；级别 emerg→debug；`--with-debug` 才编译进 `ngx_log_debug*`（核心调试手段，见第 26 节）。
- access log：模块 [src/http/modules/ngx_http_log_module.c](src/http/modules/ngx_http_log_module.c)，格式串配置期编译（同第 14 节脚本引擎），支持 buffer+gzip、syslog（[src/core/ngx_syslog.c](src/core/ngx_syslog.c)）。
- 打开文件缓存：[src/core/ngx_open_file_cache.c](src/core/ngx_open_file_cache.c)（stat/fd 缓存，静态文件热路径的隐藏加速器）。
- 时间字符串零开销：[src/core/ngx_times.c](src/core/ngx_times.c) 缓存。

---

## 20. 跨平台 OS 适配层（`src/os/unix/`）

| 文件 | 职责 |
|---|---|
| [ngx_process.c](src/os/unix/ngx_process.c) | fork、信号表、setproctitle |
| [ngx_readv_chain.c](src/os/unix/ngx_readv_chain.c) / [ngx_recv.c](src/os/unix/ngx_recv.c) | 聚集读（GCOS 优化：第一个 buf 小、后续大，防止慢客户端占大内存） |
| [ngx_writev_chain.c](src/os/unix/ngx_writev_chain.c) / 各平台 `*_sendfile_chain.c` | 聚集写/sendfile 变体（FreeBSD `SF_NODISKIO`、Solaris sendfilev、Darwin） |
| [ngx_udp_sendmsg_chain.c](src/os/unix/ngx_udp_sendmsg_chain.c) | UDP gather 发送 |
| [ngx_setaffinity.c](src/os/unix/ngx_setaffinity.c) | worker_cpu_affinity |
| [ngx_setproctitle.c](src/os/unix/ngx_setproctitle.c) | 改写 argv 内存区的进程标题（`ps` 里那串 "worker process"） |
| `ngx_gcc_atomic_*.h` | CAS/原子加减的 GCC 内联汇编 |
| [ngx_errno.c](src/os/unix/ngx_errno.c) | errno→字符串静态表（避免 strerror 锁） |

---

## 21. 构建系统（`auto/`）

- 入口 [auto/configure](auto/configure) → [auto/options](auto/options)（解析 `--with-xxx`）→ [auto/os](auto/os)、[auto/cc](auto/cc)、[auto/lib](auto/lib) 探测 → 产出 `objs/ngx_auto_config.h`（所有 `NGX_HAVE_*` 宏的来源）、`objs/Makefile`、`objs/ngx_modules.c`。
- [auto/modules](auto/modules) 是模块总装配线：**顺序决定 filter 链与 phase 内 handler 顺序**；`--add-module`/`--add-dynamic-module` 在此插入第三方模块。
- 实验建议：`./configure --with-debug --with-cc-opt='-O0'` 后看 `objs/ngx_auto_config.h` 理解"一个特性宏如何点亮一条代码路径"。

---

## 22. 推荐学习路线（四阶段，约 8-12 周）

**阶段一：跑起来 + 建立全景（1 周）**
1. Linux 下 `./configure --with-debug --prefix=/opt/nginx && make`，通读 [conf/nginx.conf](conf/nginx.conf)。
2. 对照第 0 节架构图，用 `ps -ef`、`ls /proc/<worker>/fd`、`ss -tlnp` 验证进程模型与端口继承。
3. 精读：[src/core/nginx.c](src/core/nginx.c)（main 全流程）→ [src/os/unix/ngx_process_cycle.c](src/os/unix/ngx_process_cycle.c)（master/worker 循环）。

**阶段二：事件驱动与内存（2-3 周，性能之本）**
1. [src/event/ngx_event.c](src/event/ngx_event.c) 主循环逐行 → [src/event/modules/ngx_epoll_module.c](src/event/modules/ngx_epoll_module.c) → [src/event/ngx_event_accept.c](src/event/ngx_event_accept.c)。
2. [src/core/ngx_palloc.c](src/core/ngx_palloc.c) → [src/core/ngx_slab.c](src/core/ngx_slab.c)。
3. 实验：`ab -c 1000` 压测时 `strace -p <worker> -e epoll_wait,accept4,recvfrom,writev`，亲眼验证 ET 循环读与 writev 聚集写。

**阶段三：HTTP 框架全链路（3-4 周）**
1. 顺序：[src/http/ngx_http.c](src/http/ngx_http.c) → [src/http/ngx_http_request.c](src/http/ngx_http_request.c) → [src/http/ngx_http_parse.c](src/http/ngx_http_parse.c) → [src/http/ngx_http_core_module.c](src/http/ngx_http_core_module.c)。
2. 写一个 **最简 handler 模块**（打印 header 即返回），再写一个 **body filter 模块**（追加一行），理解注册与链序。
3. 精读 [src/http/ngx_http_upstream.c](src/http/ngx_http_upstream.c) + [src/http/modules/ngx_http_proxy_module.c](src/http/modules/ngx_http_proxy_module.c)，抓包（tcpdump）对照 proxy_buffering 开/关的发送节奏。

**阶段四：专题深潜（2-4 周，按需选）**
- 缓存：[src/http/ngx_http_file_cache.c](src/http/ngx_http_file_cache.c) + strace 看缓存文件读写。
- HTTP/2：抓 `h2c` 帧；HTTP/3：`--with-http_v3_module` + QUIC trace。
- stream/mail 选一个读全框架。
- 定期回看第 25 节自测，能给别人讲明白才算掌握。

---

## 23. 动手实验清单（把原理变成肌肉记忆）

1. **看惊群**：`accept_mutex on`（旧内核）用 `strace -f -e futex` 观察 worker 争锁；换 `reuseport` 对比 QPS 与 CPU 均衡。
2. **看 ET**：`curl` 慢速发送一个跨多包的头部，error_log 开 `debug` 级别观察 `http process request line` 的多次进入。
3. **看 sendfile**：静态文件请求 `strace -e sendfile,writev,tcp_nopush`；换成 `gzip on` 对比（退化为 read+writev）。
4. **看 reload 原子性**：改坏配置执行 `nginx -s reload`，观察旧 worker 不退、error log 报错、服务不断。
5. **看热升级**：`kill -USR2 <master>` 后 `ps --forest` 观察两代 master/worker；`kill -WINCH` 收掉旧代。
6. **看共享内存**：`limit_req_zone` 压测时 `pmap <worker>` 找到 slab 段；两个 worker 命中同一限流计数。
7. **看 lingering**：`ab` 中途 kill 客户端，服务端 error_log 的 `client closed connection` 与 lingering 计时。
8. **看线程池**：`aio threads;` + 大文件 + `strace -e io_getevents,futex`。

---

## 24. 精通自测 50 问（能答 45+ 即算精通）

**架构与进程**
1. master 与 worker 各自职责边界？为什么不用多线程？
2. reload 全流程，哪一步失败不会影响现网？
3. 热升级时新老 master 如何传递监听 socket？（环境变量）
4. worker 优雅退出的判定条件（在途请求计数如何归零）？
5. cache loader/manager 各做什么？
6. channel 管道传了哪些命令？

**事件模型**
7. `ngx_event_actions_t` 如何让上层代码平台无关？
8. epoll ET 的含义，nginx 哪些 flag 组合体现它？（`NGX_CLEAR_EVENT=EPOLLET`）
9. `epoll_wait` 的 timeout 如何计算？timer_resolution 模式有什么取舍？
10. posted 队列为什么分 accept/普通两个？
11. accept 惊群三种解法与适用条件？
12. `ngx_accept_disabled` 公式及其效果？
13. `instance` 标志如何防止过期事件误触？
14. eventfd 在 nginx 里的两个用途？（notify、AIO 完成）
15. kqueue 与 epoll 在 nginx 中行为差异？

**内存与数据结构**
16. 内存池 `current` 指针与 `failed` 计数的作用？
17. `ngx_pfree` 为什么只能释放大块？
18. slab 如何处理不同 size 的小对象与整页？
19. `ngx_queue_t` 侵入式设计的好处？
20. `ngx_hash_t` 如何支持 `*.example.com` 与 `www.example.*`？
21. timer 红黑树的 insert 函数为何允许 key 相等？

**配置与模块**
22. 配置解析与 merge 两阶段各自解决什么问题？
23. `NGX_CONF_UNSET` 的作用？
24. `ngx_modules[]` 如何生成？动态模块 ABI 校验项有哪些？
25. filter 链顺序由什么决定？为什么运行期不能动态插 filter？

**HTTP 框架**
26. 11 阶段各自典型模块？FIND_CONFIG 为什么必须独立成阶段？
27. checker 与 handler 的分工？
28. `r->main->count` 在哪些路径增减？
29. 子请求与 internal redirect 的区别与实现？
30. pipelined 请求如何被 keepalive handler 顺带解析？
31. `large_client_header_buffers` 的回收时机？
32. chunked 请求体的状态机在哪，遇到 trailer 怎么处理？
33. lingering close 解决什么 TCP 问题？
34. `client_body_in_file_only` 的应用场景？

**I/O 与上游**
35. `ngx_output_chain` 何时拷贝、何时零拷贝？
36. `tcp_nopush`（TCP_CORK）的开关时机？
37. partial write 的续传机制？
38. 平滑加权轮询算法口述并证明其公平性？
39. `max_fails/fail_timeout` 的判定窗口？
40. proxy_buffering on 时 `busy` 与 temp file 如何配合防内存暴涨？
41. 缓存命中、MISS、STALE 三条路径分别经过哪些函数？
42. cache `busy` 锁如何防止缓存击穿？
43. `slice` 分片缓存如何与 Range 协同？

**专题**
44. resolver 的重试与 CNAME 链如何实现？
45. 线程池任务如何回到 epoll 循环？
46. Linux 原生 AIO 与 thread pool 的取舍？
47. TLS 握手异步化的 BIO 改造点？
48. session ticket 与 session cache 的区别、nginx 各自实现位置？
49. HTTP/2 流控窗口耗尽时 nginx 的行为？
50. stream 模块的 `preread` 阶段能解决什么问题（举例 ssl_preread）？

---

## 25. 一句话总纲

> **Nginx = 多进程隔离故障域 + 单线程事件循环榨干 CPU + 手写一切热路径（解析器/数据结构/内存池）+ 阶段化框架让功能即插即用。**
> 精通 nginx 的标志：给你任意一个 RT 问题，你能在源码里指出是哪个环节（accept、parse、phase、upstream、filter、output）的哪一行在等，并说清它的状态机如何继续。

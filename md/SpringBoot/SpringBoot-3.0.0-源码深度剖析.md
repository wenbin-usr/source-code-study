# Spring Boot 3.0.0 源码深度剖析

> 本文基于本地 Spring Boot 3.0.0 官方源码（`spring-boot-project/`）逐行研读整理，所有类名、方法名、行号均与源码一一对应。
> 涵盖五大核心原理：**启动流程**、**自动配置**、**内嵌 Servlet 容器**、**application 配置文件加载**、**@Conditional 条件体系**。
> 文中路径约定：
> - `BOOT` = `spring-boot-project/spring-boot/src/main/java/org/springframework/boot`
> - `AC` = `spring-boot-project/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure`

---

## 目录

1. [总体架构鸟瞰](#一总体架构鸟瞰)
2. [启动流程深度剖析](#二启动流程深度剖析)
3. [自动配置原理](#三自动配置原理)
4. [内嵌 Servlet 容器原理](#四内嵌-servlet-容器原理)
5. [application 配置文件加载原理](#五application-配置文件加载原理)
6. [@Conditional 条件注解实现原理](#六conditional-条件注解实现原理)
7. [五大原理的关联：一张全景图](#七五大原理的关联一张全景图)
8. [fatjar 可执行 Jar 实现原理](#八fatjar-可执行-jar-实现原理)
9. [Actuator 实现原理：/actuator 端点是谁在响应](#九actuator-实现原理)
10. [Redis 整合原理：spring-boot-starter-data-redis 的装配与通信](#十redis-整合原理spring-boot-starter-data-redis-的装配与通信)
11. [spring-boot-devtools 实现原理：自动重启、双类加载器与远程开发](#十一spring-boot-devtools-实现原理自动重启双类加载器与远程开发)
12. [spring-boot-starter-logging 实现原理：日志系统整合与初始化流程](#十二spring-boot-starter-logging-实现原理日志系统整合与初始化流程)
13. [spring-boot-starter-cache 实现原理：缓存抽象整合与 CacheManager 自动装配](#十三spring-boot-starter-cache-实现原理缓存抽象整合与-cachemanager-自动装配)
14. [spring-boot-starter-websocket 实现原理：WebSocket 整合与内嵌容器握手链路](#十四spring-boot-starter-websocket-实现原理websocket-整合与内嵌容器握手链路)

---

## 一、总体架构鸟瞰

Spring Boot 3.0.0 的基线是 **Spring Framework 6.0.0 + Jakarta EE 9+（`jakarta.*` 命名空间）+ Java 17**，并原生支持 GraalVM Native Image（AOT）。源码模块划分：

```mermaid
flowchart TB
    subgraph Starters["spring-boot-starters（依赖聚合层）"]
        S1["spring-boot-starter-web<br/>等 40+ starter"]
    end

    subgraph AutoConfig["spring-boot-autoconfigure（自动配置层）"]
        A1["141 个自动配置类<br/>META-INF/spring/….AutoConfiguration.imports"]
        A2["条件注解体系<br/>OnClassCondition / OnBeanCondition …"]
        A3["@ConfigurationProperties<br/>属性类 + 元数据"]
    end

    subgraph Core["spring-boot（核心机制层）"]
        C1["SpringApplication<br/>启动生命周期"]
        C2["ConfigData 体系<br/>application.yml / profile / import"]
        C3["web/server + web/embedded/*<br/>内嵌容器抽象（Tomcat/Jetty/Undertow）"]
        C4["Binder / ConfigurationPropertySource<br/>宽松绑定"]
        C5["spring.factories / ImportCandidates<br/>SPI 加载机制"]
    end

    subgraph Framework["Spring Framework 6（外部依赖）"]
        F1["spring-context<br/>ConfigurationClassParser / ConditionEvaluator<br/>ApplicationContext.refresh()"]
        F2["spring-web / spring-webmvc<br/>DispatcherServlet"]
    end

    subgraph External["第三方容器（外部依赖）"]
        E1["Tomcat 10.1"]
        E2["Jetty 11"]
        E3["Undertow"]
    end

    Starters --> AutoConfig
    AutoConfig --> Core
    Core --> Framework
    AutoConfig --> F2
    Core --> E1 & E2 & E3
```

三个关键分工（3.0 与 2.x 的本质差异之一）：

| 机制 | 文件 | 用途 |
|---|---|---|
| `META-INF/spring/*.imports` | 新增（2.7 引入，3.0 唯一来源） | 声明**自动配置类**候选列表 |
| `META-INF/spring.factories` | 保留 | 登记 listener / filter / initializer / EnvironmentPostProcessor 等**框架组件** |
| `META-INF/spring-autoconfigure-metadata.properties` | 编译期生成 | 自动配置类注解元数据缓存，用于**免加载类的提前过滤** |

---

## 二、启动流程深度剖析

### 2.1 入口与构造阶段（run 之前）

一切从 `SpringApplication.run(App.class, args)` 开始（`BOOT/SpringApplication.java:1290-1303`）：

```java
public static ConfigurableApplicationContext run(Class<?>[] primarySources, String[] args) {
    return new SpringApplication(primarySources).run(args);
}
```

构造函数（`SpringApplication.java:266-276`）做了 6 件事：

```java
public SpringApplication(ResourceLoader resourceLoader, Class<?>... primarySources) {
    this.resourceLoader = resourceLoader;
    this.primarySources = new LinkedHashSet<>(Arrays.asList(primarySources)); // 1. 保存主类
    this.webApplicationType = WebApplicationType.deduceFromClasspath();       // 2. 推断应用类型
    this.bootstrapRegistryInitializers = new ArrayList<>(
            getSpringFactoriesInstances(BootstrapRegistryInitializer.class)); // 3. 装载引导初始化器
    setInitializers((Collection) getSpringFactoriesInstances(ApplicationContextInitializer.class)); // 4. 5 个
    setListeners((Collection) getSpringFactoriesInstances(ApplicationListener.class));             // 5. 7 个
    this.mainApplicationClass = deduceMainApplicationClass();                 // 6. StackWalker 找 main 类
}
```

**WebApplicationType 推断**（`BOOT/WebApplicationType.java:60-71`）——纯 classpath 探测，顺序敏感：

```java
static WebApplicationType deduceFromClasspath() {
    if (ClassUtils.isPresent(WEBFLUX_INDICATOR_CLASS, null)          // DispatcherHandler
            && !ClassUtils.isPresent(WEBMVC_INDICATOR_CLASS, null)   // 且无 DispatcherServlet
            && !ClassUtils.isPresent(JERSEY_INDICATOR_CLASS, null)) {
        return WebApplicationType.REACTIVE;
    }
    for (String className : SERVLET_INDICATOR_CLASSES) {             // jakarta.servlet.Servlet
        if (!ClassUtils.isPresent(className, null)) {                // + ConfigurableWebApplicationContext
            return WebApplicationType.NONE;
        }
    }
    return WebApplicationType.SERVLET;                               // WebFlux+WebMvc 并存时 SERVLET 胜出
}
```

**deduceMainApplicationClass**（`SpringApplication.java:278-286`）：用 JDK 9+ 的 `StackWalker`（`RETAIN_CLASS_REFERENCE`）遍历调用栈找到第一个名为 `main` 的栈帧，取其声明类——该类只用于启动日志与 AOT 推断。

### 2.2 run() 方法 18 步逐行分解

`run(String... args)`（`SpringApplication.java:294-338`）骨架：

```java
public ConfigurableApplicationContext run(String... args) {
    long startTime = System.nanoTime();                                  // ① 单调时钟计时（3.0 已弃用 StopWatch）
    DefaultBootstrapContext bootstrapContext = createBootstrapContext(); // ② 引导上下文
    ConfigurableApplicationContext context = null;
    configureHeadlessProperty();                                         // ③ java.awt.headless=true
    SpringApplicationRunListeners listeners = getRunListeners(args);     // ④ EventPublishingRunListener
    listeners.starting(bootstrapContext, this.mainApplicationClass);    // ⑤ ApplicationStartingEvent
    try {
        ApplicationArguments applicationArguments = new DefaultApplicationArguments(args); // ⑥
        ConfigurableEnvironment environment = prepareEnvironment(listeners, bootstrapContext, applicationArguments); // ⑦ ★
        Banner printedBanner = printBanner(environment);                 // ⑧
        context = createApplicationContext();                            // ⑨
        context.setApplicationStartup(this.applicationStartup);          // ⑩ JFR 指标
        prepareContext(bootstrapContext, listeners, environment, context, applicationArguments, printedBanner); // ⑪
        refreshContext(context);                                         // ⑫ ★
        afterRefresh(context, applicationArguments);                     // ⑬ 空模板方法
        Duration timeTakenToStartup = Duration.ofNanos(System.nanoTime() - startTime);
        new StartupInfoLogger(this.mainApplicationClass).logStarted(getApplicationLog(), timeTakenToStartup); // ⑭
        listeners.started(context, timeTakenToStartup);                  // ⑮ ApplicationStartedEvent + Liveness CORRECT
        callRunners(context, applicationArguments);                      // ⑯ Runner 执行
    }
    catch (Throwable ex) {
        if (ex instanceof AbandonedRunException) { throw ex; }           // devtools restart 静默放弃
        handleRunFailure(context, ex, listeners);                        // ⑰ 失败处理
        throw new IllegalStateException(ex);
    }
    try {
        if (context.isRunning()) {
            Duration timeTakenToReady = Duration.ofNanos(System.nanoTime() - startTime);
            listeners.ready(context, timeTakenToReady);                  // ⑱ ApplicationReadyEvent + Readiness
        }
    }
    catch (Throwable ex) { /* ... */ }
    return context;
}
```

各步要点：

| 步骤 | 关键实现 | 说明 |
|---|---|---|
| ② | `createBootstrapContext()`（:340-344） | 执行所有 `BootstrapRegistryInitializer`——**早于任何事件**的最早插拔点；供 Environment 准备阶段使用（如 ConfigData 异步加载的 BootstrapExecutor） |
| ④ | `getRunListeners()`（:440-452） | 从 spring.factories 加载，实际只有 `EventPublishingRunListener`；Spring 6 的 `SpringFactoriesLoader.load(type, argumentResolver)` 用 ArgumentResolver 注入 `(SpringApplication, String[])` 构造参数；另通过 ThreadLocal `applicationHook`（3.0 新增 `SpringApplicationHook`）附加测试钩子 |
| ⑤ | `EventPublishingRunListener.starting`（:74-76） | 用内部 `SimpleApplicationEventMulticaster initialMulticaster` 广播——**refresh 之前的事件都走它**（context 的广播器还没初始化） |
| ⑦ | `prepareEnvironment()`（:346-363） | **配置文件加载的发生地**，详见第五节 |
| ⑧ | `printBanner()`（:544-555） | `spring.banner.location` > 自定义 Banner > 默认 `SpringBootBanner`；有个防御逻辑：资源 URL 含 `liquibase-core` 则忽略，防止误读 liquibase jar 里的 banner.txt |
| ⑨ | `createApplicationContext()`（:564-566） | 委托 `DefaultApplicationContextFactory`：SERVLET → `AnnotationConfigServletWebServerApplicationContext`；REACTIVE → `AnnotationConfigReactiveWebServerApplicationContext`；NONE → `AnnotationConfigApplicationContext`（AOT 模式分别换成无 reader/scanner 的变体） |
| ⑪ | `prepareContext()`（:377-413） | 12 个小步，见 2.3 |
| ⑫ | `refreshContext()`（:428-433） | **先注册 JVM shutdown hook 再 refresh**；`SpringApplicationShutdownHook` 是 JVM 级静态单例（:185），`AtomicBoolean` CAS 保证只 `addShutdownHook` 一次；hook 的 `run()` 依次 close 所有 context 并执行 `getShutdownHandlers()` 注册的动作 |
| ⑯ | `callRunners()`（:741-754） | ApplicationRunner + CommandLineRunner 统一收集、`AnnotationAwareOrderComparator` 全局排序、去重后**串行**执行；runner 抛异常会进入失败分支导致启动失败 |
| ⑰ | `handleRunFailure()`（:774-795） | ExitCode 体系（`ExitCodeExceptionMapper` bean + 异常链上的 `ExitCodeGenerator`）→ `ApplicationFailedEvent` → `FailureAnalyzers`（约 23 个 `FailureAnalyzer`，输出 "APPLICATION FAILED TO START" 诊断）→ `context.close()` + 从 shutdownHook 注销 |

### 2.3 prepareContext 的 12 个小步（:377-413）

1. `context.setEnvironment(environment)`——注入准备好的环境；
2. `postProcessApplicationContext`（:573-589）——注册 beanNameGenerator、resourceLoader、`ApplicationConversionService`；
3. **AOT 分支**：`AotDetector.useGeneratedArtifacts()` 为真时把 `AotApplicationContextInitializer` 插到最前（:415-426）；
4. `applyInitializers`（:597-605）——按序执行 5 个 `ApplicationContextInitializer`；
5. `listeners.contextPrepared` → `ApplicationContextInitializedEvent`；
6. `bootstrapContext.close(context)` → `BootstrapContextClosedEvent`——引导上下文向真容器的"交接"；
7. 启动日志：`StartupInfoLogger.logStarting`（"Starting XxxApplication v1.0 using Java 17 ..."）+ `logStartupProfileInfo`（活跃 profile 信息）；
8. 注册单例：`beanFactory.registerSingleton("springApplicationArguments", args)`、`springBootBanner`；
9. `setAllowCircularReferences` / `setAllowBeanDefinitionOverriding`（2.6 起两者默认 false 的两个开关落地处）；
10. `lazyInitialization=true`（`spring.main.lazy-initialization`）时注册 `LazyInitializationBeanFactoryPostProcessor`——把所有未显式设置 lazyInit 的 bean 定义改为懒加载（`SmartInitializingSingleton` 实现自动排除）；
11. 注册 `PropertySourceOrderingBeanFactoryPostProcessor`，refresh 时再次 `DefaultPropertiesPropertySource.moveToEnd`；
12. **加载 sources**（:406-412）：非 AOT 模式下由 `BeanDefinitionLoader`（:83-92）注册主类——它同时持有 `AnnotatedBeanDefinitionReader`（注解类）、`XmlBeanDefinitionReader`、`GroovyBeanDefinitionReader`、`ClassPathBeanDefinitionScanner` 四个加载器，`load(Object)`（:133-152）按 Class/Resource/Package/CharSequence 分发。**主类 `@SpringBootApplication` 在这里注册为 BeanDefinition**。最后 `listeners.contextLoaded`（:412）。

**contextLoaded 的分水岭意义**（`EventPublishingRunListener.contextLoaded`，:91-99）：

```java
public void contextLoaded(ConfigurableApplicationContext context) {
    for (ApplicationListener<?> listener : this.application.getListeners()) {
        if (listener instanceof ApplicationContextAware contextAware) {
            contextAware.setApplicationContext(context);
        }
        context.addApplicationListener(listener);   // ★ 监听器迁移到真正的 context
    }
    multicastInitialEvent(new ApplicationPreparedEvent(this.application, this.args, context));
}
```

此后（started/ready）事件都改由 `context.publishEvent` 广播，`initialMulticaster` 仅在 failed 分支兜底。

### 2.4 启动流程总图

```mermaid
flowchart TD
    A["main(args)"] --> B["new SpringApplication(primarySources)"]
    B --> B1["WebApplicationType.deduceFromClasspath()<br/>SERVLET / REACTIVE / NONE"]
    B --> B2["spring.factories 装载<br/>BootstrapRegistryInitializer×N<br/>ApplicationContextInitializer×5<br/>ApplicationListener×7"]
    B --> B3["StackWalker 推断 mainApplicationClass"]
    B1 & B2 & B3 --> C["run(args)"]
    C --> D1["createBootstrapContext()<br/>BootstrapRegistryInitializer"]
    D1 --> D2["listeners.starting()<br/>①ApplicationStartingEvent"]
    D2 --> E["prepareEnvironment()"]
    E --> E1["getOrCreateEnvironment<br/>+ commandLineArgs addFirst"]
    E1 --> E2["listeners.environmentPrepared()<br/>②ApplicationEnvironmentPreparedEvent<br/>★EnvironmentPostProcessor 们执行<br/>★ConfigData 加载 application.yml<br/>★LoggingApplicationListener 初始化日志"]
    E2 --> E3["DefaultPropertiesPropertySource.moveToEnd<br/>bindToSpringApplication(spring.main.*)"]
    E3 --> F["printBanner()"]
    F --> G["createApplicationContext()<br/>AnnotationConfigServletWebServerApplicationContext"]
    G --> H["prepareContext()"]
    H --> H1["setEnvironment / applyInitializers"]
    H1 --> H2["listeners.contextPrepared()<br/>③ApplicationContextInitializedEvent"]
    H2 --> H3["BootstrapContextClosedEvent<br/>注册 springApplicationArguments 单例<br/>BeanDefinitionLoader 注册主类"]
    H3 --> H4["listeners.contextLoaded()<br/>④ApplicationPreparedEvent<br/>监听器迁移到 context"]
    H4 --> I["refreshContext()"]
    I --> I1["shutdownHook.registerApplicationContext<br/>(仅一次 addShutdownHook)"]
    I1 --> I2["context.refresh()"]
    I2 --> I2a["invokeBeanFactoryPostProcessors<br/>★ConfigurationClassPostProcessor<br/>★自动配置在此发生"]
    I2 --> I2b["onRefresh()<br/>★createWebServer 内嵌容器"]
    I2 --> I2c["finishBeanFactoryInitialization<br/>单例预实例化"]
    I2 --> I2d["finishRefresh<br/>ContextRefreshedEvent<br/>★WebServer.start 绑端口"]
    I2d --> J["logStarted<br/>'Started ... in X seconds'"]
    J --> K["listeners.started()<br/>⑤ApplicationStartedEvent<br/>⑥LivenessState.CORRECT"]
    K --> L["callRunners()<br/>ApplicationRunner/CommandLineRunner"]
    L --> M["listeners.ready()<br/>⑦ApplicationReadyEvent<br/>⑧ReadinessState.ACCEPTING_TRAFFIC"]
    M --> N["return context"]
    I2 -.任一步异常.-> X["handleRunFailure<br/>ExitCodeEvent → ApplicationFailedEvent<br/>FailureAnalyzers 诊断 → context.close()"]
    L -.runner 异常.-> X
```

### 2.5 启动事件时序图

```mermaid
sequenceDiagram
    participant Main as main 线程
    participant SA as SpringApplication
    participant RL as EventPublishingRunListener
    participant IM as initialMulticaster
    participant CTX as ApplicationContext
    participant ENV as Environment

    Main->>SA: new SpringApplication(primarySources)
    Main->>SA: run(args)
    SA->>RL: starting(bootstrapContext)
    RL->>IM: ①ApplicationStartingEvent

    SA->>ENV: getOrCreateEnvironment()
    SA->>ENV: addFirst(commandLineArgs)
    SA->>ENV: ConfigurationPropertySources.attach()
    SA->>RL: environmentPrepared(env)
    RL->>IM: ②ApplicationEnvironmentPreparedEvent
    Note over IM: EnvironmentPostProcessorApplicationListener<br/>→ ConfigDataEnvironmentPostProcessor<br/>★加载 application.yml / profile / import
    Note over IM: LoggingApplicationListener<br/>→ 初始化日志系统

    SA->>CTX: createApplicationContext()
    SA->>RL: contextPrepared(ctx)
    RL->>IM: ③ApplicationContextInitializedEvent
    SA->>CTX: BeanDefinitionLoader 注册主类
    SA->>RL: contextLoaded(ctx)
    RL->>CTX: addApplicationListener(全部 listener)
    RL->>IM: ④ApplicationPreparedEvent

    SA->>CTX: registerShutdownHook + refresh()
    Note over CTX: ConfigurationClassPostProcessor<br/>自动配置条件评估<br/>onRefresh 创建 WebServer<br/>finishRefresh 启动 WebServer

    SA->>RL: started(ctx, timeTaken)
    RL->>CTX: ⑤ApplicationStartedEvent
    RL->>CTX: AvailabilityChangeEvent(LivenessState.CORRECT)
    SA->>CTX: callRunners()
    SA->>RL: ready(ctx, timeTaken)
    RL->>CTX: ⑦ApplicationReadyEvent
    RL->>CTX: AvailabilityChangeEvent(ReadinessState.ACCEPTING_TRAFFIC)
    SA-->>Main: return context
```

### 2.6 阶段依赖关系总结

1. **webApplicationType 三处一致性**：决定 Environment 类型（⑦.1）、context 类型（⑨）、Environment 转换目标（⑦.7），全部由同一个 `ApplicationContextFactory` 体系决定；
2. **BootstrapContext 先于一切事件**；最终经 `BootstrapContextClosedEvent` 交接给 context；
3. **Environment 必须先于 Banner**（banner.txt 位置从 environment 读取）与 **Context**（setEnvironment）；
4. **applyInitializers 必须先于 sources 加载**；
5. **contextLoaded 是监听器迁移分水岭**；
6. **shutdown hook 注册先于 refresh**（refresh 中途被 kill 也能关掉半成品 context）；
7. **Started 先于 runners、Ready 晚于 runners**——两个事件的间隔即 runner 执行耗时；Liveness 在 started 置 CORRECT，Readiness 在 ready 才 ACCEPTING_TRAFFIC（K8s 探针语义）；
8. **runner 异常 → ApplicationFailedEvent**，容器被关闭。

---

## 三、自动配置原理

### 3.1 注解层：从 @SpringBootApplication 到 ImportSelector

`@SpringBootApplication`（`AC/SpringBootApplication.java:51-59`）是组合注解：

```java
@SpringBootConfiguration        // 本质 @Configuration，标识主配置类
@EnableAutoConfiguration        // ★ 开启自动配置
@ComponentScan(excludeFilters = {
        @Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
        @Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class) })
public @interface SpringBootApplication { ... }
```

- `exclude()/excludeName()` 通过 `@AliasFor` 直接透传给 `@EnableAutoConfiguration`；
- `AutoConfigurationExcludeFilter` 防止自动配置类被组件扫描误扫（排除"既是 `@Configuration` 又在 imports 文件里"的类）。

`@EnableAutoConfiguration`（`AC/EnableAutoConfiguration.java:77-83`）只做两件事：

```java
@AutoConfigurationPackage                                    // ① 登记主类所在包
@Import(AutoConfigurationImportSelector.class)               // ② 导入核心 selector
public @interface EnableAutoConfiguration {
    String ENABLED_OVERRIDE_PROPERTY = "spring.boot.enableautoconfiguration";
    Class<?>[] exclude() default {};
    String[] excludeName() default {};
}
```

`@AutoConfiguration`（`AC/AutoConfiguration.java:54-60`，2.7 引入、3.0 全面启用）：

```java
@Configuration(proxyBeanMethods = false)   // 强制 lite 模式，禁止 CGLIB 代理（启动加速）
@AutoConfigureBefore
@AutoConfigureAfter
public @interface AutoConfiguration {
    @AliasFor(annotation = AutoConfigureBefore.class, attribute = "value")
    Class<?>[] before() default {};
    @AliasFor(annotation = AutoConfigureAfter.class, attribute = "value")
    Class<?>[] after() default {};
    // ... beforeName / afterName / value
}
```

### 3.2 核心流水线：AutoConfigurationImportSelector

`AC/AutoConfigurationImportSelector.java`——**实现 `DeferredImportSelector`**（被延迟处理，这是"用户配置永远优先于自动配置"的机制根源），且通过 `getImportGroup()`（:137-139）归属 `AutoConfigurationGroup`，走 Spring 的"分组延迟导入"路径。

`getAutoConfigurationEntry()`（:121-134）九步流水线：

```java
protected AutoConfigurationEntry getAutoConfigurationEntry(AnnotationMetadata annotationMetadata) {
    if (!isEnabled(annotationMetadata)) { return EMPTY_ENTRY; }                       // ① 开关
    AnnotationAttributes attributes = getAttributes(annotationMetadata);              // ② exclude 属性
    List<String> configurations = getCandidateConfigurations(metadata, attributes);   // ③ 读 imports 文件
    configurations = removeDuplicates(configurations);                                // ④ LinkedHashSet 去重保序
    Set<String> exclusions = getExclusions(metadata, attributes);                     // ⑤ 三类排除合并
    checkExcludedClasses(configurations, exclusions);                                  // ⑥ 快速失败校验
    configurations.removeAll(exclusions);                                              // ⑦ 应用排除
    configurations = getConfigurationClassFilter().filter(configurations);            // ⑧ ★提前过滤
    fireAutoConfigurationImportEvents(configurations, exclusions);                    // ⑨ 条件评估报告事件
    return new AutoConfigurationEntry(configurations, exclusions);
}
```

**③ getCandidateConfigurations（:179-187）——3.0 的决定性变化**：

```java
List<String> configurations = ImportCandidates.load(AutoConfiguration.class, getBeanClassLoader())
        .getCandidates();
```

不再读 `spring.factories`，而是用 `ImportCandidates`（`BOOT/context/annotation/ImportCandidates.java`）读新文件：

```java
private static final String LOCATION = "META-INF/spring/%s.imports";   // :45
// → META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

- 一行一个全限定类名，支持 `#` 注释；`classLoader.getResources()` **聚合 classpath 上所有 jar 的同名文件**——第三方 starter 提供自动配置的标准方式；
- 本仓库该文件共 **141 个自动配置类**；
- **排除项三来源**（:223-229）：`exclude` + `excludeName` + `spring.autoconfigure.exclude`（后者用 Binder 宽松绑定）。`checkExcludedClasses`（:189-199）校验："类路径上存在、却不在候选列表"的排除项说明拼错了类名，直接抛 `IllegalStateException`。

**⑧ 提前过滤（ConfigurationClassFilter，:349-390）**：从 spring.factories 加载 3 个 `AutoConfigurationImportFilter`——`OnClassCondition`（@Order HIGHEST）、`OnWebApplicationCondition`（HIGHEST+20）、`OnBeanCondition`（LOWEST）。配合 `META-INF/spring-autoconfigure-metadata.properties`（编译期由 `spring-boot-autoconfigure-processor` 注解处理器生成），**全程不加载自动配置类本身**即可剔除类路径不满足的候选（详见第六节）。

### 3.3 两阶段处理：AutoConfigurationGroup

`AutoConfigurationGroup`（:392-475）实现 `DeferredImportSelector.Group`：

- **第一阶段 `process()`（:423-434）**：每个 `@EnableAutoConfiguration` 入口调用一次，执行 `getAutoConfigurationEntry`，结果累积；
- **第二阶段 `selectImports()`（:437-450）**：所有用户配置类解析完后调用**一次**——跨 selector 全局合并候选、全局应用排除，然后 `sortAutoConfigurations` 排序，返回 Entry 列表。

排序后的类被 Spring 框架当作普通 `@Import` 逐个解析——此时每个自动配置类上的 `@ConditionalOnClass`、`@ConditionalOnMissingBean` 等条件才逐个评估（走第六节的 ConditionEvaluator 链路）。

### 3.4 排序算法：AutoConfigurationSorter

`AC/AutoConfigurationSorter.java:55-70`，三步走：

```java
List<String> getInPriorityOrder(Collection<String> classNames) {
    List<String> orderedClassNames = new ArrayList<>(classNames);
    Collections.sort(orderedClassNames);                              // 第一步：字母序（保证确定性）
    orderedClassNames.sort((o1, o2) -> Integer.compare(               // 第二步：@AutoConfigureOrder
            classes.get(o1).getOrder(), classes.get(o2).getOrder()));
    orderedClassNames = sortByAnnotation(classes, orderedClassNames); // 第三步：Before/After 拓扑排序
    return orderedClassNames;
}
```

第三步（:72-98）把 `@AutoConfigureAfter`（正向边）与 `@AutoConfigureBefore`（反向边）统一成"必须在我之后"的约束图，做 **DFS 后序遍历**得到拓扑序；`checkForCycles`（:100-103）检测回边，发现循环立即抛 "AutoConfigure cycle detected"。`AutoConfigurationClass`（:154-243）优先读 spring-autoconfigure-metadata.properties，无元数据才退化为 ASM 字节码读取并缓存。

### 3.5 @AutoConfigurationPackage：基础包登记

`@AutoConfigurationPackage` 导入 `AutoConfigurationPackages.Registrar`（`AC/AutoConfigurationPackages.java:123-135`，`ImportBeanDefinitionRegistrar`）：

- `PackageImports`（:144-155）：取 `basePackages`/`basePackageClasses`，都为空时回退**标注类自身所在包**——"主类要放根包"约定的第一条机制（另一条是 `@ComponentScan` 默认扫主类包）；
- 注册为名为 `org.springframework.boot.autoconfigure.AutoConfigurationPackages` 的 `ROLE_INFRASTRUCTURE` BeanDefinition；
- 消费者：JPA `@EntityScan`、MyBatis/Spring Data 的 mapper/repository 扫描等，通过 `AutoConfigurationPackages.get(beanFactory)` 拿根包作为默认扫描起点。

### 3.6 典型自动配置类套路

`AC/jdbc/DataSourceAutoConfiguration.java:55-60` 展示"套路四件套"：

```java
@AutoConfiguration(before = SqlInitializationAutoConfiguration.class)   // 排序约束
@ConditionalOnClass({ DataSource.class, EmbeddedDatabaseType.class })   // 类路径守门（PARSE 阶段）
@ConditionalOnMissingBean(type = "io.r2dbc.spi.ConnectionFactory")      // 响应式场景让位
@EnableConfigurationProperties(DataSourceProperties.class)              // 绑定 spring.datasource.*
@Import(DataSourcePoolMetadataProvidersConfiguration.class)
public class DataSourceAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @Conditional(EmbeddedDatabaseCondition.class)
    @ConditionalOnMissingBean({ DataSource.class, XADataSource.class }) // 用户优先（REGISTER 阶段）
    @Import(EmbeddedDataSourceConfiguration.class)
    protected static class EmbeddedDatabaseConfiguration { }
    // ... PooledDataSourceConfiguration：Hikari/Tomcat/Dbcp2/OracleUcp/Generic 按可用性择一
}
```

**分层条件设计是全部秘密**：类级 `@ConditionalOnClass`（PARSE_CONFIGURATION 阶段，防 `NoClassDefFoundError`）+ 方法/内部类级 `@ConditionalOnMissingBean`（REGISTER_BEAN 阶段，用户优先）。

### 3.7 @ConfigurationProperties 绑定简述

- `ConfigurationPropertiesAutoConfiguration`（imports 文件第 7 行）→ `@EnableConfigurationProperties` → `EnableConfigurationPropertiesRegistrar`（`BOOT/context/properties/EnableConfigurationPropertiesRegistrar.java:44-50`）：注册 `ConfigurationPropertiesBindingPostProcessor` + 把属性类注册为 BeanDefinition；
- 绑定发生在 bean 初始化时：`ConfigurationPropertiesBindingPostProcessor.postProcessBeforeInitialization`（:77-89）→ `ConfigurationPropertiesBinder` → `Binder` 从 Environment 做**宽松绑定**（kebab-case/camelCase/下划线/环境变量等价），支持构造器绑定与 JavaBean 绑定，随后跑 JSR-303 校验。

### 3.8 自动配置全链路图

```mermaid
flowchart TD
    A["@SpringBootApplication"] --> B["@EnableAutoConfiguration"]
    B --> C["@AutoConfigurationPackage<br/>→ Registrar 注册 BasePackages"]
    B --> D["@Import(AutoConfigurationImportSelector)<br/>DeferredImportSelector"]

    D --> E["ConfigurationClassParser<br/>（所有用户 @Configuration 解析完后）"]
    E --> F["AutoConfigurationGroup.process()<br/>getAutoConfigurationEntry()"]

    F --> F1["getCandidateConfigurations()<br/>ImportCandidates.load()"]
    F1 --> F1a["META-INF/spring/<br/>org.springframework.boot.autoconfigure<br/>.AutoConfiguration.imports<br/>（141 个候选）"]
    F --> F2["removeDuplicates<br/>LinkedHashSet 去重"]
    F --> F3["getExclusions<br/>exclude + excludeName<br/>+ spring.autoconfigure.exclude"]
    F3 --> F4["checkExcludedClasses<br/>非法排除快速失败"]
    F --> F5["ConfigurationClassFilter.filter()"]

    subgraph FILTER["⑧ 提前过滤（免加载类）"]
        F5a["OnClassCondition<br/>@Order 最高"]
        F5b["OnWebApplicationCondition"]
        F5c["OnBeanCondition"]
        F5d["spring-autoconfigure-metadata.properties<br/>（编译期生成）"]
        F5a & F5b & F5c --> F5d
    end
    F5 --> FILTER

    F --> F6["fireAutoConfigurationImportEvents<br/>→ ConditionEvaluationReport"]

    FILTER --> G["AutoConfigurationGroup.selectImports()<br/>全局合并候选 + 排除"]
    G --> H["AutoConfigurationSorter.getInPriorityOrder()<br/>① 字母序 ② @AutoConfigureOrder<br/>③ Before/After DFS 拓扑排序（环检测）"]
    H --> I["作为普通 @Import 逐个解析"]
    I --> J["类级条件评估<br/>@ConditionalOnClass<br/>@ConditionalOnWebApplication<br/>（PARSE_CONFIGURATION 阶段）"]
    J --> K["@Bean 方法级条件评估<br/>@ConditionalOnMissingBean<br/>@ConditionalOnProperty<br/>（REGISTER_BEAN 阶段）"]
    K --> L["注册 BeanDefinition"]
```

### 3.9 与 Spring Boot 2.x 的差异

| 维度 | 2.x | 3.0.0 |
|---|---|---|
| 候选来源 | `META-INF/spring.factories` 的 `EnableAutoConfiguration` 键 | `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`（2.7 过渡期双读合并） |
| 注解 | `@Configuration(proxyBeanMethods=false)` + 可选 Before/After | `@AutoConfiguration`（强制 lite 模式，before/after 别名属性） |
| 空候选报错 | 指向 spring.factories | 明确指向 imports 文件（:182-185） |
| 其余骨架 | Group 两阶段、过滤、排序 | 完整保留并打磨（AOT、元数据缓存重构进 ConfigurationClassFilter） |

---

## 四、内嵌 Servlet 容器原理

### 4.1 自动装配：classpath 决定用哪个容器

入口 `AC/web/servlet/ServletWebServerFactoryAutoConfiguration.java:64-73`：

```java
@AutoConfiguration
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE)          // 排到所有自动配置最前
@ConditionalOnClass(ServletRequest.class)                 // Servlet API 在 classpath
@ConditionalOnWebApplication(type = Type.SERVLET)
@EnableConfigurationProperties(ServerProperties.class)    // 绑定 server.*
@Import({ ServletWebServerFactoryAutoConfiguration.BeanPostProcessorsRegistrar.class,
        ServletWebServerFactoryConfiguration.EmbeddedTomcat.class,     // @Import 顺序即优先级
        ServletWebServerFactoryConfiguration.EmbeddedJetty.class,
        ServletWebServerFactoryConfiguration.EmbeddedUndertow.class })
public class ServletWebServerFactoryAutoConfiguration { ... }
```

三个嵌入式配置类（`AC/web/servlet/ServletWebServerFactoryConfiguration.java`）条件互斥竞争：

| 配置类 | `@ConditionalOnClass`（特征类） | 兜底 |
|---|---|---|
| EmbeddedTomcat（:63-80） | `Servlet`、`org.apache.catalina.startup.Tomcat`、`org.apache.coyote.UpgradeProtocol`（防残缺依赖） | `@ConditionalOnMissingBean(ServletWebServerFactory.class)` |
| EmbeddedJetty（:85-98） | `Servlet`、`org.eclipse.jetty.server.Server`、`jetty.util.Loader`、`WebAppContext` | 同上 |
| EmbeddedUndertow（:103-124） | `Servlet`、`io.undertow.Undertow`、`org.xnio.SslClientAuthMode` | 同上 |

三者都以 `@ConditionalOnMissingBean(value = ServletWebServerFactory.class, search = SearchStrategy.CURRENT)` 兜底：**用户自定义了工厂，或三者中有一个已匹配，其余全部失效**——"classpath 决定默认、用户可覆盖、三者互斥"。

`BeanPostProcessorsRegistrar`（:124-157）用 `ImportBeanDefinitionRegistrar` **极早地**注册两个合成 bean：`WebServerFactoryCustomizerBeanPostProcessor` 和 `ErrorPageRegistrarBeanPostProcessor`（用 Registrar 而非 `@Bean` 是为了确保 BPP 在工厂 bean 创建之前就绪）。

### 4.2 server.port 如何到达 Tomcat：三层配置链

```mermaid
flowchart LR
    A["application.yml<br/>server.port=8080"] --> B["Environment<br/>PropertySource"]
    B --> C["ServerProperties<br/>@ConfigurationProperties(prefix='server')<br/>ConfigurationPropertiesBindingPostProcessor 绑定"]
    C --> D["ServletWebServerFactoryCustomizer<br/>customize(factory)<br/>factory.setPort()"]
    D --> E["WebServerFactoryCustomizerBeanPostProcessor<br/>在工厂 bean 初始化前统一套用<br/>（LambdaSafe 回调，按 @Order 排序）"]
    E --> F["TomcatServletWebServerFactory<br/>customizeConnector(connector)"]
    F --> G["connector.setPort(8080)<br/>TomcatServletWebServerFactory.java:324-325"]
    H["用户扩展点<br/>WebServerFactoryCustomizer&lt;TomcatServletWebServerFactory&gt; bean"] --> E
```

- `ServletWebServerFactoryCustomizer`（`AC/web/servlet/ServletWebServerFactoryCustomizer.java:72-86`）用 `PropertyMapper.get().alwaysApplyingWhenNonNull()` 保证"属性为 null 就跳过"；
- `TomcatServletWebServerFactoryCustomizer`（:49-59）应用 `server.tomcat.*` 专属配置（tldSkipPatterns、useRelativeRedirects 等）；
- **用户改端口/maxThreads 的官方姿势**：声明一个 `WebServerFactoryCustomizer<TomcatServletWebServerFactory>` bean。

### 4.3 refresh 中的容器创建：onRefresh → createWebServer

`BOOT/web/servlet/context/ServletWebServerApplicationContext.java`：

```java
@Override
public final void refresh() {                       // :144
    try { super.refresh(); }
    catch (RuntimeException ex) {
        WebServer webServer = this.webServer;
        if (webServer != null) { webServer.stop(); }   // :151 启动失败停掉已创建的容器
        throw ex;
    }
}

private void createWebServer() {                    // :176
    ...
    ServletWebServerFactory factory = getWebServerFactory();              // :181 ★按类型取工厂 bean
    this.webServer = factory.getWebServer(getSelfInitializer());         // :183 ★创建并初始化 Tomcat
    getBeanFactory().registerSingleton("webServerGracefulShutdown",
            new WebServerGracefulShutdownLifecycle(this.webServer));      // :185-186
    getBeanFactory().registerSingleton("webServerStartStop",
            new WebServerStartStopLifecycle(this, this.webServer));       // :187-188
}
```

`getWebServerFactory()`（:207-218）用 `getBeanNamesForType`（刻意不遍历父容器层级）找 `ServletWebServerFactory` bean，**必须恰好一个**（0 个抛 `MissingWebServerFactoryBeanException`，多个抛异常），然后 `getBean` 触发 `TomcatServletWebServerFactory` 实例化——此刻 `WebServerFactoryCustomizerBeanPostProcessor` 把 `server.*` 写入工厂。

`getSelfInitializer()`（:227）返回 `this::selfInitialize` **方法引用（延迟执行的 lambda）**；`selfInitialize`（:231-236）：

```java
private void selfInitialize(ServletContext servletContext) throws ServletException {
    prepareWebApplicationContext(servletContext);   // 等价 ContextLoaderListener：this 挂到 ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE
    registerApplicationScope(servletContext);      // 注册 ServletContextScope("application")
    WebApplicationContextUtils.registerEnvironmentBeans(getBeanFactory(), servletContext);
    for (ServletContextInitializer beans : getServletContextInitializerBeans()) {
        beans.onStartup(servletContext);           // ★逐个执行所有注册器
    }
}
```

### 4.4 ServletContextInitializer：一切 Servlet/Filter/Listener 的收敛点

`ServletContextInitializer`（`BOOT/web/servlet/ServletContextInitializer.java:53`）是 Boot 对 Servlet 规范 `ServletContainerInitializer` 的替代——**生命周期由 Spring 管理，不被容器 SPI 扫描驱动**（避免 WAR 部署双重启动）。

`ServletContextInitializerBeans`（`BOOT/web/servlet/ServletContextInitializerBeans.java:80-90`）负责收集：

1. **直接的 `ServletContextInitializer` bean**（多为 RegistrationBean）：按目标类型归类（Servlet/Filter/EventListener），底层对象记入 `seen` 集合防双重注册；
2. **裸 Servlet/Filter/Listener bean** 适配成 RegistrationBean（`addAdaptableBeans`，:148-156）——这就是"直接 `@Bean` 一个 Filter 也能生效"的原理；
3. 同类型桶内 `AnnotationAwareOrderComparator` 排序。

**裸 Servlet 的默认 URL 规则**（`ServletRegistrationBeanAdapter.createRegistrationBean`，:270-279）：

```java
String url = (totalNumberOfSourceBeans != 1) ? "/" + name + "/" : "/";  // 单个 → "/"，多个 → "/{beanName}/"
if (name.equals(DISPATCHER_SERVLET_NAME)) { url = "/"; }                // dispatcherServlet 永远 "/"
```

`RegistrationBean.onStartup` 是 final 模板方法（:46-53）；`ServletRegistrationBean.addRegistration` = **`servletContext.addServlet(name, servlet)`**（:176-179）——底层走 Servlet 3.0 动态 API。

### 4.5 Tomcat 的组装：TomcatServletWebServerFactory.getWebServer()

`BOOT/web/embedded/tomcat/TomcatServletWebServerFactory.java:188-211`：

```java
public WebServer getWebServer(ServletContextInitializer... initializers) {
    if (this.disableMBeanRegistry) { Registry.disableRegistry(); }
    Tomcat tomcat = new Tomcat();                                  // :193 StandardServer/Service/Engine/Host
    tomcat.setBaseDir(createTempDir("tomcat"));                    // 临时工作目录
    Connector connector = new Connector(this.protocol);            // :199 默认 Http11NioProtocol
    connector.setThrowOnFailure(true);
    tomcat.getService().addConnector(connector);
    customizeConnector(connector);                                 // :202 ★端口/SSL/压缩/自定义器
    tomcat.getHost().setAutoDeploy(false);                         // 关热部署扫描
    configureEngine(tomcat.getEngine());
    prepareContext(tomcat.getHost(), initializers);                // :209 ★构建 Context
    return getTomcatWebServer(tomcat);                             // :210 → new TomcatWebServer(...)
}
```

`customizeConnector`（:323-349）关键行：

```java
connector.setPort(Math.max(getPort(), 0));                     // :324-325 server.port 落地
connector.setProperty("bindOnInit", "false");                  // :336-337 ★禁止构造时绑端口（两段式基石之一）
if (getSsl() != null && getSsl().isEnabled()) { customizeSsl(connector); }
for (TomcatConnectorCustomizer customizer : this.tomcatConnectorCustomizers) { customizer.customize(connector); }
```

`prepareContext`（:220-260）：创建 `TomcatEmbeddedContext`（自定义 Context）、`server.servlet.context-path` 落地、`WebappLoader` 委派优先（`setDelegate(true)`，与 WAR 部署相反）、注册 DefaultServlet/JspServlet、`mergeInitializers` 合并回调数组（`selfInitialize` 永远第一优先）。

`configureContext`（:372-402）最关键的一行：

```java
TomcatStarter starter = new TomcatStarter(initializers);         // :373 ★Spring→Servlet 的桥
context.addServletContainerInitializer(starter, NO_CLASSES);     // :378 注册进 Tomcat 的 SCI 机制
```

**TomcatStarter**（`BOOT/web/embedded/tomcat/TomcatStarter.java:36,51-68`）实现 `jakarta.servlet.ServletContainerInitializer`，把 Spring 的 `ServletContextInitializer[]` 接到 Tomcat 的 SCI 回调上；异常捕获存入 `startUpException` 而不在 Tomcat 线程抛出。

### 4.6 两段式启动（核心设计）

**第一段——onRefresh 阶段（`TomcatWebServer.initialize()`，:113-151）**：

```java
private void initialize() throws WebServerException {
    synchronized (this.monitor) {
        try {
            addInstanceIdToEngineName();
            Context context = findContext();
            context.addLifecycleListener((event) -> {
                if (/* Context START_EVENT */) {
                    removeServiceConnectors();     // :120-126 ★摘掉 Connector，tomcat.start() 不绑端口
                }
            });
            this.tomcat.start();                   // :129 ★Context 完整启动：SCI 回调、Servlet/Filter 注册全部发生
            rethrowDeferredStartupExceptions();    // :132 把 TomcatStarter 捕获的异常在主线程重抛
            ContextBindings.bindClassLoader(...);
            startDaemonAwaitThread();              // :143 ★非守护线程防 JVM 退出
        }
        catch (Exception ex) { stopSilently(); destroySilently(); throw new WebServerException(...); }
    }
}
```

- **removeServiceConnectors（:170-178）**：监听 Context `START_EVENT`，在组件级联 start 前把 Service 上的 Connector 摘下来存入 `serviceConnectors` 字段——`tomcat.start()` 时 Service 上无 Connector，端口不绑定，但 `StandardContext.startInternal`（SCI 回调、Filter/Servlet 注册）照常完成；
- **rethrowDeferredStartupExceptions（:180-196）**：任何 Servlet/Filter 初始化失败都在**主线程**以原始异常抛出（进而被 `ServletWebServerApplicationContext.refresh()` 的 catch 捕获并 stop 容器）——这是很多 "Unable to start embedded Tomcat" 堆栈能看到业务异常的原因；
- **startDaemonAwaitThread（:198-210）**：Tomcat 全部线程是 daemon，专门起一个**非守护**线程 `container-N` 阻塞在 `tomcat.getServer().await()` 撑住进程。

**第二段——finishRefresh 阶段**：`DefaultLifecycleProcessor` 按 phase 升序启动 `SmartLifecycle` → `WebServerStartStopLifecycle.start()`（:43-47）→ `TomcatWebServer.start()`（:213-242）：

```java
public void start() throws WebServerException {
    synchronized (this.monitor) {
        if (this.started) return;
        try {
            addPreviouslyRemovedConnectors();     // :219 ★Connector 加回 Service → init()+start() → 此刻才绑端口
            performDeferredLoadOnStartup();       // :221-223 ★补执行 loadOnStartup Servlet
            checkThatConnectorsHaveStarted();     // :224 Connector FAILED → ConnectorStartFailedException
            this.started = true;
            logger.info("Tomcat started on port(s): ...");
        }
        catch (Exception ex) {
            PortInUseException.throwIfPortBindingException(ex, ...);   // :234 端口占用友好异常
            throw new WebServerException("Unable to start embedded Tomcat server", ex);
        }
    }
}
```

`TomcatEmbeddedContext` 重写 `loadOnStartup` 直接返回 true（deferred），把 DispatcherServlet 实例化推迟到端口绑定成功之后（`performDeferredLoadOnStartup`，:307-321）——避免端口已被占用却白等 Servlet 初始化。启动成功后发布 `ServletWebServerInitializedEvent`（actuator 等监听它拿实际端口，支持 `server.port=0` 随机端口）。

**两段式的收益**：端口占用检查后置、启动异常在主线程原生抛出、容器启动时序与 Spring 生命周期（SmartLifecycle）对齐。

### 4.7 DispatcherServlet 的注册

`AC/web/servlet/DispatcherServletAutoConfiguration.java`：

- `DispatcherServletConfiguration`（:81-106）：`@Conditional(DefaultDispatcherServletCondition.class)`（已存在名为 `dispatcherServlet` 的 bean 时不重复注册），`@Bean` 创建无参 `DispatcherServlet`，搬运 `WebMvcProperties` 配置；
- `DispatcherServletRegistrationConfiguration`（:108-127）：创建 `DispatcherServletRegistrationBean`（:119-120），路径取 `spring.mvc.servlet.path`（默认 `/`），`setLoadOnStartup`（默认 -1，**首个请求才 init**；`FrameworkServlet` 从 `ServletContext` 的 `ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE` 找到 root context——正是 `selfInitialize` 里 `prepareWebApplicationContext` 提前放进去的）。

**注册发生的具体时刻**：`dispatcherServletRegistration` 只是普通 bean；`selfInitialize` 执行时（仍在 `onRefresh` 栈、`tomcat.start()` 内部），`ServletContextInitializerBeans` 构造触发 `getBean("dispatcherServletRegistration")`——**DispatcherServlet 在此刻才真正实例化**——然后 `servletContext.addServlet("dispatcherServlet", dispatcherServlet)` + `addMapping("/")`。

### 4.8 内嵌容器启动时序图

```mermaid
sequenceDiagram
    participant SA as SpringApplication
    participant CTX as ServletWebServerApplicationContext
    participant F as TomcatServletWebServerFactory
    participant TW as TomcatWebServer
    participant TC as Tomcat(StandardContext)
    participant LP as LifecycleProcessor

    SA->>CTX: refreshContext → refresh()
    CTX->>CTX: invokeBeanFactoryPostProcessors<br/>(自动配置注册工厂 BeanDefinition)

    CTX->>CTX: onRefresh() → createWebServer()
    CTX->>CTX: getWebServerFactory()<br/>getBean 触发工厂实例化
    Note over CTX: WebServerFactoryCustomizerBeanPostProcessor<br/>把 server.* 写入工厂

    CTX->>F: getWebServer(selfInitializer)
    F->>TC: new Tomcat + Connector + Engine/Host
    F->>F: customizeConnector(bindOnInit=false)
    F->>TC: prepareContext → TomcatEmbeddedContext
    F->>TW: new TomcatWebServer(tomcat)
    TW->>TW: initialize()
    TW->>TC: removeServiceConnectors()（摘掉 Connector）
    TW->>TC: tomcat.start() ★第一段启动
    TC->>TC: StandardContext.startInternal
    TC->>TC: TomcatStarter.onStartup（SCI 回调）
    TC->>CTX: selfInitialize(servletContext)
    Note over CTX: 挂 ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE<br/>执行全部 ServletContextInitializer<br/>★servletContext.addServlet("dispatcherServlet")
    TW->>TW: startDaemonAwaitThread()（非守护线程）
    TW-->>CTX: webServer 就绪（未绑端口）

    CTX->>CTX: finishBeanFactoryInitialization（单例预实例化）
    CTX->>LP: finishRefresh()
    LP->>TW: WebServerStartStopLifecycle.start()
    TW->>TC: addPreviouslyRemovedConnectors() ★第二段
    TC->>TC: Connector init()+start() → 绑定端口
    TW->>TC: performDeferredLoadOnStartup()
    TW->>CTX: publish ServletWebServerInitializedEvent
    SA-->>SA: ApplicationStartedEvent → runners → Ready
```

### 4.9 优雅停机

- `server.shutdown=graceful` 时 `TomcatWebServer` 构造 `GracefulShutdown`（:109）；
- `WebServerGracefulShutdownLifecycle`（phase = `SmartLifecycle.DEFAULT_PHASE - 1024`，最先停止）→ `GracefulShutdown.doShutdown`（:57-110）：**pause 所有 Connector + closeServerSocketGraceful（停收新连接、已建立连接继续处理）→ 每 50ms 轮询各 Context 的 async 计数与 `StandardWrapper.getCountAllocated()`，全部空闲才回调 IDLE**；
- `ServletWebServerApplicationContext.doClose()`（:168-174）：关闭前先发布 `AvailabilityChangeEvent(ReadinessState.REFUSING_TRAFFIC)` 让 LB/健康检查摘流量，再 `super.doClose()`（3.0 已无 2.x 的 `stopAndReleaseWebServer()`，停止职责移交 `WebServerStartStopLifecycle.stop()`）。

---

## 五、application 配置文件加载原理

### 5.1 触发链：从 run() 到 ConfigDataEnvironmentPostProcessor

配置文件加载发生在 `prepareEnvironment()` 的 `listeners.environmentPrepared()`（`SpringApplication.java:352`）：

```
SpringApplication.prepareEnvironment()                       (:346)
  └─ listeners.environmentPrepared()                          → ApplicationEnvironmentPreparedEvent
      └─ EnvironmentPostProcessorApplicationListener.onApplicationEvent()   (BOOT/env/...:92-111)
          └─ 按 order 依次执行 spring.factories 中的 EnvironmentPostProcessor：
              ├─ CloudFoundryVcapEnvironmentPostProcessor
              ├─ ConfigDataEnvironmentPostProcessor  ★ ORDER = HIGHEST_PRECEDENCE + 10，核心
              ├─ RandomValuePropertySourceEnvironmentPostProcessor（HIGHEST+1，注册 random.*）
              ├─ SpringApplicationJsonEnvironmentPostProcessor（HIGHEST+5，解析 SPRING_APPLICATION_JSON）
              ├─ SystemEnvironmentPropertySourceEnvironmentPostProcessor（HIGHEST+4，环境变量下划线转驼峰）
              └─ DebugAgentEnvironmentPostProcessor
```

两个顺序设计：

- `EnvironmentPostProcessorApplicationListener.DEFAULT_ORDER = HIGHEST + 10`（:46），**先于** `LoggingApplicationListener`（HIGHEST+20）——日志系统初始化时必须能读到 `application.yml` 里的 `logging.*`；
- `RandomValuePropertySource`（HIGHEST+1）**早于** ConfigData——这样配置文件里才能写 `${random.int[1024,65535]}`（`random` 源插在 `systemEnvironment` 之后，`RandomValuePropertySource.java:144-163`）；
- ConfigData 加载时日志系统可能未就绪，日志经 `DeferredLogs` 缓存、到 `ApplicationPreparedEvent` 时统一回放（:113-123）。

### 5.2 ConfigDataEnvironment：五阶段流水线

`ConfigDataEnvironmentPostProcessor.postProcessEnvironment()`（:88-97）→ `new ConfigDataEnvironment(...).processAndApply()`。

**默认搜索路径**（`BOOT/context/config/ConfigDataEnvironment.java:88-94`）：

```java
static {
    locations.add(ConfigDataLocation.of("optional:classpath:/;optional:classpath:/config/"));
    locations.add(ConfigDataLocation.of("optional:file:./;optional:file:./config/;optional:file:./config/*/"));
}
```

`processAndApply()`（:225-237）五阶段：

```mermaid
flowchart TD
    A["processAndApply()"] --> B["阶段1 processInitial<br/>无 activationContext<br/>加载非 profile 文件（含递归 import）"]
    B --> C["阶段2 createActivationContext<br/>推断 CloudPlatform"]
    C --> D["阶段3 processWithoutProfiles<br/>处理新 import 文件中再嵌套的 import"]
    D --> E["阶段4 withProfiles<br/>★绑定 spring.profiles.*<br/>确定 active/default/include profiles"]
    E --> F["阶段5 processWithProfiles<br/>加载 application-{profile}.* 变体"]
    F --> G["applyToEnvironment<br/>addLast 合入 + setActiveProfiles"]
```

**为什么 profiles 必须在加载中途决断**：`application-dev.yml` 只有知道 profile 集合后才能被发现，而 `spring.profiles.active` 又可能写在任何 `application.yml`（含 import 进来的）里——所以整体拆成 `BEFORE_PROFILE_ACTIVATION` / `AFTER_PROFILE_ACTIVATION` 两个 import 阶段。profile 一旦确定即**锁定**：profile 特定文件中出现 `spring.profiles.active/include/default` 直接抛 `InvalidConfigDataPropertyException`；旧属性 `spring.profiles` 被要求改成 `spring.config.activate.on-profile`。

**applyToEnvironment（:324-338）写回规则**：

```java
applyContributor(contributors, activationContext, propertySources);   // 只处理 BOUND_IMPORT 且 isActive 的
propertySources.addLast(propertySource);                              // :353 ★addLast——天然低于命令行/系统属性
DefaultPropertiesPropertySource.moveToEnd(propertySources);
this.environment.setActiveProfiles(...);  this.environment.setDefaultProfiles(...);
```

### 5.3 贡献者树：ConfigDataEnvironmentContributors

**不可变树结构**（每次变更生成新节点整体替换，`ConfigDataEnvironmentContributors.java:118-121`）：

| Kind | 含义 |
|---|---|
| `ROOT` | 根节点 |
| `INITIAL_IMPORT` | 初始导入位置（由三组属性生成） |
| `EXISTING` | 包装 Environment 已有源（命令行、systemProperties……只读，不会被重复合入） |
| `UNBOUND_IMPORT` | 刚加载、尚未绑定 ConfigDataProperties 的文件 |
| `BOUND_IMPORT` | 已绑定（知道 import 列表与 activate 条件），最终合入 Environment 的节点 |
| `EMPTY_LOCATION` | 合法但无内容的位置 |

`withProcessedImports`（:91-124）是**迭代收敛的递归加载**：

```java
while (true) {
    contributor = getNextToProcess(result, activationContext, importPhase); // 找下一个待处理节点
    if (contributor == null) return result;                                // 收敛退出
    if (contributor.getKind() == Kind.UNBOUND_IMPORT) { /* 先绑定 */ continue; }
    imports = contributor.getImports();                                    // 拿到该文件的 spring.config.import
    imported = importer.resolveAndLoad(activationContext, ..., imports);   // 解析+加载
    result = new ...(root.withReplacement(contributor,
            contributor.withChildren(importPhase, asContributors(imported)))); // 子节点挂上
}
```

这个 while 循环实现了**任意深度的 import 递归**（A import B、B import C……）。

**优先级的机关在"多处倒序"**：

1. 初始位置列表**倒序**加入 contributors（`ConfigDataEnvironment.java:209-214`）→ `spring.config.import` > `spring.config.additional-location` > `spring.config.location`（默认位置），且 file 段在前（外部文件 > jar 内）；
2. resolver 正序产生 references（profile 特定资源排在非 profile 之后）；
3. importer **倒序**加载入 LinkedHashMap（`ConfigDataImporter.java:116`）→ `config/` 目录 > 根目录、profile 文件 > 基础文件、多文档 YAML 后文档 > 前文档；
4. 同目录下扩展优先级 **`.properties` > `.xml` > `.yml` > `.yaml`**（2.4 起改为 properties 优先）；
5. `withChildren` 在 AFTER 阶段触发 `moveProfileSpecific`（:268-299）——把 profile 特定文件整体提到最高优先级。

### 5.4 位置解析：StandardConfigDataLocationResolver

- `ConfigDataLocation` 语法：`optional:` 前缀（不存在不报错）、`;` 分号多位置、`[.yml]` 扩展名提示、相对路径（**相对父文件所在目录**，`getResourceLocation`，:165-179）；
- **通配符 `file:./config/*/`**：`LocationResourceLoader.getResources`（:94-124）列出所有可见子目录，**按绝对路径排序**，子目录内按文件名排序——`config/dev/`、`config/prod/` 的优先级按路径字典序；
- `StandardConfigDataLocationResolver` 被 `ConfigDataLocationResolvers.reorder()`（:66-85）强制排最后——它的 `isResolvable()` 无条件返回 true，是兜底解析器；带前缀的（`configtree:`）必须先匹配；
- `spring.config.name` 可改文件名（默认 `application`，不允许含 `*`）；
- optional 语义（`StandardConfigDataReference.isSkippable`，:81-83）= `optional:` 前缀 **或** 目录引用 **或** profile 引用（profile 文件天然可缺）。

### 5.5 加载与 Origin 追踪

`StandardConfigDataLoader.load()`（`BOOT/context/config/StandardConfigDataLoader.java:43-57`）：

```java
ConfigDataResourceNotFoundException.throwIfDoesNotExist(resource, resource.getResource()); // ImportCheck
Resource originTrackedResource = OriginTrackedResource.of(resource.getResource(),
        Origin.from(reference.getConfigDataLocation()));            // 位置来源也做 Origin
List<PropertySource<?>> propertySources =
        reference.getPropertySourceLoader().load(name, originTrackedResource);  // 委托 PropertySourceLoader
return new ConfigData(propertySources, options);                     // PROFILE_SPECIFIC / NON_PROFILE_SPECIFIC
```

- **.properties**：`OriginTrackedPropertiesLoader`（自研 `CharacterReader` 逐字符解析），每个值用 `OriginTrackedValue.of(value, new TextResourceOrigin(resource, location))` 包装，**精确记录文件+行列号**；支持 `name[]=a,b,c` 索引列表展开、`#---` 多文档分隔；
- **.yml**：`OriginTrackedYamlLoader` 覆写 snakeyaml 的 `constructObject` 为每个标量包 Origin；**重复 key 直接报错**（`setAllowDuplicateKeys(false)`）；嵌套 map 拍平成 `a.b.c`；`---` 多文档；
- `OriginTrackedMapPropertySource` 实现 `OriginLookup<String>`——Actuator `/actuator/configprops`、`--debug` 输出 "origin: class path resource [application.properties]:5:3" 的数据来源；
- `configtree:/etc/config`（`ConfigTreeConfigDataLocationResolver`，K8s ConfigMap 挂载利器）：目录树映射为属性 key。

### 5.6 Profiles 决策（`BOOT/context/config/Profiles.java`）

- 构造器（:80-84）：绑定 `spring.profiles.group.*`（profile 组，栈展开）；active = binder 绑定值 + `SpringApplication.setAdditionalProfiles` + 全树收集的 `spring.profiles.include`；default 绑定 `spring.profiles.default`；
- `getProfiles()`（:95-121）裁决：若 profile 是"程序化设置"（Environment 上 `setActiveProfiles` 或来自系统属性/环境变量），则**环境优先**，配置文件值与之合并；否则以配置文件为准，都没有回落 `default`；
- **`spring.config.activate.on-profile`**（如 `"prod & cloud"`，支持表达式）控制"某个文件/文档是否生效"，与 profile 激活是两回事；判定在 `Contributor.isActive()`；另有 `on-cloud-platform`。

### 5.7 spring.config.import（3.0 核心特性）

- 早期（命令行/系统属性/环境变量）声明的 import 由 `getInitialImportContributors()`（:195-214）处理；**文件内部**的 import 经 `withBoundProperties` 绑定后由 while 循环递归处理——import 的文件还能再 import；
- 任何新增 `ConfigDataLocationResolver`/`ConfigDataLoader` SPI 实现即可支持自定义前缀——Spring Cloud 的 `configserver:`、Vault 的 `vault://` 即基于此；
- 失败语义：非 optional 的 import 找不到 → `ConfigDataLocationNotFoundException`（含 Origin）→ `ConfigDataNotFoundFailureAnalyzer` 生成诊断；可用 `spring.config.on-not-found=ignore` 全局降级。

### 5.8 配置加载全链路图

```mermaid
flowchart TD
    A["SpringApplication.prepareEnvironment()"] --> B["getOrCreateEnvironment()<br/>ApplicationServletEnvironment"]
    B --> C["configurePropertySources<br/>commandLineArgs addFirst<br/>defaultProperties addLast"]
    C --> D["ConfigurationPropertySources.attach<br/>'configurationProperties' 适配器视图"]
    D --> E["listeners.environmentPrepared<br/>ApplicationEnvironmentPreparedEvent"]
    E --> F["EnvironmentPostProcessorApplicationListener"]
    F --> G1["RandomValuePropertySource（random.*）"]
    F --> G2["SpringApplicationJson"]
    F --> G3["ConfigDataEnvironmentPostProcessor ★"]
    G3 --> H["ConfigDataEnvironment 构造<br/>contributors = EXISTING 包装现有源<br/>+ INITIAL_IMPORT 初始位置"]
    H --> I["processInitial<br/>解析 spring.config.location/<br/>additional-location/import"]

    subgraph RESOLVE["ConfigDataImporter.resolveAndLoad"]
        R1["ConfigDataLocationResolvers<br/>configtree: → Standard 兜底"]
        R2["StandardConfigDataLocationResolver<br/>目录→(name × loader × 扩展名) 引用<br/>通配符子目录按路径排序"]
        R3["倒序加载入 LinkedHashMap<br/>同一资源只加载一次"]
        R1 --> R2 --> R3
    end
    I --> RESOLVE

    R3 --> J["PropertySourceLoader<br/>OriginTrackedPropertiesLoader / YamlLoader<br/>每个值记录文件+行列 Origin"]
    J --> K["Contributors 树<br/>UNBOUND_IMPORT → 绑定 → BOUND_IMPORT"]
    K --> L["withProfiles<br/>锁定 spring.profiles.*"]
    L --> M["processWithProfiles<br/>application-{profile}.* 加载<br/>moveProfileSpecific 提优先级"]
    M --> N["applyToEnvironment<br/>BOUND_IMPORT 且 isActive 的<br/>按迭代序 addLast 合入<br/>setActiveProfiles"]
    N --> O["DefaultPropertiesPropertySource.moveToEnd"]
    O --> P["bindToSpringApplication<br/>spring.main.* 绑定回 SpringApplication"]
    P --> Q["LoggingApplicationListener<br/>读 logging.* 初始化日志"]
```

### 5.9 最终外部化配置优先级（高 → 低）

1. `configurationProperties` —— attach 的适配器（非真实数据源，动态代理其余源以支持松散绑定）
2. `commandLineArgs` —— 命令行参数 `--foo=bar`
3. `spring.application.json` —— `SPRING_APPLICATION_JSON` 环境变量/系统属性
4. `servletConfigInitParams`、`servletContextInitParams`（Servlet 环境）
5. `systemProperties` —— `-Dfoo=bar`
6. `systemEnvironment` —— OS 环境变量
7. `random` —— RandomValuePropertySource
8. **Config data**（按 ContributorIterator 序，先入者优先级更高），内部细分（高 → 低）：
   1. `spring.config.import` 导入的内容（递归子节点 > 被导入文件本身）
   2. `spring.config.additional-location` 指定的位置
   3. 默认搜索位置中的 **profile 特定文件**：`file:./config/*/`（子目录字典序）> `file:./config/` > `file:./` > `classpath:/config/` > `classpath:/` 下的 `application-{profile}.properties`（同目录 `.properties` > `.xml` > `.yml` > `.yaml`；多文档后文档 > 前文档）
   4. 默认搜索位置中的**基础文件**（目录顺序同上）
9. `defaultProperties` —— `SpringApplication.setDefaultProperties(...)`

---

## 六、@Conditional 条件注解实现原理

### 6.1 spring-context 契约与两个评估阶段

`@Conditional` 是元注解，标注在所有 Boot 条件注解上，例如 `ConditionalOnClass.java:62-66`：

```java
@Target({TYPE, METHOD}) @Retention(RUNTIME)
@Conditional(OnClassCondition.class)
public @interface ConditionalOnClass { ... }
```

契约链（spring-framework 侧，本仓库不含其源码）：

- `Condition.matches(ConditionContext, AnnotatedTypeMetadata)`；`ConditionContext` 暴露 `getRegistry()/getBeanFactory()/getEnvironment()/getResourceLoader()/getClassLoader()`；
- `ConfigurationCondition.getConfigurationPhase()` 声明评估阶段，`ConditionEvaluator.shouldSkip(metadata, phase)` 语义：阶段不一致则本次不评估；否则实例化 Condition 并调 `matches()`，false = 跳过该配置类/Bean 方法；
- 两个调用点：
  - **PARSE_CONFIGURATION**：`ConfigurationClassParser.processConfigurationClass()` 解析每个 `@Configuration` 类**之前**——跳过则整棵子树（含 @Bean 方法、嵌套配置）都不进模型；
  - **REGISTER_BEAN**：`ConfigurationClassBeanDefinitionReader` 把 `@Bean` 方法注册为 BeanDefinition **之前**。

**阶段分离是核心设计**：类路径/环境类条件（`OnClassCondition`、`OnWebApplicationCondition`）在 PARSE 阶段剪掉整棵子树；`OnBeanCondition` 显式声明 `REGISTER_BEAN`（`OnBeanCondition.java:80-82`）——bean 存在性只有等用户配置的 BeanDefinition 全部注册后判断才有意义，这正是 `@ConditionalOnMissingBean` "用户覆盖自动配置" 语义的基础（配合 DeferredImportSelector 的延迟处理 + AutoConfigurationSorter 的自动配置间排序）。

### 6.2 SpringBootCondition：模板方法骨架

`AC/condition/SpringBootCondition.java:44-62`——`final matches()` 模板方法：

```java
public final boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String classOrMethodName = getClassOrMethodName(metadata);
    try {
        ConditionOutcome outcome = getMatchOutcome(context, metadata);   // 唯一抽象方法
        logOutcome(classOrMethodName, outcome);        // TRACE 日志
        recordEvaluation(context, classOrMethodName, outcome);   // ★写入 ConditionEvaluationReport
        return outcome.isMatch();
    }
    catch (NoClassDefFoundError ex) { throw new IllegalStateException("Could not evaluate condition on " ...); }
    catch (RuntimeException ex) { throw new IllegalStateException("Error processing condition on " ...); }
}
```

**关键增强**：把 Spring 的 `matches(): boolean` 一元契约升级为 `getMatchOutcome(): ConditionOutcome`（布尔 + 可读消息 + 可记录）三元契约——评估结果必须可解释，供条件评估报告使用。`ConditionMessage` 是流式 Builder（`forCondition(...).didNotFind("required class").items(Style.QUOTE, missing)`）。

### 6.3 OnClassCondition：类存在性检测 + 预过滤

`AC/condition/OnClassCondition.java`，`@Order(HIGHEST_PRECEDENCE)`（:42）——同时是 `Condition` 和 `AutoConfigurationImportFilter`（经父类 `FilteringSpringBootCondition`，一鱼两吃）。

**正式评估 `getMatchOutcome`（:84-110）**：

1. `getCandidates(metadata, ConditionalOnClass.class)`（:112-121）：ASM 读注解的 `value`/`name` 属性——**`Class` 属性以字符串返回，无需加载被引用类**（`@ConditionalOnClass(SomeService.class)` 写在类上是安全的；写在 @Bean 方法上有风险，因为方法返回类型在评估前可能被 JVM 加载——注解 Javadoc 明确建议拆出嵌套 @Configuration 类）；
2. `filter(onClasses, ClassNameFilter.MISSING, classLoader)`（:89）：任一缺失即 noMatch；
3. `@ConditionalOnMissingClass` 反向处理（:98-108）。

**类探测的轻量化**（`FilteringSpringBootCondition.java:106-148`）：`resolve()` 直接 `Class.forName(className, false, classLoader)`——**false 表示不触发静态初始化**，且吞掉一切 Throwable（包括 LinkageError，"类存在但静态依赖断裂"也视为缺失）。

**预过滤 `getOutcomes`（:46-59）**——启动性能的招牌优化：

```java
// StandardOutcomesResolver.getOutcomes()（:187-200）
String candidates = autoConfigurationMetadata.get(autoConfigurationClass, "ConditionalOnClass");
if (candidates != null) { outcomes[i - start] = getOutcome(candidates); }
```

- **完全不读自动配置类字节码**，只查 `spring-autoconfigure-metadata.properties`（编译期由 `spring-boot-autoconfigure-processor` 注解处理器生成，键形如 `全限定名.ConditionalOnClass=...`）；
- 候选多且多核时**双线程对半拆分**（`resolveOutcomesThreaded`，:61-81；注释明确"单个额外线程性能最好"）；
- filter 阶段异常吞掉（:214-216 注释 "We'll get another chance later"）——预过滤不致命，未被剔除的类后续在正式 Condition 评估中兜底。

### 6.4 OnBeanCondition：bean 存在性 / 用户覆盖语义

`AC/condition/OnBeanCondition.java`，`implements ConfigurationCondition` → **`REGISTER_BEAN`**（:80-82），`@Order(LOWEST_PRECEDENCE)`。

一个条件类服务三个注解（`getMatchOutcome`，:114-164）：

- `@ConditionalOnBean`：`getMatchingBeans()` 后 `!isAllMatched()` 即 noMatch；
- `@ConditionalOnMissingBean`：`isAnyMatched()`（任一匹配 bean 存在）即 noMatch——**让位**；
- `@ConditionalOnSingleCandidate`：匹配多于 1 个时要求**恰好一个 primary**（`getPrimaryBeans`，:355-365；多个 primary → "multiple primary beans"，零个 → "did not find a primary bean"）。

**getMatchingBeans 核心查找（:166-217）**：

1. `ignored/ignoredType` 先行剔除；
2. **按 type**：`beanFactory.getBeanNamesForType(type, true, false)`（allowEagerInit=true 触发 FactoryBean/泛型解析）——条件匹配 **FactoryBean 产出的对象类型**而非 FactoryBean 本身；支持 `parameterizedContainer`（`@ConditionalOnMissingBean(value=X.class, parameterizedContainer=Repository.class)` 匹配 `Repository<X>`）；剔除 `scopedTarget.` 前缀的内部 bean；类型类不存在返回空集合（= 没有这种 bean）；
3. **按 annotation**：`getBeanNamesForAnnotation`；
4. **按 name**：`containsBean`（含层级）或 `containsLocalBean`。

**SearchStrategy**（:25-42）：`CURRENT`（只查当前容器）、`ALL`（默认，沿父容器递归）、`ANCESTORS`（只查父容器）。

**类型推导**（:510-552）：type/name/annotation 都为空时，对 `@Bean` 方法推导返回类型（:527 注释 "Safe to load at this point since we are in the REGISTER_BEAN phase"）。

**顺序敏感语义**（`ConditionalOnMissingBean` Javadoc）："The condition can only match the bean definitions that have been processed by the application context so far"——匹配的是**当前已注册的 BeanDefinition 集合**，因此自动配置必须（且经 DeferredImportSelector 保证）在用户配置之后处理。

### 6.5 其他条件实现速览

| 条件类 | Order | 判定逻辑 |
|---|---|---|
| `OnWebApplicationCondition` | HIGHEST+20 | SERVLET 锚点 `GenericWebApplicationContext` 存在 + beanFactory 有 `"session"` scope + `ConfigurableWebEnvironment` + `WebApplicationContext` 四级判定（:124-142）；REACTIVE 锚点 `HandlerResult`。注意：DispatcherServlet 检查发生在 `WebApplicationType.deduceFromClasspath()`，本类用更粗粒度锚点 |
| `OnPropertyCondition` | HIGHEST+40 | `prefix+name` → `containsProperty` 则比对 `havingValue`（忽略大小写；**未指定 havingValue 时默认"值 ≠ false 即匹配"**）；缺失时看 `matchIfMissing`（:115-136） |
| `OnResourceCondition` | — | `resolvePlaceholders` 后 `ResourceLoader.getResource().exists()` |
| `OnExpressionCondition` | LOWEST-20 | SpEL：`#{...}` 自动包装，`BeanExpressionResolver` 求值（评估昂贵故排最后） |
| `OnJavaCondition` / `OnCloudPlatformCondition` / `OnJndiCondition` | — | Java 版本区间 / `CloudPlatform.isActive(environment)` / JNDI location 探测 |
| `OnAvailableEndpointCondition`（actuator） | — | 启用判定三级优先：`management.endpoint.<id>.enabled` > `management.endpoints.enabled-by-default` > `@Endpoint(enableByDefault)`；暴露判定查 exposure filter；两个 `ConcurrentReferenceHashMap` 软引用缓存 |

### 6.6 组合条件：AbstractNestedCondition

`@Conditional` 数组语义是 AND；OR/NOT 需嵌套条件族（同包）：

- `AbstractNestedCondition`（:45-220）：用 ASM 读**自身类的内部成员类**上的 `@Conditional`，反射实例化各 Condition，聚合后交 `getFinalMatchOutcome`；外层 PARSE 阶段时成员条件不允许声明 REGISTER_BEAN（`validateMemberCondition`，:133-143）；
- `AllNestedConditions` = AND、`AnyNestedCondition` = OR、`NoneNestedConditions` = NOT；典型用法：`AnyNestedCondition` + `@ConditionalOnJndi`/`@ConditionalOnProperty` 两个静态内部类表达"JNDI 或属性"。

### 6.7 FilteringSpringBootCondition：Condition 与 ImportFilter 合体

`FilteringSpringBootCondition.java:39-61`——`OnClassCondition`/`OnBeanCondition`/`OnWebApplicationCondition` 三个子类**一鱼两吃**（经 spring.factories 注册为 `AutoConfigurationImportFilter`）：

```java
public boolean[] match(String[] autoConfigurationClasses, AutoConfigurationMetadata autoConfigurationMetadata) {
    ConditionEvaluationReport report = ConditionEvaluationReport.find(this.beanFactory);
    ConditionOutcome[] outcomes = getOutcomes(autoConfigurationClasses, autoConfigurationMetadata);
    for (int i = 0; i < outcomes.length; i++) {
        match[i] = (outcomes[i] == null || outcomes[i].isMatch());
        if (!match[i] && outcomes[i] != null) {
            report.recordConditionEvaluation(autoConfigurationClasses[i], this, outcomes[i]);  // skip 记录
        }
    }
    return match;
}
```

`outcomes[i] == null` = 该过滤器对此类"无意见"（metadata 无对应条目），视为通过；明确 noMatch 则**当场写入条件评估报告**（报告里被预过滤的 Negative matches 条目来源，此后不再走正式评估）。

### 6.8 条件评估报告（--debug）

`AC/condition/ConditionEvaluationReport.java`：

- **存储**：以单例 `"autoConfigurationReport"` 注册进 beanFactory（:51, :180-193），支持父子上下文报告链；
- **三个写入口**：① `SpringBootCondition.matches()`（正常评估，最完整）；② `FilteringSpringBootCondition.match()`（预过滤 skip）；③ `ConditionEvaluationReportAutoConfigurationImportListener`（候选/排除名单）；
- **输出**：`ConditionEvaluationReportLoggingListener`（spring.factories 注册的 ApplicationContextInitializer）监听 `ContextRefreshedEvent`/`ApplicationFailedEvent`，DEBUG 级别输出 `ConditionEvaluationReportMessage` 四段：**Positive matches / Negative matches / Exclusions / Unconditional classes**——`--debug` 启动参数即打开。

### 6.9 条件评估链路图

```mermaid
flowchart TD
    A["@ConditionalOnXxx<br/>@Conditional(OnXxxCondition.class)"] --> B["spring-context<br/>ConditionEvaluator.shouldSkip(metadata, phase)"]

    B --> C1["phase = PARSE_CONFIGURATION<br/>ConfigurationClassParser<br/>.processConfigurationClass()"]
    B --> C2["phase = REGISTER_BEAN<br/>ConfigurationClassBeanDefinitionReader<br/>.loadBeanDefinitionsForBeanMethod()"]

    C1 & C2 --> D["SpringBootCondition.matches()<br/>final 模板方法"]

    D --> E["getMatchOutcome(context, metadata)<br/>子类实现"]
    E --> E1["OnClassCondition<br/>Class.forName(name, false, loader)<br/>不触发静态初始化"]
    E --> E2["OnBeanCondition (REGISTER_BEAN)<br/>getBeanNamesForType(type, true, false)<br/>层级/ignored/泛型容器/primary"]
    E --> E3["OnWebApplicationCondition<br/>锚点类+session scope+环境类型"]
    E --> E4["OnPropertyCondition<br/>havingValue/matchIfMissing"]
    E --> E5["OnExpressionCondition<br/>SpEL 求值"]
    E --> E6["AbstractNestedCondition<br/>ASM 读成员类 AND/OR/NOT"]

    D --> F1["logOutcome (TRACE)"]
    D --> F2["recordEvaluation<br/>→ ConditionEvaluationReport 单例<br/>--debug 输出 Positive/Negative matches"]

    G["另一并行入口<br/>AutoConfigurationImportSelector<br/>ConfigurationClassFilter.filter()"] --> H["FilteringSpringBootCondition.match()<br/>OnClass/OnBean/OnWebApplicationCondition<br/>+ spring-autoconfigure-metadata.properties<br/>免加载类提前过滤"]
    H --> I["不匹配 → 置 null 剔除<br/>并当场记录进报告"]

    style H fill:#e8f5e9
    style I fill:#e8f5e9
```

### 6.10 性能优化机制汇总

1. **编译期元数据预过滤**（免 ASM 读类，最多可省 141 次字节码解析）；
2. **并行探测**（OnClassCondition 双线程对半拆分）；
3. **轻量类加载**（`Class.forName(name, false, loader)` 不初始化）；
4. **阶段分离**（PARSE 剪掉整棵子树，REGISTER 才做容器状态判定）；
5. **ImportFilter 顺序**（最廉价的 OnClassCondition 最先，最贵的 OnBeanCondition 最后）；
6. **filter 阶段容错**（"We'll get another chance later"）；
7. **环境级软引用缓存**（OnAvailableEndpointCondition）；
8. **报告单例化**（避免每次评估重建）。

---

## 七、五大原理的关联：一张全景图

五大原理不是孤立的——它们在 `SpringApplication.run()` 的时间轴上精确交织：

```mermaid
flowchart TB
    subgraph T1["阶段一：Environment 准备"]
        direction LR
        P1["④事件机制<br/>ApplicationEnvironmentPreparedEvent"] --> P2["⑤配置文件加载<br/>ConfigDataEnvironmentPostProcessor<br/>application.yml + profile + import<br/>→ 最终 PropertySource 优先级链"]
        P2 --> P3["spring.main.* 绑定回 SpringApplication<br/>（可改 webApplicationType/banner/lazy）"]
    end

    subgraph T2["阶段二：上下文 refresh"]
        direction LR
        Q1["②自动配置<br/>DeferredImportSelector 延迟处理<br/>imports 文件 141 候选<br/>→ 过滤 → 排序 → 导入"] --> Q2["⑥条件体系<br/>PARSE: @ConditionalOnClass 守门<br/>REGISTER: @ConditionalOnMissingBean 让位<br/>（用户配置永远优先）"]
        Q2 --> Q3["③内嵌容器<br/>onRefresh → createWebServer<br/>工厂唯一 + Customizer 链<br/>两段式启动"]
        Q3 --> Q4["finishRefresh<br/>WebServer.start 绑端口<br/>DispatcherServlet 首请求 init"]
    end

    T1 --> T2

    A["①SpringApplication.run 生命周期<br/>BootstrapContext → 事件时间线<br/>→ refresh → runners → Ready"] -.调度.-> T1
    A -.调度.-> T2
    P3 -.webApplicationType 决定 context 类型.-> Q3
    P2 -.Binder 宽松绑定 ServerProperties.-> Q3
    Q2 -.@ConditionalOnClass 选中 Tomcat 容器.-> Q3
```

几条贯穿性的设计哲学：

1. **约定优于配置，但随时让位**：自动配置用 `DeferredImportSelector` 保证用户配置先处理，用 `@ConditionalOnMissingBean` 在 REGISTER 阶段让位，用 `@AutoConfigureBefore/After` 拓扑排序保证自动配置间确定性；
2. **一切可解释**：`ConditionOutcome` + `ConditionMessage` + `ConditionEvaluationReport`（--debug 报告）、`OriginTrackedValue`（配置来源精确到行列）、`FailureAnalyzer`（启动失败诊断）——每个"魔法"决策都可追溯；
3. **SPI 驱动的可插拔**：spring.factories（框架组件）与 `*.imports`（自动配置候选）双层 SPI，第三方 starter 与 `spring.config.import` 的 `configserver:`/`configtree:` 扩展都建立其上；
4. **启动性能是设计出来的**：编译期元数据免类加载预过滤、lite 配置类免 CGLIB、两段式容器启动、DeferredLogs 延迟日志、阶段分离剪枝——3.0 在此之上叠加 GraalVM AOT 支持（`AotDetector`、`aot.factories`、AOT 专用 context 类型）。

---

## 八、fatjar 可执行 Jar 实现原理

fatjar（可执行 jar）的完整实现位于 `spring-boot-project/spring-boot-tools/spring-boot-loader` 模块（包 `org.springframework.boot.loader`，下文简写 `LOADER`）--它是**零依赖的纯 JDK 实现**（主代码不依赖 Spring），由打包插件（`spring-boot-loader-tools/Packager.java` + maven/gradle 插件）在构建期把 loader 类注入 fatjar 根目录并改写 MANIFEST。

### 8.1 fatjar 内部结构与 MANIFEST

```
app.jar
├── META-INF/MANIFEST.MF          # Main-Class: JarLauncher / Start-Class: com.x.App
├── org/springframework/boot/loader/   # loader 类本身（内嵌到 fatjar 根目录，系统类加载器可见）
└── BOOT-INF/
    ├── classes/                  # 应用自己的类与资源（等价普通 jar 的 classpath 根）
    ├── classpath.idx             # 类路径顺序索引（2.3+ 引入）
    ├── layers.idx                # 分层索引（配合 jarmode-layertools，Docker 分层构建）
    └── lib/                      # 全部依赖 jar，STORED（不压缩）方式存放
```

- **war 差异**：应用类在 `WEB-INF/classes`，依赖在 `WEB-INF/lib` 与 `WEB-INF/lib-provided`（provided 作用域依赖，传统容器部署时由容器提供，`java -jar` 直跑也需要，所以 `WarLauncher` 把两个目录都算进 classpath）；
- **打包端关键动作**（`spring-boot-loader-tools/loader/tools/Packager.java`）：
  - `buildManifest()`（L313-320）：把用户原 main 类挪到 `Start-Class`，`Main-Class` 改写为 launcher 类（`Layouts.java`：jar -> `JarLauncher`，war -> `WarLauncher`，zip 默认 -> `PropertiesLauncher`）；
  - **BOOT-INF/lib 下的嵌套 jar 必须以 STORED（非 DEFLATED）方式写入**--这是 loader 能把嵌套 jar 当作随机访问数据段直接解析的前提（运行时还会强制校验，见 8.4）。

### 8.2 完整启动链路

```mermaid
flowchart TD
    A["java -jar app.jar"] --> B["JVM 读 MANIFEST.MF<br/>Main-Class = JarLauncher"]
    B --> C["JarLauncher.main(args)<br/>JarLauncher.java:64-66"]
    C --> D["new JarLauncher().launch(args)<br/>Launcher.java:51"]
    D --> E["createArchive()<br/>Launcher.java:124-137<br/>CodeSource.getLocation 定位自身<br/>目录->ExplodedArchive / jar文件->JarFileArchive"]
    D --> F["JarFile.registerUrlProtocolHandler()<br/>JarFile.java:430-436 ★接管 jar: 协议"]
    D --> G["getClassPathArchivesIterator()<br/>ExecutableArchiveLauncher.java:120-128<br/>isSearchCandidate: BOOT-INF/ 前缀<br/>isNestedArchive: classes/ 目录 + lib/*.jar"]
    G --> H["JarFileArchive.getNestedArchive<br/>BOOT-INF/classes -> NESTED_DIRECTORY 虚拟 jar<br/>BOOT-INF/lib/x.jar -> NESTED_JAR 字节区间"]
    H --> I["createClassLoader<br/>new LaunchedURLClassLoader(urls, appClassLoader)"]
    I --> J["launch(args, mainClass, classLoader)<br/>Launcher.java:93-96<br/>Thread.currentThread.setContextClassLoader"]
    J --> K["MainMethodRunner.run()<br/>MainMethodRunner.java:45-50<br/>Class.forName(Start-Class, false, TCCL)<br/>mainMethod.invoke(null, args)"]
    K --> L["用户 Start-Class.main()<br/>-> SpringApplication.run()"]
```

关键源码细节：

- `JarLauncher` 的过滤规则（`LOADER/JarLauncher.java:35-40`）：

```java
static final EntryFilter NESTED_ARCHIVE_ENTRY_FILTER = (entry) -> {
    if (entry.isDirectory()) {
        return entry.getName().equals("BOOT-INF/classes/");   // classes 目录
    }
    return entry.getName().startsWith("BOOT-INF/lib/");       // lib 下的 jar
};
```

- `getMainClass()`（`ExecutableArchiveLauncher.java:88-98`）：读 MANIFEST 的 `Start-Class`，缺失直接抛 `IllegalStateException`；
- `Launcher.launch` L56-57 支持 **jarmode 钩子**：`-Djarmode=layertools` 时不跑用户 main，而是执行 `JarModeLauncher`（Docker 分层提取靠它）；
- `LaunchedURLClassLoader.loadClass`（L121-131）对 `org.springframework.boot.loader.jarmode.` 前缀类做特殊处理--**从父加载器读字节流、但由子加载器 defineClass**（故意破坏双亲委派），否则 jarmode 类会落在系统 classloader 里而找不到 fatjar 内的依赖。

### 8.3 Archive 抽象与类加载架构

`Archive` 接口（`LOADER/archive/Archive.java:34`）：`getUrl()` / `getManifest()` / `getNestedArchives(searchFilter, includeFilter)`。两个实现：

- **`JarFileArchive`**：核心是 `getNestedArchive(Entry)`（:110-122）--entry comment 以 `UNPACK:` 开头则先解压到临时目录；否则 `jarFile.getNestedJarFile(jarEntry)` **直接在外层 jar 的数据段上再建 JarFile，全程不落盘**；
- **`ExplodedArchive`**：以目录为根（IDE 里 exploded 运行时用），URL 全是普通 `file:`，完全绕过自定义协议。

类加载层级：

```mermaid
flowchart TB
    A["Bootstrap ClassLoader<br/>JDK 核心类"] --> B["Platform ClassLoader<br/>java.sql 等"]
    B --> C["Application / System ClassLoader<br/>loader 类自身 JarLauncher 在这里<br/>能看见 fatjar 根目录的 loader 类"]
    C -->|"parent 双亲委派"| D["LaunchedURLClassLoader<br/>extends URLClassLoader<br/>urls = BOOT-INF/classes + BOOT-INF/lib/*.jar<br/>应用所有类与依赖都从这里加载"]
    D --> E["Thread.contextClassLoader<br/>launch 时被设为 LaunchedURLClassLoader<br/>Launcher.java:94"]
    style D fill:#e8f5e9
```

- `LaunchedURLClassLoader.loadClass`（:120-154）顺序：jarmode 特例 -> exploded 直接 super -> `Handler.setUseFastConnectionExceptions(true)` 包裹 -> `definePackageIfNecessary` -> `super.loadClass`；
- **definePackage 修正**（:191-233）--理解这个就理解了为何要重写：JDK `URLClassLoader.definePackage` 只会用 URL 本身的 manifest，而 fatjar 的每个 URL 都是嵌套 URL，原生 handler 拿不到正确的嵌套 manifest，会导致 `Package` 的版本/Sealed 属性丢失。Boot 的做法是在 findClass 前**主动遍历 getURLs()**，对每个 URL `openConnection()`，找到同时含目标 class entry、包目录 entry 与 manifest 的 jar，用**该嵌套 jar 自己的 MANIFEST** `definePackage`；
- **资源一致性**：`getResources("META-INF/spring.factories")` 能正确返回所有嵌套 jar + BOOT-INF/classes 中的命中项（Spring Boot 的 SPI 加载、`@Configuration` 扫描完全依赖这一点）；fast-exception 优化延续到**枚举消费时刻**（`UseFastConnectionExceptionsEnumeration`，:315-346）。

### 8.4 JarFile 体系：zip 中央目录随机访问（核心）

#### 为什么 JDK 自带的 `java.util.jar.JarFile` / `URLClassLoader` 处理不了嵌套 jar--根本问题

1. **`JarFile` 构造器只接受 `java.io.File`**。`BOOT-INF/lib/spring-web.jar` 是外层 zip 里的一个 entry，磁盘上不存在独立文件。JDK 的兜底行为是**解压到临时目录**再打开--启动慢、临时文件泄漏、签名/资源缓存失效；
2. **JDK 的 `jar:` 协议 handler 只认一层 `!/`**。`sun.net.www.protocol.jar.Handler` 无法理解 `jar:file:/app.jar!/BOOT-INF/lib/a.jar!/...` 的多层嵌套；
3. **`JarFile` 打开时顺序读整个流校验签名**，顺序访问模型对"只读一个 class 文件"的代价不可接受。

Boot loader 的解法：**外层 jar 用 `RandomAccessFile` 打开，按 zip 格式直接在原文件的字节区间上解析嵌套 jar 的中央目录，零解压、零临时文件、惰性按需解压单个 entry**。

#### 关键实现

**JarFile 构造器**（`LOADER/jar/JarFile.java:128-161`）：

```java
private JarFile(RandomAccessDataFile rootFile, String pathFromRoot, RandomAccessData data,
        JarEntryFilter filter, JarFileType type, Supplier<Manifest> manifestSupplier) {
    super(rootFile.getFile());
    super.close();                                  // 立刻关掉父类句柄，全靠自己解析
    CentralDirectoryParser parser = new CentralDirectoryParser();
    this.entries = parser.addVisitor(new JarFileEntries(this, filter));
    this.data = parser.parse(data, filter == null); // 解析中央目录
}
```

- `rootFile` 永远指向**最外层物理文件**；嵌套 JarFile 通过 `data`（某个字节子区间）+ `pathFromRoot` 区分；
- **目录型嵌套**（BOOT-INF/classes，:315-325）：用 `JarEntryFilter` 把外层 entry 名去掉前缀映射成"虚拟 jar"的 entry 集合（type = NESTED_DIRECTORY），共享外层 manifestSupplier（保证 definePackage 拿到 fatjar 主 MANIFEST）；
- **文件型嵌套**（BOOT-INF/lib/*.jar，:327-337）--**"嵌套 jar 必须不压缩"的运行时强制校验**：

```java
if (entry.getMethod() != ZipEntry.STORED) {
    throw new IllegalStateException("nested jar files must be stored without compression...");
}
RandomAccessData entryData = this.entries.getEntryData(entry.getName());
return new JarFile(this.rootFile, pathFromRoot + "!/" + entry.getName(), entryData, JarFileType.NESTED_JAR);
```

只有 STORED，字节子区间才能直接当作 zip 文件去解析其中的中央目录。

**中央目录解析流水线**：

```mermaid
flowchart LR
    A["CentralDirectoryEndRecord<br/>从文件尾部按256字节块向前搜 EOCD 签名<br/>支持 Zip64 定位<br/>CentralDirectoryEndRecord.java:60-78"] --> B["getCentralDirectory<br/>EOCD 记录的 offset/length<br/>getSubsection 得到目录数据段（视图非拷贝）"]
    B --> C["CentralDirectoryParser<br/>一次读整个目录到 byte[]<br/>逐条解析46字节头+变长name/extra<br/>-> CentralDirectoryFileHeader"]
    C --> D["JarFileEntries<br/>hashCodes/offsets/positions 三数组<br/>按hash快排 + Arrays.binarySearch 查找<br/>约10500个entry仅占~122K内存"]
    D --> E["getEntryData<br/>重读 local file header 拿真实偏移<br/>兼容 aspectjrt 等目录与local头不一致的jar"]
    E --> F["getInputStream<br/>仅此刻才解压这一段<br/>DEFLATED -> ZipInflaterInputStream raw inflate"]
    style F fill:#fff3e0
```

- `RandomAccessData.getSubsection(offset, length)` 返回**视图而非拷贝**（`RandomAccessDataFile.java:81-86`），内部惰性打开 `RandomAccessFile`，所有读 `synchronized(monitor) { seek; read }`；
- **签名策略**：构造期遍历中央目录时探测 `META-INF/**.SF` entry 置 `signed` 标志；未签名 jar（绝大多数依赖）`getCertificates()` 直接返回 `NONE`，**完全跳过 JDK 全流签名校验**（启动加速关键）；签名 jar 才回退用 `JarInputStream` 顺序读一遍取证书并按 entry 缓存（`JarFileEntries.java:334-355`）；
- **Multi-Release jar 支持**：`getEntry`（:228-243）若 MANIFEST 有 `Multi-Release: true`，从运行时版本向下逐级尝试 `META-INF/versions/N/<name>`；
- **零 String 分配的字节切片工具**（`AsciiBytes`/`Bytes`）：hash/startsWith 全在 byte[] 上做，避免解析期产生海量字符串。

### 8.5 URL 协议层：接管 `jar:` 协议

**注册机制**（`JarFile.registerUrlProtocolHandler()`，:430-450）：

```java
String handlers = System.getProperty(PROTOCOL_HANDLER, "");        // java.protocol.handler.pkgs
System.setProperty(PROTOCOL_HANDLER,
    (handlers.isEmpty() ? "" : handlers + "|") + "org.springframework.boot.loader");
resetCachedUrlHandlers();       // URL.setURLStreamHandlerFactory(null) -> 清 JDK handler 缓存
```

原理：JDK `URL` 查找协议 handler 时会扫描 `java.protocol.handler.pkgs` 列出的包，对协议 `jar` 找 `<pkg>.jar.Handler`--loader 的类恰好位于 `org.springframework.boot.loader.jar.Handler`（**包名必须以 `.jar` 结尾、类必须叫 Handler**），追加后 JDK 命中它，从而**替换** `sun.net.www.protocol.jar.Handler`。`Handler.captureJarContextUrl()`（:414-440）在替换前先捕获一个仍指向原生 handler 的 URL 留作 fallback。exploded 模式全是 `file:` URL，无需注册。

**嵌套 URL 下钻**（`JarURLConnection.get(URL, JarFile)`，`JarURLConnection.java:244-267`）--多层嵌套的核心：

```java
while ((separator = spec.indexOf(SEPARATOR, index)) > 0) {      // 逐个 !/ 切分
    JarEntryName entryName = JarEntryName.get(spec.subSequence(index, separator));
    JarEntry jarEntry = jarFile.getJarEntry(entryName.toCharSequence());
    if (jarEntry == null) { return notFound(...); }
    jarFile = jarFile.getNestedJarFile(jarEntry);               // ★下钻到下一层嵌套 jar
    index = separator + SEPARATOR.length();
}
return new JarURLConnection(url, jarFile.getWrapper(), jarEntryName);  // 最后一段是 entry 名
```

即 `jar:file:/app.jar!/BOOT-INF/lib/a.jar!/com/Foo.class` 中 `!/` 分隔的每一段都当作上一层 jar 的 entry，逐层下钻。返回的连接包 `JarFileWrapper`（关闭时不误关共享的底层 JarFile）。`rootFileCache`（SoftReference）保证同一外层 jar 只被解析一次。

**fast-exception 优化**：类加载扫几百个 jar 找一个资源时，用静态预建的 `NOT_FOUND_CONNECTION`/`FILE_NOT_FOUND_EXCEPTION`（:42-46）+ `useFastExceptions` ThreadLocal，避免为每个 miss 构造带堆栈的异常。

### 8.6 fatjar 启动时序图

```mermaid
sequenceDiagram
    participant JVM as JVM
    participant JL as JarLauncher<br/>(系统类加载器)
    participant JF as loader JarFile
    participant LCL as LaunchedURLClassLoader
    participant APP as Start-Class

    JVM->>JL: 读 Main-Class 启动 main(args)
    JL->>JL: createArchive -> JarFileArchive(app.jar)
    JL->>JF: registerUrlProtocolHandler<br/>接管 jar: 协议
    JL->>JF: getNestedJarFile(BOOT-INF/classes)<br/>NESTED_DIRECTORY 虚拟 jar
    JL->>JF: getNestedJarFile(BOOT-INF/lib/*.jar)<br/>NESTED_JAR 字节区间<br/>校验 STORED 不压缩
    Note over JF: 零解压：RandomAccessData 视图<br/>+ 自解析 zip 中央目录
    JL->>LCL: new LaunchedURLClassLoader(urls, appClassLoader)
    JL->>JL: TCCL = LaunchedURLClassLoader
    JL->>APP: MainMethodRunner<br/>Class.forName(Start-Class, false, TCCL)
    APP->>LCL: 加载应用类
    LCL->>JF: findClass -> JarURLConnection 下钻多层 !/
    Note over LCL: definePackage 用嵌套 jar 的 MANIFEST<br/>fast-exception 跳过 miss 堆栈
    APP->>APP: main() -> SpringApplication.run()
    Note over APP: 此后走第二节启动流程
```

### 8.7 PropertiesLauncher 简述

外部化启动器（zip 默认 layout）：属性来自 `loader.properties`（`loader.config.location` 指定，查找 `file:${loader.home}/`、`classpath:`、`classpath:BOOT-INF/classes/`）。关键属性：`loader.main`（回退 Start-Class）、`loader.path`（外部 classpath，支持目录/jar/`jar!dir` 嵌套写法）、`loader.classLoader`（自定义类加载器类名）。BOOT-INF/classes、BOOT-INF/lib 始终会被加入。

### 8.8 设计精髓总结

Spring Boot loader **没有发明新的类加载协议**，而是：

1. **`RandomAccessData` 字节区间视图 + 自解析 zip 中央目录**，让嵌套 jar 可以像物理文件一样被随机访问（零解压、零临时文件、惰性单 entry 解压）；
2. 通过 `java.protocol.handler.pkgs` **接管 `jar:` 协议**，使这一能力对标准 `URLClassLoader` 完全透明；
3. `LaunchedURLClassLoader` 的 **definePackage 修正**（嵌套 MANIFEST 绑定到 Package）与 **fast-exception 优化**，保证与 JDK 工具链（`JarURLConnection`、`getResources`、包元数据、签名 API）的语义兼容；
4. 配合打包端的 **STORED 不压缩约定**、`classpath.idx` 顺序索引、`layers.idx` 分层索引与 `-Djarmode=layertools` 钩子，形成完整的可执行 jar 体系。

> 3.2 才将这些类迁移到 `org.springframework.boot.loader.launch` 等新包并 JPMS 模块化；3.0.0 仍是根包布局，`Handler` 所在包 `org.springframework.boot.loader.jar` 正好满足 JDK 协议 handler 命名约定。

---

## 九、Actuator 实现原理

> 路径约定：**ACT** = `spring-boot-project/spring-boot-actuator/.../boot/actuate`，**AUTO** = `spring-boot-project/spring-boot-actuator-autoconfigure/.../boot/actuate/autoconfigure`。
>
> 核心问题先答：**访问 `/actuator/health` 时，响应者不是用户的 Controller，而是一个 Spring MVC 自定义 `HandlerMapping`--`WebMvcEndpointHandlerMapping`（order=-100，优先于 `RequestMappingHandlerMapping` 的 0）**。它在初始化时把所有 `@Endpoint` bean 的操作方法动态注册为标准 `RequestMappingInfo`（如 `/actuator/health`），处理器是 Actuator 内部私有的迷你 handler `OperationHandler`（一个带 `@ResponseBody` 的 `handle()` 方法）；真正业务由 `ReflectiveOperationInvoker` 反射调用 `HealthEndpointWebExtension.health(...)` 完成；返回值实现 `OperationResponseBody` 标记接口，命中 3.0 新增的**隔离专用 ObjectMapper** 注册，由 `MappingJackson2HttpMessageConverter` 写出 JSON。

### 9.1 端点定义与发现机制

**注解层**（`ACT/endpoint/annotation/`）：

- `@Endpoint`（:52-69）：`id` + `enableByDefault`，"最低公分母"，可被 `@EndpointWebExtension`/`@EndpointJmxExtension` 按暴露技术扩展；
- `@ReadOperation/@WriteOperation/@DeleteOperation`（HTTP 动词 GET/POST/DELETE）+ `@Selector`（路径变量，`SINGLE` 映射 `{name}`，`ALL_REMAINING` 映射 `{*name}` 收 `String[]`）；
- `@EndpointWebExtension` = `@EndpointExtension(endpoint = X.class, filter = WebEndpointFilter.class)`--health 的**双轨制**：JMX 用 `HealthEndpoint` 本体，HTTP 用 `HealthEndpointWebExtension`。

**发现流程**（`ACT/endpoint/annotation/EndpointDiscoverer.java`，懒发现 + volatile 缓存）三步：

1. `createEndpointBeans()`（:128-141）：`beanNamesForAnnotationIncludingAncestors(context, Endpoint.class)` 按**注解**扫描容器（含父容器--独立管理端口的 child context 场景关键），同 id 重复直接失败；
2. `addExtensionBeans()`（:149-176）：扫描 `@EndpointExtension`，反查出被扩展端点的 id 后挂上；
3. `convertToEndpoints()`（:178-186）：`isEndpointExposed()` 执行**暴露过滤**（include/exclude 生效点）；`convertToEndpoint`（:188-204）收集本体操作后，对扩展 bean 以 `replaceLast=true` 再收集--**同 key 的本体操作被扩展操作顶掉**，这就是 `HealthEndpointWebExtension.health()` 替换 `HealthEndpoint.health()` 的机制；`OperationKey` 重复且未被替换则抛 `IllegalStateException`。

**方法 -> Operation**（`DiscoveredOperationsFactory.java:71-105`）：`MethodIntrospector.selectMethods` 枚举方法 -> 构造 `DiscoveredOperationMethod` -> `new ReflectiveOperationInvoker(target, method, parameterValueMapper)`（:91）-> `applyAdvisors()` 套用 `OperationInvokerAdvisor`（如 `CachingOperationInvokerAdvisor`，对应 `management.endpoint.<id>.cache.time-to-live` 读操作缓存）。

**WebOperationRequestPredicate 构造规则**（`RequestPredicateFactory.java:54-144`）--全部在**启动期**算好，请求期只做匹配：

| 谓词 | 规则 |
|---|---|
| path | rootPath + 每个 `@Selector` 追加 `/{参数名}`；`ALL_REMAINING` 为 `{*参数名}` |
| httpMethod | READ->GET，WRITE->POST，DELETE->DELETE |
| consumes | 仅 POST 且有非 @Selector 参数（请求体）时 `application/json` |
| produces | 显式声明 > 返回 `Resource` 时 `application/octet-stream` > 否则 `EndpointMediaTypes.getProduced()` |

`EndpointMediaTypes.DEFAULT`（`ACT/endpoint/web/EndpointMediaTypes.java:39-41`）= **`application/vnd.spring-boot.actuator.v3+json, application/vnd.spring-boot.actuator.v2+json, application/json`**--"为什么是 JSON"的第一层答案：produces 谓词本身就只声明 JSON 媒体类型。

### 9.2 注册为 HTTP 路由："谁响应"的装配侧

**装配入口**：`@ManagementContextConfiguration`（`AUTO/web/ManagementContextConfiguration.java:48`，带 ANY/SAME/CHILD 类型）的 10 个实现经 `ManagementContextConfiguration.imports` 导入--这套机制保证同端口与独立 management child context 两种形态下端点装配一致。

`AUTO/endpoint/web/servlet/WebMvcEndpointManagementContextConfiguration.java` 核心 bean `webEndpointServletHandlerMapping`（:83-100）：汇聚 `WebEndpointsSupplier`（@Endpoint）+ `ServletEndpointsSupplier` + `ControllerEndpointsSupplier` 三类端点，读 `management.endpoints.web.base-path`（默认 `/actuator`），构造 `EndpointLinksResolver`，`new WebMvcEndpointHandlerMapping(...)`。

`WebMvcEndpointHandlerMapping`（`ACT/endpoint/web/servlet/`）继承链：

```java
public class WebMvcEndpointHandlerMapping extends AbstractWebMvcEndpointHandlerMapping { ... }
// 构造器 setOrder(-100)                      WebMvcEndpointHandlerMapping.java:66-72

public abstract class AbstractWebMvcEndpointHandlerMapping
        extends RequestMappingInfoHandlerMapping implements InitializingBean { ... }
// 即：一个完整的标准 Spring MVC HandlerMapping        :90
```

**DispatcherServlet 如何发现它**：`DispatcherServlet.initHandlerMappings()`（Spring Framework）通过 `beansOfTypeIncludingAncestors(context, HandlerMapping.class)` 收集所有 HandlerMapping bean 并按 order 排序：

| HandlerMapping | order | 处理什么 |
|---|---|---|
| `WebMvcEndpointHandlerMapping` | **-100** | `/actuator/**` 端点 |
| `ControllerEndpointHandlerMapping` | -100 | `@ControllerEndpoint` |
| `RequestMappingHandlerMapping` | 0 | 用户 `@RequestMapping` |

`DispatcherServlet.getHandler()` 按 order 升序遍历，第一个命中者获胜--**用户在 `/actuator/xxx` 下自定义的 `@RequestMapping` 会被无声遮蔽**。

**注册流程**（`AbstractWebMvcEndpointHandlerMapping.java`）：

- `afterPropertiesSet()`（:142-147）-> `initHandlerMethods()`（:150-159）：遍历端点快照的每个 operation 调 `registerMappingForOperation`，`shouldRegisterLinksMapping` 时再注册根路径 links 映射。**不扫描容器 bean**（`isHandler()` 恒 false），端点集合是构造传入的快照；
- `registerMapping()`（:177-183）：`registerMapping(createRequestMappingInfo(predicate, path), new OperationHandler(servletWebOperation), this.handleMethod)`--`handleMethod` 是构造器（:103-104）预解析的 `OperationHandler#handle(HttpServletRequest, Map)` 反射引用；
- `createRequestMappingInfo()`（:198-203）：`RequestMappingInfo.paths(endpointMapping.createSubPath(path)).methods(GET).produces(三种JSON).build()`--**`/actuator/health` 的 RequestMappingInfo 就在这里诞生**。

**`/actuator` 根路径 links 响应**：`WebMvcLinksHandler.links()`（`WebMvcEndpointHandlerMapping.java:82-98`，`@ResponseBody`）调 `EndpointLinksResolver.resolveLinks(requestUrl)`（`web/EndpointLinksResolver.java:68-83`：先放 `self` 再遍历所有端点，`Link` 即 `{href, templated}`），以 `OperationResponseBody.of()` 包裹返回--`{"_links":{"self":...,"health":...}}` 的生产者就是它。

### 9.3 请求处理与 JSON 序列化：完整证据链

**OperationHandler 与调用链**（`AbstractWebMvcEndpointHandlerMapping.java:412-424, 301-322`）：

```java
private static final class OperationHandler {
    @ResponseBody
    Object handle(HttpServletRequest request, @RequestBody(required = false) Map<String, String> body) {
        return this.operation.handle(request, body);
    }
}
```

`ServletWebOperationAdapter.handle()`（:301-322）组装参数与上下文：

1. `getArguments()`（:329-353）：合并 URI 模板变量、`{*name}` 剩余路径段（路径 token 与匹配 pattern token 的差集）、POST body、query 参数；
2. 构造 `ServletSecurityContext`（:466-484，委托 `request.getUserPrincipal()`--health details 权限判断的数据源）、`ProducibleOperationArgumentResolver`（按 `Accept` 头解析 ApiVersion）、`WebServerNamespace` resolver；
3. `new InvocationContext(...)` -> `operation.invoke(context)`；
4. `handleResult()`（:373-386）：null -> GET 404 / 非 GET 204；**`WebEndpointResponse` -> `ResponseEntity.status(response.getStatus())`--HTTP 状态码透传点**。

invoke 链：`AbstractDiscoveredOperation.invoke`（`annotation/AbstractDiscoveredOperation.java:59-61`）-> `CachingOperationInvoker`（若配 TTL）-> **`ReflectiveOperationInvoker.invoke()`**（`invoke/reflect/ReflectiveOperationInvoker.java:69-107`）：

```java
public Object invoke(InvocationContext context) {
    validateRequiredParameters(context);               // @Nullable 缺失检查
    Object[] resolvedArguments = resolveArguments(context);  // 类型转换 + ApiVersion/SecurityContext 注入
    return ReflectionUtils.invokeMethod(method, this.target, resolvedArguments);  // target = 端点 bean
}
```

**"为什么一定是 JSON"--三层机制**：

1. **produces 谓词**（启动期）：`WebOperationRequestPredicate.produces` = 三种 JSON 媒体类型（9.1 节）；
2. **@ResponseBody**：`OperationHandler.handle` 的返回值走 `RequestResponseBodyMethodProcessor` -> message converter，而非视图渲染；
3. **隔离 ObjectMapper（3.0 新特性，决定性一层）**：
   - `ACT/endpoint/OperationResponseBody.java:31-43`：标记接口 + `of(Map)` 工厂；`HealthComponent`、`CompositeHealth`、`SystemHealth`、links 的 `OperationResponseBodyMap` 都实现它；
   - `AUTO/endpoint/jackson/JacksonEndpointAutoConfiguration.java:43-52`（`management.endpoints.jackson.isolated-object-mapper` 默认 true）：构造一个**与应用主 ObjectMapper 完全无关**的 mapper（禁用日期时间戳、`NON_NULL`）；
   - 接线：`EndpointObjectMapperWebMvcConfigurer`（`WebMvcEndpointManagementContextConfiguration.java:144-171`）实现 `configureMessageConverters`，对每个 `MappingJackson2HttpMessageConverter` 执行：

```java
converter.registerObjectMappersForType(OperationResponseBody.class, (associations) -> {
    MEDIA_TYPES.forEach((mimeType) -> associations.put(mimeType, this.endpointObjectMapper.get()));
});
```

   - 运行期命中（spring-web `AbstractJackson2HttpMessageConverter.selectObjectMapper`）：`OperationResponseBody.isAssignableFrom(targetType)` 命中则取隔离 mapper；**命中类型但媒体类型非 JSON 时直接返回 null（不可写）--从机制上排除了这些类型返回 HTML/其他格式的可能**。
   - 序列化注解塑造输出形态：`HealthComponent.getStatus()` 标 `@JsonUnwrapped`（status 展开为顶层 `"status":"UP"`）、`@JsonInclude(NON_EMPTY)`--所以默认（show-details=never）输出就是 `{"status":"UP"}`。

**WebFlux 对照**（`ACT/endpoint/web/reactive/AbstractWebFluxEndpointHandlerMapping.java`）：同样 order=-100、RequestMappingInfo 注册；阻塞 invoke 由 `ReactiveWebOperationAdapter` 包成 `Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())`（:235-248）避免阻塞事件循环；JSON 编码经 `ServerCodecConfigurer` 后置处理向 `Jackson2JsonEncoder` 注册同样的类型->mapper 映射。

### 9.4 /actuator/health 完整请求时序

```mermaid
sequenceDiagram
    participant B as 浏览器/curl
    participant T as 内嵌 Tomcat
    participant DS as DispatcherServlet
    participant HM as WebMvcEndpointHandlerMapping<br/>(order=-100)
    participant OH as OperationHandler
    participant IV as ReflectiveOperationInvoker
    participant HE as HealthEndpointWebExtension
    participant HI as 各 HealthIndicator
    participant MC as MessageConverter

    B->>T: GET /actuator/health
    T->>DS: FilterChain -> doDispatch()
    DS->>DS: getHandler: 遍历 HandlerMapping<br/>WebMvcEndpointHandlerMapping 命中<br/>(RequestMappingHandlerMapping 未轮到)
    DS->>HM: RequestMappingInfo(/actuator/health, GET, produces=JSON) 匹配
    HM-->>DS: OperationHandler#handle 的 HandlerMethod
    DS->>OH: RequestMappingHandlerAdapter 反射调用 handle(req, body)
    OH->>OH: ServletWebOperationAdapter<br/>组装 InvocationContext<br/>(参数 + SecurityContext + ApiVersion)
    OH->>IV: operation.invoke(context)
    IV->>HE: 反射调用 health(apiVersion, namespace, securityContext)
    Note over HE: HealthEndpointSupport.getHealth<br/>沿 path 下钻 HealthContributorRegistry<br/>聚合各指标
    HE->>HI: getHealth(includeDetails)<br/>db/diskSpace/redis...
    HI-->>HE: Health(status, details)
    HE->>HE: SimpleStatusAggregator 取最坏状态<br/>DOWN > OUT_OF_SERVICE > UP > UNKNOWN
    HE-->>OH: WebEndpointResponse(CompositeHealth, 200/503)
    OH-->>DS: ResponseEntity.status(...).body(CompositeHealth)
    DS->>MC: @ResponseBody -> RequestResponseBodyMethodProcessor
    Note over MC: CompositeHealth 实现 OperationResponseBody<br/>selectObjectMapper 命中隔离 ObjectMapper<br/>非 JSON 媒体类型不可写
    MC-->>T: ObjectMapper.writeValue -> JSON 字节
    T-->>B: 200 {"status":"UP",...}
```

### 9.5 health 端点深入

- **双轨制**：`HealthEndpoint`（`ACT/health/HealthEndpoint.java:41-42`，`@Endpoint(id="health")`）与 `HealthEndpointWebExtension`（:47-49，`@EndpointWebExtension`）**都继承 `HealthEndpointSupport`**--同一套聚合逻辑、不同方法签名；发现期 WebExtension 顶掉本体的同名操作，JMX discoverer 看不到 WebExtension（filter 是 `WebEndpointFilter`）；
- **注册表**：`DefaultHealthContributorRegistry` 以 bean 名为指标名注册 `HealthIndicator`（叶子）/`CompositeHealthContributor`（复合）；响应式指标经 `AdaptedReactiveHealthContributors` 以 `.block()` 适配；
- **聚合算法**（`HealthEndpointSupport.java:73-202`）：path 首段先匹配 group 名 -> `getAggregateContribution` 递归收集全部叶子指标（附慢指标告警，阈值 `management.endpoint.health.logging.slow-indicator-threshold`）-> `SimpleStatusAggregator`（默认顺序 `DOWN > OUT_OF_SERVICE > UP > UNKNOWN`，`management.endpoint.health.status.order` 可覆盖）**取最坏状态** -> `SystemHealth`/`CompositeHealth`；
- **状态码映射**（`HealthEndpointWebExtension.health`，:78-90）：`HttpCodeStatusMapper.getStatusCode(status)`（默认 `UP->200, DOWN/OUT_OF_SERVICE->503`，`management.endpoint.health.status.http-mapping.*` 可覆盖）-> `WebEndpointResponse(health, statusCode)`；
- **show-details 权限**：不在 Security filter 层，而在 `AutoConfiguredHealthEndpointGroup.showDetails(SecurityContext)`（`Show.NEVER/WHEN_AUTHORIZED/ALWAYS`，默认 NEVER）--未认证时 details/components 被裁掉，只剩 `{"status":"UP"}`；
- **Availability Probes**：`AvailabilityProbesAutoConfiguration`（:73-95）--显式设置 `management.endpoint.health.probes.enabled` 优先，否则 **CloudPlatform 为 KUBERNETES 才启用**。为什么 K8s 要单独的 `/actuator/health/liveness|readiness`：liveness 只应反映进程内部状态（避免下游 DB 抖动触发无意义的容器重启），readiness 反映是否可接流量；`LivenessStateHealthIndicator`/`ReadinessStateHealthIndicator` 的状态来自 `ApplicationAvailability`（即启动流程第 ⑮/⑱ 步发布的 `AvailabilityChangeEvent`，与第二节的启动流程呼应）；`probes.add-additional-paths=true` 时额外挂 `/livez`、`/readyz`。

### 9.6 暴露控制与独立管理端口

- **默认只有 `/actuator/health` 可 HTTP 访问**：`WebEndpointProperties.Exposure` 默认 include 为空，`IncludeExcludeEndpointFilter`（`AUTO/endpoint/expose/`，:108-139）回退到 `EndpointExposure.WEB.getDefaultIncludes()` = `"health"`；JMX 侧默认 `*` 全开；
- **enabled 与 exposed 是两道闸**：`management.endpoint.<id>.enabled`（`OnAvailableEndpointCondition` 评估）决定**端点 bean 是否存在**；exposure filter 决定**存在后是否暴露**；两者都过才注册路由；
- **`management.server.port` ≠ server.port 时**：`ChildManagementContextInitializer`（`AUTO/web/server/`，:58-133）监听主上下文的 `WebServerInitializedEvent`，以主上下文为 parent 创建 child context（id `parent:management`，serverNamespace=`management`）并 refresh--**child context 的 `onRefresh()` 创建第二个 Tomcat**（与第四节内嵌容器原理完全同构）；`beanNamesForAnnotationIncludingAncestors` 让 child 能看到主上下文的全部 `@Endpoint` bean。

### 9.7 端点机制架构图

```mermaid
flowchart TB
    subgraph DISCOVER["启动期：发现与装配"]
        A1["@Endpoint bean<br/>HealthEndpoint / InfoEndpoint / MetricsEndpoint ..."] --> A2["EndpointDiscoverer<br/>扫描注解 + 暴露过滤<br/>WebExtension 顶掉本体操作"]
        A2 --> A3["WebEndpointDiscoverer<br/>方法 -> DiscoveredWebOperation<br/>+ WebOperationRequestPredicate<br/>path/httpMethod/produces 启动期定死"]
        A3 --> A4["WebMvcEndpointManagementContextConfiguration<br/>new WebMvcEndpointHandlerMapping<br/>(order=-100) + EndpointLinksResolver"]
        A4 --> A5["afterPropertiesSet -> registerMapping<br/>RequestMappingInfo:/actuator/health GET produces JSON<br/>handler = OperationHandler#handle"]
        A5 --> A6["DispatcherServlet.initHandlerMappings<br/>收集全部 HandlerMapping 并按 order 排序"]
    end

    subgraph RUNTIME["请求期：路由与执行"]
        B1["Tomcat Connector"] --> B2["DispatcherServlet.doDispatch"]
        B2 --> B3["getHandler 遍历<br/>order=-100 的 actuator 映射<br/>先于 order=0 的用户映射"]
        B3 --> B4["RequestMappingHandlerAdapter<br/>反射调用 OperationHandler.handle<br/>@ResponseBody + @RequestBody Map"]
        B4 --> B5["ServletWebOperationAdapter<br/>组装 InvocationContext<br/>URI变量/路径段/body/query + SecurityContext"]
        B5 --> B6["CachingOperationInvoker 可选"]
        B6 --> B7["ReflectiveOperationInvoker<br/>ReflectionUtils.invokeMethod<br/>target = 端点/WebExtension bean"]
    end

    subgraph JSON["JSON 序列化层"]
        C1["返回值 implements OperationResponseBody<br/>CompositeHealth / OperationResponseBodyMap"]
        C2["EndpointObjectMapperWebMvcConfigurer<br/>registerObjectMappersForType"]
        C3["隔离 ObjectMapper 默认开启<br/>禁日期时间戳 + NON_NULL"]
        C4["selectObjectMapper isAssignableFrom 命中<br/>非 JSON 媒体类型不可写"]
        C2 --> C3
        C3 --> C4
    end

    DISCOVER --> RUNTIME --> JSON
```

### 9.8 其他端点与安全（速览）

| 端点 | 类 | 说明 |
|---|---|---|
| info | `info/InfoEndpoint` | 聚合 `InfoContributor`（build/git...） |
| metrics | `metrics/MetricsEndpoint` | `GET /actuator/metrics` 列名 + `GET /actuator/metrics/{name}?tag=a:b`，数据来自 Micrometer composite registry |
| env | `env/EnvironmentEndpoint` | 暴露 Environment 全部 PropertySource，值经 `Sanitizer` 脱敏（呼应第五节的配置优先级链） |
| beans / mappings / conditions | `BeansEndpoint` / `MappingsEndpoint` / `ConditionsReportEndpoint` | conditions 端点输出的正是第六节 `ConditionEvaluationReport` 的内容 |
| threaddump / heapdump | 同模式 | heapdump 返回 `Resource`（produces=octet-stream） |

- **JMX 侧**：`JmxEndpointDiscoverer` + `JmxEndpointExporter`（`afterPropertiesSet` 时 `MBeanServer.registerMBean`，响应同样经 Jackson JSON 化）；
- **安全**：`EndpointRequest.toAnyEndpoint()/to(X.class)/toLinks()`（`AUTO/security/servlet/EndpointRequest.java`）生成感知 exposure 配置的 Spring Security matcher（未暴露的端点不在 matcher 中）。

## 十、Redis 整合原理：spring-boot-starter-data-redis 的装配与通信

> 本章路径约定（在文首 `BOOT`/`AC` 基础上补充）：
> - `AUTO` = `spring-boot-project/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure`
> - `ACT` = `spring-boot-project/spring-boot-actuator/src/main/java/org/springframework/boot/actuate`
> - `ACTA` = `spring-boot-project/spring-boot-actuator-autoconfigure/src/main/java/org/springframework/boot/actuate/autoconfigure`
> - `SDR` = 外部依赖 `spring-data-redis`（Spring Boot 3.0.0 托管版本为 **Spring Data 2022.0.0 -> spring-data-redis 3.0**，见 `spring-boot-dependencies/build.gradle:1355`）
> - `LTT` = 外部依赖 `io.lettuce:lettuce-core`（托管版本 **6.2.1.RELEASE**，见 `spring-boot-dependencies/build.gradle:776`）
>
> Spring Boot 自身**不含任何 Redis 通信代码**--它只做"装配"（把 `spring.data.redis.*` 属性翻译成 Spring Data Redis 的连接工厂），真正的"通信"由 Spring Data Redis 的连接抽象 + Lettuce/Jedis 驱动完成。本章两条主线：**启动期装配**（10.1–10.6）与**运行期通信**（10.7–10.9）。

### 10.1 starter 的真相：一个没有任何代码的"依赖聚合器"

`spring-boot-starter-data-redis` 模块全部内容就是一个 `build.gradle`（`spring-boot-project/spring-boot-starters/spring-boot-starter-data-redis/build.gradle`）：

```gradle
description = "Starter for using Redis key-value data store with Spring Data Redis and the Lettuce client"

dependencies {
    api(project(":spring-boot-project:spring-boot-starters:spring-boot-starter"))
    api("org.springframework.data:spring-data-redis")
    api("io.lettuce:lettuce-core")
}
```

只有三行依赖，没有一行 Java 代码：

| 依赖 | 作用 |
|---|---|
| `spring-boot-starter` | 传递 `spring-boot`、`spring-boot-autoconfigure`、`spring-boot-starter-logging`--**把自动配置引擎带进来**（呼应第三章） |
| `spring-data-redis` | Spring Data Redis：`RedisTemplate`/`RedisConnectionFactory` 抽象层 + 自带 Jedis/Lettuce 集成代码 |
| `io.lettuce:lettuce-core` | 默认驱动（基于 Netty），**同时把 Netty 传递进来** |

**版本由谁定**：`spring-boot-dependencies/build.gradle` 的 BOM 机制统一托管--Lettuce `6.2.1.RELEASE`（:776），Spring Data BOM `2022.0.0`（:1355，其中 spring-data-redis 为 3.0）。这就是"引一个 starter 就能跑"的完整闭环：**starter 管依赖、autoconfigure 管装配、Spring Data 管抽象、Lettuce 管字节**。

**3.0 的破坏性变更**：配置前缀从 2.x 的 `spring.redis.*` 迁移为 `spring.data.redis.*`（`AUTO/data/redis/RedisProperties.java:35` 的 `@ConfigurationProperties(prefix = "spring.data.redis")`）--迁移到 3.0 后老前缀会**静默失效**（属性不报错，只是绑定不上，全部走默认值 `localhost:6379`）。

整套整合的分层架构：

```mermaid
flowchart TB
    subgraph User["应用代码层"]
        U1["@Autowired RedisTemplate / StringRedisTemplate<br/>redisTemplate.opsForValue().set(k, v)"]
        U2["RedisRepository（可选）"]
    end

    subgraph SDR["Spring Data Redis（抽象层）"]
        D1["RedisTemplate / ReactiveRedisTemplate<br/>模板方法 + 序列化器"]
        D2["RedisConnection 接口<br/>400+ 个命令方法<br/>（StandAlone/Sentinel/Cluster 统一）"]
        D3["RedisConnectionFactory<br/>LettuceConnectionFactory | JedisConnectionFactory"]
        D4["LettuceConnection | JedisConnection<br/>把 RedisConnection 命令翻译为驱动 API"]
    end

    subgraph Driver["驱动层（二选一）"]
        L1["Lettuce 6.2（默认）<br/>RedisClient -> StatefulRedisConnection<br/>sync/async/reactive 三种门面"]
        J1["Jedis<br/>JedisPool -> BinaryJedis"]
    end

    subgraph Boot["Spring Boot 3.0（装配层）"]
        B1["RedisAutoConfiguration<br/>+ Lettuce/JedisConnectionConfiguration<br/>（AUTO/data/redis/，本章主角）"]
        B2["RedisProperties<br/>spring.data.redis.*"]
    end

    subgraph Trans["传输层"]
        N1["Netty（Lettuce 自带）<br/>NIO EventLoop + RESP 编解码"]
        N2["阻塞 Socket + 手写流协议（Jedis）"]
    end

    R[("Redis Server<br/>RESP2/RESP3 协议")]

    U1 --> D1
    U2 --> D1
    B2 --> B1
    B1 -->|按条件二选一| D3
    D1 --> D2
    D2 --> D4
    D3 --> D4
    D4 --> L1
    D4 --> J1
    L1 --> N1
    J1 --> N2
    N1 --> R
    N2 --> R
```

关键认知：**Spring Boot 的 `RedisAutoConfiguration` 只负责"造出 `LettuceConnectionFactory` 和两个模板 bean"，一条 Redis 字节都没碰**；`RedisTemplate` 执行命令时才会向工厂要连接、驱动才会建 TCP。整个整合是"启动期纯装配 + 运行期惰性连接"。

### 10.2 自动配置的注册与触发：三个 imports + 两个 @Import

`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`（3.0 新注册机制，见第三章）中注册了三行（:37-39）：

```
org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration
org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration
org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration
```

再加上 Actuator 侧的 `RedisHealthContributorAutoConfiguration`（`ACTA/data/redis/`），共四个入口。装配的开关与骨架（`AUTO/data/redis/RedisAutoConfiguration.java:46-49`）：

```java
@AutoConfiguration
@ConditionalOnClass(RedisOperations.class)          // ① classpath 闸门：引了 spring-data-redis 才生效
@EnableConfigurationProperties(RedisProperties.class) // ② 绑定 spring.data.redis.*
@Import({ LettuceConnectionConfiguration.class, JedisConnectionConfiguration.class }) // ③ 客户端二选一
public class RedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "redisTemplate")
    @ConditionalOnSingleCandidate(RedisConnectionFactory.class) // ④ 有且仅有一个连接工厂时才给模板
    public RedisTemplate<Object, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) { ... }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnSingleCandidate(RedisConnectionFactory.class)
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) { ... }
}
```

四个条件的语义（呼应第六章条件注解原理）：

- **① `@ConditionalOnClass(RedisOperations.class)`**：starter 引入 spring-data-redis 后成立。这是"引依赖即生效"的第一道闸门--没引 starter，整个 Redis 装配直接跳过，`--debug` 的条件报告里是 negative match；
- **③ `@Import` 两个连接配置**：注意这**不是**二选一的互斥发生在 `RedisAutoConfiguration` 上，而是下放给两个子配置各自的 `@ConditionalOnProperty(spring.data.redis.client-type)` + `@ConditionalOnClass` 完成（见 10.5）；
- **④ `@ConditionalOnSingleCandidate(RedisConnectionFactory.class)`**：只有在"恰好一个（或一个 Primary）连接工厂"时才自动配模板。用户自定义第二个 `RedisConnectionFactory`（多 Redis 数据源场景）后，Boot 的模板自动装配会**整体退出**，把模板的定义权完全交给用户--这是"用户配置优先"的又一处体现（同 `OnBeanCondition` 的 no-bean 语义，见 6.4）；
- **② `@ConditionalOnMissingBean(name = "redisTemplate")`**：按 **bean 名字**匹配。用户自己定义了名为 `redisTemplate` 的 bean，Boot 的同名 bean 不再注册。

### 10.3 RedisProperties：spring.data.redis.* 全量属性

`RedisProperties`（`AUTO/data/redis/RedisProperties.java`）是一个纯 POJO 属性类（非 record，因含大量可空嵌套结构），层级与默认值：

| 属性 | 默认值 | 行号 | 说明 |
|---|---|---|---|
| `host` | `localhost` | :52 | 单机地址 |
| `port` | `6379` | :67 | 单机端口 |
| `database` | `0` | :41 | SELECT 的 db index |
| `username` / `password` | null | :57/:62 | Redis 6 的 ACL 用户名 + 密码 |
| `url` | null | :47 | `redis://user:pass@host:port`，**覆盖** host/port/password（见 10.4） |
| `ssl` | false | :72 | 开启 TLS |
| `timeout` | null | :77 | **命令读超时**（发给 Lettuce 的 commandTimeout） |
| `connect-timeout` | null | :82 | TCP 建连超时（Lettuce 的 SocketOptions.connectTimeout） |
| `client-name` | null | :87 | `CLIENT SETNAME`，排查连接归属时有用 |
| `client-type` | null | :92 | 显式指定 `lettuce` / `jedis`，不配则按 classpath 自动裁决 |
| `sentinel.*` | - | :363-417 | master、nodes、**独立的 sentinel username/password**（:376-383，sentinel 与数据节点的密码可以不同） |
| `cluster.*` | - | :328-358 | nodes、max-redirects（MOVED/ASK 重定向上限） |
| `lettuce.pool.*` | - | :448 | max-active=8 / max-idle=8 / min-idle=0 / max-wait=-1 |
| `lettuce.shutdown-timeout` | 100ms | :443 | 优雅停机等待时间 |
| `lettuce.cluster.refresh.*` | - | :476-520 | 拓扑刷新：period（定时）、adaptive（事件触发）、dynamic-refresh-sources=true |
| `jedis.pool.*` | - | :427 | 同 Pool |

两个容易忽略的细节：

- **`Pool.enabled` 是 `Boolean` 而非 `boolean`**（:241）：三态。未配置时 `RedisConnectionConfiguration.isPoolEnabled()`（:138-141）回退为"`commons-pool2` 是否在 classpath"--**只引 starter、不显式加 `commons-pool2` 依赖时，Lettuce 根本不建池**（见 10.9，这不影响默认性能，因为 Lettuce 默认共享单连接）；
- **Lettuce 的 `cluster.refresh` 只在 `lettuce` 侧存在**（:468-522），Jedis 没有对应项--集群拓扑自动刷新是 Lettuce 驱动的能力，属性类的形状本身就反映了两个驱动的功能差距。

### 10.4 三态拓扑 + URL 解析：RedisConnectionConfiguration 基类

`LettuceConnectionConfiguration` 与 `JedisConnectionConfiguration` 的公共逻辑抽在抽象基类 `RedisConnectionConfiguration`（`AUTO/data/redis/RedisConnectionConfiguration.java`）。它构造时注入三个 `ObjectProvider`（:56-64）：

```java
protected RedisConnectionConfiguration(RedisProperties properties,
        ObjectProvider<RedisStandaloneConfiguration> standaloneConfigurationProvider,
        ObjectProvider<RedisSentinelConfiguration> sentinelConfigurationProvider,
        ObjectProvider<RedisClusterConfiguration> clusterConfigurationProvider) {
    this.properties = properties;
    this.standaloneConfiguration = standaloneConfigurationProvider.getIfAvailable();
    this.sentinelConfiguration = sentinelConfigurationProvider.getIfAvailable();
    this.clusterConfiguration = clusterConfigurationProvider.getIfAvailable();
}
```

这是 Spring Boot Redis 装配的**第一个扩展点**：用户可以直接声明 `RedisSentinelConfiguration`/`RedisClusterConfiguration`/`RedisStandaloneConfiguration` bean（注意是 Spring Data 类型，不是属性），它们**优先级高于一切属性配置**--三个 getter（:66-132）的第一行都是 `if (this.xxxConfiguration != null) return ...`。

**拓扑裁决优先级**（每个连接配置类里都遵循同一顺序，见 `LettuceConnectionConfiguration.createLettuceConnectionFactory` :86-94）：

1. 用户自定义 `RedisSentinelConfiguration` bean，或配置了 `spring.data.redis.sentinel.*` -> **Sentinel 模式**；
2. 否则用户自定义 `RedisClusterConfiguration` bean，或配置了 `cluster.nodes` -> **Cluster 模式**；
3. 否则 -> **Standalone 模式**（`getStandaloneConfig()` :66-86 一定返回非 null，作为兜底）。

属性 -> Spring Data 配置对象的翻译（`getStandaloneConfig` :71-84）：若配了 `url` 则用 URL 解析结果覆盖 host/port/username/password；否则取离散属性；`database` 总是生效。

**URL 解析 `parseUrl`（:156-182）**只接受 `redis://` 与 `rediss://`（TLS）两种 scheme，其余抛 `RedisUrlSyntaxException`。这个异常并不普通--它有专属的**失败分析器** `RedisUrlSyntaxFailureAnalyzer`（通过 `spring.factories:26` 注册，呼应第二章启动流程里的 `FailureAnalyzers`），把晦涩的异常翻译成人话：

- 配了 `redis-sentinel://...` -> *"Use spring.data.redis.sentinel properties instead of spring.data.redis.url"*；
- 配了 `redis-socket://...` -> *"Configure the appropriate Spring Data Redis connection beans directly"*；
- 其他 scheme -> *"Use the scheme 'redis://' for insecure or 'rediss://' for secure"*。

URL 中的 userinfo 解析（:166-176）：`user:pass` 拆成 username+password，只有 `pass` 则视为"旧式只有密码"。`ConnectionInfo`（:184-221）内部类把 `useSsl` 也带出来--注意 **URL 的信息被拆到两处用**：地址部分进了 `RedisStandaloneConfiguration`，而 `useSsl` 在 `LettuceConnectionConfiguration.customizeConfigurationFromUrl`（:163-168）单独补到客户端配置上（`builder.useSsl()`），因为 SSL 属于"客户端行为"而非"拓扑"。

拓扑裁决决策图：

```mermaid
flowchart TB
    P["spring.data.redis.* 属性<br/>+ 用户自定义 Configuration bean"] --> S1{"用户定义了<br/>RedisSentinelConfiguration bean<br/>或配置 sentinel.master/nodes?"}
    S1 -->|是| SEN["Sentinel 模式<br/>RedisSentinelConfiguration<br/>master + nodes + 独立 sentinel 账号"]
    S1 -->|否| S2{"用户定义了<br/>RedisClusterConfiguration bean<br/>或配置 cluster.nodes?"}
    S2 -->|是| CLU["Cluster 模式<br/>RedisClusterConfiguration<br/>nodes + max-redirects"]
    S2 -->|否| ST["Standalone 模式（兜底）<br/>RedisStandaloneConfiguration"]
    P --> U{"配置了 url?"}
    U -->|"redis://user:pass@h:p"| UR["parseUrl 覆盖<br/>host/port/username/password"]
    U -->|"rediss://..."| URS["同上 + useSsl"]
    U -->|未配置| DIS["离散属性<br/>host/port/username/password/database"]
    UR --> ST
    URS --> ST
    DIS --> ST
    SEN --> D["驱动配置<br/>LettuceClientConfiguration /<br/>JedisClientConfiguration<br/>（超时/SSL/池/自定义）"]
    CLU --> D
    ST --> D
    D --> F["new LettuceConnectionFactory(拓扑, 客户端配置)<br/>new JedisConnectionFactory(拓扑, 客户端配置)"]
```

### 10.5 客户端二选一：LettuceConnectionConfiguration 逐行剖析

两个客户端配置是**互斥兜底**关系（`AUTO/data/redis/LettuceConnectionConfiguration.java:56-59`）：

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(RedisClient.class)                                              // A
@ConditionalOnProperty(name = "spring.data.redis.client-type",
                       havingValue = "lettuce", matchIfMissing = true)               // B
class LettuceConnectionConfiguration extends RedisConnectionConfiguration {
```

Jedis 侧（`JedisConnectionConfiguration.java:46-50`）则多一个条件：

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({ GenericObjectPool.class, JedisConnection.class, Jedis.class }) // A'
@ConditionalOnMissingBean(RedisConnectionFactory.class)                             // C
@ConditionalOnProperty(name = "spring.data.redis.client-type",
                       havingValue = "jedis", matchIfMissing = true)                 // B'
class JedisConnectionConfiguration extends RedisConnectionConfiguration {
```

**自动裁决规则**（不配 `client-type` 时，B 与 B' 的 `matchIfMissing=true` 都成立，靠其余条件区分）：

- 默认 starter 只带 Lettuce：`A` 成立、`A'` 因 classpath 没有 `Jedis` 类不成立 -> **只有 Lettuce 装配**；
- 额外引入 `jedis` 依赖：两个类的类条件都成立，但 **`C`（Jedis 类级 `@ConditionalOnMissingBean(RedisConnectionFactory)`）成为裁决者**--`@Import` 的顺序是 `LettuceConnectionConfiguration` 在前（`RedisAutoConfiguration.java:49`），**先注册的 Lettuce 工厂让后评估的 Jedis 整体跳过**；
- 显式配 `spring.data.redis.client-type=jedis`：B 不成立、B' 成立 -> 强制 Jedis（前提是引了 jedis + commons-pool2 依赖）；
- `@Configuration(proxyBeanMethods = false)`：无 proxybean 方法互调，跳过 CGLIB 代理，启动更快（与第三章的启动优化一致）。

**bean 一：`lettuceClientResources`（:68-74）**--Lettuce 的全局共享资源（EventLoopGroup、定时器、netty 基础设施）：

```java
@Bean(destroyMethod = "shutdown")                       // 容器关闭时优雅关停线程池
@ConditionalOnMissingBean(ClientResources.class)
DefaultClientResources lettuceClientResources(ObjectProvider<ClientResourcesBuilderCustomizer> customizers) {
    DefaultClientResources.Builder builder = DefaultClientResources.builder();
    customizers.orderedStream().forEach((customizer) -> customizer.customize(builder));
    return builder.build();
}
```

单独成 bean 的意义：**`ClientResources` 被设计为跨客户端实例共享**（EventLoopGroup 很贵），暴露成 bean 后，用户的其他 Lettuce 客户端（如自建的 RedisClient）可以复用同一组 IO 线程；同时开放了 `ClientResourcesBuilderCustomizer` SPI（本包内函数式接口）供定制 IO 线程数、command buffer 等。

**bean 二：`redisConnectionFactory`（:76-84）**，装配主流程 `getLettuceClientConfiguration`（:96-108）是一个严格的 builder 流水线：

```java
private LettuceClientConfiguration getLettuceClientConfiguration(...) {
    LettuceClientConfigurationBuilder builder = createBuilder(pool);        // ① 池化 or 普通 builder
    applyProperties(builder);                                               // ② ssl/timeout/shutdownTimeout/clientName
    if (StringUtils.hasText(getProperties().getUrl())) {
        customizeConfigurationFromUrl(builder);                             // ③ URL 的 rediss:// -> useSsl()
    }
    builder.clientOptions(createClientOptions());                           // ④ ClientOptions（超时/集群拓扑）
    builder.clientResources(clientResources);                               // ⑤ 挂全局共享资源
    builderCustomizers.orderedStream().forEach((customizer) -> customizer.customize(builder)); // ⑥ 用户 SPI
    return builder.build();
}
```

**④ `createClientOptions`（:137-144）值得细看**：

```java
private ClientOptions createClientOptions() {
    ClientOptions.Builder builder = initializeClientOptionsBuilder();
    Duration connectTimeout = getProperties().getConnectTimeout();
    if (connectTimeout != null) {
        builder.socketOptions(SocketOptions.builder().connectTimeout(connectTimeout).build());
    }
    return builder.timeoutOptions(TimeoutOptions.enabled()).build();  // ← 无条件启用
}
```

`TimeoutOptions.enabled()` 被无条件打开--**命令超时（`spring.data.redis.timeout`）对 Lettuce 是强制语义**：超时命令会抛 `RedisCommandTimeoutException` 而不是无限挂起。集群模式下（`initializeClientOptionsBuilder` :146-161）builder 换成 `ClusterClientOptions`，并把 `lettuce.cluster.refresh.*` 翻译成 `ClusterTopologyRefreshOptions`：`period`（定时全量刷新拓扑）、`adaptive`（对 MOVED/ASK/PERSISTENT 重定向等事件**即时**刷新）、`dynamicRefreshSources`（是否把发现的全部节点都作为拓扑源）--这决定了集群故障转移后客户端多久能自愈。

**① 池化的可选依赖隔离**--`PoolBuilderFactory`（:170-193）是整个文件最精巧的设计：

```java
private LettuceClientConfigurationBuilder createBuilder(Pool pool) {
    if (isPoolEnabled(pool)) {
        return new PoolBuilderFactory().createBuilder(pool);   // 内部类里才引用 commons-pool2 类型
    }
    return LettuceClientConfiguration.builder();
}

private static class PoolBuilderFactory {                      // 隔离 commons-pool2
    LettuceClientConfigurationBuilder createBuilder(Pool properties) {
        return LettucePoolingClientConfiguration.builder().poolConfig(getPoolConfig(properties));
    }
    private GenericObjectPoolConfig<?> getPoolConfig(Pool properties) { ... }
}
```

只有走到 `isPoolEnabled` 为 true 的分支，`GenericObjectPoolConfig`（commons-pool2 类型）才会被加载--只要不配池，`commons-pool2` 缺失也不会 `NoClassDefFoundError`。这和第六章 6.3 的预过滤思想同源：**可选依赖必须在"确保存在"的分支内才触碰**。

**Jedis 对照**（`JedisConnectionConfiguration.java`）：装配逻辑同构（`getJedisClientConfiguration` :77-89），差异有三--① 必须 classpath 有 `GenericObjectPool`（Jedis 无"共享连接"模式，**池是硬需求**）；② 属性映射用的是声明式 `PropertyMapper`（:91-98）而非 Lettuce 的手写 if 链--`map.from(getProperties().getTimeout()).to(builder::readTimeout)`；③ 没有 ClientResources 等价物（Jedis 是传统阻塞 BIO 客户端，无共享事件循环概念）。

### 10.6 模板与 Reactive、Repositories 的装配收尾

**模板 bean**（`RedisAutoConfiguration.java:52-66`）：`redisTemplate` 是 `RedisTemplate<Object,Object>`--**默认 JDK 序列化**（key/value 都是 `JdkSerializationRedisSerializer`），这是"存进去的 key 在 redis-cli 里看到的是乱码引号串"的根源；`stringRedisTemplate` 则预置 `StringRedisSerializer`。注意两者都只注入 `RedisConnectionFactory`，**不配置序列化器以外的任何东西**--所以生产实践几乎必然要自己覆盖 `redisTemplate`（配 Jackson/GenericJackson2JsonRedisSerializer），而 `@ConditionalOnMissingBean(name = "redisTemplate")` 保证了覆盖无冲突。

**响应式**（`AUTO/data/redis/RedisReactiveAutoConfiguration.java:42-65`）：`@ConditionalOnClass({ReactiveRedisConnectionFactory, ReactiveRedisTemplate, Flux.class})` + `@ConditionalOnBean(ReactiveRedisConnectionFactory.class)`--`LettuceConnectionFactory` 同时实现了 `RedisConnectionFactory` 与 `ReactiveRedisConnectionFactory`，所以 WebFlux 应用零额外配置即可拿到 `ReactiveRedisTemplate`。坑点：它的默认序列化上下文也是 **JDK 序列化**（:51-55 显式 `new JdkSerializationRedisSerializer(resourceLoader.getClassLoader())` 并塞满 key/value/hashKey/hashValue 四个槽位），连 `StringRedisTemplate` 对应的便利都没有--响应式场景务必自定义。

**Repositories**（`AUTO/data/redis/RedisRepositoriesAutoConfiguration.java:39-46`）：类体为空，全部语义在注解上--`@ConditionalOnClass(EnableRedisRepositories.class)` + `@ConditionalOnBean(RedisConnectionFactory.class)` + `@Import(RedisRepositoriesRegistrar.class)`。`RedisRepositoriesRegistrar` 是 `ImportBeanDefinitionRegistrar`（同 MyBatis 的 `@Mapper` 注册器模式），等价于自动帮你打 `@EnableRedisRepositories`：扫描 repository 接口，为每个 `XxxRepository` 注册一个 `RedisRepositoryFactoryBean`，由 SDR 生成代理实现（实体 id 作 key，`@RedisHash` 注解定前缀，二级索引靠 `SADD`/`SINTER`）。开关：`spring.data.redis.repositories.enabled=false`。

### 10.7 运行期（一）：RedisTemplate 执行一条命令的全链路

装配完成后的静态图景：容器里躺着 `LettuceConnectionFactory`（未连接）+ `redisTemplate` + `stringRedisTemplate`。**第一条命令发出时**才发生真实交互（以 `redisTemplate.opsForValue().set("k","v")` 为例）：

**第 1 步：模板方法入口**。`opsForValue()` 返回 `ValueOperations`（绑定到当前 template），`set` 最终进入 `RedisTemplate.execute(RedisCallback, ...)`--`RedisTemplate`（SDR）的核心骨架（经 `javap` 验证的方法族：`execute(RedisCallback)` / `execute(RedisCallback, boolean, boolean)` / `execute(SessionCallback)` / `executePipelined` / `executeWithStickyConnection`）：

- `RedisConnectionUtils.getConnection(factory)` 获取连接：非事务/非管道时直接 `factory.getConnection()`；
- 把连接包成**代理**（`createRedisConnectionProxy`）：事务中暂存到 `ThreadLocal`（`RedisSynchronizationManager`），保证 `MULTI...EXEC` 内多条命令走同一连接；
- 回调 `doInRedis(connection)` 里各 ops 调 `connection.set(rawKey, rawValue)`--**序列化发生在这一步之前**：`rawKey = keySerializer.serialize(key)`，`rawValue = valueSerializer.serialize(value)`；
- finally `releaseConnection`（非事务、非独占时归还/共享）。

**第 2 步：工厂交付连接**。`LettuceConnectionFactory.afterPropertiesSet()`（SDR，bean 初始化阶段被容器调用）做一次性初始化：按拓扑创建 `RedisClient`（standalone）/`RedisClusterClient`（cluster），并依据 `clientConfiguration` 决定连接提供者（`ExceptionTranslatingConnectionProvider` 包装异常翻译；池化时再包一层 commons-pool2）。运行期 `getConnection()` 的关键分叉是 **`shareNativeConnection`（默认 true）**：

- **共享模式（默认）**：惰性创建**一条**底层 `StatefulRedisConnection<byte[],byte[]>`（SDR 的 `SharedConnection` 内部类持有，`getSharedConnection()` 可见），所有线程的所有命令复用它--`RedisConnection` 只是这条 TCP 连接上的一个**无状态门面**，每次 `getConnection()` 代价近似为零；
- **独占连接的例外**：事务（MULTI）、管道（pipelined）、阻塞命令（BLPOP）会**绕开共享连接**，取一条专用连接（从池借或新建），用完归还--避免一条阻塞命令卡死全局唯一的连接；
- **池化模式**：若装配时走了 `LettucePoolingClientConfiguration`（见 10.5 ①），`getConnection()` = `pool.borrowObject()`，连接被 `LettuceConnection` 包裹，`close()` -> `returnObject()`。

**第 3 步：命令翻译**。`LettuceConnection.set(byte[] key, byte[] value)`（SDR）调用 `connection.sync().set(key, value)`--即底层 Lettuce 的同步 API。

```mermaid
sequenceDiagram
    autonumber
    participant C as 业务代码
    participant T as RedisTemplate
    participant S as keySerializer/valueSerializer
    participant RC as RedisConnectionUtils
    participant F as LettuceConnectionFactory
    participant LC as LettuceConnection
    participant LK as StatefulRedisConnection<br/>(共享, Netty)
    participant N as Netty Channel
    participant R as Redis Server

    C->>T: opsForValue().set("k", "v")
    T->>S: serialize(key) / serialize(value)
    Note over S: JDK 序列化(默认)或 String/Jackson
    T->>RC: getConnection(factory)
    alt 事务/管道/阻塞命令
        RC->>F: 专用连接(池借/新建)
    else 普通命令(默认)
        RC->>F: getConnection()
        F-->>RC: 共享 native 连接的门面<br/>(近似零成本)
    end
    RC-->>T: RedisConnection 代理
    T->>LC: connection.set(rawKey, rawValue)
    LC->>LK: sync().set(...) -> async().set(...)
    Note over LK: 命令写入 Channel outbound buffer<br/>登记到该命令的完成句柄
    LK->>N: channel.writeAndFlush(outbound)
    N->>R: RESP 编码字节流<br/>*3 $3 SET $1 k $1 v (RESP2)
    R-->>N: 响应字节流 (+OK)
    N->>LK: RedisStateMachine 解码 -> 完成 Promise
    LK-->>LC: future.get(commandTimeout) 返回<br/>(TimeoutOptions 超时保护)
    LC-->>T: 命令结果
    T->>RC: releaseConnection (共享连接: 无操作)
    T-->>C: 完成
```

### 10.8 运行期（二）：Lettuce 内部--一条 TCP 连接跑出高并发

Lettuce 6.x（`LTT`）的线程模型是理解"为什么默认不需要连接池"的钥匙：

- **`RedisClient.connect()`**（由 SDR 工厂首次需要连接时调用）：基于 Netty `Bootstrap` 建立 TCP（TLS 则加 SslHandler），pipeline 里装上编解码器--发送侧把命令对象编码为 RESP（REdis Serialization Protocol）字节，接收侧 `RedisStateMachine` 增量解码 RESP 响应；
- **`StatefulRedisConnection` 是线程安全的单连接多路复用**：任意多线程并发调用其 API，命令只是被**追加进 netty 发送缓冲区**（天然串行化到 socket），每条命令带一个完成句柄（`RedisFuture`，底层是 netty `Promise`/`CompletableFuture`）；
- **三种门面同一连接**：`connection.sync()` 返回的同步 `RedisCommands` 其实是**对 async 命令的动态代理**（JDK proxy + `future.get(commandTimeout)`），`connection.async()` 返回 `RedisAsyncCommands`（`RedisFuture`），`connection.reactive()` 返回 `RedisReactiveCommands`（`Flux/Mono`，把 future 适配为 Reactive Streams）--这就是 SDR 的 `ReactiveRedisTemplate` 能零成本切到响应式的底层原因（呼应 10.6）；
- **超时闭环**：10.5 的 `TimeoutOptions.enabled()` 在这一层生效--同步门面的 `get(timeout)`、异步的 future 超时，超时抛 `RedisCommandTimeoutException`；
- **`ClientResources` 共享**（10.5 bean 一）：所有连接的 IO 读写都在同一组 `EventLoopGroup`（CPU 核数相关的共享线程池）上执行--多客户端、多连接复用一套 reactor 线程；
- **断线自动重连**：`ConnectionWatchdog`（netty handler）在连接断开后自动重连并重置连接状态，配合 SDR 共享连接的重建，应用层通常无感。

与 Jedis 的本质差异一览：

| 维度 | Lettuce（默认） | Jedis |
|---|---|---|
| IO 模型 | Netty NIO，非阻塞 | 阻塞 socket + 手写流读写 |
| 连接共享 | 单连接天然线程安全，多路复用 | `Jedis` 实例非线程安全，**必须池化** |
| 连接池 | 可选（commons-pool2 在 classpath 才启用） | 必须（类级 `@ConditionalOnClass(GenericObjectPool)`） |
| 响应式 | 原生 `RedisReactiveCommands` | 无 |
| 集群拓扑刷新 | `ClusterTopologyRefreshOptions`（定时+自适应） | 无对应配置 |
| 同步/异步 | sync/async/reactive 三门面 | 仅同步 |

### 10.9 连接管理全景：共享连接 vs 池化

把 10.5（装配决定）与 10.7（运行时行为）合起来看连接的归属：

```mermaid
flowchart TB
    subgraph Assemble["启动期装配决定模式(AUTO/data/redis/LettuceConnectionConfiguration)"]
        A1{"commons-pool2 在 classpath?<br/>(或 pool.enabled 显式 true)"}
        A1 -->|否| SH["LettuceClientConfiguration(非池化)<br/>shareNativeConnection=true"]
        A1 -->|是| PL["LettucePoolingClientConfiguration<br/>GenericObjectPoolConfig<br/>(max-active/max-idle/minIdle/maxWait)"]
    end

    subgraph Runtime["运行期 getConnection() 分叉"]
        SH --> R1{"事务/管道/阻塞命令?"}
        R1 -->|否| S1["共享 StatefulRedisConnection<br/>全应用一条 TCP 连接<br/>多线程多路复用"]
        R1 -->|是| S2["临时专用连接<br/>(用完即关)"]
        PL --> P1["pool.borrowObject()<br/>LettuceConnection 持有独占连接"]
        P1 --> P2["connection.close()<br/>-> pool.returnObject()"]
    end

    S1 --> NET["Netty EventLoopGroup<br/>(ClientResources 共享)<br/>容器关闭 destroyMethod=shutdown"]
    S2 --> NET
    P1 --> NET
```

实践要点：**Lettuce 默认（无池）共享单连接对绝大多数场景就是最优解**--Redis 单连接吞吐瓶颈远低于应用层；需要池化的典型理由是大量阻塞命令（BLPOP/BLMOVE）或长事务。配了 `lettuce.pool.*` 却没引 `commons-pool2` 依赖是常见事故：池开关静默不生效。

### 10.10 与 Actuator 的联动：redis 健康指示器

`ACTA/data/redis/RedisHealthContributorAutoConfiguration.java:36-52` 把 Redis 接进第九章的 health 体系：

- `@AutoConfiguration(after = RedisAutoConfiguration.class)` + `@ConditionalOnBean(RedisConnectionFactory.class)`--**有连接工厂才有 redis 指标**（工厂又以 classpath 上有 SDR 为前提，条件链条环环相扣）；
- `@ConditionalOnEnabledHealthIndicator("redis")`--受 `management.health.redis.enabled` 控制；
- bean 方法注入 `Map<String, RedisConnectionFactory>`：**多数据源时每个工厂各生成一个 `RedisHealthIndicator`，组装成 `CompositeHealthContributor`**（`createContributor`，继承 `CompositeHealthContributorConfiguration`，与 db 指标的多数据源处理同构，见 9.5 的聚合算法）；
- `RedisHealthIndicator.doHealthCheck()`（`ACT/data/redis/`）：执行 `connection.info()`（取 server 版本）+ `dbSize()`，连接失败 -> DOWN（最终被 `SimpleStatusAggregator` 聚合为整体 503，见 9.4/9.5）。

### 10.11 Redis 整合启动全链路时序图

```mermaid
sequenceDiagram
    autonumber
    participant R as SpringApplication.run()
    participant AC as AutoConfigurationImportSelector
    participant RA as RedisAutoConfiguration
    participant LC as LettuceConnectionConfiguration
    participant JC as JedisConnectionConfiguration
    participant CTX as BeanFactory 容器
    participant F as LettuceConnectionFactory
    participant APP as 业务代码

    R->>AC: 处理 @EnableAutoConfiguration
    AC->>AC: 读 AutoConfiguration.imports<br/>发现 Redis* 三入口
    AC->>RA: OnClassCondition: RedisOperations ✓<br/>(引了 starter) -> 保留
    RA->>LC: @Import 评估
    Note over LC: OnClassCondition: RedisClient ✓<br/>OnProperty(client-type) 缺省匹配 ✓
    LC->>JC: 接力评估
    Note over JC: OnClassCondition: Jedis 不在 classpath ✗<br/>(或 Lettuce 工厂已注册) -> 跳过
    LC->>CTX: 注册 lettuceClientResources<br/>(destroyMethod=shutdown)
    LC->>LC: getLettuceClientConfiguration:<br/>池判定->属性->URL->ClientOptions->customizers
    LC->>CTX: 注册 redisConnectionFactory bean 定义<br/>(此时尚未建连!)
    RA->>CTX: redisTemplate / stringRedisTemplate<br/>(OnMissingBean + OnSingleCandidate)
    RA->>CTX: Reactive 模板 / Repositories 注册器
    CTX->>F: 初始化: afterPropertiesSet<br/>创建 RedisClient(仍未建 TCP)
    CTX->>APP: 注入 RedisTemplate
    APP->>F: 首条命令: getConnection()
    F->>F: 共享连接惰性建立<br/>RedisClient.connect() -> Netty TCP
    APP->>APP: 后续命令复用该连接
    Note over CTX: 容器关闭: destroy()<br/>-> ClientResources.shutdown()
```

### 10.12 装配全景架构图（本章总结图）

```mermaid
flowchart TB
    subgraph Reg["注册与触发"]
        T1["AutoConfiguration.imports:37-39<br/>Redis/Reactive/Repositories 三入口"] --> T2["RedisAutoConfiguration<br/>@ConditionalOnClass(RedisOperations)"]
        T2 --> T3["@Import{Lettuce, Jedis}<br/>ConnectionConfiguration"]
    end

    subgraph Props["属性层"]
        P1["RedisProperties<br/>spring.data.redis.*(3.0 新前缀)"] --> P2["RedisConnectionConfiguration 基类<br/>三态拓扑裁决 + parseUrl"]
        P2 --> P3["用户自定义 Configuration bean<br/>(ObjectProvider, 最高优先)"]
    end

    subgraph Client["客户端装配层(二选一)"]
        T3 --> L["LettuceConnectionConfiguration<br/>client-type 默认 lettuce"]
        T3 --> J["JedisConnectionConfiguration<br/>需 jedis+pool2 依赖"]
        L --> L1["ClientResources bean<br/>(共享 EventLoopGroup)"]
        L --> L2["LettuceClientConfiguration<br/>ssl/超时/ClientOptions/池"]
        J --> J1["JedisClientConfiguration<br/>PropertyMapper 映射"]
    end

    subgraph Beans["产出 bean"]
        L2 --> B1["LettuceConnectionFactory<br/>(惰性建连)"]
        J1 --> B1
        B1 --> B2["redisTemplate (JDK 序列化)"]
        B1 --> B3["StringRedisTemplate"]
        B1 --> B4["ReactiveRedisTemplate<br/>(Lettuce 天然响应式)"]
        B1 --> B5["RedisRepository (Registrar)"]
        B1 --> B6["RedisHealthIndicator<br/>(Actuator 联动)"]
    end

    subgraph Ext["四个用户扩展点"]
        E1["① 覆盖 bean(模板/工厂/Configuration)"]
        E2["② Customizer SPI<br/>ClientResources/Lettuce/Jedis"]
        E3["③ 属性 spring.data.redis.*"]
        E4["④ client-type 切换驱动"]
    end

    Reg --> Props --> Client --> Beans
    Ext -.-> Props
    Ext -.-> Client
    Ext -.-> Beans
```

### 10.13 常见坑与最佳实践（源码依据）

| 坑 | 源码依据 | 解法 |
|---|---|---|
| 2.x 升 3.0 后 Redis 配置全部失效 | `RedisProperties.java:35` 前缀改为 `spring.data.redis` | 迁移前缀；排查时直接看该类的属性表 |
| redis-cli 里 key 是乱码 `\xac\xed...` | `redisTemplate` 默认 JDK 序列化（`RedisAutoConfiguration.java:55-59` 未设 serializer） | 覆盖 `redisTemplate` bean 配 String/Jackson 序列化，或改用 `StringRedisTemplate` |
| 配了 `lettuce.pool.*` 不生效 | `isPoolEnabled` 回退判定 classpath（`RedisConnectionConfiguration.java:138-141`） | 补 `org.apache.commons:commons-pool2` 依赖 |
| 想用 Jedis 切不过去 | `JedisConnectionConfiguration.java:47` 需要 `GenericObjectPool` + `Jedis` 都在 classpath | 加 jedis + commons-pool2 依赖，必要时 `client-type=jedis` |
| 命令莫名超时抛 `RedisCommandTimeoutException` | `createClientOptions` 无条件 `TimeoutOptions.enabled()`（`LettuceConnectionConfiguration.java:143`） | 调大 `spring.data.redis.timeout`（这是命令级，非建连级） |
| 多数据源场景 Boot 模板消失 | `@ConditionalOnSingleCandidate(RedisConnectionFactory.class)`（`RedisAutoConfiguration.java:54`） | 自行定义两个工厂 + 两个模板（`@Primary` 标其一） |
| Reactive 模板存取乱码 | `RedisReactiveAutoConfiguration.java:51-55` 显式 JDK 序列化 | 自定义 `reactiveRedisTemplate` 的 `RedisSerializationContext` |
| URL 配了 sentinel 地址报错 | `parseUrl` 只认 redis/rediss（`RedisConnectionConfiguration.java:160-162`） | sentinel 用 `spring.data.redis.sentinel.*` 离散属性 |

### 10.14 设计精髓总结

1. **starter 零代码**：`spring-boot-starter-data-redis` 只是依赖聚合；所有智能在 autoconfigure 的条件装配里--"starter = 依赖 + 自动配置入口"是 Spring Boot 生态扩展的标准范式（可对照任何第三方 starter）；
2. **条件注解编排出"驱动自动裁决"**：`@ConditionalOnClass` × `@ConditionalOnProperty(matchIfMissing=true)` × `@ConditionalOnMissingBean(类级)` 三者组合，让"引谁用谁、都引默认 Lettuce、显式配置可强制"这套策略无需一行 if-else；
3. **可选依赖的类加载隔离**：`PoolBuilderFactory` 内部类把 commons-pool2 的触碰限制在已验证存在的分支内（与 6.3 的预过滤、第四章可选容器同构）；
4. **装配与通信彻底解耦**：Boot 只造 `LettuceConnectionFactory`（惰性），连接的建立、复用、超时、重连全部下放给 Spring Data Redis + Lettuce--Boot 的边界感是"装配层"，这也是它能把任意中间件纳入而自身代码不膨胀的原因；
5. **扩展点分层克制**：属性（弱）-> Customizer SPI（中，`orderedStream` 支持多实现排序）-> 直接覆盖 bean（强，`@ConditionalOnMissingBean` 让位），三级逃生舱覆盖 99% 定制需求；
6. **单一共享连接的默认值**：默认不池化不是偷懒，而是匹配 Lettuce 的多路复用线程模型--**默认值的选择反映了驱动架构**，反过来 Jedis 类级条件就要求池必须存在。


## 十一、spring-boot-devtools 实现原理：自动重启、双类加载器与远程开发

> 本章路径约定：
> - `DT` = `spring-boot-project/spring-boot-devtools/src/main/java/org/springframework/boot/devtools`
> - `DTR` = `spring-boot-project/spring-boot-maven-plugin/src/main/java/org/springframework/boot/maven`
>
> devtools 与前面所有章节都不同：它**不是给生产用的，而是给开发者省时间的**。它没有对外 API，业务代码一行不改就能获得"改完代码 1~2 秒内应用自动重启"的体验。其实现横跨本书已剖析的多个机制：`spring.factories` SPI（第二章启动流程）、`EnvironmentPostProcessor`（第五章配置加载）、`Condition` 体系（第六章）、自动配置注册（第三章）--是把这些"启动期基建"反向用于开发期的一个精巧范例。
>
> 核心结论先行：**devtools 的自动重启 = 轮询 classpath 目录快照 + 丢弃式"restart classloader" + 反射重跑 `main` 方法**。冷启动要重新加载全部类，而重启只丢弃装着"你正在开发的代码"的那个小 classloader，第三方库全部留在 base classloader 里不动，因此快一个数量级。

### 11.1 模块结构与功能全景

`spring-boot-devtools` 不是 starter（没有 `-starter-` 后缀，也不该传递进生产依赖），它的 `build.gradle` 只有两个 api 依赖：`spring-boot` + `spring-boot-autoconfigure`，其余（spring-web、security、jdbc、redis 等）全部是 `optional`--**optional 依赖保证 devtools 的自动配置只在应用自己已经引入对应技术时才生效**（与第六章 `@ConditionalOnClass` 的"可选依赖"思想一致）。

它向 Spring Boot 注入的入口全在 `DT/src/main/resources/META-INF/`：

| 注册文件 | 注册内容 | 生效时机 |
|---|---|---|
| `spring.factories` | `RestartScopeInitializer`（ApplicationContextInitializer）、`RestartApplicationListener`（ApplicationListener）、`DevToolsLogFactory.Listener`、`DevToolsHomePropertiesPostProcessor` + `DevToolsPropertyDefaultsPostProcessor`（EnvironmentPostProcessor） | **比自动配置早得多**：Environment 准备期 / 最早的 `ApplicationStartingEvent` |
| `org.springframework.boot.autoconfigure.AutoConfiguration.imports` | `DevToolsDataSourceAutoConfiguration`、`DevToolsR2dbcAutoConfiguration`、`LocalDevToolsAutoConfiguration`、`RemoteDevToolsAutoConfiguration` | 常规自动配置阶段 |
| `spring-devtools.properties` | `restart.exclude.*` 模式（把 spring-boot 系列模块划出 restart 范围） | Restarter 初始化时读取 |
| `devtools-property-defaults.properties` | 一批开发期友好属性默认值 | EnvironmentPostProcessor 装载 |

注意**两套注册文件并存**的原因：需要"尽早"介入的逻辑（初始化 Restarter、注入属性）走 `spring.factories` 的 listener/initializer/EPP 通道；可以等到容器装配期的逻辑（文件监听 bean、LiveReload server）走常规自动配置。

功能与源码包的对应关系：

```mermaid
flowchart TB
    subgraph Entry["注册入口(META-INF)"]
        E1["spring.factories<br/>RestartApplicationListener(最早)<br/>RestartScopeInitializer<br/>两个 EnvironmentPostProcessor"]
        E2["AutoConfiguration.imports<br/>LocalDevToolsAutoConfiguration<br/>RemoteDevToolsAutoConfiguration<br/>DevToolsDataSource/R2dbc"]
    end

    subgraph Core["restart/ 核心引擎"]
        C1["Restarter(单例)<br/>捕获 main/args/urls<br/>stop() + start()"]
        C2["RestartClassLoader<br/>丢弃式 restart 加载器"]
        C3["RestartLauncher<br/>'restartedMain' 线程反射重跑 main"]
        C4["RestartScope<br/>跨重启存活的 bean 作用域"]
    end

    subgraph Watch["filewatch/ + classpath/ 变更感知"]
        W1["FileSystemWatcher<br/>轮询+静默期"]
        W2["ClassPathFileChangeListener<br/>-> ClassPathChangedEvent"]
        W3["PatternClassPathRestartStrategy<br/>排除静态资源等"]
    end

    subgraph Aux["周边能力"]
        A1["livereload/<br/>LiveReloadServer(35729)"]
        A2["env/<br/>属性默认值 + 全局配置文件"]
        A3["remote/ + tunnel/<br/>远程调试与上传"]
        A4["autoconfigure/<br/>DevToolsDataSource(R2dbc)"]
    end

    E1 --> C1
    E2 --> W1
    W1 --> W2 --> W3
    W3 -->|"ClassPathChangedEvent<br/>(restartRequired=true)"| C1
    C1 --> C2 --> C3
    C1 -. RestartScope .-> A1
    E1 --> A2
    E2 --> A4
    E2 --> A3
```

### 11.2 触发链路：比自动配置早得多的 Restarter 初始化

自动重启的一切始于 `RestartApplicationListener`（`DT/restart/RestartApplicationListener.java`）。它实现 `ApplicationListener<ApplicationEvent>` 且 **order = `HIGHEST_PRECEDENCE`**（:45），通过 `spring.factories` 注册后，`SpringApplication.run()` 发布的第一个事件 `ApplicationStartingEvent`（此时 Environment、context 都还没建，见第二章启动流程第 ④ 步之前）就会到达它：

```java
private void onApplicationStartingEvent(ApplicationStartingEvent event) {
    // It's too early to use the Spring environment but we should still allow
    // users to disable restart using a System property.
    String enabled = System.getProperty(ENABLED_PROPERTY);          // :66  spring.devtools.restart.enabled
    RestartInitializer restartInitializer = null;
    if (enabled == null) {
        restartInitializer = new DefaultRestartInitializer();       // :69  未配置 -> 默认初始化器
    }
    else if (Boolean.parseBoolean(enabled)) {
        restartInitializer = new DefaultRestartInitializer() {      // :72  显式 true -> 无视打包形态强制启用
            @Override
            protected boolean isDevelopmentClassLoader(ClassLoader classLoader) {
                return true;
            }
        };
    }
    if (restartInitializer != null) {
        String[] args = event.getArgs();
        boolean restartOnInitialize = !AgentReloader.isActive();    // :86  JRebel/HotswapAgent 在场则让位
        Restarter.initialize(args, false, restartInitializer, restartOnInitialize);
    }
    else {
        Restarter.disable();                                        // :95  显式 false -> 彻底关闭
    }
}
```

**两个值得咀嚼的细节**：

- **`spring.devtools.restart.enabled` 只认 System property**（注释写得直白："It's too early to use the Spring environment"）。在 `application.yml` 里配它是无效的--这个坑的根源是本 listener 监听的是 `ApplicationStartingEvent`，Environment 此时根本不存在（第五章的 ConfigData 流程尚未开始）；
- **Agent 检测让位**（`AgentReloader.isActive()`）：类路径出现 JRebel（`org.zeroturnaround.javarebel.*`）或 HotswapAgent（`org.hotswap.agent.HotswapAgent`）的类即视为 agent 重载器在场（`DT/restart/AgentReloader.java:30-39` 硬编码这三个类名），devtools 不做"立即重启"，把热重载让给更专业的 agent--同类产品互相尊重的绅士协议。

**`Restarter.initialize`（`DT/restart/Restarter.java:529-541`）**是标准单例懒初始化：首次调用时用**当前线程（main 线程）**构造 `Restarter`。构造器（:131-146）把四样东西"拍快照"存起来：

```java
protected Restarter(Thread thread, String[] args, boolean forceReferenceCleanup, RestartInitializer initializer) {
    SilentExitExceptionHandler.setup(thread);                       // :138 给 main 线程装"静默退出"handler
    this.initialUrls = initializer.getInitialUrls(thread);          // :140 可变 URL 集合
    this.mainClassName = getMainClassName(thread);                  // :141 从栈帧反查 main 所在类
    this.applicationClassLoader = thread.getContextClassLoader();   // :142 base classloader
    this.args = args;
    this.exceptionHandler = thread.getUncaughtExceptionHandler();
    this.leakSafeThreads.add(new LeakSafeThread());                 // :145 预创建防泄漏线程
}
```

- **`mainClassName` 的获取方式很特别**（`MainMethod.java:35-44`）：遍历**当前线程的栈帧**，找方法名为 `main` 且存在静态 `main(String[])` 的栈帧，反射拿到其声明类名。重启时就用这个类名在新 classloader 里 `Class.forName` 再反射调用 `main`；
- **`initialUrls` 判定"该不该启用 restart"**，由 `DefaultRestartInitializer.getInitialUrls`（:35-43）三连问：
  1. `isMain`：线程名必须是 `"main"`（:64-66，排除线程池里误启动的）且 context classloader 是 `AppClassLoader`（:76-78，名字包含即算--排除了 fatjar 的 `LaunchedURLClassLoader` 和测试用的隔离加载器）；
  2. `DevToolsEnablementDeducer.shouldEnable(thread)`：扫描**栈轨迹**，若出现 `org.junit.runners.`、`org.junit.platform.`、`org.springframework.boot.test.`、`SpringApplicationAotProcessor`、`cucumber.runtime.`（`DT/system/DevToolsEnablementDeducer.java:27-36`）任一前缀即返回 null--**测试与 AOT 处理中 devtools 自动失效**。用调用栈做开关是很"脏"但极有效的一招，它能区分"谁在启动我"而非"我在哪运行"；
  3. `getUrls` -> `ChangeableUrls.fromClassLoader(...)`。

**`ChangeableUrls`（`DT/restart/ChangeableUrls.java:55-67`）决定哪些 URL 属于"restart 范围"**：

```java
for (URL url : urls) {
    if ((settings.isRestartInclude(url) || isDirectoryUrl(url.toString())) && !settings.isRestartExclude(url)) {
        reloadableUrls.add(url);
    }
}
```

即：**(目录 URL（`file:` 且以 `/` 结尾）或被 `restart.include` 显式命中）且不被 `restart.exclude` 命中**。URL 列表的来源（:95-110）：优先 `URLClassLoader.getURLs()`（IDE 启动时 main classloader 就是它），否则回退 `java.classpath` 系统属性，还会展开 jar 清单里的 `Class-Path` 属性（:121-180）。

`restart.include/exclude` 模式来自 `DevToolsSettings`（`DT/settings/DevToolsSettings.java:96-107`）：加载**classpath 上所有** `META-INF/spring-devtools.properties`。devtools 自带的这份把 Spring Boot 自家模块全部排除：

```properties
restart.exclude.spring-boot=/spring-boot/(bin|build|out)/
restart.exclude.spring-boot-devtools=/spring-boot-devtools/(bin|build|out)/
restart.exclude.spring-boot-autoconfigure=/spring-boot-autoconfigure/(bin|build|out)/
restart.exclude.spring-boot-actuator=/spring-boot-actuator/(bin|build|out)/
restart.exclude.spring-boot-starter=/spring-boot-starter/(bin|build|out)/
restart.exclude.spring-boot-starters=/spring-boot-starter-[\w-]+/
```

多模块开发（你在 IDE 里同时打开 spring-boot 源码工程）时，这些模块的输出目录不会被划进 restart 范围--否则每改一行 Boot 源码都要连 base 层一起重建。

**"立即重启"的魔法（`initialize(restartOnInitialize=true)`）**：初始化完成后 devtools 并不直接用当前的类跑应用，而是立刻做一次 `immediateRestart()`（:168-180）：

```java
private void immediateRestart() {
    try {
        getLeakSafeThread().callAndWait(() -> {
            start(FailureHandler.NONE);          // 用 RestartClassLoader 启动应用
            cleanupCaches();
            return null;
        });
    }
    catch (Exception ex) { this.logger.warn("Unable to initialize restarter", ex); }
    SilentExitExceptionHandler.exitCurrentThread();   // :179 让 main 线程"无声地"抛异常退出
}
```

这就是你在 IDE 控制台看到的现象背后的真相：**你亲手启动的 main 线程其实很快就被干掉了，真正跑应用的是 `restartedMain` 线程 + RestartClassLoader**。杀掉原线程的手段是抛一个 `SilentExitException`（:95-97，private 嵌套类，外人无法 catch），由构造期装到 main 线程上的 `SilentExitExceptionHandler`（:38-49）识别并吞掉：如果是该异常且 JVM 正在退出（无其他非 daemon 线程存活），还会 `System.exit(0)` 防止非零退出码（:79-81）。**用一个"无人能捕获的私有异常"实现线程的静默自杀**，是整个 devtools 里最黑客的一笔。

初始化引导期完整时序：

```mermaid
sequenceDiagram
    autonumber
    participant IDE as IDE main 线程
    participant SA as SpringApplication.run()
    participant RAL as RestartApplicationListener<br/>(order=HIGHEST)
    participant R as Restarter(单例)
    participant DRI as DefaultRestartInitializer
    participant CU as ChangeableUrls
    participant LST as LeakSafeThread
    participant RM as restartedMain 线程<br/>(RestartClassLoader)

    IDE->>SA: main(args)
    SA->>RAL: ApplicationStartingEvent(最早期事件)
    RAL->>RAL: 读 System property<br/>spring.devtools.restart.enabled
    RAL->>R: initialize(args, initializer)
    R->>R: SilentExitExceptionHandler.setup(main线程)
    R->>DRI: getInitialUrls(main)
    DRI->>DRI: 线程名=main? CCL=AppClassLoader?
    DRI->>DRI: 栈里有 junit/boot-test/AOT? -> 有则返回 null
    DRI->>CU: fromClassLoader(类加载器)
    CU->>CU: URLClassLoader.getURLs()<br/>+ jar 清单 Class-Path
    CU-->>DRI: 目录URL + restart.include - restart.exclude
    DRI-->>R: initialUrls(非 null 才继续)
    R->>LST: callAndWait(start)
    LST->>R: start(): new RestartClassLoader
    R->>RM: RestartLauncher 启动<br/>Class.forName(main类).main(args)
    Note over RM: 应用真正运行在这个线程上
    LST-->>R: 新应用启动完成
    R->>IDE: SilentExitExceptionHandler.exitCurrentThread()
    Note over IDE: 原 main 线程无声消亡<br/>控制台无任何异常栈
```

### 11.3 双类加载器：RestartClassLoader 的"parent-last"设计（核心）

`Restarter` 类的 javadoc（:58-73）已把架构说透：**应用类加载器被拆成两层--上层（base）装载不会变的第三方 jar，下层（restart）装载可能被你改动的工程输出目录**。重启时只丢弃下层重建，上层的几千个类原封不动。

`doStart()`（:274-283）展示了每次重启的全部动作：

```java
private Throwable doStart() throws Exception {
    URL[] urls = this.urls.toArray(new URL[0]);
    ClassLoaderFiles updatedFiles = new ClassLoaderFiles(this.classLoaderFiles);
    ClassLoader classLoader = new RestartClassLoader(this.applicationClassLoader, urls, updatedFiles);
    return relaunch(classLoader);      // relaunch -> RestartLauncher 线程反射调 main
}
```

`RestartClassLoader`（`DT/restart/classloader/RestartClassLoader.java:38`）继承 `URLClassLoader` 并实现 spring-core 的 `SmartClassLoader`，其灵魂是 **parent-last（子优先）加载顺序**（:108-129）：

```java
@Override
public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    String path = name.replace('.', '/').concat(".class");
    ClassLoaderFile file = this.updatedFiles.getFile(path);
    if (file != null && file.getKind() == Kind.DELETED) {
        throw new ClassNotFoundException(name);              // 已删除的类 -> 直接 CNFE
    }
    synchronized (getClassLoadingLock(name)) {
        Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass == null) {
            try {
                loadedClass = findClass(name);               // ① 先在自己管理的 URL 里找
            }
            catch (ClassNotFoundException ex) {
                loadedClass = Class.forName(name, false, getParent());  // ② 找不到才委托 parent
            }
        }
        if (resolve) { resolveClass(loadedClass); }
        return loadedClass;
    }
}
```

标准 `URLClassLoader.loadClass` 是**先 parent 后自己**（delegate-first），这里被刻意反转：工程类必须由 restart 层定义，否则会被 base 层的旧版本"抢答"。`findClass`（:132-143）还有一个后门--若 `updatedFiles` 里有该类的字节（`ClassLoaderFile`，Kind 为 ADDED/MODIFIED/DELETED，来自远程更新场景，见 11.9），**直接用内存里的字节 `defineClass`，连磁盘文件都不读**；配套的 `createFileUrl`（:155-162）用自定义协议名 `reloaded` 造出 `URL("reloaded", null, -1, "/" + name, new ClassLoaderFileURLStreamHandler(file))`，让 `getResource` 也能返回内存资源。

资源加载（`getResources` :66-80）则反着来：**优先 parent**--因为 restart 层故意不把第三方 jar 放进自己的 URL 集合，资源统一从 base 层拿，只在 `updatedFiles` 覆盖时替换第一项，避免同一资源出现两份。

**重启 = stop + start（`restart()` :243-254）**，在 `LeakSafeThread`（防泄漏线程，见下）里执行：

- **`stop()`（:303-321）**：遍历 `rootContexts` 逐个 `context.close()`（这就是文档强调"devtools 依赖 context 的 shutdown 机制、禁用 shutdown hook 会失效"的原因）-> `cleanupCaches()` -> `System.gc()` + `runFinalization()`；
- **`cleanupCaches()`（:328-374）**是防内存泄漏的关键：清 `Introspector` 缓存、`ResolvableType` 缓存、`ReflectionUtils`/`AnnotationUtils` 缓存、`CachedIntrospectionResults` 的 acceptedClassLoaders/strong/soft 缓存（:337-341，靠反射直接捅私有字段）--这些框架缓存以 `Class` 对象为 key，**只要缓存里还留着 restart 层的 Class，旧 RestartClassLoader 就永远无法被 GC**。`clear` 时只移除 `isFromRestartClassLoader` 的条目（:362），精确打击。极端情况下还有 `forceReferenceCleanup()`（:379-389）：**故意 `new long[102400]` 循环分配直到 OOM**，用 OutOfMemoryError 逼 JVM 清空全部软/弱引用--为了回收类加载器，主动制造一次 OOM，又一处"疯狂但正确"的代码；
- **`start()` 的失败重试**（:261-272）是 `do { error = doStart(); } while(failureHandler.handle(error) != ABORT)` 死循环：新代码编译错了导致启动失败时，由 `FileWatchingFailureHandler`（`DT/autoconfigure/FileWatchingFailureHandler.java:34-55`）挂一个临时 `FileSystemWatcher` 等待下一次文件变更，等到就返回 `RETRY` 再试一轮--**改错了代码应用停在"等待修复"状态，改对了自动活过来**。

**`LeakSafeThread`（:578-622）**：在 Restarter 构造期（还只有 base classloader 时）预先创建的线程池（BlockingDeque 复用），其线程的栈上**永远不会出现 RestartClassLoader 加载的类**，restart/stop 逻辑都在它上面执行。若在 `restartedMain` 线程里直接跑 `Restarter` 的类，`Restarter.class` 会被旧 restart 层引用，形成自我锁死的类加载器泄漏。`LeakSafeThreadFactory`（:627-638）造的新线程一律设 context classloader 为 base 层的 `applicationClassLoader`--LiveReload 线程（11.6）因此能跨重启存活。

双类加载器全景：

```mermaid
flowchart TB
    subgraph Base["base classloader = AppClassLoader(常驻)"]
        B1["spring-core / spring-webmvc / Jackson / Netty ...<br/>全部第三方 jar"]
        B2["Restarter 自身 / devtools 类"]
    end

    subgraph Restart["RestartClassLoader #N(可丢弃)"]
        R1["target/classes(你的业务类)"]
        R2["src/main/resources(你的配置/模板)"]
        R3["updatedFiles 覆盖的内存字节(远程场景)"]
    end

    subgraph Ctx["第 N 代 ApplicationContext"]
        C1["全部 bean 都由 RestartClassLoader #N 的类构成"]
    end

    R1 --> Ctx
    R2 --> Ctx
    R3 --> Ctx
    Restart -->|"parent(委托兜底)"| Base
    Ctx -->|"类引用"| Restart

    subgraph Cycle["重启循环(Restarter.restart)"]
        S1["stop(): context.close()"] --> S2["cleanupCaches():<br/>Introspector/ResolvableType/<br/>ReflectionUtils/AnnotationUtils<br/>只清 restart 层来源的条目"]
        S2 --> S3["System.gc() + runFinalization()<br/>(可选 forceReferenceCleanup:<br/>故意 OOM 逼出软引用)"]
        S3 --> S4["丢弃旧 RestartClassLoader #N"]
        S4 --> S5["new RestartClassLoader #N+1<br/>(同 urls + 最新 updatedFiles)"]
        S5 --> S6["RestartLauncher('restartedMain')<br/>Class.forName(main类).main(args)"]
    end

    Cycle -.重建.-> Restart
```

为什么快？**一次冷启动 = JVM 加载几万个类 + 容器全量 refresh；一次 devtools 重启 = 只重新加载 target/classes 里你的几十~几百个类 + 容器 refresh**。base 层类加载全部命中缓存，这就是"比冷启动快一个数量级"的全部秘密。

### 11.4 变更感知：FileSystemWatcher 的"轮询 + 静默期"

自动配置侧的接线在 `LocalDevToolsAutoConfiguration`（`DT/autoconfigure/LocalDevToolsAutoConfiguration.java`）。**总开关是 `@ConditionalOnInitializedRestarter`**（:63）--自定义条件（`DT/restart/OnInitializedRestarterCondition.java`）要求 `Restarter.getInstance()` 成功且 `getInitialUrls() != null`：fatjar 运行（或测试、或被 System property 禁用）时 URLs 为 null，整个本地 devtools 自动配置**静默跳过**（条件报告里是 negative match，呼应 6.8）。

`RestartConfiguration`（:97-158）注册的监听链：

| bean | 行号 | 职责 |
|---|---|---|
| `classPathFileSystemWatcher` | :114-123 | 组装 watcher，URL 取 `Restarter.getInstance().getInitialUrls()`；`setStopWatcherOnRestart(true)`（:121）重启时先停监听，避免旧 watcher 持有旧 classloader |
| `classPathRestartStrategy` | :125-129 | `new PatternClassPathRestartStrategy(properties.getRestart().getAllExclude())` |
| `fileSystemWatcherFactory` | :131-134 | lambda 工厂 -> `newFileSystemWatcher`（:143-156）注入 pollInterval/quietPeriod/triggerFile/additionalPaths |
| `restartingClassPathChangeChangedEventListener` | :108-112 | 收到 restart 事件后调用 `Restarter.restart(new FileWatchingFailureHandler(...))`（:213） |
| `conditionEvaluationDeltaLoggingListener` | :136-141 | 条件评估差异日志 |

**监听目录的收敛**：`ClassPathFileSystemWatcher` 构造器（`DT/classpath/ClassPathFileSystemWatcher.java:55-62`）把 URLs 交给 `ClassPathDirectories` 过滤--**只保留 `file:` 协议且以 `/` 结尾的目录**（`DT/classpath/ClassPathDirectories.java:56-62`）。jar 一律不监听（jar 内容不会原地变），IDE 的 `target/classes`、`bin/` 输出目录天然命中。`afterPropertiesSet`（:78-88）挂上 `ClassPathFileChangeListener` 后 `start()`。

**`FileSystemWatcher`（`DT/filewatch/FileSystemWatcher.java`）的监听算法是纯轮询 + 快照对比**，没用 WatchService：

```java
private void scan() throws InterruptedException {
    Thread.sleep(this.pollInterval - this.quietPeriod);   // :273 睡到本周期尾段
    Map<File, DirectorySnapshot> previous;
    Map<File, DirectorySnapshot> current = this.directories;
    do {
        previous = current;
        current = getCurrentSnapshots();                  // 对每个目录拍 DirectorySnapshot
        Thread.sleep(this.quietPeriod);                   // :279 静默期
    }
    while (isDifferent(previous, current));               // 静默期内还在变 -> 重拍
    if (isDifferent(this.directories, current)) {
        updateSnapshots(current.values());                // 稳定后才真正触发
    }
}
```

- **静默期（quiet period，默认 400ms，`DevToolsProperties.java:90`）**解决"编译是渐进过程"的问题：IDE 增量编译会分批写 class 文件，若一有变化就重启，可能拿着半套类启动。算法是"拍快照 -> 等 400ms -> 再拍，直到两次一致"，**确保触发重启时文件系统已经安静下来**。轮询周期默认 1s（:84）；
- **快照相等性**（`FileSnapshot.java:45-56`）只比较 `exists + length + lastModified`，**不读文件内容**--对比成本极低，也解释了为什么 devtools 文档建议某些 IDE 下要调大这两个参数；
- **`SnapshotStateRepository.STATIC`**（`LocalDevToolsAutoConfiguration.java:146`）让快照存在静态字段里：**重启后新 watcher 恢复旧快照**，同一次变更不会被第二次触发；
- **trigger-file**（`newFileSystemWatcher` :148-150 -> `TriggerFileFilter`）：配了 `spring.devtools.restart.trigger-file` 后，只有该文件名变化才会计入 diff（`DirectorySnapshot.getChangedFiles` 带 triggerFilter），实现"攒一批修改，编译触发文件才重启"的手动档模式。

变更事件的裁决链（`DT/classpath/ClassPathFileChangeListener.java:61-85`）：

```mermaid
flowchart LR
    A["File Watcher 线程<br/>轮询+静默期确认变更"] --> B["ClassPathFileChangeListener.onChange"]
    B --> C{"AgentReloader.isActive()?<br/>(JRebel/HotswapAgent)"}
    C -->|是| D["restartRequired=false"]
    C -->|否| E{"PatternClassPathRestartStrategy<br/>AntPathMatcher 逐文件匹配"}
    E -->|"命中 exclude 模式<br/>(static/**, templates/**, *Test.class...)"| F["该变更不要求重启"]
    E -->|未命中| G["restartRequired=true"]
    D --> H["发布 ClassPathChangedEvent"]
    F --> H
    G --> H
    H -->|"restartRequired=true"| I["停止当前 watcher +<br/>Restarter.restart(FileWatchingFailureHandler)"]
    H -->|"restartRequired=false"| J["LiveReloadServerEventListener<br/>-> triggerReload() 仅刷浏览器"]
```

exclude 默认值（`DevToolsProperties.java:62-64`）值得背下来：`META-INF/maven/**, META-INF/resources/**, resources/**, static/**, public/**, templates/**, **/*Test.class, **/*Tests.class, git.properties, META-INF/build-info.properties`--**静态资源/模板变更不重启（只触发 LiveReload 刷浏览器），测试类变更被无视**。这就是"改了 html/css 秒级生效不重启"的机制真相：不是 devtools 智能，而是这些路径在策略里被排除了。

### 11.5 属性层：开发期默认值与全局配置

两个 `EnvironmentPostProcessor`（呼应第五章 ConfigData 流程的扩展点）：

**① `DevToolsPropertyDefaultsPostProcessor`（`DT/env/DevToolsPropertyDefaultsPostProcessor.java`）**--把 `devtools-property-defaults.properties` 里的"开发期友好"属性以名为 `devtools` 的 PropertySource **addLast**（:87，最低优先级，用户任何显式配置都能覆盖）塞进 Environment：

```properties
server.error.include-binding-errors=always      # 错误页显示绑定错误
server.error.include-message=always             # 错误页显示异常消息
server.error.include-stacktrace=always          # 错误页显示栈
server.servlet.jsp.init-parameters.development=true
server.servlet.session.persistent=true          # session 落盘,重启不丢登录态
spring.freemarker.cache=false                   # 各模板引擎关缓存
spring.graphql.graphiql.enabled=true
spring.groovy.template.cache=false
spring.h2.console.enabled=true                  # H2 控制台直接可用
spring.mustache.servlet.cache=false
spring.mvc.log-resolved-exception=true
spring.reactor.debug=true                       # Reactor 调试栈
spring.template.provider.cache=false
spring.thymeleaf.cache=false
spring.web.resources.cache.period=0             # 静态资源不缓存
spring.web.resources.chain.cache=false
```

生效门槛（:83-106）很讲究：`canAddProperties` 要求 `spring.devtools.add-properties` 未被显式关掉，且（本地 Restarter 已初始化 **或** 远程 restart 已配置）；`isLocalApplication`（:97-99）靠**检查名为 `remoteUrl` 的 PropertySource 是否存在**区分本地/远程客户端进程（见 11.9）。启动日志那句 *"Devtools property defaults active! Set 'spring.devtools.add-properties' to 'false' to disable"* 就来自这里（:85-86）。

**② `DevToolsHomePropertiesPostProcessor`（`DT/env/DevToolsHomePropertiesPostProcessor.java`）**--机器级全局配置：按 `~/.config/spring-boot/spring-boot-devtools.{properties,yaml,yml}`（:58-61）加载，找不到再回退旧版 `~/.spring-boot-devtools.properties`（:92-93）；home 目录可被 `SPRING_DEVTOOLS_HOME` 环境变量或 `spring.devtools.home` 系统属性改写（:140-144）。加载的 PropertySource **addFirst**（:94）--**优先级高于一切应用配置**（毕竟是"这台机器的开发约定"），在第五章的优先级链里位于最顶端的位置。注意它不支持 profile（文档明确说明），因为它的加载时机早于 profile 语义可用。

### 11.6 @RestartScope 与 LiveReload：跨重启存活

**`@RestartScope`** 解决"有些 bean 重启也不该重建"的问题（比如 LiveReloadServer 若跟着重启重建，浏览器的连接就全断了）。实现是一个自定义 Spring Scope（`DT/restart/RestartScopeInitializer.java`，经 spring.factories 的 ApplicationContextInitializer 通道在**每个 context 创建前**注册 `registerScope("restart", ...)`）：

```java
private static class RestartScope implements Scope {
    @Override
    public Object get(String name, ObjectFactory<?> objectFactory) {
        return Restarter.getInstance().getOrAddAttribute(name, objectFactory);  // :44 存进 Restarter 的属性表
    }
    ...
}
```

Scope 的存储介质不是 context 而是 **`Restarter` 单例的 `attributes` Map**（`Restarter.java:442-455`）--它活在与任何一代 context、任何一代 RestartClassLoader 都无关的 devtools 类空间里，天然跨重启。`LocalDevToolsAutoConfiguration` 里 `liveReloadServer` bean 就标了 `@RestartScope`（:75）。

**LiveReloadServer**（`DT/livereload/LiveReloadServer.java:66-75`）默认监听 **35729** 端口（livereload.com 协议标准端口），是一个**手写的极简 WebSocket 服务器**（ServerSocket + 自实现帧编解码 + `livereload.js` 资源内置），浏览器装 LiveReload 插件连上后，收到 `reload` 命令即整页刷新。触发时机（`LiveReloadServerEventListener`，`LocalDevToolsAutoConfiguration.java:184-189`）：**context 刷新完成**（`ContextRefreshedEvent`）**或 classpath 变更但不需要重启**（静态资源变更）-> `triggerReload()`。即"改 Java 代码 -> 重启完成 -> 浏览器自动刷新；改静态资源 -> 不重启 -> 浏览器自动刷新"，两条路最终都汇到浏览器刷新。`OptionalLiveReloadServer`（`DT/autoconfigure/OptionalLiveReloadServer.java:44-58`）负责优雅启动：端口被占用只 warn 一句，绝不让应用挂掉--开发工具绝不能比应用本身更脆弱。

### 11.7 数据源联动：DevToolsDataSourceAutoConfiguration

`DT/autoconfigure/DevToolsDataSourceAutoConfiguration.java` 处理"重启时内存数据库怎么办"：重启会 `context.close()`，但 JVM 还活着，行为必须与冷启动（JVM 退出销毁一切）对齐。条件类 `DevToolsDataSourceCondition`（:195-224）要求**恰好一个 DataSource bean、恰好一个 DataSourceProperties bean、且 DataSource 由 Boot 自动配置的 `DataSourceConfiguration$*` 工厂方法产生**（用户自建数据源不插手）。命中后注册 `NonEmbeddedInMemoryDatabaseShutdownExecutor`（:69-77）：

- 连接池型（HikariCP）+ 内存库（`jdbc:h2:mem:` / `jdbc:hsqldb:mem:` / Derby embedded，enum 定义 :102-148）：context 关闭时显式执行 `SHUTDOWN`--**每次重启内存库干净重置，与冷启动行为一致**（外部库如 PostgreSQL 不会 shutdown，测试 `autoConfiguredExternalDataSourceIsNotShutdown` 验证）；
- 非 pooling 的 embedded 数据源：不 shutdown（连接关闭即自然销毁）；
- 同时 `@Import(DatabaseShutdownExecutorEntityManagerFactoryDependsOnPostProcessor)`（:59-67）保证 `EntityManagerFactory` 先于 shutdown 执行器销毁，避免 JPA 还攥着连接时库被拆。

（`DevToolsR2dbcAutoConfiguration` 是同样思想在响应式数据层的镜像，R2dbc 内存库在重启时 close。）

### 11.8 条件评估差异日志：把第六章的报告用活了

`ConditionEvaluationDeltaLoggingListener`（`DT/autoconfigure/ConditionEvaluationDeltaLoggingListener.java`，默认开启）监听 `ApplicationReadyEvent`，把本次的 `ConditionEvaluationReport`（第六章 6.8 的产物）与**上一代 context 的报告**（存在 `static ConcurrentHashMap`，键为 context id，因此跨重启保留）做 `getDelta` 差量对比：

- 有差异 -> 打印 `CONDITION EVALUATION DELTA` 报告：你新加了一个 `@ConditionalOnClass` 相关的依赖/属性后，哪些自动配置从 match 变 noMatch（或反之）一目了然；
- 无差异 -> 一句 `Condition evaluation unchanged`。

这是"重启很快"之外的另一层反馈闭环：**不仅应用快速重启了，还告诉你这次重启装配层面发生了什么变化**。

### 11.9 远程 devtools：把"本地改代码"的效果发到远端

远程场景：应用部署在远端服务器（云端/容器），开发者在本地 IDE 调试。两端各有一套 devtools 代码：

**服务端**（`RemoteDevToolsAutoConfiguration`，`DT/autoconfigure/RemoteDevToolsAutoConfiguration.java`）：

- 总闸是 `@ConditionalOnProperty(prefix = "spring.devtools.remote", name = "secret")`（:64）--**不配 secret 整套远程端点不装配**（远程通道必须鉴权）；
- 架构是一个微型 MVC：`DispatcherFilter`（:95-99，普通 Servlet Filter，挂在应用自己的 FilterChain 里）+ `AccessManager`（`HttpHeaderAccessManager`，校验 `X-AUTH-TOKEN` 头等于 secret，:80-83）+ `HandlerMapper` 路由表。所有端点挂在**混淆过的 context path** `/.~~spring-boot!~`（`RemoteDevToolsProperties.java:26`）之下，肉眼一看即知不是给人类的；
- 两个 Handler：健康检查（`HttpStatusHandler`，:86-91）与重启端点 `/.~~spring-boot!~/restart`（:127-130，warn 日志 *"Listening for remote restart updates on ..."* 就是它打的）。

**客户端**（IDE 里运行 `RemoteSpringApplication` 的 main，`DT/RemoteSpringApplication.java:34-46`）：一个 `WebApplicationType.NONE` 的迷你 SpringApplication（配置类 `RemoteClientConfiguration`），显式只挂 4 个 listener（含 `RemoteUrlPropertyExtractor`），跑完 `waitIndefinitely()` 挂住。它本地同样起 `ClassPathFileSystemWatcher` 监听 IDE 输出目录；检测到变更后 `ClassPathChangeUploader`（`DT/remote/client/ClassPathChangeUploader.java:74-83`）把变更文件读出字节、包装成 `ClassLoaderFiles`，**Java 序列化后以 `application/octet-stream` POST 到服务端 restart 端点**（`SocketException` 则无限重试）。

**服务端收到上传后**（`HttpRestartServer.handle`，`DT/restart/server/HttpRestartServer.java:58-76`）：`ObjectInputStream` 反序列化出 `ClassLoaderFiles` -> `RestartServer.updateAndRestart`（`RestartServer.java:66-82`）逐文件写回匹配的目录 URL、更新时间戳 -> `Restarter.addUrls` + `Restarter.addClassLoaderFiles` + `restart()`。**远程重启与本地重启走的是同一个 `Restarter`**，只是新字节的来源从"IDE 编译输出"换成了"HTTP 上传"（`RestartClassLoader` 的 `updatedFiles` 通道正是为它准备的，见 11.3）。

此外 `remoteUrl` 的传递本身也是属性体系的小把戏：`RemoteUrlPropertyExtractor` 把命令行第一个非选项参数（URL）包成名为 `remoteUrl` 的 MapPropertySource **addLast**（`RemoteUrlPropertyExtractor.java:51-56`），供 `@Value("${remoteUrl}")` 注入--而 11.5 的本地属性默认值判断"是否远程进程"正是看这个 PropertySource 名字存不存在（`isLocalApplication`）。`tunnel/` 包还实现了把本地 TCP 端口经 HTTP 长轮询隧道转发到远端的通道（`TunnelClient` 本地开 ServerSocket，`HttpTunnelConnection` 用 HTTP 收发字节），用于远程 JDWP 调试；`DelayedLiveReloadTrigger` 则轮询远端重启完成后再触发本地浏览器刷新。

```mermaid
sequenceDiagram
    autonumber
    participant IDE as 本地 IDE
    participant RSA as RemoteSpringApplication<br/>(本地客户端进程)
    participant FS as 本地 ClassPathFileSystemWatcher
    participant UPL as ClassPathChangeUploader
    participant SRV as 远端应用<br/>DispatcherFilter /.~~spring-boot!~
    participant AM as HttpHeaderAccessManager
    participant HRS as HttpRestartServer
    participant RS as RestartServer
    participant R as Restarter(远端)

    IDE->>RSA: main("https://remote-host:8443")
    RSA->>RSA: Restarter.initialize(NONE)<br/>(本地不重启,只监听)
    RSA->>FS: 监听本地输出目录
    IDE->>IDE: 编译 MyClass.java
    FS-->>RSA: ClassPathChangedEvent
    RSA->>UPL: 序列化 ClassLoaderFiles
    UPL->>SRV: POST /.~~spring-boot!~/restart<br/>Header X-AUTH-TOKEN: secret<br/>Body: 序列化字节
    SRV->>AM: 校验 secret
    AM-->>SRV: 通过
    SRV->>HRS: handle(request, response)
    HRS->>HRS: ObjectInputStream 反序列化<br/>-> ClassLoaderFiles
    HRS->>RS: updateAndRestart(files)
    RS->>RS: 字节写回目录 URL<br/>+ 更新时间戳
    RS->>R: addUrls + addClassLoaderFiles<br/>+ restart()
    R->>R: stop 旧 context<br/>new RestartClassLoader(含内存字节)<br/>restartedMain 重跑 main
    SRV-->>UPL: 200 OK
    Note over RSA: DelayedLiveReloadTrigger<br/>轮询远端就绪后刷浏览器
```

### 11.10 一次完整自动重启的全链路时序

```mermaid
sequenceDiagram
    autonumber
    participant Dev as 开发者
    participant IDEc as IDE 编译器
    participant FW as File Watcher 线程<br/>(FileSystemWatcher)
    participant CPCL as ClassPathFileChangeListener
    participant Ctx as 第 N 代 ApplicationContext<br/>(含监听器 bean)
    participant RCE as RestartingClassPath<br/>ChangedEventListener
    participant R as Restarter
    participant LST as LeakSafeThread
    participant RCN as RestartClassLoader #N+1
    participant RM as restartedMain #N+1

    Dev->>IDEc: 修改 UserController.java 并保存/构建
    IDEc->>IDEc: 增量编译 -> target/classes 更新
    IDEc-->>FW: (文件系统层面变化)
    FW->>FW: sleep(pollInterval-quietPeriod)=600ms
    FW->>FW: 拍快照 -> sleep(quietPeriod=400ms)<br/>-> 再拍,直到两次一致
    FW->>CPCL: onChange(Set<ChangedFiles>)
    CPCL->>CPCL: PatternClassPathRestartStrategy<br/>排除 static/** 等 -> .class 未命中 -> 需重启
    CPCL->>Ctx: publishEvent(ClassPathChangedEvent)
    CPCL->>FW: stop()(避免旧 watcher 泄漏旧 loader)
    Ctx->>RCE: 事件派发
    RCE->>R: Restarter.restart(FileWatchingFailureHandler)
    R->>LST: call(stop + start)
    LST->>Ctx: context.close()(shutdown 机制)
    Note over Ctx: 销毁全部 bean<br/>DevToolsDataSource:<br/>内存库执行 SHUTDOWN
    LST->>LST: cleanupCaches(只清 restart 层来源)<br/>System.gc()
    LST->>RCN: new RestartClassLoader(base, urls, updatedFiles)
    LST->>RM: RestartLauncher 启动,反射调用 main(args)
    Note over RM: SpringApplication.run 全流程重走<br/>(类加载全部命中 base 层缓存)
    RM->>R: ApplicationReadyEvent
    R->>R: finish()(DeferredLog 回放)
    alt 新代码有编译期外的启动错误
        LST->>LST: FileWatchingFailureHandler:<br/>挂临时 watcher 等下次变更 -> RETRY
    else 启动成功
        RM-->>Dev: "Started App in 1.2 seconds"<br/>LiveReload -> 浏览器自动刷新
    end
```

### 11.11 常见坑与最佳实践（源码依据）

| 现象/坑 | 源码依据 | 解法 |
|---|---|---|
| fatjar 里 devtools 不生效 | maven 插件默认排除（`AbstractPackagerMojo.java:97-98` `excludeDevtools=true`）；且 `DefaultRestartInitializer` 因 `LaunchedURLClassLoader` 非 `AppClassLoader` 返回 null（:76-78）；`OnInitializedRestarterCondition` 再兜底 | 远程调试要显式 `spring-boot.repackage.excludeDevtools=false` |
| 在 application.yml 里配 `spring.devtools.restart.enabled=false` 无效 | `RestartApplicationListener.java:63-66` 只读 **System property**（事件太早，Environment 不存在） | `-Dspring.devtools.restart.enabled=false` 或构建期排除依赖 |
| `@SpringBootTest` 里 devtools 不生效 | `DevToolsEnablementDeducer.java:27-36` 栈帧含 `org.springframework.boot.test.` 即禁用 | 设计如此：测试有自己的 context 缓存机制 |
| 改 html/js 不重启但页面也不刷新 | exclude 默认值（`DevToolsProperties.java:62-64`）+ LiveReload 触发（`LocalDevToolsAutoConfiguration.java:185-188`） | 浏览器装 LiveReload 插件，确认 35729 端口未被占用 |
| IDEA 改完代码不重启 | 文档明示：devtools 只认 **classpath 变化**，IDEA 默认不自动编译 | 开启 `Build project automatically` + `compiler.automake.allow.when.app.running`，或手动 Build |
| mvn spring-boot:run 时重启失效 | 文档明示：禁用 fork 则隔离 classloader 不会创建 | 保持 fork enabled（默认） |
| 多模块工程改了另一个模块的代码也触发重启 | `ChangeableUrls.java:55-67` + `spring-devtools.properties` 的 `restart.exclude` | 在自己 jar 里放 `META-INF/spring-devtools.properties` 配 `restart.exclude.xxx=...`，或用 `restart.include` |
| 嫌重启太频繁/太快 | quiet period 默认 400ms（`DevToolsProperties.java:90`） | 配 `spring.devtools.restart.trigger-file`，改完代码 touch 触发文件才重启 |
| 重启后日志刷出 CONDITION EVALUATION DELTA | `ConditionEvaluationDeltaLoggingListener`（默认开） | `spring.devtools.restart.log-condition-evaluation-delta=false` |
| 不想要模板缓存关闭等默认值 | `DevToolsPropertyDefaultsPostProcessor.java:55` | `spring.devtools.add-properties=false` |
| 关了 shutdown hook 后重启行为异常 | `Restarter.stop()` 依赖 context close（`Restarter.java:307-310` + 文档 NOTE） | 别 `setRegisterShutdownHook(false)` |

### 11.12 设计精髓总结

1. **"丢弃式"重载单元是 classloader 而不是类**：不追求单类热替换（JRebel 路线，需字节码织入），而是把"易变代码"整体圈进一个小 classloader，重启 = 换掉整个 loader。粒度粗，但**零侵入、零字节码魔法、绝对正确**--这正是 Spring Boot 的工程品味：用笨办法解决 80% 的需求；
2. **parent-last + 精简 URL 集合的乘法效应**：快不只因为"少加载类"，还因为 base 层的类加载结果被 JVM 的 loaded-class 缓存直接复用（委托链不变），两层缺一不可；
3. **与启动流程的深度融合**：`ApplicationStartingEvent` 时机选择（足够早：能在用户类加载前接管；又足够晚：`spring.factories` 已可用）、System property 判定、栈帧判定"谁在启动我"、`Restarter` 属性表实现的 `@RestartScope`--每一个都是对第二章启动生命周期的精准利用；
4. **防御性设计贯穿始终**：`SilentExitException`（无人能 catch 的私有异常）、`LeakSafeThread`（栈上永不出现 restart 层类）、`cleanupCaches` 的按 loader 过滤清理、故意 OOM 的 `forceReferenceCleanup`、`OptionalLiveReloadServer` 的优雅失败、JRebel 检测让位、测试栈帧检测自动禁用--devtools 对"自己可能搞坏宿主应用"有极强的敬畏；
5. **本地与远程共用同一套 Restarter 内核**：远程只是把"新字节"的来源从文件系统换成 HTTP 上传（`ClassLoaderFiles` 抽象居功至伟），架构没有为远程单开一条腿；
6. **开发期与生产期的边界纪律**：optional 依赖 + maven 插件默认排除 + 三重"是否该启用"判定，保证这套带 OOM 和静默杀线程等危险操作的代码**永远不可能溜进生产环境**。


## 十二、spring-boot-starter-logging 实现原理：日志系统整合与初始化流程

> 本章路径缩写约定（均相对 `spring-boot-project/`）：
> - **BOOT** = `spring-boot/src/main/java/org/springframework/boot`
> - **STARTERS** = `spring-boot-starters`
> - **RES** = `spring-boot/src/main/resources`
>
> 核心源文件：
> - `STARTERS/spring-boot-starter-logging/build.gradle`
> - `BOOT/context/logging/LoggingApplicationListener.java`（489 行，初始化总控）
> - `BOOT/logging/LoggingSystem.java`、`AbstractLoggingSystem.java`、`LoggingSystemFactory.java`
> - `BOOT/logging/logback/`（LogbackLoggingSystem、DefaultLogbackConfiguration、SpringBootJoranConfigurator 等）
> - `BOOT/logging/log4j2/Log4J2LoggingSystem.java`、`BOOT/logging/java/JavaLoggingSystem.java`
> - `RES/META-INF/spring.factories`

日志是每一个应用的第一行输出，也是 Spring Boot 里**初始化最早、退出最晚**的子系统--它必须赶在 Environment 准备好之后、其余所有 Bean 创建之前完成装配，又要在 JVM 退出前最后关闭。本章自顶向下剖析：starter 如何把"门面 + 桥接"网络拼出来（12.1）、Spring Boot 用什么抽象统一三种日志实现（12.2–12.3）、`LoggingApplicationListener` 如何以事件驱动方式在 `SpringApplication.run()` 的夹缝里完成整个初始化（12.4–12.12）、Log4J2/JUL 两套平行实现（12.13）、AOT 原生镜像支持（12.14）。

### 12.1 starter 组成：一张"三进一出"的日志汇聚网络

`STARTERS/spring-boot-starter-logging/build.gradle` 全文只有三行依赖：

```gradle
dependencies {
	api("ch.qos.logback:logback-classic")
	api("org.apache.logging.log4j:log4j-to-slf4j")
	api("org.slf4j:jul-to-slf4j")
}
```

三行依赖各自的角色：

| 依赖 | 角色 | 传递引入 |
|---|---|---|
| `ch.qos.logback:logback-classic` | **SLF4J 的原生实现**（唯一的真正日志引擎） | `logback-core`、`slf4j-api` |
| `org.apache.logging.log4j:log4j-to-slf4j` | **桥接**：把 Log4j2 API 调用转投 SLF4J | `log4j-api`（只引入 API，不含 log4j-core 引擎） |
| `org.slf4j:jul-to-slf4j` | **桥接**：把 `java.util.logging`(JUL) 调用转投 SLF4J | - |

理解这张网络的关键是分清三种角色：**门面（API）**、**实现（引擎）**、**桥接（伪装成实现的 API 转发器）**。Java 生态有四大日志 API：SLF4J、Log4j2 API、JUL、还有 Spring 自带的 Commons Logging。starter-logging 的设计意图是：**无论业务代码依赖哪门 API，最终只有 Logback 一台引擎在工作**：

```mermaid
flowchart TB
    subgraph APIS["业务代码可能使用的 4 门日志 API"]
        SLF4J["slf4j-api<br/>Logger / LoggerFactory"]
        L4J2["log4j-api<br/>org.apache.logging.log4j.LogManager"]
        JUL["java.util.logging<br/>Logger.getLogger()"]
        JCL["spring-jcl (Commons Logging)<br/>Spring 框架自身全部日志"]
    end

    subgraph BRIDGES["starter-logging 提供的桥接层"]
        B1["log4j-to-slf4j<br/>Log4j2 API -> SLF4J"]
        B2["jul-to-slf4j<br/>SLF4JBridgeHandler"]
    end

    subgraph ENGINE["唯一的日志引擎"]
        LB["logback-classic<br/>LoggerContext / Appender / TurboFilter"]
    end

    SLF4J -->|直接绑定| LB
    L4J2 --> B1 -->|转投| SLF4J
    JUL --> B2 -->|转投| SLF4J
    JCL -->|spring-jcl 运行时探测到 SLF4J 存在| SLF4J

    LB --> CONSOLE["ConsoleAppender<br/>(stdout)"]
    LB --> FILE["RollingFileAppender<br/>(可选, logging.file.name)"]
```

其中 `spring-jcl` 是 Spring Framework 的 `commons-logging` 兼容层（`org.apache.commons.logging.LogFactory`），它在运行时按 **Log4j2 -> SLF4J -> JUL** 的顺序探测可用实现--由于 starter-logging 只放入了 SLF4J（log4j2 只有 API 没有引擎），探测结果必然落在 SLF4J 上，于是 Spring 框架自身的全部日志也汇入 Logback。

**递归扩散机制**：`STARTERS/spring-boot-starter/build.gradle` 中 `api` 引入了 starter-logging：

```gradle
dependencies {
	api project(":spring-boot-starter-logging")
	api project(":spring-boot")
	...
}
```

而所有功能 starter（web、data-redis、devtools……）都依赖 spring-boot-starter，所以 starter-logging 是整个 starter 体系的"公共底座"--第十章、十一章里看到的所有日志输出，都由这张网络承接。想要换成 Log4j2 引擎，必须先 `exclude` 掉 starter-logging 再引入 `spring-boot-starter-log4j2`（见 12.16 坑位表）。

### 12.2 LoggingSystem：日志系统的统一抽象

Spring Boot 不想为每种日志引擎写一套初始化逻辑，于是在 `BOOT/logging/LoggingSystem.java` 里定义了抽象基类，它是整个日志整合的"操作面"：

| 抽象方法（LoggingSystem.java） | 作用 | 触发时机 |
|---|---|---|
| `beforeInitialize()` | 初始化前的"灭火"动作（Logback 实现为挂 DENY 过滤器） | `ApplicationStartingEvent` |
| `initialize(LoggingInitializationContext, LogFile)` | 完整初始化（加载配置/默认值） | `ApplicationEnvironmentPreparedEvent` |
| `cleanUp()` | 清理上下文资源 | 容器关闭/启动失败 |
| `setLogLevel(String, LogLevel)` | **运行时**修改某 logger 级别 | actuator `/actuator/loggers` 端点 |
| `getShutdownHandler()` | 返回 JVM 退出时的清理 Runnable | shutdown hook |

几个关键常量（`LoggingSystem.java`）：

- `SYSTEM_PROPERTY = LoggingSystem.class.getName()`（:44）--系统属性强制指定实现类全限定名；
- `NONE = "none"`（:50）--把该属性设为 `none` 时得到 `NoOpLoggingSystem`，完全关闭 Spring Boot 的日志管理；
- `ROOT_LOGGER_NAME = "ROOT"`（:57）--统一各框架的根 logger 名称（Log4J2/JUL 里根 logger 名是空串）。

工厂入口（`LoggingSystem.java:151-162`）：

```java
public static LoggingSystem get(ClassLoader classLoader) {
    String systemName = System.getProperty(SYSTEM_PROPERTY);
    if (StringUtils.hasLength(systemName)) {
        if (NONE.equals(systemName)) {
            return new NoOpLoggingSystem();
        }
        return instantiate(classLoader, systemName);   // 反射直建，绕过 SPI
    }
    return SYSTEM_FACTORY.getLoggingSystem(classLoader); // 否则走 SPI
}
```

`SYSTEM_FACTORY`（:59）是静态字段 `LoggingSystemFactory.fromSpringFactories()`--SPI 机制见下节。注意**优先级：系统属性 > spring.factories SPI**，这给了运维一个不改代码强制切换实现的逃生门（`-Dorg.springframework.boot.logging.LoggingSystem=none`）。

日志级别的统一枚举是 `LogLevel`（TRACE/DEBUG/INFO/WARN/ERROR/FATAL/OFF），各实现用 `LogLevels<T>` 双向映射表翻译成自己的原生级别，例如 Logback（`LogbackLoggingSystem.java:84-93`）：

```java
LEVELS = new LogLevels<>();
LEVELS.map(LogLevel.TRACE, Level.TRACE);
LEVELS.map(LogLevel.DEBUG, Level.DEBUG);
LEVELS.map(LogLevel.INFO, Level.INFO);
LEVELS.map(LogLevel.WARN, Level.WARN);
LEVELS.map(LogLevel.ERROR, Level.ERROR);
LEVELS.map(LogLevel.FATAL, Level.ERROR);  // Logback 无 FATAL，降级为 ERROR
LEVELS.map(LogLevel.OFF, Level.OFF);
```

`AbstractLoggingSystem`（`BOOT/logging/AbstractLoggingSystem.java`）把"解析配置文件位置"这一通用流程模板化，是下一节的主角。

### 12.3 LoggingSystemFactory SPI：classpath 上谁在场，谁就掌权

`RES/META-INF/spring.factories` 中注册了三个工厂（`LoggingSystemFactory` key）：

```properties
org.springframework.boot.logging.LoggingSystemFactory=\
	org.springframework.boot.logging.logback.LogbackLoggingSystem.Factory,\
	org.springframework.boot.logging.log4j2.Log4J2LoggingSystem.Factory,\
	org.springframework.boot.logging.java.JavaLoggingSystem.Factory
```

`LoggingSystemFactory.fromSpringFactories()`（`LoggingSystemFactory.java`）用 `SpringFactoriesLoader.loadFactories()` 装载这三个工厂，包一层 `DelegatingLoggingSystemFactory`：**按序逐个 `getLoggingSystem()`，第一个返回非 null 者胜出**。

每个 Factory 内部用静态 `PRESENT` 探测自己引擎是否在场（例：`LogbackLoggingSystem.java:419-433`）：

```java
@Order(Ordered.LOWEST_PRECEDENCE)
public static class Factory implements LoggingSystemFactory {
    private static final boolean PRESENT = ClassUtils
            .isPresent("ch.qos.logback.classic.LoggerContext", Factory.class.getClassLoader());
    @Override
    public LoggingSystem getLoggingSystem(ClassLoader classLoader) {
        if (PRESENT) {
            return new LogbackLoggingSystem(classLoader);
        }
        return null;   // 不在场就让贤
    }
}
```

三个工厂都是 `@Order(LOWEST_PRECEDENCE)`（Log4J2 见 `Log4J2LoggingSystem.java:503`，JUL 见 `JavaLoggingSystem.java:178`），排序退化为 **spring.factories 文件内的书写顺序**：Logback -> Log4J2 -> JUL。完整的仲裁流程：

```mermaid
flowchart TB
    START["LoggingSystem.get(classLoader)"] --> SYSPROP{"系统属性<br/>org.springframework.boot.logging.LoggingSystem<br/>已设置？"}
    SYSPROP -->|"值为 none"| NOOP["NoOpLoggingSystem<br/>关闭全部日志管理"]
    SYSPROP -->|"值为实现类全名"| REFL["反射实例化指定类"]
    SYSPROP -->|"未设置"| SPI["DelegatingLoggingSystemFactory<br/>（spring.factories 装载三个工厂）"]

    SPI --> F1{"classpath 有<br/>ch.qos.logback.classic.LoggerContext？"}
    F1 -->|有| LB["LogbackLoggingSystem<br/>（默认胜出者）"]
    F1 -->|无| F2{"classpath 有<br/>org.apache.logging.log4j.core.impl.Log4jContextFactory？"}
    F2 -->|有| L4J["Log4J2LoggingSystem<br/>（需换用 starter-log4j2）"]
    F2 -->|无| JUL["JavaLoggingSystem<br/>（JDK 自带, 保底）"]
```

默认依赖下（starter-logging 在 classpath），探测链在第一环就命中，**Logback 是默认实现**；只有当用户主动排除 starter-logging、换成 `spring-boot-starter-log4j2` 时，Log4J2 才上场；两者都被排除时 JUL 兜底--这保证了 Spring Boot 永远能初始化出"某个"日志系统。

### 12.4 LoggingApplicationListener：事件驱动的初始化总控

`BOOT/context/logging/LoggingApplicationListener.java` 是整个日志初始化的指挥官。它在 spring.factories 中注册为 `ApplicationListener`（`spring.factories:50`），监听五个事件（`:185-189`）：

| 事件 | 触发时机（见第二章 run() 流程） | 监听器动作 |
|---|---|---|
| `ApplicationStartingEvent` | `run()` 一开始、Environment 创建之前 | 探测 LoggingSystem + `beforeInitialize()`（挂静默过滤器） |
| `ApplicationEnvironmentPreparedEvent` | Environment 装配完毕 | `initialize()`--**日志初始化的主战场** |
| `ApplicationPreparedEvent` | 容器刷新前 | 把 LoggingSystem/LogFile/LoggerGroups 注册为单例 Bean |
| `ContextClosedEvent` | 容器关闭 | `cleanUp()`（经 Lifecycle 检查） |
| `ApplicationFailedEvent` | 启动失败 | `cleanUp()` |

它的 order 是 `DEFAULT_ORDER = Ordered.HIGHEST_PRECEDENCE + 20`（`:109`）。+20 而非 +0 是刻意的排程：必须排在 `EnvironmentPostProcessorApplicationListener`（HIGHEST+10，负责加载 application.yml/config data）之后，这样 `initialize()` 执行时 Environment 里才能读到 `logging.*` 配置项；又必须尽量靠前，让日志在其他 ApplicationEnvironmentPreparedEvent 监听器（如 background preinitializer）输出日志前就位。

`onApplicationEvent` 分发到五个私有方法，其中 `onApplicationStartingEvent`（`:236-239`）：

```java
private void onApplicationStartingEvent(ApplicationStartingEvent event) {
    this.loggingSystem = LoggingSystem.get(event.getSpringApplication().getClassLoader());
    this.loggingSystem.beforeInitialize();
}
```

注意这里 `LoggingSystem.get()` 被调用在 **run() 的最早时刻**--因为 Logback 在应用代码首次触发 `LoggerFactory.getLogger()` 时会自我初始化（读 logback.xml），Spring Boot 必须抢在这个时刻之前把 DENY 过滤器挂上（见 12.5）。

`ApplicationPreparedEvent` 阶段（`registerLoggingSystem` 等，`:249-265`）把三个对象以 Bean 形式注入容器：

- `springBootLoggingSystem`（`LOGGING_SYSTEM_BEAN_NAME`，`:127`）--供 actuator `/actuator/loggers` 端点运行时调用 `setLogLevel()`；
- `springBootLogFile`、`springBootLoggerGroups`--供端点展示文件位置与日志组。

### 12.5 beforeInitialize：静默过滤器与 JUL 桥接

`LogbackLoggingSystem.beforeInitialize()`（`LogbackLoggingSystem.java:120-128`）：

```java
@Override
public void beforeInitialize() {
    LoggerContext loggerContext = getLoggerContext();
    if (isAlreadyInitialized(loggerContext)) {
        return;
    }
    super.beforeInitialize();
    loggerContext.getTurboFilterList().add(FILTER);   // 挂全局静默过滤器
    configureJdkLoggingBridgeHandler();               // 安装 JUL 桥
}
```

`FILTER` 是一个静态 TurboFilter（`:95-103`），无条件返回 `FilterReply.DENY`。TurboFilter 是 Logback 的**全局拦截器**（在 Logger 调用入口处判定，先于 level 检查），它一挂上，从 `ApplicationStartingEvent` 到 `initialize()` 完成之间的一切日志调用都被静默丢弃--这就是"为什么 Banner 之前的启动日志一张都看不到"的真相：不是没打，是被 DENY 了。若不这么做，Environment 还没就绪时海量的 INFO 日志会以 Logback **自我初始化的默认格式**（读不到任何 Spring 配置）倾泻到控制台。

`configureJdkLoggingBridgeHandler()`（`:130-154`）安装 `SLF4JBridgeHandler`（jul-to-slf4j 的核心类）把 JUL 日志转投 SLF4J，但有个前置条件：

```java
private void configureJdkLoggingBridgeHandler() {
    LogManager julLogManager = LogManager.getLogManager();
    Logger rootLogger = julLogManager.getLogger("");
    if (rootLogger.getHandlers().length == 1
            && rootLogger.getHandlers()[0] instanceof ConsoleHandler) {
        // 只有当 JUL 根 logger 还是"出厂默认"（单个 ConsoleHandler）时才接管，
        // 说明用户没有自定义过 JUL；否则尊重用户配置，不动。
        SLF4JBridgeHandler.install();
        this.julBridgeHandlerInstalled = true;
    }
}
```

`isAlreadyInitialized` 通过在 LoggerContext 里查找以 `LoggingSystem.class.getName()` 为 key 的标记对象（`markAsInitialized`，`:394-404`）判断重复初始化--第十一章 devtools 的 RestartClassLoader 每次重启都会拿到全新的 LoggerContext，因此每次重启都会走完整初始化，这也是 `@RestartScope` 之外日志能"跟随重启"的原因。

### 12.6 initialize() 六步曲：主初始化流程

`ApplicationEnvironmentPreparedEvent` 到达后执行 `initialize()`（`LoggingApplicationListener.java:290-301`）：

```java
protected void initialize(ConfigurableEnvironment environment, ClassLoader classLoader) {
    getLoggingSystemProperties(environment).apply();   // ① Spring属性 -> 系统属性
    this.logFile = LogFile.get(environment);           // ② 解析日志文件位置
    if (this.logFile != null) {
        this.logFile.applyToSystemProperties();        //    写入 LOG_FILE/LOG_PATH
    }
    this.loggerGroups = new LoggerGroups(DEFAULT_GROUP_LOGGERS);  // ③ 内置日志组
    initializeEarlyLoggingLevel(environment);          // ④ 提前应用 --debug/--trace
    initializeSystem(environment, this.loggingSystem, this.logFile);  // ⑤ 核心:加载配置
    initializeFinalLoggingLevels(environment);         // ⑥ 应用 logging.level.*
    registerShutdownHookIfNecessary(environment);      // ⑦ 注册 shutdown hook
}
```

**① 系统属性翻译**（详见 12.9）：`LoggingSystemProperties.apply()` 把 `logging.pattern.console` 等 Spring 属性写进 System properties--因为 logback.xml 里的 `${CONSOLE_LOG_PATTERN}` 占位符只能解析**系统属性与 logback 上下文属性**，读不到 Spring Environment。这是两套配置体系之间的"翻译官"。

**② LogFile**（`BOOT/logging/LogFile.java`）：合并 `logging.file.name`（`FILE_NAME_PROPERTY`）与 `logging.file.path`（`FILE_PATH_PROPERTY`）。二者都未设置时返回 null（`:114-121`）；只有 path 时文件名取 `${LOG_PATH}/spring.log`。解析结果写入系统属性 `LOG_FILE`/`LOG_PATH`，并作为 `springBootLogFile` Bean 暴露。

**⑤ initializeSystem**（`:324-349`）从 Environment 读取 `logging.config`（显式指定配置文件路径），交给 `AbstractLoggingSystem.initialize()`；若加载抛异常，**不能**用 logger 报错（此时日志系统状态不明），只能 `System.err` 原样打印（`:344-347`，源码注释原话："We can't use the logger here"），这是日志子系统独特的错误处理姿态。

**⑥ initializeFinalLoggingLevels** -> `setLogLevels`（`:390-395`）用 Binder 把 `logging.level.*` 绑定为 `Map<String, LogLevel>`，再逐个 `configureLogLevel`：

```java
private void configureLogLevel(String name, LogLevel level) {
    LoggerGroup group = this.loggerGroups.get(name);
    if (group != null && group.hasMembers()) {
        group.configureLogLevel(level, this.loggingSystem::setLogLevel); // 组名:批量设置
        return;
    }
    this.loggingSystem.setLogLevel(name, level);  // 普通 logger 名
}
```

（`:397-406`）--先查日志组，命中则批量下发。特殊名称 `ROOT` 被翻译为 null（`:411`）以适配各引擎的根 logger 叫法。

**⑦ shutdown hook**（`:420-431`）：`logging.register-shutdown-hook`（默认 true）时调用 `SpringApplication.getShutdownHandlers()` 注册 `loggingSystem.getShutdownHandler()`--注意注册的是 **SpringApplication 自己的 shutdown handler 列表**（由 SpringApplication 的 JVM shutdown hook 统一触发），而非直接 `Runtime.addShutdownHook`；全局 `AtomicBoolean shutdownHookRegistered` 去重，防止多 SpringApplication 实例重复注册。

### 12.7 配置裁决三级制：logback.xml / logback-spring.xml / 编程式默认

整个初始化最精妙的部分是 `AbstractLoggingSystem.initializeWithConventions`（`AbstractLoggingSystem.java:69-84`）--当用户没有设置 `logging.config` 时，配置从哪里来：

```java
protected void initializeWithConventions(LoggingInitializationContext context, LogFile logFile) {
    String config = getSelfInitializationConfig();     // 标准 logback.xml
    if (config != null && logFile == null) {
        // logback 已自行加载过它，这里只做"接管后的补票"
        reinitialize(context);
        return;
    }
    if (config == null) {
        config = getSpringInitializationConfig();      // logback-spring.xml
    }
    if (config != null) {
        loadConfiguration(context, config, logFile);   // 用 SpringBootJoranConfigurator 解析
        return;
    }
    loadDefaults(context, logFile);                    // 编程式默认配置
}
```

候选位置的来源：`LogbackLoggingSystem.getStandardConfigLocations()` 返回 `{"logback-test.groovy","logback-test.xml","logback.groovy","logback.xml"}`（`:115-117`）；`getSpringConfigLocations()`（`AbstractLoggingSystem.java:128-136`）把每个名字的扩展名前插入 `-spring` 后缀，得到 `logback-spring.xml` 等变体：

```java
protected String[] getSpringConfigLocations() {
    String[] locations = getStandardConfigLocations();
    for (int i = 0; i < locations.length; i++) {
        String extension = StringUtils.getFilenameExtension(locations[i]);
        locations[i] = locations[i].substring(0,
                locations[i].length() - extension.length() - 1) + "-spring." + extension;
    }
    return locations;
}
```

三级裁决背后的原理差异（**这是面试高频点**）：

1. **`logback.xml`（self-initialization config）**：logback 自己认识它。应用代码第一次触发 `LoggerFactory.getLogger()` 时 logback 就会自动加载它完成自我初始化--往往早于 Spring Boot 的监听器执行。所以 Spring Boot 检测到它存在且 `logFile == null` 时**不重复加载**，只调 `reinitialize()` 补做 Spring 侧的收尾（应用 logger groups 等）。代价：它由 logback 原生 JoranConfigurator 解析，**不认识 `<springProfile>`/`<springProperty>`**，且解析时 Environment 尚未就绪。
2. **`logback-spring.xml`（spring initialization config）**：logback **不认识**这个名字，绝不会自我初始化；只有 Spring Boot 主动加载它，用扩展的 `SpringBootJoranConfigurator` 解析，因此可以使用 Spring 扩展标签。若同一目录下 `logback.xml` 与 `logback-spring.xml` 并存且 logFile 为 null：`getSelfInitializationConfig()` 命中后直接 `reinitialize` 返回，**logback-spring.xml 会被忽略**（logback.xml 优先）。
3. **都没有 -> `loadDefaults`**：Spring Boot 用**编程方式**构造一份等价于"约定优于配置"的默认配置（12.8）。

若同时设置了 `logging.file.name`（logFile != null），即使用户放了 `logback.xml`，也不能走 `reinitialize` 快捷路径--文件输出需要 Boot 侧的 LOG_FILE 属性参与，因此转入第 2/3 级正常加载。

```mermaid
flowchart TB
    START["AbstractLoggingSystem.initialize()"] --> SPECIFIC{"logging.config<br/>显式指定？"}
    SPECIFIC -->|是| LOAD_SPEC["loadConfiguration(指定文件)<br/>（支持扩展标签）"]
    SPECIFIC -->|否| SELF{"classpath 存在<br/>logback-test.xml / logback.xml？"}
    SELF -->|"存在 且 logFile==null"| REINIT["reinitialize()<br/>logback 早已自我初始化过<br/>只补 Spring 侧收尾<br/>⚠️ 不支持 springProfile/springProperty"]
    SELF -->|"存在 且 logFile!=null"| SPRING{"classpath 存在<br/>logback-spring.xml？"}
    SELF -->|不存在| SPRING
    SPRING -->|存在| LOAD_SPRING["loadConfiguration(logback-spring.xml)<br/>SpringBootJoranConfigurator 解析<br/>✅ 支持 springProfile / springProperty"]
    SPRING -->|不存在| DEFAULTS["loadDefaults()<br/>DefaultLogbackConfiguration<br/>编程式构造默认配置"]
    REINIT --> DONE
    LOAD_SPEC --> DONE
    LOAD_SPRING --> DONE
    DEFAULTS --> DONE["markAsInitialized + 移除 DENY 过滤器"]
```

`LogbackLoggingSystem.initialize()`（`:180-194`）在配置加载完成后收尾：移除 DENY TurboFilter（恢复输出）、`markAsInitialized(loggerContext)`（打标记防重入）、并对用户设置了 `logback.configurationFile` 系统属性的情形发出 WARN（`:190-193`）--因为 Boot 接管后 logback 的自动初始化早已被绕过，该属性不生效。

### 12.8 默认配置：DefaultLogbackConfiguration 与 XML 片段

`loadDefaults` 路径（`LogbackLoggingSystem.java:212-226`）会 stop 掉现有 LoggerContext、应用 Logback 专属系统属性、然后交给 `DefaultLogbackConfiguration`（`BOOT/logging/logback/DefaultLogbackConfiguration.java`）**纯 Java 代码**搭出默认配置：

```java
void apply(LogbackConfigurator config) {
    Defaults defaults = defaults();          // 转换规则 + 默认属性 + 静音表
    consoleAppender(defaults, config);       // 控制台输出:始终有
    if (this.logFile != null) {
        fileAppender(defaults, config);      // 文件输出:仅当设置了 logging.file.*
    }
    config.root(Level.INFO);                 // 根 logger 默认 INFO
}
```

（`:54-66`）几个值得逐条看的设计：

- **彩色与格式**（`defaults()`，`:68-92`）：注册三个自定义转换规则 `clr`（`ColorConverter`，ANSI 颜色）、`wex`/`wEx`（`WhitespaceThrowableProxyConverter`，异常堆栈紧凑/分行渲染），并把 `CONSOLE_LOG_PATTERN` 等属性以 `${CONSOLE_LOG_PATTERN:-默认值}` 的形式注册进 logback 上下文--**属性优先取系统属性（Spring Boot 已从 `logging.pattern.console` 写入），缺省才用内置默认格式**，形成"用户配置覆盖默认值"的链条。默认控制台格式即我们熟悉的那行带 PID、线程、彩色级别的 pattern（`PID` 占位符由 LoggingSystemProperties 写入系统属性）。
- **内置静音表**：Tomcat 的 `org.apache.catalina`、`org.apache.coyote`、`org.apache.tomcat`、Jetty、Hibernate validator 等十几个底层框架 logger 被显式置为 WARN，避免第三方启动噪声淹没业务日志。
- **滚动策略**（`setRollingPolicy`，`:118-131`）：`SizeAndTimeBasedRollingPolicy`，文件名模式 `${LOG_FILE}.%d{yyyy-MM-dd}.%i.gz`，单文件 10MB、保留 7 天--这就是不写任何配置时 spring.log 的滚动行为来源。

同一套默认值还有 **XML 形态**，位于 `RES/org/springframework/boot/logging/logback/`：`defaults.xml`（转换规则/属性/静音表）、`console-appender.xml`、`file-appender.xml`、以及三者合集 `base.xml`。它们供用户**在自己写的 logback.xml 里 `<include resource="..."/>`** 复用--编程式默认（Boot 兜底时用）与 XML 片段（用户定制时用）双形态同源，保证两条路径的行为一致。

`loadConfiguration`（`:229-253`）负责解析用户指定的 xml：以 `SpringBootJoranConfigurator.doConfigure(url)` 解析，并**收集 logback Status 中的 ERROR**（logback 解析出错通常只往 StatusManager 写记录而不抛异常）聚合成异常重新抛出，让"日志配置写错"能以启动失败的形式暴露，而不是无声降级。

### 12.9 属性翻译链：Spring Environment -> 系统属性 -> 日志框架

`LoggingSystemProperties.apply()`（`BOOT/logging/LoggingSystemProperties.java:137-149`）是 Spring 配置体系与日志框架配置体系之间的翻译官。翻译对照表：

| Spring 属性（application.yml） | 写入的系统属性 | 默认值 | 用途 |
|---|---|---|---|
| `logging.exception-conversion-word` | `LOG_EXCEPTION_CONVERSION_WORD` | `%wEx` | 异常渲染转换符 |
| -（自动探测） | `PID` | 当前进程号 | 日志格式中的进程占位 |
| `logging.pattern.console` | `CONSOLE_LOG_PATTERN` | 内置默认格式 | 控制台 pattern |
| `logging.charset.console` | `CONSOLE_LOG_CHARSET` | 平台默认 | 控制台字符集 |
| `logging.pattern.dateformat` | `LOG_DATEFORMAT_PATTERN` | `yyyy-MM-dd HH:mm:ss.SSS` | 日期格式 |
| `logging.pattern.file` | `FILE_LOG_PATTERN` | 内置默认格式 | 文件 pattern |
| `logging.charset.file` | `FILE_LOG_CHARSET` | 平台默认 | 文件字符集 |
| `logging.pattern.level` | `LOG_LEVEL_PATTERN` | `%5p` | 级别列样式 |
| `logging.file.name` / `logging.file.path` | `LOG_FILE` / `LOG_PATH` | - | 文件位置 |

每个 setter 遵守"**只在系统属性尚不存在时才写**"（`:93-97`）--同一 JVM 里第二个 SpringApplication 启动时不会覆盖第一次的属性，这是多应用共存的基本保证，也意味着"后启动的子上下文改 pattern 不生效"（见坑位表）。

`LogbackLoggingSystemProperties`（`BOOT/logging/logback/LogbackLoggingSystemProperties.java`）追加 Logback 专属翻译，把 `logging.logback.rollingpolicy.*`（file-name-pattern/clean-history-on-start/max-file-size/total-size-cap/max-history）映射为 `LOGBACK_ROLLINGPOLICY_*` 系统属性（兼容读取已废弃的 `logging.file.max-*`），并顺手在 classpath 有 JBoss Logging 时设置 `org.jboss.logging.provider=slf4j`（把 Hibernate 的日志也焊死在 SLF4J 上，堵住最后一个"绕开 starter-logging 汇聚网络"的出口）。

翻译链全景：

```mermaid
flowchart LR
    subgraph ENV["Spring Environment<br/>（application.yml / 命令行 / 环境变量）"]
        P1["logging.pattern.console=..."]
        P2["logging.file.name=app.log"]
        P3["logging.logback.rollingpolicy.max-file-size=..."]
    end

    TR["LoggingSystemProperties.apply()<br/>LoggingApplicationListener.initialize() 第①步"]

    subgraph SYS["JVM 系统属性"]
        S1["CONSOLE_LOG_PATTERN"]
        S2["LOG_FILE"]
        S3["LOGBACK_ROLLINGPOLICY_MAX_FILE_SIZE"]
        S4["PID"]
    end

    XML["logback.xml / logback-spring.xml<br/>${CONSOLE_LOG_PATTERN:-默认}<br/>${LOG_FILE} 等占位符"]

    P1 & P2 & P3 --> TR
    TR --> S1 & S2 & S3 & S4
    SYS -->|"logback 占位符解析<br/>（优先系统属性, 缺省用上下文默认值）"| XML
```

### 12.10 Spring 扩展标签：`<springProfile>` 与 `<springProperty>`

`SpringBootJoranConfigurator`（`BOOT/logging/logback/SpringBootJoranConfigurator.java`）继承 logback 的 `JoranConfigurator`，在其规则仓库里追加三条规则（`addElementSelectorAndActionAssociations`，`:97-102`）：

```java
ruleStore.addRule(new ElementSelector("configuration/springProperty"), SpringPropertyAction::new);
ruleStore.addRule(new ElementSelector("*/springProfile"), SpringProfileAction::new);
ruleStore.addTransparentPathPart("springProfile");  // 允许 springProfile 包裹任意节点
```

两个标签的语义：

- **`<springProperty scope="context" name="appName" source="spring.application.name" defaultValue="unknown"/>`**--从 Spring Environment 取属性注入 logback 上下文。执行体在 `SpringPropertyModelHandler`：`this.environment.getProperty(source, defaultValue)` 后 `ModelUtil.setProperty(context, name, value)`。之后 logback 配置里即可写 `<pattern>${appName}</pattern>`，实现"日志格式直接引用 Spring 配置"。
- **`<springProfile name="prod">...</springProfile>`**--按激活的 profile 条件启用其包裹的任意配置节点（appender、logger……）。执行体在 `SpringProfileModelHandler`：`environment.acceptsProfiles(Profiles.of(profileNames))`，不接受则把该模型标记为跳过--整个子树不生效。

**为什么这两个标签只在 `logback-spring.xml` 里可用**：普通 `logback.xml` 由 logback 自我初始化，走的是原生 `JoranConfigurator`，规则仓库里没有这两条规则（遇到会报"no applicable action"）；而且自我初始化发生时 Environment 往往还没装配，`environment` 引用无从谈起。`-spring` 后缀让文件名对 logback"隐形"，强制由 Boot 在 Environment 就绪后亲手解析。

### 12.11 日志组与运行时级别管理

`LoggerGroups`（`BOOT/logging/LoggerGroups.java`）是"逻辑名 -> 一组 logger 名"的注册表。除了用户通过 `logging.group.xxx=a.b,c.d` 自定义，Boot 内置两组（`LoggingApplicationListener.java:146-158`）：

```java
loggers.add("web", "org.springframework.core.codec");
loggers.add("web", "org.springframework.http");
loggers.add("web", "org.springframework.web");
loggers.add("web", "org.springframework.boot.actuate.endpoint.web");
loggers.add("web", "org.springframework.boot.web.servlet.ServletContextInitializerBeans");
loggers.add("sql", "org.springframework.jdbc.core");
loggers.add("sql", "org.hibernate.SQL");
loggers.add("sql", "org.jooq.tools.LoggerListener");
```

于是 `logging.level.web=DEBUG` 一行即可打开整个 Web 层明细，无需逐个点名。

`--debug` / `--trace` 命令行开关则走 `SPRING_BOOT_LOGGING_LOGGERS` 预设表（`:160-172`）：debug 打开 `sql`、`web`、`org.springframework.boot`；trace 进一步打开 `org.springframework`、Tomcat/Catalina/Jetty、Hibernate DDL--注意它**不提升根 logger 级别**，只精准放行"框架诊断"相关的 logger，避免全局 TRACE 噪声。

运行时级别管理依赖 12.4 注册的 `springBootLoggingSystem` Bean：actuator 的 `/actuator/loggers` 端点 GET 读取 `getLoggerConfigurations()`，POST `{ "configuredLevel": "DEBUG" }` 直接调 `setLogLevel()`--不重启、立即生效（devtools 重启场景下因 LoggerContext 全新，级别回到配置值）。

### 12.12 生命周期收尾：Lifecycle、cleanUp 与 DeferredLog

容器关闭时日志不能先死--Web 服务器停机过程中的日志还要写。为此 `ApplicationPreparedEvent` 阶段额外注册了内部类 `Lifecycle`（`LoggingApplicationListener.java:460-486`）：

```java
class Lifecycle implements SmartLifecycle {
    private final LoggingSystem loggingSystem;
    private Runnable cleanUp;
    @Override
    public int getPhase() {
        return Integer.MIN_VALUE + 1;   // 在几乎所有 SmartLifecycle 之后 stop（含 WebServer）
    }
    @Override
    public void stop() {
        this.cleanUp = this.loggingSystem::cleanUp;
    }
    ...
}
```

`ContextClosedEvent` 到达时监听器检查 `Lifecycle` 是否已 stop（即容器正常走完关闭流程），是则把 `cleanUp` 推迟到**容器 refresh 之后的 cleanup 阶段**执行；异常路径（`ApplicationFailedEvent`）则立即 `cleanUp()`。Logback 的 `cleanUp` + shutdown handler 最终都会调 `LoggerContext.stop()`--顺带触发 `stopAndReset`（`:267-289`）里挂的 `LevelChangePropagator`：若装过 JUL 桥，把 SLF4J 侧的级别变化**反向传播**回 JUL，保证桥接期间两边级别一致。

**DeferredLog**（`BOOT/logging/DeferredLog.java`）解决"日志系统就绪之前就要打日志"的鸡生蛋问题：它实现 `org.apache.commons.logging.Log` 接口，`info()/warn()/error()`（`:107-162`）只把消息缓存进队列；待日志系统初始化完成后调用 `switchTo(Class)` / `switchTo(Log)`（`:186-187`）把队列**原样回放**到真正的 logger。spring-boot 自身在嵌入式容器启动、`Restarter`（第十一章 11.7 的 LeakSafeThread 场景）等"早于日志就绪"的组件里大量使用它。

### 12.13 平行实现：Log4J2 与 JUL

`Log4J2LoggingSystem`（`BOOT/logging/log4j2/Log4J2LoggingSystem.java`）展示了抽象的可扩展性--Log4J2 没有类似 logback Joran 的"规则注入"口，Boot 换了一套等价机制：

- **配置位置**（`:136-160`）：`log4j2-test/….{properties,xml,yaml,json}` + `log4j2-spring.*` 变体，yaml/json 需对应 jackson 数据格式模块在场，另读取系统属性 `log4j2.configurationFile`；
- **Spring 扩展**：`SpringBootConfigurationFactory`（把 Spring Environment 包装成 Log4J2 的 `PropertySource`）、`SpringEnvironmentLookup`（`${spring:属性名}` 语法，等价于 `<springProperty>`）、`SpringProfileArbiter`（`<SpringProfile>` 节点，等价于 `<springProfile>`）--三件套与 Logback 的 Joran 扩展一一对应；
- **默认配置**：`RES/org/springframework/boot/logging/log4j2/log4j2.xml`，用 `${sys:CONSOLE_LOG_PATTERN}` 等 lookup 消费同一批系统属性--**属性翻译链（12.9）是跨引擎共享的**，这正是把翻译逻辑放在 `LoggingSystemProperties` 而非 Logback 类里的原因。

`JavaLoggingSystem`（`BOOT/logging/java/JavaLoggingSystem.java`）是最简实现：配置位置只有 `logging.properties`（`:76-78`），默认值通过 jar 内打包的 `logging.properties` / `logging-file.properties` 资源加载。JUL 无法关闭、无法热重载格式，仅作兜底。

### 12.14 AOT：原生镜像下的日志初始化

Spring Boot 3.0 的 AOT 原生镜像不支持运行时反射解析 XML，`LogbackLoggingSystem` 为此新增双通道（`:196-209`、`:407-414`）：

- **构建期** `processAheadOfTime()`：用 `SpringBootJoranConfigurator` 正常解析 `logback-spring.xml`，把 logback 内部的 **Model 树序列化**到 `META-INF/spring/logback-model`，自定义 Pattern 规则序列化到 `META-INF/spring/logback-pattern-rules`，并注册相应序列化/反射 hints；
- **运行期** `initializeFromAotGeneratedArtifactsIfPossible()`：优先读这两份产物直接重建配置，跳过所有 XML 解析与 springProfile 判定（构建期已按激活 profile 求值完毕--这符合 AOT"构建时定型"的哲学）。

### 12.15 完整初始化时序图

```mermaid
sequenceDiagram
    autonumber
    participant S as SpringApplication.run()
    participant L as LoggingApplicationListener<br/>(HIGHEST+20)
    participant LS as LoggingSystem<br/>(LogbackLoggingSystem)
    participant E as ConfigurableEnvironment
    participant LC as LoggerContext<br/>(logback引擎)
    participant CTX as ApplicationContext

    Note over S: run() 开始
    S->>L: ApplicationStartingEvent
    L->>LS: LoggingSystem.get(classLoader)<br/>spring.factories SPI 仲裁 -> Logback
    L->>LS: beforeInitialize()
    LS->>LC: addTurboFilter(DENY)  🔇 静默期开始
    LS->>LS: configureJdkLoggingBridgeHandler()<br/>SLF4JBridgeHandler.install()

    Note over S: 创建并装配 Environment<br/>(config data 加载完毕)
    S->>L: ApplicationEnvironmentPreparedEvent
    L->>E: 读取 logging.* 属性
    L->>LS: initialize()
    LS->>LS: ① LoggingSystemProperties.apply()<br/>logging.pattern.* -> CONSOLE_LOG_PATTERN 等系统属性
    LS->>LS: ② LogFile.get(env)<br/>logging.file.name/path -> LOG_FILE/LOG_PATH
    LS->>LS: ③ LoggerGroups(web / sql)
    LS->>LS: ④ --debug/--trace 预设级别
    alt logging.config 已设置
        LS->>LS: loadConfiguration(指定文件)
    else 存在 logback.xml 且 logFile==null
        LS->>LC: reinitialize()（logback 早已自我初始化）
    else 存在 logback-spring.xml
        LS->>LC: SpringBootJoranConfigurator.doConfigure(url)<br/>（解析 springProfile/springProperty）
    else 都没有
        LS->>LC: DefaultLogbackConfiguration.apply()<br/>console(+file) appender, root=INFO
    end
    LS->>LC: removeTurboFilter(DENY) 🔊 恢复输出
    LS->>LC: markAsInitialized()
    LS->>LS: ⑥ setLogLevels(env)<br/>logging.level.* / ROOT / 日志组
    LS->>LS: ⑦ registerShutdownHookIfNecessary

    S->>L: ApplicationPreparedEvent
    L->>CTX: 注册单例 springBootLoggingSystem<br/>springBootLogFile / springBootLoggerGroups / Lifecycle

    Note over S: refreshContext() ... 容器运行
    CTX-->>LS: 运行时: actuator /actuator/loggers<br/>GET/POST setLogLevel()

    Note over S: 容器关闭
    CTX->>LS: SmartLifecycle.stop()<br/>(phase=MIN_VALUE+1, 在 WebServer 之后)
    S->>L: ContextClosedEvent
    L->>LS: cleanUp()
    Note over LS: JVM 退出: shutdown handler<br/>LoggerContext.stop()
```

### 12.16 常见坑位表

| 坑 | 现象 | 根因（源码依据） |
|---|---|---|
| `<springProfile>` 写在 `logback.xml` 里不生效还报错 | "no applicable action for [springProfile]" | 原生 JoranConfigurator 无此规则（12.10）；必须改名 `logback-spring.xml` |
| `logback.xml` 与 `logback-spring.xml` 并存，后者被忽略 | 只见前者格式 | `AbstractLoggingSystem.java:69-84`：self-init 命中且 logFile==null 直接 `reinitialize` 返回 |
| 设置 `logback.configurationFile` 系统属性不生效 | 启动时打 WARN | `LogbackLoggingSystem.java:190-193`：Boot 接管后 logback 自动初始化被绕过，该属性失效 |
| `logging.config` 指向的文件写错只看到裸异常 | 没有 Spring 风格错误报告 | `LoggingApplicationListener.java:344-347`：此时不能用 logger，只能 System.err |
| 启动最早期（Banner 前）看不到任何日志 | 无输出 | DENY TurboFilter 静默期（12.5），属设计行为 |
| 同 JVM 第二个 SpringApplication 改 pattern 无效 | 仍是第一次的格式 | `LoggingSystemProperties.java:93-97`：系统属性"已存在则不覆盖" |
| 想用 Log4J2 却两套引擎并存、行为混乱 | SLF4J 多实现绑定告警 | 未 exclude starter-logging；仲裁链（12.3）按 classpath 在场优先级而非用户意图 |
| `logging.level` 设 FATAL 无效 | 不被识别/降级为 ERROR | `LogLevel` 有 FATAL，但 Logback 侧映射为 ERROR（`LogbackLoggingSystem.java:84-93`） |
| JUL 自定义过 handler 后 JUL 日志没进 Logback | 双通道输出或丢失 | `LogbackLoggingSystem.java:150-154`：仅当 JUL 根 logger 仍是单 ConsoleHandler 才装桥 |
| devtools 重启后运行时改的日志级别丢了 | 回到配置值 | 每次 RestartClassLoader 全新 LoggerContext（12.5 `isAlreadyInitialized`） |

### 12.17 设计精髓总结

1. **依赖即架构**：starter-logging 三行依赖构成"门面 + 桥 + 单引擎"汇聚网络，把 Java 四大日志 API 的历史包袱收敛为唯一事实来源；`spring-jcl` 的运行时探测和 `org.jboss.logging.provider=slf4j` 则把最后两个旁路也焊死--整合靠的不是代码，是依赖拓扑。
2. **SPI 仲裁而非硬编码**：`LoggingSystemFactory` + spring.factories + PRESENT 探测，让"用哪个引擎"由 classpath 自然裁决，新增引擎零改动核心（只注册一个工厂）；系统属性留了一个强制的逃生门（`none`）。
3. **事件驱动的时机编排**：日志初始化横跨 `SpringApplication.run()` 的五个阶段，`HIGHEST_PRECEDENCE+20` 的 order 精确卡在 config data 加载之后、其余监听器输出之前；关闭时用 `SmartLifecycle(MIN_VALUE+1)` 保证"最后死"。
4. **静默期 + 自我初始化的识别**：DENY TurboFilter 挡住"日志系统的日志黑洞期"，`markAsInitialized` 标记识别 devtools 重启等重复初始化--两个小机制合起来缓解了"初始化日志系统本身也要打日志"的自指难题（配合 DeferredLog 的缓存回放彻底闭环）。
5. **两套配置体系的翻译官模式**：Spring Environment 与日志框架各说各话，`LoggingSystemProperties` 用"只写不存在的系统属性"这一幂等规则把前者翻译成后者能懂的占位符来源，且翻译层跨引擎复用。
6. **三级配置裁决暴露扩展边界**：`logback.xml`（自我初始化，快而无扩展）/ `logback-spring.xml`（Boot 解析，支持 Spring 扩展标签）/ 编程式默认（零配置兜底）--文件名的一个 `-spring` 后缀，实质是"谁负责解析"的契约分界。
7. **AOT 延续同一套语义**：构建期序列化 Model 树、运行期直接重建，把"XML 解析 + profile 判定"整体前移到构建期，日志成为 3.0 AOT 化改造的范本。

## 十三、spring-boot-starter-cache 实现原理：缓存抽象整合与 CacheManager 自动装配

> 本章路径缩写约定（在文首 `BOOT`/`AC`/`AUTO` 基础上补充）：
> - **CACHE** = `spring-boot-project/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/cache`
> - **STARTERS** = `spring-boot-project/spring-boot-starters`
> - **SCTX** = 外部依赖 `org.springframework:spring-context`（缓存抽象的真正所在，Spring Boot 3.0.0 托管版本为 Spring Framework 6.0.2）
> - **SCTX-SUP** = 外部依赖 `org.springframework:spring-context-support`（Caffeine/EhCache 等适配层）
> - **SDR** = 外部依赖 `spring-data-redis`（同第十章）
>
> 核心源文件（本仓库内）：
> - `STARTERS/spring-boot-starter-cache/build.gradle`
> - `CACHE/CacheAutoConfiguration.java`（总装配）
> - `CACHE/CacheConfigurations.java`、`CacheType.java`、`CacheCondition.java`、`CacheProperties.java`
> - `CACHE/` 下 10 个 `*CacheConfiguration.java` 实现配置类
>
> 与 Redis 章节（第十章）同构的故事：**Spring Boot 自身不含任何缓存数据结构**——AOP 拦截、CacheManager SPI 全部来自 Spring Framework 的缓存抽象，Spring Boot 只做一件事：**在启动期根据 classpath 与配置仲裁出唯一的 CacheManager Bean**。本章两条主线：**装配期仲裁**（13.1–13.12）与**运行期拦截**（13.13）。

### 13.1 starter 的真相：两行依赖的分工

`STARTERS/spring-boot-starter-cache/build.gradle` 全文：

```gradle
dependencies {
	api(project(":spring-boot-project:spring-boot-starters:spring-boot-starter"))
	api("org.springframework:spring-context-support")
}
```

| 依赖 | 提供什么 |
|---|---|
| `spring-boot-starter`（传递） | `spring-context`：`@EnableCaching`、`CacheInterceptor`、`ConcurrentMapCacheManager`、`NoOpCacheManager`、`SimpleCacheManager` 等缓存抽象核心（`org.springframework.cache.*` 包） |
| `spring-context-support` | `CaffeineCacheManager`、`EhCacheCacheManager` 等**第三方适配层**（`org.springframework.cache.caffeine/ehcache` 包） |

也就是说**不加这个 starter，`@EnableCaching` + ConcurrentMap 缓存照样能跑**（spring-context 随 spring-boot 必然在场）；starter-cache 的增量价值是把 spring-context-support 拉进来，让 Caffeine 等适配类对 classpath 探测可见（第十章 starter-redis 的条件注解需要 `RedisOperations` 类可见是同一逻辑）。而 `CacheAutoConfiguration` 注册在 `spring-boot-autoconfigure` 的 `META-INF/spring/...AutoConfiguration.imports:5`，随任何 Spring Boot 应用自动在场。

### 13.2 运行基石：Spring Framework 缓存抽象（外部依赖速览）

Spring Boot 装配的一切对象都来自 SCTX 的这套 SPI（无本仓库行号，按类名描述）：

```mermaid
flowchart TB
    subgraph AOP["SCTX: AOP 拦截层（@EnableCaching 开启）"]
        EC["@EnableCaching<br/>-> Import ProxyCachingConfiguration"]
        CI["CacheInterceptor<br/>(extends CacheAspectSupport)<br/>MethodInterceptor"]
        COS["CacheOperationSource<br/>解析 @Cacheable/@CachePut/@CacheEvict<br/>的方法元数据"]
    end

    subgraph SPI["SCTX: 缓存 SPI"]
        CM["CacheManager<br/>getCache(name)"]
        C["Cache<br/>get/put/evict/clear"]
        CR["CacheResolver<br/>(可选,解析用哪个 Cache)"]
    end

    subgraph IMPL["各引擎的 CacheManager 实现"]
        C1["ConcurrentMapCacheManager<br/>(spring-context)"]
        C2["CaffeineCacheManager<br/>(spring-context-support)"]
        C3["RedisCacheManager<br/>(spring-data-redis)"]
        C4["JCacheCacheManager / HazelcastCacheManager<br/>CouchbaseCacheManager / ..."]
    end

    EC --> CI
    CI --> COS
    CI -->|"KeyGenerator 生成 key"| CM
    CM -->|"按名字取"| C
    CI -.->|"可替换默认解析"| CR
    CR --> CM
    CM -.-> C1 & C2 & C3 & C4
```

运行分工：**AOP 层负责“何时读写缓存”**（方法调用前后），**CacheManager 负责“缓存放哪、怎么放”**（数据结构与过期策略）。Spring Boot 的全部工作就是为虚线下方挑选并造出唯一一个实现。

### 13.3 CacheAutoConfiguration：四个条件决定的“总开关”

`CACHE/CacheAutoConfiguration.java` 的类级注解（`:56-62`）是全章的纲：

```java
@AutoConfiguration(after = { CouchbaseDataAutoConfiguration.class, HazelcastAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class, RedisAutoConfiguration.class })
@ConditionalOnClass(CacheManager.class)
@ConditionalOnBean(CacheAspectSupport.class)
@ConditionalOnMissingBean(value = CacheManager.class, name = "cacheResolver")
@EnableConfigurationProperties(CacheProperties.class)
@Import({ CacheConfigurationImportSelector.class, CacheManagerEntityManagerFactoryDependsOnPostProcessor.class })
public class CacheAutoConfiguration {
```

逐条解读：

- **`@ConditionalOnClass(CacheManager.class)`**（:58）：spring-context 在场（必然满足，防御性注解）；
- **`@ConditionalOnBean(CacheAspectSupport.class)`**（:59）：**最关键的门**。`CacheAspectSupport` 是 `CacheInterceptor` 的父类，只有用户写了 `@EnableCaching`（它 `@Import` 的 `ProxyCachingConfiguration` 才注册 `CacheInterceptor` Bean）这个 Bean 才存在。**没写 @EnableCaching，整个缓存自动装配直接跳过**——这是“声明式开关”而非“开关式声明”；
- **`@ConditionalOnMissingBean(value = CacheManager.class, name = "cacheResolver")`**（:60）：用户已自定义 CacheManager、**或**定义了名为 `cacheResolver` 的 Bean（走 CacheResolver 路线）时整体退位；
- **`@AutoConfiguration(after = …)`**（:56-57）：必须在 Redis/Hazelcast/Couchbase/JPA 自动配置**之后**求值——这些配置负责创建 `RedisConnectionFactory`/`HazelcastInstance` 等“原料”Bean，而 13.5 的仲裁要用 `@ConditionalOnBean` 探测它们，顺序颠倒会导致永远探测不到；
- **`@Import`**（:62）：全量导入 10 个实现配置类（13.4）+ 一个 JPA 依赖编排后处理器（13.12）。

类体内只有两个 Bean：

```java
@Bean
@ConditionalOnMissingBean
public CacheManagerCustomizers cacheManagerCustomizers(ObjectProvider<CacheManagerCustomizer<?>> customizers) {
    return new CacheManagerCustomizers(customizers.orderedStream().toList());   // :65-69
}

@Bean
public CacheManagerValidator cacheAutoConfigurationValidator(CacheProperties cacheProperties,
        ObjectProvider<CacheManager> cacheManager) {
    return new CacheManagerValidator(cacheProperties, cacheManager);            // :71-75
}
```

`CacheManagerValidator`（内部类，`:92-110`）是**仲裁失败的报警器**：`afterPropertiesSet()` 断言容器里最终存在 CacheManager，否则抛出：

```java
Assert.notNull(this.cacheManager.getIfAvailable(),
        () -> "No cache manager could be auto-configured, check your configuration (caching type is '"
                + this.cacheProperties.getType() + "')");    // :104-108
```

（用 `ObjectProvider` 延迟获取：Validator 自身初始化时 CacheManager 可能尚未创建，取的是“最终态”。）

### 13.4 ImportSelector 全量导入：10 个配置类的“自由竞争”

类级 `@Import(CacheConfigurationImportSelector.class)` 的实现（`:115-127`）短得出奇：

```java
static class CacheConfigurationImportSelector implements ImportSelector {
    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        CacheType[] types = CacheType.values();
        String[] imports = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            imports[i] = CacheConfigurations.getConfigurationClass(types[i]);
        }
        return imports;
    }
}
```

它把 `CacheType` 枚举的**全部 10 个值**映射成 10 个配置类一次性导入（映射表在 `CacheConfigurations.java:36-49` 的静态 EnumMap）。为什么不像 Redis 章节那样“一个自动配置类内部用 @ConditionalOnProperty 分支”，而要 10 个独立配置类？

因为**优先级是“排列出来的”而不是“判断出来的”**：

1. 10 个配置类按 `CacheType.values()` 的枚举声明顺序注册成 10 个 `@Configuration`；
2. 每个类都有 `@ConditionalOnMissingBean(CacheManager.class)`；
3. 容器按注册顺序处理配置类，**排在前面的先创建 cacheManager Bean，后面的因“Bean 已存在”全部短路**；
4. 于是一份枚举声明顺序（`CacheType.java:27-79`）就同时是**自动探测优先级表**（见 13.5 注释原文："defined in order of precedence"）。

```java
public enum CacheType {
    GENERIC, JCACHE, HAZELCAST, COUCHBASE, INFINISPAN,
    REDIS, CACHE2K, CAFFEINE, SIMPLE, NONE
}
```

这个设计的精妙在于**“仲裁逻辑零代码”**：不需要任何 if-else 去判断“classpath 有什么”，只要让每个配置类自己声明“我需要什么”，加上统一的 `@ConditionalOnMissingBean(CacheManager)` 排他锁，注册顺序就自动完成裁决。

### 13.5 优先级仲裁矩阵：谁的条件下谁就上

10 个配置类的条件组合全景（行号均为 `CACHE/` 下对应文件）：

| 序 | CacheType | 配置类 | 类级条件（除共同的 `@ConditionalOnMissingBean(CacheManager)` + `@Conditional(CacheCondition)` 外） | 产物 |
|---|---|---|---|---|
| 1 | GENERIC | GenericCacheConfiguration | `@ConditionalOnBean(Cache.class)`（:37） | `SimpleCacheManager`（装用户自定义的 Cache Bean 集合） |
| 2 | JCACHE | JCacheCacheConfiguration | `@ConditionalOnClass({Caching.class, JCacheCacheManager.class})` + JCacheAvailableCondition（:57-61） | `JCacheCacheManager`（包 JSR-107 provider 的 CacheManager） |
| 3 | HAZELCAST | HazelcastCacheConfiguration | `@ConditionalOnClass({HazelcastInstance, HazelcastCacheManager})` + `@ConditionalOnSingleCandidate(HazelcastInstance)`（:43-47） | `HazelcastCacheManager` |
| 4 | COUCHBASE | CouchbaseCacheConfiguration | `@ConditionalOnClass({Cluster, CouchbaseClientFactory, CouchbaseCacheManager})` + `@ConditionalOnSingleCandidate(CouchbaseClientFactory)`（:43-47） | `CouchbaseCacheManager` |
| 5 | INFINISPAN | InfinispanCacheConfiguration | `@ConditionalOnClass(SpringEmbeddedCacheManager)`（:46-49） | `SpringEmbeddedCacheManager` |
| 6 | REDIS | RedisCacheConfiguration | `@ConditionalOnClass(RedisConnectionFactory)` + `@ConditionalOnBean(RedisConnectionFactory)`（:47-51） | `RedisCacheManager` |
| 7 | CACHE2K | Cache2kCacheConfiguration | `@ConditionalOnClass({Cache2kBuilder, SpringCache2kCacheManager})`（:40-43） | `SpringCache2kCacheManager` |
| 8 | CAFFEINE | CaffeineCacheConfiguration | `@ConditionalOnClass({Caffeine, CaffeineCacheManager})`（:41-44） | `CaffeineCacheManager` |
| 9 | SIMPLE | SimpleCacheConfiguration | **无额外条件**（:33-35） | `ConcurrentMapCacheManager` |
| 10 | NONE | NoOpCacheConfiguration | **无额外条件**（:31-33） | `NoOpCacheManager`（禁用缓存的空实现） |

自动探测（未设 `spring.cache.type`）时的裁决流程：

```mermaid
flowchart TB
    S["CacheAutoConfiguration 生效前提<br/>@EnableCaching 已开 + 无自定义 CacheManager/cacheResolver"] --> IMP["ImportSelector 按枚举序导入 10 个配置类"]

    IMP --> G{"1. 容器里有用户自定义<br/>Cache 实现 Bean？"}
    G -->|有| GEN["GenericCacheConfiguration<br/>SimpleCacheManager"]
    G -->|无| J{"2. JSR-107：Caching API 在场<br/>且恰好一个 provider？"}
    J -->|是| JC["JCacheCacheConfiguration"]
    J -->|否| H{"3. HazelcastInstance 在场？"}
    H -->|是| HZ["HazelcastCacheConfiguration"]
    H -->|否| CB{"4. CouchbaseClientFactory 在场？"}
    CB -->|是| CBS["CouchbaseCacheConfiguration"]
    CB -->|否| INF{"5. Infinispan 类在场？"}
    INF -->|是| IS["InfinispanCacheConfiguration"]
    INF -->|否| R{"6. RedisConnectionFactory Bean 在场？<br/>（第十章 RedisAutoConfiguration 的产物）"}
    R -->|是| RD["RedisCacheConfiguration<br/>RedisCacheManager"]
    R -->|否| C2{"7. cache2k 类在场？"}
    C2 -->|是| CK["Cache2kCacheConfiguration"]
    C2 -->|否| CF{"8. Caffeine 类在场？<br/>（spring-context-support 提供）"}
    CF -->|是| CA["CaffeineCacheConfiguration"]
    CF -->|否| SMP["9. SimpleCacheConfiguration<br/>ConcurrentMapCacheManager（兜底）"]

    style SMP fill:#e8f5e9
```

排到第 9 位的 SIMPLE 没有任何 classpath 条件，**永远成立**，因此是“什么都不装”时的最终兜底（一个 `ConcurrentHashMap` 本地缓存）。NONE 排在最后且条件同样恒真，**只有 `spring.cache.type=none` 显式点名时才会赢**——因为此时排在前面的 9 个全部被 CacheCondition 否决（见 13.6）。

值得注意的细节：**顺序即语义**。GENERIC 排第一意味着“用户手工造好的 Cache Bean 永远优先于任何自动探测”；REDIS 排在 CAFFEINE 之前意味着“同时引了 redis 和 caffeine 依赖时默认选分布式缓存”——这些倾向都固化在枚举声明顺序里。

### 13.6 CacheCondition：`spring.cache.type` 的精确制导

每个配置类都挂 `@Conditional(CacheCondition.class)`，它实现了“用户点名”与“自动探测”的分岔（`CacheCondition.java:41-61`）：

```java
public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
    ...
    try {
        BindResult<CacheType> specified = Binder.get(environment).bind("spring.cache.type", CacheType.class);
        if (!specified.isBound()) {
            return ConditionOutcome.match(message.because("automatic cache type"));  // 未设置:全部放行
        }
        CacheType required = CacheConfigurations.getType(((AnnotationMetadata) metadata).getClassName());
        if (specified.get() == required) {
            return ConditionOutcome.match(message.because(specified.get() + " cache type"));  // 点名命中
        }
    }
    catch (BindException ex) {
    }
    return ConditionOutcome.noMatch(message.because("unknown cache type"));
}
```

三种结局：

1. **未设 `spring.cache.type`**：所有配置类条件放行，交给 13.5 的顺序 + `@ConditionalOnMissingBean` 自由竞争；
2. **设置了合法值**：只有对应的一个配置类通过 CacheCondition，**无视优先级直接指定**（比如 type=simple 时即使 Redis 在场也用 ConcurrentMap）；
3. **设置了非法值**（如 `spring.cache.type=foo`）：`Binder` 抛 BindException 被吞，所有配置类 noMatch——**没有任何 CacheManager 被创建**，最终由 13.3 的 CacheManagerValidator 在 `afterPropertiesSet` 抛出 `No cache manager could be auto-configured, check your configuration (caching type is 'foo')`。这正是 Validator 存在的意义：把“所有条件静默失败”翻译成一句人话。

`CacheConfigurations.getType()`（`CacheConfigurations.java:60-67`）是反向查表——从配置类类名反推 CacheType，用于条件里判断“当前这个配置类对应哪个类型”。

### 13.7 三个“本地”实现：Generic / Simple / NoOp

**GenericCacheConfiguration**（`GenericCacheConfiguration.java:36-49`）：

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnBean(Cache.class)
@ConditionalOnMissingBean(CacheManager.class)
@Conditional(CacheCondition.class)
class GenericCacheConfiguration {
    @Bean
    SimpleCacheManager cacheManager(CacheManagerCustomizers customizers, Collection<Cache> caches) {
        SimpleCacheManager cacheManager = new SimpleCacheManager();
        cacheManager.setCaches(caches);      // 直接打包用户定义的所有 Cache Bean
        return customizers.customize(cacheManager);
    }
}
```

用户自己 `@Bean` 若干 `Cache` 实现（甚至混合引擎），Boot 只负责用一个 `SimpleCacheManager` 把它们包成统一门面。

**SimpleCacheConfiguration**（`SimpleCacheConfiguration.java:38-47`）：

```java
@Bean
ConcurrentMapCacheManager cacheManager(CacheProperties cacheProperties, CacheManagerCustomizers cacheManagerCustomizers) {
    ConcurrentMapCacheManager cacheManager = new ConcurrentMapCacheManager();
    List<String> cacheNames = cacheProperties.getCacheNames();
    if (!cacheNames.isEmpty()) {
        cacheManager.setCacheNames(cacheNames);   // 注意副作用
    }
    return cacheManagerCustomizers.customize(cacheManager);
}
```

`ConcurrentMapCacheManager` 默认**动态创建**任意名字的缓存（首次 `getCache("x")` 现造一个）；但一旦 `setCacheNames` 传入了非空列表，就变成**固定集合**——再请求未注册的名字将返回 null。`spring.cache.cache-names` 因此是一把双刃剑（见坑位表）。

**NoOpCacheConfiguration**（`NoOpCacheConfiguration.java:36-39`）：产出 `NoOpCacheManager`——`getCache()` 永远返回一个不做任何事的 NoOpCache。用途：**让 @EnableCaching 的 AOP 链路照常运行但全部旁路**，测试或降级场景下比去掉注解安全得多。

### 13.8 Redis 实现：衔接第十章的装配流水线

`CACHE/RedisCacheConfiguration.java` 是理解“Boot 如何把属性翻译成引擎配置”的最佳样本。条件（`:47-52`）：

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(RedisConnectionFactory.class)
@AutoConfigureAfter(RedisAutoConfiguration.class)
@ConditionalOnBean(RedisConnectionFactory.class)   // 只有第十章的 RedisAutoConfiguration 已造出连接工厂才上场
@ConditionalOnMissingBean(CacheManager.class)
@Conditional(CacheCondition.class)
class RedisCacheConfiguration {
```

`cacheManager` Bean 的装配流水线（`:55-71`）：

```java
@Bean
RedisCacheManager cacheManager(CacheProperties cacheProperties, CacheManagerCustomizers cacheManagerCustomizers,
        ObjectProvider<org.springframework.data.redis.cache.RedisCacheConfiguration> redisCacheConfiguration,
        ObjectProvider<RedisCacheManagerBuilderCustomizer> redisCacheManagerBuilderCustomizers,
        RedisConnectionFactory redisConnectionFactory, ResourceLoader resourceLoader) {
    RedisCacheManagerBuilder builder = RedisCacheManager.builder(redisConnectionFactory).cacheDefaults(
            determineConfiguration(cacheProperties, redisCacheConfiguration, resourceLoader.getClassLoader()));
    List<String> cacheNames = cacheProperties.getCacheNames();
    if (!cacheNames.isEmpty()) {
        builder.initialCacheNames(new LinkedHashSet<>(cacheNames));
    }
    if (cacheProperties.getRedis().isEnableStatistics()) {
        builder.enableStatistics();
    }
    redisCacheManagerBuilderCustomizers.orderedStream().forEach((customizer) -> customizer.customize(builder));
    return cacheManagerCustomizers.customize(builder.build());
}
```

注意一个同名陷阱：方法签名里有两个“RedisCacheConfiguration”——本仓库的装配类，和 SDR 的**值对象** `org.springframework.data.redis.cache.RedisCacheConfiguration`（描述序列化器/TTL/前缀，相当于一个缓存的“模板”）。`determineConfiguration`（`:73-78`）的取值顺序是：**容器里用户自定义的 SDR RedisCacheConfiguration Bean 优先**，没有才用 `createConfiguration` 从 `spring.cache.redis.*` 属性现场造一个（`:80-100`）：

```java
private org.springframework.data.redis.cache.RedisCacheConfiguration createConfiguration(
        CacheProperties cacheProperties, ClassLoader classLoader) {
    Redis redisProperties = cacheProperties.getRedis();
    org.springframework.data.redis.cache.RedisCacheConfiguration config = org.springframework.data.redis.cache.RedisCacheConfiguration
            .defaultCacheConfig();
    config = config.serializeValuesWith(
            SerializationPair.fromSerializer(new JdkSerializationRedisSerializer(classLoader)));  // ⚠️ 默认 JDK 序列化
    if (redisProperties.getTimeToLive() != null) {
        config = config.entryTtl(redisProperties.getTimeToLive());          // spring.cache.redis.time-to-live
    }
    if (redisProperties.getKeyPrefix() != null) {
        config = config.prefixCacheNameWith(redisProperties.getKeyPrefix()); // spring.cache.redis.key-prefix
    }
    if (!redisProperties.isCacheNullValues()) {
        config = config.disableCachingNullValues();                          // 默认允许缓存 null（防穿透）
    }
    if (!redisProperties.isUseKeyPrefix()) {
        config = config.disableKeyPrefix();                                  // 默认带前缀
    }
    return config;
}
```

运行期最终写入 Redis 的 key 形如 `keyPrefix + cacheName + "::" + 方法 key`（`::` 分隔符由 SDR 约定），value 是 JDK 序列化字节（默认）。与第十章呼应：`RedisCacheManager` 复用的正是 `RedisAutoConfiguration` 造出的 `RedisConnectionFactory`（Lettuce 共享连接、RESP 通信），缓存读写只是站在连接工厂之上的又一位消费者。

### 13.9 Caffeine 与 Cache2k：spec 字符串与 Builder 函数式定制

**CaffeineCacheConfiguration**（`CaffeineCacheConfiguration.java:47-80`）展示了一条三级降级的配置来源链：

```java
private void setCacheBuilder(CacheProperties cacheProperties, CaffeineSpec caffeineSpec,
        Caffeine<Object, Object> caffeine, CaffeineCacheManager cacheManager) {
    String specification = cacheProperties.getCaffeine().getSpec();
    if (StringUtils.hasText(specification)) {
        cacheManager.setCacheSpecification(specification);   // ① spring.cache.caffeine.spec 属性（最高优先）
    }
    else if (caffeineSpec != null) {
        cacheManager.setCaffeineSpec(caffeineSpec);          // ② 用户定义的 CaffeineSpec Bean
    }
    else if (caffeine != null) {
        cacheManager.setCaffeine(caffeine);                  // ③ 用户定义的 Caffeine Builder Bean
    }
}
```

`spring.cache.caffeine.spec=maximumSize=500,expireAfterAccess=60s` 一行即可完成容量/过期配置（W-TinyLFU 算法由 Caffeine 本身提供）；需要 LoadingCache 等高级特性时注入 `CacheLoader` Bean（`:64`）即可。

**Cache2kCacheConfiguration**（`Cache2kCacheConfiguration.java:47-57`）的亮点是**函数式默认值**：

```java
SpringCache2kCacheManager cacheManager = new SpringCache2kCacheManager();
cacheManager.defaultSetup(configureDefaults(cache2kBuilderCustomizers));   // 把 customizer 链组合成一个 Builder 函数
```

`defaultSetup` 接收一个 `Function<Cache2kBuilder, Cache2kBuilder>`，每个缓存创建时都会套用这串函数——`Cache2kBuilderCustomizer` 们被**组合（compose）而非顺序执行**，这是比“逐个 customize”更函数式的定制模型。

### 13.10 JCache（JSR-107）：标准门面的两难处理

JCache 是标准 API，provider 可以是 Hazelcast/EhCache/Infinispan 中的任何一个，因此 `JCacheCacheConfiguration` 的条件最复杂（`:57-62`）：

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({ Caching.class, JCacheCacheManager.class })
@ConditionalOnMissingBean(org.springframework.cache.CacheManager.class)
@Conditional({ CacheCondition.class, JCacheCacheConfiguration.JCacheAvailableCondition.class })
@Import(HazelcastJCacheCustomizationConfiguration.class)
class JCacheCacheConfiguration implements BeanClassLoaderAware {
```

`JCacheAvailableCondition` 是 `AnyNestedCondition`（`:126-143`），任一分支成立即通过：

- **JCacheProviderAvailableCondition**（`:150-171`）：`spring.cache.jcache.provider` 属性已设置；**或** `Caching.getCachingProviders()` 恰好返回**一个** provider。多个 provider 并存且未指定时**拒绝**自动选择（`"multiple JSR-107 providers"`，:166）——标准 API 最忌讳的就是猜测用户想用哪家；
- **`@ConditionalOnSingleCandidate(CacheManager.class)`**：注意这里的 CacheManager 是 **javax.cache.CacheManager**（JSR-107 的类型），即用户自己造了原生 provider CacheManager 时，Boot 只需在外面套一层 Spring 适配器。

Bean 的创建（`:77-93`）：

```java
@Bean
@ConditionalOnMissingBean
CacheManager jCacheCacheManager(CacheProperties cacheProperties, ...) throws IOException {
    CacheManager jCacheCacheManager = createCacheManager(cacheProperties, cachePropertiesCustomizers);
    List<String> cacheNames = cacheProperties.getCacheNames();
    if (!CollectionUtils.isEmpty(cacheNames)) {
        for (String cacheName : cacheNames) {
            jCacheCacheManager.createCache(cacheName, defaultCacheConfiguration.getIfAvailable(MutableConfiguration::new));
        }
    }
    cacheManagerCustomizers.orderedStream().forEach((customizer) -> customizer.customize(jCacheCacheManager));
    return jCacheCacheManager;
}
```

`createCacheManager`（`:95-104`）用 `spring.cache.jcache.provider` 选 provider、`spring.cache.jcache.config` 指配置文件 URI；`JCachePropertiesCustomizer` 允许向 provider 注入键值属性。

配套的 `HazelcastJCacheCustomizationConfiguration`（`HazelcastJCacheCustomizationConfiguration.java:36-63`）是**引擎专属特判**：Hazelcast 的 JCache provider 不把 URI 当配置来源，于是注入一个 `JCachePropertiesCustomizer`，把配置位置转写为 `hazelcast.config.location` 属性；若容器里已有 `HazelcastInstance` Bean，则直接通过 `hazelcast.instance.itself` 属性**复用同一实例**（与 HazelcastAutoConfiguration 共享集群拓扑）。外层的 `JCacheCacheManager` Bean（`:71-75`）把这个 JSR-107 manager 包成 Spring 的 `JCacheCacheManager` 适配器。

### 13.11 Hazelcast / Couchbase / Infinispan：复用全局客户端

三者模式一致：**复用各自数据访问自动配置造出的客户端 Bean，不另起炉灶**：

- **HazelcastCacheConfiguration**（`HazelcastCacheConfiguration.java:43-57`）：`@ConditionalOnSingleCandidate(HazelcastInstance)`——直接拿 `HazelcastAutoConfiguration` 配好的实例（或用户自定义实例）构造 `HazelcastCacheManager`，缓存与分布式数据共享同一个网格连接；
- **CouchbaseCacheConfiguration**（`CouchbaseCacheConfiguration.java:51-71`）：拿 `CouchbaseClientFactory` 走 builder 流水线，`spring.cache.couchbase.expiration` 映射为 `entryExpiry`，同样支持 `CouchbaseCacheManagerBuilderCustomizer`；
- **InfinispanCacheConfiguration**（`InfinispanCacheConfiguration.java:52-87`）：额外自己造底层 `EmbeddedCacheManager`（`@Bean(destroyMethod = "stop")`，:59-60），`spring.cache.infinispan.config` 指向 XML 配置；`cacheNames` 逐个 `defineConfiguration` 预定义。

### 13.12 定制化体系与 JPA 依赖编排

**两级定制通道**：

1. **CacheManagerCustomizer**（`CACHE/CacheManagerCustomizer.java`，`@FunctionalInterface`）：面向**最终产物**的泛型回调，如 `Customizer<ConcurrentMapCacheManager>`。由 `CacheManagerCustomizers.customize()`（`CacheManagerCustomizers.java:50-54`）统一驱动：

```java
public <T extends CacheManager> T customize(T cacheManager) {
    LambdaSafe.callbacks(CacheManagerCustomizer.class, this.customizers, cacheManager)
            .withLogger(CacheManagerCustomizers.class).invoke((customizer) -> customizer.customize(cacheManager));
    return cacheManager;
}
```

   `LambdaSafe`（`BOOT/util/LambdaSafe.java`）保证两点：泛型**类型过滤**（`RedisCacheManager` 不会被交给声明为 `CaffeineCacheManager` 的 customizer）与**异常隔离**（某个 customizer 抛异常只打日志，不中断装配）。每个 `cacheManager` Bean 的最后一行都是 `customizers.customize(...)`——这是所有配置类的统一收尾动作。
2. **Builder 级 customizer**（`RedisCacheManagerBuilderCustomizer` / `CouchbaseCacheManagerBuilderCustomizer` / `Cache2kBuilderCustomizer`）：在 builder 阶段介入，能设置 builder 独有选项（如 per-cache 不同 TTL），随后仍会走第 1 级。

**JPA 依赖编排**（`CacheAutoConfiguration.java:77-86`）：

```java
@ConditionalOnClass(LocalContainerEntityManagerFactoryBean.class)
@ConditionalOnBean(AbstractEntityManagerFactoryBean.class)
static class CacheManagerEntityManagerFactoryDependsOnPostProcessor
        extends EntityManagerFactoryDependsOnPostProcessor {
    CacheManagerEntityManagerFactoryDependsOnPostProcessor() {
        super("cacheManager");     // 让 cacheManager Bean dependsOn entityManagerFactory
    }
}
```

背景是二级缓存（如 EhCache 作为 JCache provider）与 Hibernate 都可能在启动期互相触发初始化产生死锁/竞态——这个后处理器给 `cacheManager` 强加一条 `dependsOn("entityManagerFactory")` 边，固定初始化顺序，与第九章 JPA 章节的同类后处理器同款手法。

### 13.13 运行期：@Cacheable 一次调用的完整旅程

装配期结束后，运行期的主角完全交还 SCTX 的 AOP 层（外部依赖，按类名描述）：

```mermaid
sequenceDiagram
    autonumber
    participant CL as 调用方
    participant PX as CGLIB/JDK 动态代理<br/>(@EnableCaching 织入)
    participant CI as CacheInterceptor<br/>(CacheAspectSupport)
    participant COS as CacheOperationSource
    participant CM as CacheManager<br/>(装配期仲裁出的唯一实现)
    participant C as Cache
    participant TG as 目标对象方法

    CL->>PX: userMapper.findById(42)
    PX->>CI: invoke(methodInvocation)
    CI->>COS: 解析方法上的 @Cacheable<br/>(cacheNames=["user"], key="#id")
    CI->>CI: KeyGenerator 生成 key<br/>(SimpleKeyGenerator: 42)
    CI->>CM: getCache("user")<br/>(ConcurrentMapCacheManager: 首次调用动态建 cache)
    CM->>C: 返回 Cache 句柄
    CI->>C: get(42)

    alt 命中
        C-->>CI: 缓存值 (直接反序列化/引用)
        CI-->>CL: 返回缓存值（目标方法被旁路 ⏭️）
    else 未命中
        C-->>CI: null
        CI->>TG: proceed() 执行真实方法
        TG-->>CI: 返回值（可能为 null）
        CI->>C: put(42, value)<br/>(null 是否入缓存取决于 cacheNullValues)
        CI-->>CL: 返回值
    end

    Note over CI,C: @CachePut: 先执行方法再 put（无旁路）<br/>@CacheEvict: 方法前/后 evict 或 allEntries 清空
```

对应到各实现的差异：`ConcurrentMapCache` 直接存对象引用（无序列化、无 TTL）；`RedisCache` 的 get/put 翻译成 RESP 命令（GET/SET PX 等，value 为 JDK 序列化字节，走第十章的 Lettuce 共享连接）；`CaffeineCache` 的 put 进入 W-TinyLFU 淘汰队列。

### 13.14 常见坑位表

| 坑 | 现象 | 根因（源码依据） |
|---|---|---|
| 只加 starter 不写 `@EnableCaching` | 所有 @Cacheable 失效，无任何缓存 | `CacheAutoConfiguration.java:59` 的 `@ConditionalOnBean(CacheAspectSupport.class)` 门未开，CacheManager 根本不存在 |
| `spring.cache.type` 拼错（如 `rediss`） | 启动失败：`No cache manager could be auto-configured... (caching type is 'rediss')` | `CacheCondition.java:58-60` BindException 静默 noMatch 全部配置类 + `:104-108` Validator 报警 |
| 引了 redis + caffeine，期望本地缓存却上了 Redis | 缓存全进 Redis | 枚举序 REDIS(6) < CAFFEINE(8)（`CacheType.java:57-67`），自动探测偏爱分布式实现 |
| 同时引入两个 JSR-107 provider | JCache 自动配置不生效，回退 SIMPLE | `JCacheCacheConfiguration.java:165-167` 拒绝在多 provider 间猜测；需显式 `spring.cache.jcache.provider` |
| `spring.cache.cache-names` 一填，新缓存名报错 | `Cannot find cache 'xxx'`（IllegalArgumentException） | `SimpleCacheConfiguration.java:43-45`：setCacheNames 把动态创建模式改为固定集合 |
| Redis 缓存的值不能反序列化/跨语言不通 | ClassCastException 或乱码 | `RedisCacheConfiguration.java:85-86` 默认 `JdkSerializationRedisSerializer`；应自定义 SDR RedisCacheConfiguration Bean 换 GenericJackson2JsonRedisSerializer |
| 自己定义了 CacheManager Bean，`spring.cache.*` 全部失灵 | 属性被无视 | `CacheAutoConfiguration.java:60` 的 `@ConditionalOnMissingBean(CacheManager)` 让整套自动装配退位 |
| ConcurrentMap 缓存越滚越大 | 无过期、无容量上限 | ConcurrentMapCache 无 TTL/LRU 语义（SCTX 行为）；要过期请显式 type=caffeine 或 redis |
| 自定义 Cache Bean + 自动探测并存时 Generic 意外胜出 | 引擎与预期不符 | `CacheType.java:32`：GENERIC 排第一，容器里有任何 `Cache` Bean 即被 SimpleCacheManager 收编 |

### 13.15 设计精髓总结

1. **starter 只是“可见性开关”**：缓存核心在 spring-context 里随 Boot 必达，starter-cache 的两行依赖只为把 spring-context-support 的适配类拉进 classpath 供条件探测——与第十章 Redis starter 的“零代码聚合器”如出一辙，但动机更隐蔽。
2. **顺序即仲裁**：10 个配置类不写一行优先级代码，用“ImportSelector 按枚举序全量导入 + 每类挂 `@ConditionalOnMissingBean(CacheManager)` 排他锁”让注册顺序天然裁决。新增一种缓存引擎 = 加一个枚举值 + 一个配置类 + 一行映射，零侵入。
3. **点名单独走一条条件**：`CacheCondition` 把“自动探测”（放行全部）与“显式指定”（只放行一个）统一进同一个 Condition，非法值静默 noMatch 后由 `CacheManagerValidator` 兜底翻译成人话——条件体系的“静默失败”被一个 InitializingBean 补偿。
4. **总闸门交给框架语义**：`@ConditionalOnBean(CacheAspectSupport)` 把“是否启用缓存”的决定权交给 `@EnableCaching` 的副作用 Bean——Boot 不发明新开关，只探测用户意图。
5. **定制三级降级**：Spring 属性（spec/type）> 专用值对象 Bean（SDR RedisCacheConfiguration/CaffeineSpec）> 通用 customizer 链（LambdaSafe 类型过滤 + 异常隔离），同一配置项的覆盖面逐级放宽。
6. **复用而非重建客户端**：Redis/Hazelcast/Couchbase 缓存全部复用数据访问自动配置的连接工厂/实例，一个进程一条连接拓扑服务多种用途（缓存、Repository、Session）。
7. **标准与实现的边界处理**：JCache 面对多 provider“宁可不配也不错配”，Hazelcast 的 JCache 特判用 PropertiesCustomizer 通道解决——对标准 API 的尊重体现为拒绝猜测。

## 十四、spring-boot-starter-websocket 实现原理：WebSocket 整合与内嵌容器握手链路

> 本章路径缩写约定（在文首 `BOOT`/`AC`/`AUTO` 基础上补充）：
> - **WS** = `spring-boot-project/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/websocket`
> - **STARTERS** = `spring-boot-project/spring-boot-starters`
> - **SWEB** = 外部依赖 `org.springframework:spring-websocket`（WebSocket 抽象、握手、SockJS；托管版本随 Spring Framework 6.0.2）
> - **SMSG** = 外部依赖 `org.springframework:spring-messaging`（STOMP 协议与消息通道抽象）
>
> 核心源文件（本仓库内）：
> - `STARTERS/spring-boot-starter-websocket/build.gradle`
> - `WS/servlet/WebSocketServletAutoConfiguration.java`（内嵌容器 ServerContainer 装配）
> - `WS/servlet/WebSocketMessagingAutoConfiguration.java`（STOMP 消息转换器整合）
> - `WS/servlet/{Tomcat,Jetty,Undertow}WebSocketServletWebServerCustomizer.java`
> - `WS/reactive/WebSocketReactiveAutoConfiguration.java`、`WS/reactive/TomcatWebSocketReactiveWebServerCustomizer.java`
> - 注册位置：`spring-boot-autoconfigure/src/main/resources/META-INF/spring/...AutoConfiguration.imports:138-140`
>
> 这是全书"整合面最小"的一章：**Spring Boot 仓库里与 WebSocket 相关的代码只有 8 个类**，加起来不足 400 行。原因和第十章（Redis）、第十三章（缓存）一脉相承--握手、帧收发、STOMP 子协议全部在 Spring Framework 与 Tomcat/Jetty/Undertow 里，Spring Boot 的整合工作只有两件半：**① 让内嵌容器长出 WebSocket 能力（装 ServerContainer）**、**② 让 STOMP 的 JSON 序列化复用应用的 ObjectMapper**、外加半个 Reactive 侧的 Tomcat 特判。

### 14.1 starter 组成：三行依赖，三个世界

`STARTERS/spring-boot-starter-websocket/build.gradle` 全文：

```gradle
dependencies {
	api(project(":spring-boot-project:spring-boot-starters:spring-boot-starter-web"))
	api("org.springframework:spring-messaging")
	api("org.springframework:spring-websocket")
}
```

| 依赖 | 引入的世界 | 关键内容 |
|---|---|---|
| `spring-boot-starter-web` | **Servlet 容器世界** | spring-webmvc（DispatcherServlet）+ `spring-boot-starter-tomcat` -> **`tomcat-embed-websocket`**（内含 `WsSci`、jakarta.websocket API、Tomcat 的 `ServerContainer` 实现） |
| `spring-messaging` | **消息世界** | `Message<T>`/`MessageChannel` 抽象、STOMP 编解码、订阅式编程模型（与 RabbitMQ/Kafka 的 Spring 消息模型同宗） |
| `spring-websocket` | **WebSocket 世界** | `WebSocketHandler` SPI、握手协商、`WebSocketClient`、SockJS 降级方案、`@EnableWebSocket`/`@EnableWebSocketMessageBroker` 注解体系 |

注意一个隐蔽事实：**WebSocket 的 Jakarta API（`jakarta.websocket.*`）与 Tomcat 实现不是 starter-websocket 引入的，而是随 starter-web 的 tomcat-embed-websocket 早在场**（`spring-boot-starter-tomcat/build.gradle` 显式引入 `org.apache.tomcat.embed:tomcat-embed-websocket`）。这正是 14.4 的条件注解能够生效的前提--换 Jetty 时 starter-jetty 同样携带 `jetty-websocket-*-server` 模块。

### 14.2 双栈模型：原生 WebSocket 与 STOMP 子协议（外部依赖速览）

理解整合代码之前必须先分清用户写 WebSocket 应用的两种姿态（SWEB/SMSG，无本仓库行号，按类名描述）：

```mermaid
flowchart TB
    subgraph RAW["姿态一：原生 WebSocket（仅 spring-websocket）"]
        EW["@EnableWebSocket<br/>-> WebSocketConfigurationSupport"]
        WH["WebSocketHandler<br/>handleMessage(session, message)"]
        HM["WebSocketHandlerMapping<br/>把 /ws 映射到 handler"]
    end

    subgraph JSR["姿态一的另一入口：JSR-356 原生注解"]
        SEE["@ServerEndpoint / @OnMessage<br/>（jakarta.websocket API）"]
    end

    subgraph STOMP["姿态二：STOMP 消息子协议（+ spring-messaging）"]
        EWMB["@EnableWebSocketMessageBroker<br/>-> DelegatingWebSocketMessageBrokerConfiguration"]
        SHM["stompWebSocketHandlerMapping"]
        SPH["SubProtocolWebSocketHandler<br/>+ StompSubProtocolHandler<br/>（CONNECT/SUBSCRIBE/SEND 帧解析）"]
        CIC["clientInboundChannel"]
        COC["clientOutboundChannel"]
        BROKER["SimpleBrokerMessageHandler<br/>（/topic /queue 内存订阅表）<br/>或 StompBrokerRelay（转发 RabbitMQ/ActiveMQ）"]
        AMH["@MessageMapping / @SendTo<br/>注解方法处理（同 @RequestMapping 风格）"]
    end

    CLIENT["浏览器 new WebSocket('ws://...')<br/>或 SockJS / stomp.js 客户端"] -->|"HTTP GET + Upgrade: websocket"| UPG["容器层握手升级<br/>(jakarta ServerContainer)"]
    UPG --> RAW
    UPG --> STOMP
    EW --> HM
    EWMB --> SHM --> SPH
    SPH <--> CIC
    SPH <--> COC
    CIC --> AMH --> BROKER
    BROKER --> COC
```

- **姿态一（原生）**：`WebSocketHandler` 直接收发二进制/文本帧，协议完全自定义；
- **姿态二（STOMP）**：在 WebSocket 之上跑 STOMP 帧协议，换来 **SUBSCRIBE 语义（发布订阅）、@MessageMapping 注解路由、跨浏览器消息路由**--Spring 官方指南的 "gs-messaging-stomp" 即此模式；
- **JSR-356 直连**：完全不经过 Spring 抽象，直接用 `@ServerEndpoint` 注解（整合方式见 14.11 坑位表）。

Spring Boot 对两种姿态的整合深度完全不同：姿态一只依赖"容器有 ServerContainer"（14.4–14.6）；姿态二还叠加了 JSON 序列化的复用（14.7）。

### 14.3 整合面全景：三个自动配置类

`AutoConfiguration.imports:138-140` 注册了全部三个：

| 自动配置类 | 作用 | 生效条件概览 |
|---|---|---|
| `WS/servlet/WebSocketServletAutoConfiguration` | 给内嵌 Tomcat/Jetty/Undertow 装上 Jakarta `ServerContainer` | SERVLET 应用 + `Servlet`/`ServerContainer` 类在场 |
| `WS/servlet/WebSocketMessagingAutoConfiguration` | 用应用的 ObjectMapper 定制 STOMP 消息转换器 | SERVLET 应用 + `WebSocketMessageBrokerConfigurer` 类在场 + 用户已 `@EnableWebSocketMessageBroker` |
| `WS/reactive/WebSocketReactiveAutoConfiguration` | Reactive 侧 Tomcat 特判 | REACTIVE 应用 + 同样的 Servlet 类条件 |

### 14.4 WebSocketServletAutoConfiguration：给内嵌容器"长出"WebSocket 能力

完整类体（`WebSocketServletAutoConfiguration.java:55-96`）：

```java
@AutoConfiguration(before = ServletWebServerFactoryAutoConfiguration.class)
@ConditionalOnClass({ Servlet.class, ServerContainer.class })
@ConditionalOnWebApplication(type = Type.SERVLET)
public class WebSocketServletAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({ Tomcat.class, WsSci.class })
    static class TomcatWebSocketConfiguration {
        @Bean
        @ConditionalOnMissingBean(name = "websocketServletWebServerCustomizer")
        TomcatWebSocketServletWebServerCustomizer websocketServletWebServerCustomizer() {
            return new TomcatWebSocketServletWebServerCustomizer();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(JakartaWebSocketServletContainerInitializer.class)
    static class JettyWebSocketConfiguration { /* 同名 Bean，返回 Jetty 版 */ }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(io.undertow.websockets.jsr.Bootstrap.class)
    static class UndertowWebSocketConfiguration { /* 同名 Bean，返回 Undertow 版 */ }
}
```

四个设计要点：

- **`before = ServletWebServerFactoryAutoConfiguration`**（:55）：必须抢在 Web 服务器工厂自动配置之前把 customizer Bean 定义注册好（第四章分析过：`WebServerFactoryCustomizerBeanPostProcessor` 在工厂 Bean 初始化时统一收口，但 Bean 定义必须先在场）；
- **`@ConditionalOnClass({ Servlet.class, ServerContainer.class })`**（:56）：`ServerContainer` 是 Jakarta WebSocket 服务端 API 的顶层接口（`jakarta.websocket.server.ServerContainer`），在场即说明 WebSocket API 已可用；
- **三个嵌套配置类各自再探一层**：Tomcat 版要求 `Tomcat.class + WsSci.class`（:61，`WsSci` 是 tomcat-embed-websocket 里的 `ServletContainerInitializer`）、Jetty 版要求 `JakartaWebSocketServletContainerInitializer`（:73）、Undertow 版要求 `Bootstrap`（:85）--**换服务器 = 换依赖，条件自动切换**；
- **互斥靠"同名 Bean"**：三个 `@Bean` 方法都声明 `@ConditionalOnMissingBean(name = "websocketServletWebServerCustomizer")`（:65/:77/:89）且方法同名。与第十章 Lettuce/Jedis 互斥是同一手法：第一个胜出的配置类注册了这个名字的 Bean，另外两家因"名字已存在"短路--即使 classpath 同时有三家容器（罕见但合法）也只会有一份。

### 14.5 三大容器的定制细节：同一个目标，三种容器方言

三个 customizer 都实现 `WebServerFactoryCustomizer<T>`（第四章的扩展点体系），但"如何让容器支持 WebSocket"各家方言不同：

**Tomcat**（`TomcatWebSocketServletWebServerCustomizer.java:36-39`）：

```java
@Override
public void customize(TomcatServletWebServerFactory factory) {
    factory.addContextCustomizers((context) -> context.addServletContainerInitializer(new WsSci(), null));
}
```

一行代码的分量：**WAR 部署时，Servlet 容器启动会自行扫描并执行 jar 里所有 `ServletContainerInitializer`（SCI），tomcat-embed-websocket 的 `WsSci` 负责把 Jakarta `ServerContainer` 实现注册进 ServletContext 属性**；而 Spring Boot 的内嵌 Tomcat 是"裸启动"，不执行 SCI 扫描--这个 customizer 把 `WsSci` 手动挂到 Tomcat Context 上（`TomcatServletWebServerFactory` 在 `getWebServer()` 构建 Context 时统一应用收集到的 context customizer，`TomcatServletWebServerFactory.java:399-401`）。这是全章最核心的一行代码。

**Jetty**（`JettyWebSocketServletWebServerCustomizer.java:43-63`）：

```java
@Override
public void customize(JettyServletWebServerFactory factory) {
    factory.addConfigurations(new AbstractConfiguration() {
        @Override
        public void configure(WebAppContext context) throws Exception {
            if (JettyWebSocketServerContainer.getContainer(context.getServletContext()) == null) {
                WebSocketServerComponents.ensureWebSocketComponents(context.getServer(), context.getServletContext());
                JettyWebSocketServerContainer.ensureContainer(context.getServletContext());
            }
            if (JakartaWebSocketServerContainer.getContainer(context.getServletContext()) == null) {
                WebSocketServerComponents.ensureWebSocketComponents(context.getServer(), context.getServletContext());
                WebSocketUpgradeFilter.ensureFilter(context.getServletContext());      // 注册升级过滤器
                WebSocketMappings.ensureMappings(context.getServletContext());
                JakartaWebSocketServerContainer.ensureContainer(context.getServletContext());
            }
        }
    });
}
```

Jetty 要装**两套容器**：自家原生 API 的 `JettyWebSocketServerContainer` 和 Jakarta 标准的 `JakartaWebSocketServerContainer`。后者的关键机关是 `WebSocketUpgradeFilter`--Jetty 的 Jakarta WebSocket 依赖一个 Servlet Filter 拦截 `Upgrade: websocket` 请求完成协议切换（对应 Tomcat 由 `WsSci` 一并搞定的事），另外还要 `WebSocketMappings`（URL 到端点的映射表）。

**Undertow**（`UndertowWebSocketServletWebServerCustomizer.java:37-55`）：

```java
@Override
public void customize(UndertowServletWebServerFactory factory) {
    WebsocketDeploymentInfoCustomizer customizer = new WebsocketDeploymentInfoCustomizer();
    factory.addDeploymentInfoCustomizers(customizer);
}
...
@Override
public void customize(DeploymentInfo deploymentInfo) {
    WebSocketDeploymentInfo info = new WebSocketDeploymentInfo();
    deploymentInfo.addServletContextAttribute(WebSocketDeploymentInfo.ATTRIBUTE_NAME, info);
}
```

Undertow 的方言是 **ServletContext 属性传参**：把一个空的 `WebSocketDeploymentInfo` 挂到部署信息上，Undertow 的 `Bootstrap`（SCI）启动时读到这个属性就会激活 WebSocket 部署（XNIO worker、帧处理管线）。

三个 customizer 生效的公共机制回到第四章：`WebServerFactoryCustomizerBeanPostProcessor.postProcessBeforeInitialization()`（`BOOT/web/server/WebServerFactoryCustomizerBeanPostProcessor.java:56-73`）在工厂 Bean 初始化前，用 **LambdaSafe 回调**（类型过滤：`TomcatServletWebServerFactory` 不会被 Jetty 版 customizer 碰到）逐个应用。customizer 的 `getOrder()` 都返回 0（:41-44）。

```mermaid
flowchart TB
    subgraph BOOT_SIDE["Spring Boot 装配期（本章主角）"]
        AC["WebSocketServletAutoConfiguration<br/>imports:139"]
        C1["TomcatWebSocketServletWebServerCustomizer"]
        C2["JettyWebSocketServletWebServerCustomizer"]
        C3["UndertowWebSocketServletWebServerCustomizer"]
        BPP["WebServerFactoryCustomizerBeanPostProcessor<br/>(LambdaSafe 统一收口, 见第四章)"]
    end

    subgraph FACTORY["WebServerFactory.getWebServer()（第四章）"]
        T["Tomcat Context<br/>addServletContainerInitializer(WsSci)"]
        J["Jetty WebAppContext<br/>ensureContainer x2 + UpgradeFilter"]
        U["Undertow DeploymentInfo<br/>WebSocketDeploymentInfo 属性"]
    end

    subgraph CONTAINER["容器层的 WebSocket 基础设施"]
        SC["jakarta.websocket.server.ServerContainer<br/>（ServletContext 属性）"]
    end

    AC -->|"classpath 探测三选一<br/>(同名 Bean 互斥)"| C1 & C2 & C3
    C1 & C2 & C3 --> BPP --> FACTORY
    T & J & U --> SC
```

### 14.6 握手链路：从 `Upgrade: websocket` 到 WebSocketSession

装配期造出的 `ServerContainer` 在运行期被谁消费？（SWEB 侧按类名描述）Spring 的 `DefaultHandshakeHandler` 内部按容器选择 `RequestUpgradeStrategy`（Tomcat 用 `TomcatRequestUpgradeStrategy`），升级时调用 Jakarta WebSocket 2.1 的标准方法 **`ServerContainer.upgradeHttpToWebSocket(...)`** 把当前 HTTP 连接就地升级--它拿到的 `ServerContainer` 正是 Boot 装配期通过 `WsSci` 等机制挂进 ServletContext 的那个实例。**两章代码在此接头**：

```mermaid
sequenceDiagram
    autonumber
    participant B as 浏览器/SockJS 客户端
    participant DS as DispatcherServlet
    participant HM as websocketHandlerMapping /<br/>stompWebSocketHandlerMapping
    participant HRH as WebSocketHttpRequestHandler /<br/>SubProtocolWebSocketHandler
    participant HH as DefaultHandshakeHandler<br/>(SWEB)
    participant RUS as TomcatRequestUpgradeStrategy<br/>(SWEB)
    participant SC as ServerContainer<br/>(Boot 装配期经 WsSci 挂入)

    B->>DS: GET /ws HTTP/1.1<br/>Upgrade: websocket / Sec-WebSocket-Key...
    DS->>HM: 按 URL 找到 WebSocket 入口
    HM->>HRH: 转交处理
    HRH->>HH: doHandshake(request, response, handler)
    HH->>HH: 校验 Origin / 协商子协议(STOMP) / 选择版本
    HH->>RUS: upgrade(...)
    RUS->>SC: upgradeHttpToWebSocket(...)  ← 接头点
    SC-->>B: 101 Switching Protocols<br/>(Sec-WebSocket-Accept 校验值)
    Note over B,SC: TCP 连接原样保留，此后说 WebSocket 帧协议
    B->>HRH: 帧: STOMP CONNECT / SUBSCRIBE / SEND
    HRH-->>B: 帧: CONNECTED / MESSAGE
```

这也解释了源码 javadoc（`WebSocketServletAutoConfiguration.java:38-48`）反复强调的 "In a non-embedded server it should already be there"：**WAR 部署到外部容器时，容器启动本身就会执行 SCI/等价初始化，ServerContainer 天然在场，Boot 的 customizer 无用武之地也无需生效**（外置容器场景没有 `WebServerFactory` Bean，customizer 永远不会被触发，天然安全）。

### 14.7 WebSocketMessagingAutoConfiguration：STOMP 的 JSON 序列化复用

第二个自动配置类解决一个真实的重复建设问题。`@EnableWebSocketMessageBroker`（SWEB 的 `DelegatingWebSocketMessageBrokerConfiguration`）默认会**自造一个 `MappingJackson2MessageConverter`** 处理 STOMP 消息的 JSON--它与应用 REST 层用的 `ObjectMapper` **毫无关系**：你在 `Jackson2ObjectMapperBuilderCustomizer` 里注册的日期格式、Long 转字符串、命名策略等定制，对 WebSocket 消息体一律无效。

`WebSocketMessagingAutoConfiguration.java:49-83` 的解法：

```java
@AutoConfiguration(after = JacksonAutoConfiguration.class)
@ConditionalOnWebApplication(type = Type.SERVLET)
@ConditionalOnClass(WebSocketMessageBrokerConfigurer.class)
public class WebSocketMessagingAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean({ DelegatingWebSocketMessageBrokerConfiguration.class, ObjectMapper.class })
    @ConditionalOnClass({ ObjectMapper.class, AbstractMessageBrokerConfiguration.class })
    static class WebSocketMessageConverterConfiguration implements WebSocketMessageBrokerConfigurer {

        private final ObjectMapper objectMapper;

        WebSocketMessageConverterConfiguration(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;   // 注入的就是容器里那个唯一的 ObjectMapper
        }

        @Override
        public boolean configureMessageConverters(List<MessageConverter> messageConverters) {
            MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
            converter.setObjectMapper(this.objectMapper);              // 复用！
            DefaultContentTypeResolver resolver = new DefaultContentTypeResolver();
            resolver.setDefaultMimeType(MimeTypeUtils.APPLICATION_JSON);
            converter.setContentTypeResolver(resolver);                // 缺省内容类型 application/json
            messageConverters.add(new StringMessageConverter());
            messageConverters.add(new ByteArrayMessageConverter());
            messageConverters.add(converter);
            return false;   // false = 不再追加 Spring 默认转换器，本列表即全集
        }
        ...
    }
}
```

设计细节逐条：

- **`@ConditionalOnBean(DelegatingWebSocketMessageBrokerConfiguration.class)`**（:55）：只有用户写了 `@EnableWebSocketMessageBroker`（它 `@Import` 这个配置类）才介入--与第十三章 `@ConditionalOnBean(CacheAspectSupport)` 探测 `@EnableCaching` 完全同构的"意图探测"手法；
- **`after = JacksonAutoConfiguration.class`**（:49）：先等 Jackson 自动配置造出 `ObjectMapper`，否则 `@ConditionalOnBean(ObjectMapper.class)` 永远不成立；
- **实现 `WebSocketMessageBrokerConfigurer` 本身**：这是 SWEB 提供的聚合式扩展点--`DelegatingWebSocketMessageBrokerConfiguration` 会把容器里**所有**该接口的 Bean 收集起来逐个回调，Boot 的这个配置类就是"以配置类身份混进用户的扩展点列表"；
- **`configureMessageConverters` 返回 false**：按 SWEB 约定，false 表示"不再注册 Spring 默认转换器"（String/ByteArray/独立 ObjectMapper 的 Jackson 三件套）；Boot 用**镜像的**三件套顶替，唯一差别是 Jackson 转换器复用容器 ObjectMapper--REST 与 WebSocket 的 JSON 行为从此一致；
- **`DefaultContentTypeResolver` + `APPLICATION_JSON`**：浏览器 SockJS/STOMP 客户端发消息常常不带 content-type 头，缺省类型保证了 JSON 反序列化路径不因缺头而失效。

### 14.8 懒初始化的例外：eagerStompWebSocketHandlerMapping

`WebSocketMessagingAutoConfiguration` 还有一个不起眼的 Bean（`:78-81`）：

```java
@Bean
static LazyInitializationExcludeFilter eagerStompWebSocketHandlerMapping() {
    return (name, definition, type) -> name.equals("stompWebSocketHandlerMapping");
}
```

背景：`spring.main.lazy-initialization=true` 会把所有 Bean 改为按需创建，但 STOMP 的总入口 `stompWebSocketHandlerMapping`（`DelegatingWebSocketMessageBrokerConfiguration` 注册的 HandlerMapping Bean）**必须急切创建**--第一个 WebSocket 握手请求到达时才临时初始化它会与 Servlet/STOMP 通道的启动时序冲突（SockJS 握手尤其敏感）。`LazyInitializationExcludeFilter`（`BOOT/LazyInitializationExcludeFilter.java:47-69`，函数式接口，`isExcluded(beanName, definition, type)` 返回 true 即豁免懒加载）按 **Bean 名字**精确豁免这一个 Bean。这是全书看到的第一个"为懒初始化全局特性打补丁"的自动配置，与第七章 LazyInitializationBeanFactoryPostProcessor 的机制直接呼应。

### 14.9 Reactive 侧：只有一个 Tomcat 特判

`WS/reactive/WebSocketReactiveAutoConfiguration.java:43-60`：

```java
@AutoConfiguration(before = ReactiveWebServerFactoryAutoConfiguration.class)
@ConditionalOnClass({ Servlet.class, ServerContainer.class })
@ConditionalOnWebApplication(type = Type.REACTIVE)
public class WebSocketReactiveAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({ Tomcat.class, WsSci.class })
    static class TomcatWebSocketConfiguration {
        @Bean
        @ConditionalOnMissingBean(name = "websocketReactiveWebServerCustomizer")
        TomcatWebSocketReactiveWebServerCustomizer websocketReactiveWebServerCustomizer() {
            return new TomcatWebSocketReactiveWebServerCustomizer();
        }
    }
}
```

结构与 Servlet 版完全镜像（`before = ReactiveWebServerFactoryAutoConfiguration`、同一个 `WsSci`、同名 Bean 防重复），唯一的定制器 `TomcatWebSocketReactiveWebServerCustomizer`（`:35-37`）同样一行 `addServletContainerInitializer(new WsSci(), null)`。

为什么 Reactive 侧只有 Tomcat？Reactive 栈的 WebSocket 处理不走 Jakarta WebSocket API，而是 **Reactor Netty / Jetty / Undertow 各自的原生升级支持**直接桥接为 `Flux<WebSocketFrame>`（`spring-webflux` 的 `WebSocketHandlerAdapter` 与各 server adapter 配合，升级在 HTTP 层原生完成）--唯独 Tomcat 的 Reactive 适配跑在 Servlet 之上，仍需要 Servlet 世界的 `ServerContainer` 才能升级，于是 Boot 必须为它单独补课。**同一个问题的两种解法**恰好暴露了各容器的架构差异。

### 14.10 运行期全景：一条 STOMP 消息的完整旅程

装配期结束后，STOMP 模式下一次典型请求-推送的全链路（外部依赖按类名描述）：

```mermaid
sequenceDiagram
    autonumber
    participant B as 浏览器 stomp.js
    participant WS as WebSocket 连接<br/>(14.6 已升级)
    participant SPH as SubProtocolWebSocketHandler<br/>+ StompSubProtocolHandler
    participant CIC as clientInboundChannel<br/>(线程池解耦)
    participant AMH as @MessageMapping 方法<br/>(SimpAnnotationMethodMessageHandler)
    participant BC as brokerChannel
    participant BROKER as SimpleBrokerMessageHandler<br/>(内存订阅表)
    participant COC as clientOutboundChannel

    B->>WS: STOMP 帧 CONNECT
    WS->>SPH: 文本帧 -> Message<byte[]>
    SPH-->>B: CONNECTED
    B->>SPH: SUBSCRIBE /topic/greetings (session 订阅登记)
    B->>SPH: SEND /app/hello {name:...}
    SPH->>CIC: 投递 (异步, 解耦 IO 线程)
    CIC->>AMH: 路由到 @MessageMapping("/hello") 方法<br/>JSON -> 入参对象 (14.7 的转换器!)
    AMH->>BC: 方法返回值 + @SendTo("/topic/greetings")
    BC->>BROKER: 广播给订阅表
    BROKER->>COC: 逐订阅者投递
    COC->>WS: MESSAGE 帧 (JSON 序列化同样走 14.7 转换器)
    WS-->>B: MESSAGE /topic/greetings
```

`@SendToUser` 则走 user destination 体系：BROKER 前把 `/user/queue/x` 翻译成 `/{sessionId}/queue/x` 定向投递，支持多实例下经 `UserDestinationResolver` 的中转（SimpUserRegistry）。这一切对 Boot 完全透明--Boot 的两个自动配置类分别只保证了"升级能发生"和"JSON 长得和 REST 一样"。

### 14.11 常见坑位表

| 坑 | 现象 | 根因（源码依据） |
|---|---|---|
| 以为引了 starter 就能用 WebSocket | 握手 404，`websocketHandlerMapping` 不存在 | Boot 只装容器基础设施（14.4）；`WebSocketHandlerMapping` 由用户 `@EnableWebSocket` 注册，不写注解就没有入口 |
| JSR-356 `@ServerEndpoint` 在内嵌容器不工作 | 连接 404 | 内嵌 Tomcat 不做 WAR 式端点扫描，需额外注册 `ServerEndpointExporter` Bean（SWEB 提供）；**WAR 部署时反而不能加**（容器已扫描，重复注册冲突） |
| 定制了 ObjectMapper，REST 生效但 STOMP 消息不生效 | 日期/命名等序列化差异 | 未触发 14.7：`@ConditionalOnBean(DelegatingWebSocketMessageBrokerConfiguration)` 不成立（没写 `@EnableWebSocketMessageBroker`），或 converter 被自己 override 掉 |
| 同时引 Tomcat 与 Jetty 依赖 | 只有一家的 customizer 生效 | `@ConditionalOnMissingBean(name = "websocketServletWebServerCustomizer")` 同名互斥（:65/:77/:89） |
| 用 Jetty 时只想要原生 API 却多了 Jakarta 容器（或反之） | 行为困惑 | `JettyWebSocketServletWebServerCustomizer.java:48-59` 无条件确保**两套**容器都在场，按需自己移除 |
| `spring.main.lazy-initialization=true` 后 WebSocket 第一条消息丢失/握手失败 | 时序异常 | 已有豁免机制只覆盖 `stompWebSocketHandlerMapping`（14.8）；用户自定义的 Handler/通道 Bean 仍懒加载，必要时自行声明 `LazyInitializationExcludeFilter` |
| 期望 `spring.websocket.*` 配置项 | 找不到属性 | 3.0.0 尚无 `WebSocketProperties`（后来的版本才引入），STOMP 配置只能走 `WebSocketMessageBrokerConfigurer` 代码 |
| Reactive + Undertow/Jetty 以为有 customizer | 无该自动配置分支 | `WebSocketReactiveAutoConfiguration` 只有 Tomcat 分支（14.9）：其余容器原生支持 reactive 升级无需补课 |
| WAR 部署担心 Boot 的 customizer 捣乱 | 无害 | 外置容器无 `WebServerFactory` Bean，customizer 无触发点；javadoc ":40-48" 明示 "In a non-embedded server it should already be there" |

### 14.12 设计精髓总结

1. **整合面与复杂度成反比**：WebSocket 协议栈（RFC 6455 帧、握手、STOMP、SockJS）千行级复杂度全在 Spring Framework 与容器里；Spring Boot 的整合只有 8 个类，且第一件事竟是"把 WAR 世界免费的 SCI 手动补回来"--**内嵌容器的本质代价：放弃 Servlet 规范的自动发现，就要逐项手工复原**。
2. **扩展点体系的一贯复用**：三个 customizer 站在第四章的 `WebServerFactoryCustomizer` + `WebServerFactoryCustomizerBeanPostProcessor`（LambdaSafe 收口）之上，零新增基础设施；同名 Bean 互斥复用第十章 Lettuce/Jedis 的手法。
3. **意图探测式条件**：`@ConditionalOnBean(DelegatingWebSocketMessageBrokerConfiguration)` 把"是否启用 STOMP"交还 `@EnableWebSocketMessageBroker` 决定，与缓存章的 `CacheAspectSupport` 探测同构--Boot 从不替用户做启用决定。
4. **配置类混进扩展点列表**：`WebSocketMessageConverterConfiguration` 以 `WebSocketMessageBrokerConfigurer` 实现类的身份参与用户扩展点的聚合回调，把"自动配置"翻译成"一个默认参与者"而不是"一套平行机制"。
5. **一致性而非功能性**：本章唯一的行为级整合（复用 ObjectMapper）目标是让 WebSocket 消息的 JSON 与 REST **长得一样**--Spring Boot 的隐含产品哲学：同一应用内同一序列化语义。
6. **懒加载例外暴露全局特性的成本**：一个 `LazyInitializationExcludeFilter` Bean 表明任何"默认全局"特性（懒初始化、AOT、devtools 重启）都要为"入口必须急切"的基础设施打补丁，第四、十一章已见同款模式。
7. **容器方言的三种抽象**：Tomcat 的 SCI、Jetty 的双容器 + UpgradeFilter、Undertow 的 DeploymentInfo 属性--同一个目标（ServerContainer 就位）在不同容器里以完全不同的机构实现，Spring Boot 用统一的 customizer 通道把它们折叠成一条装配流水线。

---


*文档生成：基于 spring-boot-3.0.0 源码逐行研读整理；所有行号对应本仓库 `spring-boot-project/` 下的 3.0.0 版本源文件。*


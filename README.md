# ChronoLake

> A data lake engine purpose-built for time-series data processing.

ChronoLake 是一个面向时序数据场景的数据加工引擎，借鉴数据湖（Data Lake）理念，提供时序数据的统一接入、存储、加工与查询能力。

## 核心特性

- **时序原生模型**：以时间维度为一等公民的数据模型与 Schema 抽象
- **冷热分层存储**：基于列存（Parquet / Arrow）的高压缩比时序存储
- **流批一体加工**：支持窗口聚合、降采样、插值、补齐等典型时序加工算子
- **可扩展连接器**：通过 SPI 机制扩展上下游连接器（Kafka、HDFS、对象存储等）
- **统一查询接口**：面向时序场景的查询 API 与执行引擎

> **项目定位**：ChronoLake 是一个**以 SQL 为表达力、以数据治理为基础、
> 以流式告警为闭环出口的可观测加工平台**。详见
> [`docs/design/business-architecture.md`](./docs/design/business-architecture.md)。
>
> **能力定位**：ChronoLake 解决**复杂场景、高基数场景下的时序数据加工
> 问题**；通过与 OLAP 引擎（ClickHouse）的后计算能力互补，综合解决
> **高基数复杂场景下的时序数据分析能力**。流式增量加工由 ChronoLake
> 自研引擎承担，历史回看 / 复杂查询 / 巡检试运行 / 可视化由 ClickHouse
> 承担，两者通过同一份 Calcite SQL 语义层协同。详见
> [ADR-0026 存储后端选型](./docs/adr/0026-storage-backend-selection.md)。

## 核心特性

- **SQL 灵活数据加工**：用流式 SQL 表达任意时序加工任务（pre-agg / 降采样 / 派生指标 / 复合 SLI）
- **数据全生命周期治理**：以数仓式 Meta 模型管理 Stream / Dim / Pipeline / AlertRule，自动产出 lineage / quality / SLA
- **流式告警 + 事件治理**：检测 / 丰富 / 生命周期管理一体化，AlertEvent 即流，可二次加工
- **维表一等公民**：注册 / 同步 / 版本化 / 时点 lookup，同时服务 SQL join 与告警丰富
- **高基数 + 复杂查询友好**：底层时序存储选定 ClickHouse，原生承载高基数 tag 与多 field（含 STRING field），完整 SQL 表达力支持窗口函数 / CTE / JOIN / 近似聚合
- **生态适配（非核心能力）**：通过协议层接入 OTLP / Prometheus remote_write / Grafana / Alertmanager 等

## 项目结构

```
chronolake/
├── chronolake-bom/                       # 依赖 BOM
├── chronolake-core/                      # L1 内核：SQL → LakeRelNode → LakePlan → runtime
├── chronolake-meta/                      # L2 治理：Asset / Lineage / Glossary / Quality
├── chronolake-dim/                       # L2 维表：注册 / 同步 / 版本化 / 时点 lookup
├── chronolake-alert/                     # L2 告警 (aggregator)
│   ├── chronolake-alert-core/            #   检测 / 事件总线 / 生命周期
│   └── chronolake-alert-notify/          #   通知出口（Webhook / IM / Email）
├── chronolake-stream/                    # L1.5 流式 data-plane（aggregator）
│   ├── chronolake-stream-core/           #   host-neutral SPI + 默认实现
│   └── chronolake-stream-flink/          #   Flink host adapter
├── chronolake-ingest/                    # L4 协议（采集，aggregator）
│   ├── chronolake-ingest-otlp/           #   OTLP gRPC / HTTP
│   ├── chronolake-ingest-prometheus/     #   remote_write
│   ├── chronolake-ingest-http/           #   通用 JSON / line-protocol
│   ├── chronolake-ingest-kafka/          #   Kafka source
│   └── chronolake-ingest-jdbc-cdc/       #   JDBC CDC（主用于 dim 同步）
├── chronolake-export/                    # L4 协议（外送，aggregator）
│   ├── chronolake-export-promwrite/      #   remote_write 输出
│   ├── chronolake-export-otlp/           #   OTLP exporter
│   ├── chronolake-export-promscrape/     #   /metrics scrape endpoint
│   └── chronolake-export-alertmanager/   #   Alertmanager v2 push
├── chronolake-storage-adapter/           # L4 协议（存储适配，aggregator）
│   └── chronolake-storage-clickhouse/    #   ClickHouseStorage：唯一存储后端（ADR-0026）
├── chronolake-server/                    # L5 进程：单进程入口，装配所有平面
├── chronolake-front/                     # L6 客户端：Web 管理控制台（Umi + Ant Design Pro）
├── chronolake-test-utils/                # L7 工具：跨模块测试辅助
├── chronolake-dist/                      # L8 发布：tar.gz 打包
├── build/                                # 跨语言构建脚本
├── docs/                                 # ADR / Design 文档
└── pom.xml                               # 顶层聚合 POM
```

## 模块说明

| 层级 | artifactId | 状态 | 说明 |
|------|------------|:----:|------|
| L0 | `chronolake-bom` | ⚪ 骨架 | 依赖 BOM，集中版本管理 |
| L1 内核 | `chronolake-core` | ✅ 已实现 | 流式引擎核心：SQL → LakeRelNode → LakePlan → runtime（详见 ADR-0001~0013） |
| L2 治理 | `chronolake-meta` | ⚪ 骨架 | Asset / Lineage / Glossary / Quality 模型，AI 集成基础（ADR-0014） |
| L2 维表 | `chronolake-dim` | ⚪ 骨架 | 维表生命周期与时点 lookup（ADR-0018） |
| L2 告警 | `chronolake-alert-core` / `chronolake-alert-notify` | ⚪ 骨架 | 流式检测 / 事件总线 / 生命周期 / 通知（ADR-0019~0021） |
| L1.5 流式 | `chronolake-stream-core` / `chronolake-stream-flink` | ⚪ 骨架 | 流式 data-plane runtime SPI + Flink host adapter |
| L4 协议 | `chronolake-ingest-*` | ⚪ 骨架 | 采集协议适配（OTLP / Prom / HTTP / Kafka / JDBC CDC） |
| L4 协议 | `chronolake-export-*` | ⚪ 骨架 | 外送协议适配（remote_write / OTLP / scrape / Alertmanager） |
| L4 协议 | `chronolake-storage-clickhouse` | ⏳ stub | ClickHouse 存储适配（唯一后端，ADR-0026） |
| L5 进程 | `chronolake-server` | ⚪ 骨架 | 单进程入口（HTTP/gRPC 框架选型见 ADR-0027） |
| L6 客户端 | `chronolake-front` | ⚪ 骨架 | Web 控制台 |
| L7 工具 | `chronolake-test-utils` | ⚪ 骨架 | 跨模块测试辅助 |
| L8 发布 | `chronolake-dist` | ⚪ 骨架 | 二进制分发打包（含前端静态资源） |

> **图例**：✅ 已实现并被测试覆盖；⚪ 骨架已落地（POM + 关键 SPI 接口），
> 实质实现按 ADR 路线图推进。
>
> 模块依赖严格自上而下分层（L1 → L2 → L3 → L4 → L5 → L6），同一层模块横向不互相依赖。

## 命名约定

- **groupId**：`io.chronolake`
- **artifactId**：全小写，连字符分隔，例如 `chronolake-core`
- **package**：`io.chronolake.<module>`（嵌套子模块用 `<aggregator>.<sub>`，例如 `io.chronolake.alert.core`、`io.chronolake.ingest.kafka`）
- **前端目录**：与后端模块同级，沿用 `chronolake-` 前缀（`chronolake-front`）

## 构建

完整构建（后端 + 前端 + 发行包）使用顶层脚本：

```bash
build/build-all.sh                # 完整构建
build/build-all.sh --skip-tests   # 跳过后端测试
build/build-all.sh --no-frontend  # 仅后端
```

也可分别构建：

```bash
# 仅后端（透传 Maven 参数）
build/build-backend.sh clean install -DskipTests

# 仅前端
build/build-frontend.sh

# 仅打发行包（依赖前两步产物）
build/build-dist.sh
```

构建脚本说明详见 [`build/README.md`](./build/README.md)。

## 本地开发启动

后端是 Spring Boot 单进程服务（默认 8080），前端是 umi-max SPA（默认 8000，
`.umirc.ts` 把 `/api` proxy 到 `127.0.0.1:8080`）。零外部依赖（默认 H2 in-memory，
启动时自动 apply IAM/Stream DDL）。

```bash
# 后端：起 dev server（任意目录可跑，自动 cd 到项目根）
JAVA_HOME=$(/usr/libexec/java_home -v 11) build/dev-backend.sh

# 前端：另开终端
build/dev-frontend.sh
# 浏览器打开 http://localhost:8000
```

完整步骤、IDE 配置、跨域 / 持久化 / 首启管理员等细节见
[`docs/dev/local-development.md`](./docs/dev/local-development.md)。

## 环境要求

- **后端**：JDK 11+，Maven 3.6.3+（推荐 3.9+）。基线版本与升级路径见 [ADR-0008](./docs/adr/0008-jdk-version.md)
- **前端**：Node.js 18+，pnpm（推荐）/ npm / yarn

> 构建会在 `validate` 阶段通过 [`build/checkstyle.xml`](./build/checkstyle.xml) 拦截
> 任何 JDK 12+ 语法（`sealed` / `record` / instanceof 模式 / switch 模式 /
> text block / `var`）。如果你在升级 baseline，请同步修改 ADR-0008、`pom.xml`
> 与该规则文件。

## 设计文档

架构与领域模型决策记录见 [`docs/`](./docs/README.md)：

### Design

- [产品整体设计（初稿 v0.1）](./docs/design/product-overview.md) — 产品视角整合稿：定位 / 用户 / 能力 / 形态 / 生态 / 路线图
- [业务架构设计：数据驱动的可观测加工平台](./docs/design/business-architecture.md) — 四项核心业务能力、模块结构、生态边界、ADR 路线图
- [SQL 语法设计](./docs/design/sql-language.md) — 当前 `chronolake-core` 实现的 SQL 子集、约束、示例库

### ADR（已落地）

- [ADR-0001：时序数据领域模型选型](./docs/adr/0001-time-series-data-model.md) — Multi-Field 模型（被 ADR-0004 / 0006 校正）
- [ADR-0002：SQL 作为查询入口的集成路线](./docs/adr/0002-sql-integration-strategy.md) — Calcite frontend（被 ADR-0003 部分校正）
- [ADR-0003：流式加工引擎定位（非 OLAP）](./docs/adr/0003-streaming-engine-positioning.md) — SQL 是任务定义入口
- [ADR-0004：Row 物理布局——独立紧凑行](./docs/adr/0004-row-physical-layout.md) — 内部行式紧凑 Row
- [ADR-0005：状态后端选型——单机内存先行](./docs/adr/0005-state-backend.md) — Phase 1 内存
- [ADR-0006：移除 core 的 Arrow Batch](./docs/adr/0006-remove-arrow-from-core.md) — core 零外部依赖
- [ADR-0007：流加工三原语 map / groupBy / join](./docs/adr/0007-streaming-primitives.md) — 完备最小集合
- [ADR-0008：JDK 版本基线 = Java 11](./docs/adr/0008-jdk-version.md) — 工程基线与升级路径
- ADR-0009 ~ ADR-0013 — 事件时间硬约束、温度处理、状态双层寻址、窗口对齐、EARLIEST/LATEST

## 参与贡献

ChronoLake 是一个开源项目，欢迎贡献代码、文档与设计反馈。

- 协作约定与开发规范：[CONTRIBUTING.md](./CONTRIBUTING.md)
- 行为准则：[CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)
- 安全漏洞报告：[SECURITY.md](./SECURITY.md)
- 第三方组件归属：[NOTICE](./NOTICE)

## License

ChronoLake 以 [Apache License 2.0](./LICENSE) 协议发布。提交贡献即表示同意以同一协议授权。

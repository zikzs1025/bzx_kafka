# kafka-file-loader

消费 Kafka `DISTRIBUTE_SQAJXYRXX_TO3301` 主题的消息，**按《数据表字段清单》31 个字段的顺序生成 CSV 文件（`\t` 分隔，5 万行一个分片）→ 本地备份 → 上传 FTP**。

**最终交付物就是 FTP 上的文件。本服务不连任何数据库**：
连库、建表、写库一概没有，**所有配置都写在 `application.yml` 里**（不读数据库、不读配置中心），数据一条都不落库，入库由下游自行按需处理。

配置与依赖**外置**：`application.yml`、`logback-spring.xml` 在 `config/` 目录，第三方 jar 在 `lib/` 目录，
业务 jar 只有 200 多 KB —— 改配置、换依赖版本都不用重新打包（详见「四、打包与一键启动」）。

三条设计主线：

1. **连接信息全在 application.yml** — `bootstrap.servers`、主题、FTP、文件管道参数集中在一处，每个配置项都带作用注释
2. **稳定性大于一切** — 数据写盘 `fsync` 成功才提交 offset；上传失败不丢数据；磁盘水位与积压背压自动限速
3. **可运维** — 定时/手动封盘与上传、Swagger 页面、一键启动脚本、日志按天压缩保留 15 天

## 数据流

```
application.yml: kafka.cluster.bootstrap-servers（16 个 broker）
        │
        ▼
ConfigKafkaNodeRepository ──> ClusterDefinitionProvider（不可变快照，启动即校验）
                                        │
                                        ▼
                              RefreshingConsumerFactory ──> KafkaConsumer
                                                                 │
Kafka topic ──> 批量监听(500 条/批) ──> FileStagingManager
                                            │ ① RecordRenderer 按 31 字段清单渲染成一行（\t 分隔）
                                            │    无法映射（非 JSON 对象）──> file-reject/ 隔离
                                            │ ② 按 (topic, partition) 独立写入
                                            │    每 5 万行滚动；空闲 60s 也封盘
                                            ▼
                                    stage/<topic>/*.writing
                                            │ flush + fsync + 原子改名
                                            ▼
                                    pending/系统配置名称_时间戳.txt   ← 崩溃重启会恢复此目录
                                            │
                                    ① 复制到 backup/dt=YYYY-MM-DD/（失败则不上传）
                                            │
                                    ② FTP 上传（临时名 .uploading → rename）
                                            │
                                    ③ 校验远端字节数一致
                                            │
                                    ④ 删除 pending（备份保留 7 天）
                                            ▼
                                    FTP: /kafka/incoming  ← 终点，流程到此结束
                                                            （不落库，下游自行取用）
```

## 一、稳定性设计（重点）

| 环节 | 机制 | 解决什么 |
|---|---|---|
| 写入 | 每批写完 `flush + fsync` 才提交 offset | 进程崩溃/断电后，已 ack 的数据一定在磁盘 |
| 滚动 | 写满 5 万条立即封盘；空闲 60s 也封盘 | 文件大小可控，低峰期数据不长期滞留 |
| 封盘 | `flush + fsync + 原子 rename` | 下游永远读不到半截文件 |
| 崩溃恢复 | 启动时扫描 `stage/**/*.writing`，非空即转 pending | 异常退出不丢已写入未封盘的数据 |
| 备份 | 上传**之前**先复制到 `backup/dt=.../` 并 `fsync` | 任何时刻数据至少两份 |
| 备份失败 | 跳过上传，下轮重试 | 宁可延迟，也不让数据只剩一份 |
| 上传 | 先传 `.uploading` 临时名，成功后 rename | 远端不会出现半截文件被误加载 |
| 上传校验 | 比对远端与本地字节数 | 传了半截或传错文件不会被当作成功 |
| 上传失败 | 保留 pending，按次重试（默认 10 次） | 不丢数据，网络抖动自动恢复 |
| 超限 | 移入 `failed/` 并写 `.reason` | 不静默丢弃，可人工介入 |
| 脏数据 | 消息体不是 JSON 对象 → 按天写入 `file-reject/` 后再提交 | 主文件永远 31 列不串行，原始报文完整留档 |
| 背压 | pending 文件数 > 2000 或磁盘剩余 < 2GB → 停止消费且不 ack | 上传追不上时自动降速，避免磁盘写满拖垮节点 |
| 停机 | `server.shutdown=graceful` + 关闭时封盘 | 停机不产生半截分片 |

目录结构：

```
data/
├── file-stage/<topic>/*.writing        # 正在写入（临时名，不对外）
├── file-pending/*.txt                  # 已封盘，等待备份+上传（31 列）
├── file-backup/dt=2026-10-22/*.txt     # 本地备份，保留 7 天
├── file-failed/*.txt + *.reason        # 重试上限仍未成功，人工处理
└── file-reject/<系统配置名称>_reject_yyyyMMdd.txt   # 无法映射的脏数据，按天归档
```

文件名规则：**`系统配置名称_时间戳<扩展名>`**（扩展名由 `file.file-extension` 决定，默认 `.txt`）

```
DISTRIBUTE_20261022173000123.txt
   │            │       └── 扩展名来自 file.file-extension（默认 .txt），字段 \t 分隔
   │            └────────── 封盘时刻，17 位毫秒时间戳 yyyyMMddHHmmssSSS（可直接按名排序）
   └─────────────────────── 系统配置名称 file.system-name，未配置时回退 kafka.cluster.name
```

同一毫秒内多个分区同时封盘时，后一个追加 `-1`、`-2` 区分（`DISTRIBUTE_20261022173000123-1.txt`）。
分配名字前会同时检查内存占位与磁盘同名文件，**绝不覆盖已有文件**。
pending / backup / FTP 全链路使用同一个名字，便于端到端核对。

## 二、文件格式

文本文件（扩展名由 `file.file-extension` 决定，默认 `.txt`），**字段之间用 `\t` 分隔**，一行一条消息，无表头，**固定 31 列**。
列顺序 = 《数据表字段清单.xlsx》序号 1~31，**按位置装载，顺序不可调整**：

| 序 | 字段名 | 类型 | 说明 |
|---:|---|---|---|
| 1 | `md_id` | STRING | 表主键 |
| 2 | `clue_id` | STRING | 线索ID/日志查询标识 |
| 3 | `clue_src_sys` | STRING | 线索布控/日志查询源 |
| 4 | `clue_dst_sys` | STRING | 线索布控目的地址/日志产生源 |
| 5 | `capture_time` | BIGINT | 截获时间(上线时间) |
| 6 | `protocol` | STRING | 协议代码(本数据) |
| 7 | `original_protocol` | STRING | 原始数据协议 |
| 8 | `data_source3` | STRING | 数据来源系统分类代码 |
| 9 | `mwbs` | STRING | 记录唯一标识 |
| 10 | `sensitive_level` | STRING | 数据敏感级别 |
| 11 | `saved_time` | BIGINT | 信息入库时间 |
| 12 | `remark` | STRING | 数据来源系统备注 |
| 13 | `data_check_id` | STRING | 对账标识ID |
| 14 | `bunit` | STRING | 采集地单位名称 |
| 15 | `data_set_name` | STRING | 科信数据集名称 |
| 16 | `dzwjwz` | STRING | 电子文件位置 |
| 17 | `asjxrybh` | STRING | 案事件相关人员编号 |
| 18 | `asjbh` | STRING | 案事件编号 |
| 19 | `zjhm` | STRING | 常用证件 证件号码 |
| 20 | `xm` | STRING | 姓名 |
| 21 | `xbdm` | STRING | 性别 |
| 22 | `xbmc` | STRING | 性别名称 |
| 23 | `mzdm` | STRING | 民族 |
| 24 | `mzmc` | STRING | 民族名称 |
| 25 | `csrq` | DATETIME | 出生日期 日期值下限 |
| 26 | `xzzqhdm` | STRING | 户籍地址 行政区划代码 |
| 27 | `xzzqhmc` | STRING | 户籍地址 地址名称 |
| 28 | `criminal_time` | DATETIME | 抓获日期 |
| 29 | `gajgjgdm` | STRING | 抓获单位 公安机关机构代码 |
| 30 | `gajgjmc` | STRING | 抓获单位 公安机关名称 |
| 31 | `synchro_time` | DATETIME | 更新时间 |

取值规则：

三种处理方式，由 `file.parse-mode` 决定：

| 模式 | 行为 |
|---|---|
| **RAW**（现场采用） | 原样透传：不解析、不校验列数、不补位，行内 TAB 与空字段原封不动 |
| **AUTO** | 首字符 `{` 按 JSON 对象取值；否则按 `file.delimiter-regex`（默认 `\t`）切分，按位置装载 |
| **JSON / DELIMITER** | 只用其中一种方式 |

- **一条消息可含多行**：上游按「批」投递时，一个消息体里常有成百上千行（行间 `\n`、列间 `\t`），
  程序按行拆分后**每行产出一行**，不会把整批当成一条
- 值**原样输出**，不做类型转换——`BIGINT` 的长整型精度、`DATETIME` 的原始格式都保留，避免 `double` 精度丢失与时区漂移
- 字段缺失 / 值为 `null` / 分隔符之间的空字段 → 输出**空字符串**（不写 `\N`，避免下游入库报错）
- 值里的制表符 / 回车 / 换行替换为空格，否则整行列错位
- **主文件严格 31 列**：`file.strict-field-count: true`（现场采用）——列数不等于 31 的消息**整条舍弃**
  （原始报文完整留存到隔离文件，`reason` 记 `line#N:field-count-mismatch(expected=31,actual=M)`，可按 offset 补数），
  主文件因此永远是干净的 31 列；设 `false` 才会补空值/截断（仅临时排查用，有错位风险）
- **分隔符必须用 `\t` 而不是 `\s+`**：空字段靠连续 TAB 占位，`\s+` 会合并连续分隔符导致整行左移
- 上游列序与清单不一致时用 `file.column-mapping` 转换（按报文顺序写目标列号，如 `"19,20,21,25"`）
- JSON 形态下 payload 里的**其他字段（含嵌套对象/数组）自动忽略**；流式解析只取这 31 个键，报文再大也不会整棵解析进内存
- 空体、截断 JSON、字段数不符 → 不写主文件，转存 `file-reject/` 隔离文件（详见下文），主文件因此永远是干净的 31 列

> 分隔符是制表符而不是逗号：`payload` 类字段里逗号极多，用 `\t` 才能保证不串列。
> 用 Excel 打开时，在导入向导里把分隔符选为“制表符”。
> 下游任何按位置装载的工具（LOAD DATA / sqoop / DataX / 自写脚本）都可以直接按 `\t` 切分；
> 没有表头，第 1 列就是 `md_id`，列数与顺序以 `GET /api/admin/schema` 返回为准。

**隔离文件**（`data/file-reject/<系统配置名称>_reject_yyyyMMdd<扩展名>`，扩展名同 `file.file-extension`，默认 `.txt`）列顺序固定为
`topic / partition / offset / record_ts / consumed_at / payload / reason`，带表头，用于事后补数与排查——
保留 offset 是为了能用 `kafka-console-consumer --offset` 精确重取；
`payload` 里的制表符与换行**保存为字面量 `\t` / `\n`**（否则会撑坏 CSV 列），排查时按需还原；
`reason` 取值 `empty-payload` / `json-invalid` / `line#N:field-count-mismatch(expected=31,actual=M)`。

配套脚本：
- `./bin/inspect-payload.sh` — 从隔离文件取一条报文，还原 `\t/\n` 后统计行数与每行列数
- `./bin/dump-message.sh <partition> <offset> [client.properties]` — 用 Kafka 客户端按 offset 取**原始报文**，
  `cat -A` 显示 TAB(`^I`) 与行尾(`$`)，是判断真实格式最可靠的方式

本服务只负责生成文件，**不提供任何建表语句**；下游按上表自建表即可。

## 三、Kafka 节点配置（全部在 application.yml）

节点只有一个来源：`application.yml` 的 `kafka.cluster.bootstrap-servers`。**不读任何外部节点文件，也不查数据库。**

```yaml
kafka:
  cluster:
    name: DISTRIBUTE
    topic: DISTRIBUTE_SQAJXYRXX_TO3301
    default-port: 9092
    bootstrap-servers:
      - cmkafka:9092    # 12.100.1.180
      - kafka01:9092    # 12.100.1.181
      # ... 共 16 个
```

两种写法等价，任选其一：

```yaml
# 写法 1：列表，一行一个节点，便于逐个加注释（当前采用）
bootstrap-servers:
  - cmkafka:9092
  - kafka01:9092

# 写法 2：逗号分隔串，一行写完
bootstrap-servers: cmkafka:9092,kafka01:9092
```

解析规则：

- 端口可省略，省略时用 `kafka.cluster.default-port`
- 重复的 `host:port` 自动去重
- 单条格式非法（端口非数字、端口超出 1~65535、主机名含非法字符、IPv6 括号未闭合）只打 WARN 跳过，不影响其他节点
- 一条都解析不出来 / 完全没配 → **启动直接失败**，绝不带着空地址去连 Kafka
- IPv6 必须写成 `[::1]:9092`（方括号保留，这是 Kafka 唯一认的写法）

> **Kerberos 场景必须用主机名而非 IP**：客户端向 KDC 申领的服务票据是
> `kafka/<连接用的名字>@SYNWAY.COM`，用 IP 连会去要 `kafka/12.100.1.180@SYNWAY.COM`（KDC 中不存在），
> 报 `GSSException: Server not found in Kerberos database (7) - LOOKING_UP_SERVER`。
> 部署机需先执行 `./bin/setup-hosts.sh` 写入 16 条主机名映射（幂等，重复执行安全）。
> 认证相关配置见「Kafka 连接安全」（`kafka.cluster.security`），细节见 `docs/kafka-security-plan.md`。

> 改节点 = 改 yml + 重启进程。本版本不再支持热重载：配置文件在启动时一次性绑定，
> 不再有定时重读任务，也没有 `/api/admin/cluster/refresh` 接口。
> 当前生效的节点可用 `GET /api/admin/status` 查看，`nodeSource` 固定为
> `application.yml (kafka.cluster.bootstrap-servers)`，`nodeCount` 为解析出的节点数。

## 四、打包与一键启动

`mvn clean package` 除了 jar，还会在 `target/dist/` 产出一套**可直接拷到服务器运行**的分发包，
并打成发布包 **`target/kafka-file-loader-1.0.0.tar.gz`**（`服务名-版本号.tar.gz`，
`includeBaseDirectory=false`，解压即部署）：

```
dist/
├── kafka-file-loader.jar      业务代码，约 220KB（不含第三方依赖、不含配置文件）
├── lib/                       63 个第三方 jar，约 41MB
├── config/
│   ├── application.yml        全部业务配置，改完重启生效，**不用重新打包**
│   └── logback-spring.xml     日志策略
├── bin/                       start.sh / start.bat
├── 更新说明.txt                按年月日累积的变更与打包记录
├── logs/                      运行日志（首次启动自动创建）
└── README.md
```

### 更新说明（`CHANGELOG.txt`）

项目根的 `CHANGELOG.txt` 是变更记录的**唯一来源**，打包时以 `更新说明.txt` 随包分发：

- 每次 `mvn package` 都会在**当天日期分组**下自动追加一条
  `- 2026-10-23 12:30:15  打包构建 v1.0.0`；当天还没有记录时，先自动补
  `【2026-10-23】版本 1.0.0` 标题，同一天多次打包不会重复建标题
- 功能变更由开发者在同一日期分组内手写（按 `  - 内容` 缩进即可），
  自动追加的构建行会接在手写内容之后
- 文件编码固定 UTF-8（构建时以 UTF-8 读写，避免 Windows 下中文乱码）

三处配套的构建配置（改 `pom.xml` 时别漏）：

| 位置 | 作用 |
|---|---|
| `spring-boot-maven-plugin`：`layout=ZIP` + `includes` 排除全部依赖 | 启用 `PropertiesLauncher`，jar 只留自身 class |
| `maven-jar-plugin` 的 `manifestEntries.Loader-Path=lib` | 裸 `java -jar` 也能找到 `lib/`，不必手写 `-Dloader.path` |
| `resources` 排除 `application.yml` / `logback-spring.xml` | 配置不进 jar，只能从外部 `config/` 读 |

Windows：`bin\start.bat`（前台运行，关窗口即停）
Linux：

```bash
cd dist
./bin/start.sh start                        # 后台启动
./bin/start.sh start --server.port=18090    # 透传任意 Spring 参数
./bin/start.sh status                       # 显示真实 PID 与实际端口
./bin/start.sh restart
./bin/start.sh stop                         # 优雅停机：等待批次完成并封盘
```

脚本最终执行的命令：

```bash
java -Dfile.encoding=UTF-8 \
     -Dloader.path=lib \
     -Dlogging.config=config/logback-spring.xml \
     -jar kafka-file-loader.jar \
     --logging.file.path=logs
```

`bin/start.sh` 在源码根目录也能直接跑，会自动切到 `target/dist`；分发包缺产物时自动执行
`mvn clean package -DskipTests`。

**配置优先级**（高 → 低）：命令行参数 > 操作系统环境变量 > `config/application.yml`。
`--server.port=18090` 一定压过配置文件；反过来，若服务器上已有 `SERVER_PORT` 之类环境变量，
它会盖掉 yml 里的值 —— 端口和预期不一致时先 `env | grep -i server` 看一眼。

`stop` 的 PID 不是简单取 `$!`：脚本用 `jps` 反查命令行含 `kafka-file-loader.jar` 的真实 JVM 进程，
避免 Git Bash 等环境下 `$!` 只是外层包装 shell、导致进程杀不掉却提示「已停止」。
Windows 控制台进程收不到 SIGTERM，`stop` 走 `taskkill /F`；Linux 下是标准 SIGTERM，
会等批次完成并把残留分片封盘。

## 五、日志

日志由外置的 `config/logback-spring.xml` 控制（启动时通过 `-Dlogging.config` 加载，不进 jar），
路径与级别由 `config/application.yml` 的 `logging.*` 决定。

| 项 | 值 |
|---|---|
| 运行日志 | `logs/kafka-file-loader.log`（当前写入文件，可 `tail -f`） |
| 归档日志 | `logs/kafka-file-loader.yyyy-MM-dd.N.log.gz`（**每日切分 + 自动 gzip**） |
| 单文件上限 | 200MB，同一天超过则切 `.0 / .1 / .2` 分片 |
| 保留时长 | **15 天**（`maxHistory=15`），第 16 天的文件自动删除 |
| 总量上限 | 20GB（`totalSizeCap`），超出先删最旧的 |
| 启动清理 | `cleanHistoryOnStart=true`，服务长期不重启也能按时清理 |
| 写入方式 | 异步队列（8192 容量），队列满时阻塞业务线程而不丢日志 |
| 行格式 | `时间(毫秒) 级别 [线程] PID logger:行号 - 消息` |

详细程度：业务包 `com.example.kafkaloader` 默认 **DEBUG**，能看到节点解析结果、每批写入条数、
文件滚动、备份、上传尝试、脏数据隔离、背压触发；`org.apache.kafka` 压到 **WARN** 避免刷屏。

需要临时调整时改 `application.yml` 的 `logging.level.*`（优先级高于 logback 文件），或启动时加参数：
`java -jar xxx.jar --logging.level.org.apache.kafka=DEBUG --logging.file.path=/data/logs`。

滚动参数也可用 JVM 参数临时覆盖，不必改 XML：
`-DLOG_MAX_FILE_SIZE=100MB -DLOG_MAX_HISTORY=30`。

`bin/start.sh` 会把进程早期输出（logback 就绪前的异常）单独写到 `logs/console.out`，
不会与滚动日志抢同一个文件句柄。

## 六、Swagger 与运维接口

启动后访问 **http://localhost:8080/swagger-ui.html**

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/admin/files/flush` | 立即封盘当前分片（低峰期/停机前手动推进） |
| POST | `/api/admin/files/upload` | 立即备份并上传待处理文件（与定时任务互斥） |
| GET | `/api/admin/files/stats` | 写入中/待上传/备份/隔离区文件数、磁盘剩余 |
| GET | `/api/admin/schema` | 文件 31 个字段的名称/类型/顺序（核对下游建表用） |
| GET | `/api/admin/status` | 集群、FTP、文件管道状态总览 |

```bash
curl -X POST http://localhost:8080/api/admin/files/flush
# {"sealedFiles":1,"pendingFiles":1,"message":"已封盘 1 个分片"}

curl -X POST http://localhost:8080/api/admin/files/upload
# {"executed":true,"scanned":1,"uploaded":1,"failed":0,"backupSkipped":0,"elapsedMs":820}
```

## 七、主要配置

每个配置项在 `application.yml` 里都带有作用说明，下表为速查。**全部配置集中在 application.yml，无外部配置文件。**

连接与集群：

| 配置项 | 说明 | 默认 |
|---|---|---|
| `kafka.cluster.name` | 集群标识，仅用于日志与展示 | `DISTRIBUTE` |
| `kafka.cluster.topic` | 消费主题（必填，启动时校验格式） | `DISTRIBUTE_SQAJXYRXX_TO3301` |
| `kafka.cluster.bootstrap-servers` | broker 列表，唯一节点来源 | 16 个节点 |
| `kafka.cluster.default-port` | 节点省略端口时的兜底端口 | `9092` |
| `kafka.consumer.group-id` | 消费组名（必须与 ACL 授权的组一致） | `330100` |
| `kafka.consumer.concurrency` | 并发消费线程数，不宜超过分区数 | `3` |
| `kafka.consumer.max-poll-records` | 单批条数，等同写入批量 | `500` |
| `kafka.consumer.auto-offset-reset` | 无 offset 时起点，`earliest` 优先不丢 | `earliest` |
| `kafka.consumer.poll-timeout-ms` | 单次 poll 等待时间 | `3000` |
| `kafka.consumer.max-poll-interval-ms` | 两次 poll 最大间隔，须大于批量落盘耗时 | `300000` |
| `kafka.consumer.fetch-max-bytes` | 单次 fetch 上限，限制内存峰值 | `52428800` |
| `kafka.consumer.retry-interval-ms` | 处理失败重试间隔 | `2000` |
| `kafka.consumer.max-retries` | 重试次数，`-1` 无限（数据安全优先） | `-1` |

文件管道：

| 配置项 | 说明 | 默认 |
|---|---|---|
| `file.records-per-file` | 单文件行数，达到即滚动 | `50000` |
| `file.system-name` | 文件名前缀（系统配置名称），留空回退 `kafka.cluster.name` | `DISTRIBUTE` |
| `file.fsync-per-batch` | 每批强制刷盘（稳定性关键，不建议关） | `true` |
| `file.idle-flush-ms` | 空闲多久封盘 | `60000` |
| `file.flush-check-interval-ms` | 空闲检查周期 | `30000` |
| `file.upload-interval-ms` | 上传任务间隔 | `60000` |
| `file.max-pending-files` | 待上传积压上限（触发背压） | `2000` |
| `file.min-free-disk-mb` | 磁盘低水位（触发背压） | `2048` |
| `file.reject-dir` | 脏数据隔离目录 | `./data/file-reject` |
| `file.backup-retention-days` | 本地备份保留天数 | `7` |
| `file.upload-max-attempts` | 单文件上传最大重试次数 | `10` |
| `file.backup-cleanup-cron` | 备份清理 cron | `0 30 3 * * ?` |
| `file.recover-on-start` | 启动时恢复遗留的 `.writing` 分片 | `true` |

FTP：

| 配置项 | 说明 | 默认 |
|---|---|---|
| `ftp.enabled` | 总开关，关掉后只落 pending 不外发 | `true` |
| `ftp.host` / `port` / `username` / `password` | FTP 连接（密码建议走环境变量） | — |
| `ftp.remote-dir` | 远端目录，不存在自动逐级创建 | `/kafka/incoming` |
| `ftp.passive-mode` | 被动模式 | `true` |
| `ftp.encoding` | 控制连接编码，文件名含中文须与服务端一致 | `UTF-8` |
| `ftp.connect-timeout-ms` / `data-timeout-ms` | 连接 / 传输超时 | `10000` / `120000` |
| `ftp.buffer-size` | 传输缓冲区 | `1048576` |
| `ftp.verify-size` | 回读远端大小并与本地比对 | `true` |
| `ftp.delete-after-upload` | 上传确认后删除本地 pending（备份仍留） | `true` |

日志：

| 配置项 | 说明 | 默认 |
|---|---|---|
| `logging.file.path` | 日志根目录（logback 从这里取） | `./logs` |
| `logging.level.root` | 全局级别 | `INFO` |
| `logging.level.com.example.kafkaloader` | 业务包级别 | `DEBUG` |
| `logging.level.org.apache.kafka` | Kafka 客户端，过详细故压到 WARN | `WARN` |

账号密码走环境变量：`FTP_PASSWORD=xxx ./bin/start.sh start`

## 八、监控

`/actuator/health`、`/actuator/metrics`：

- `file.chunks.backup` — 已备份文件数
- `file.chunks.uploaded` — 上传成功文件数
- `file.chunks.upload_failed` — 上传失败次数

**重点告警**：
1. `pendingFiles` 持续上涨（上传追不上，接近 2000 会触发背压）
2. `diskFreeMb` 逼近 `min-free-disk-mb`
3. `failedFiles > 0`（需人工处理隔离区）
4. `stageFiles` 长期不变但 consumer 有流量（写入异常）
5. 日志目录出现 `kafka-file-loader.*.log.gz` 数量异常增长（说明 DEBUG 量偏大）

## 九、测试

```bash
mvn test
```

48 个用例，覆盖：5 万条滚动切分与跨批滚动、空闲封盘、崩溃恢复、背压拒绝、**31 字段顺序映射**（乱序报文/清单外字段/嵌套对象/重复键/长整型精度/`\N` 占位/脏字符）、**脏数据隔离**、文件名格式/唯一性/系统配置名称回退与非法字符清理、备份-上传-清理链路、上传失败保留与超限隔离、备份过期清理、**yml 节点解析**（列表/逗号串/端口兜底/去重/IPv6/非法项跳过/全非法失败）、**集群定义校验**（主题格式、初始化前后访问、异常透传）。

实测记录：
- 本地冒烟（`--server.port=18082`）：`/api/admin/status` 返回 `nodeCount=16`、`nodeSource=application.yml (kafka.cluster.bootstrap-servers)`，日志首行含毫秒时间戳与 PID。
- FTP 不可达时 `uploaded=0 / failed=1`，pending 文件保留、`backup/dt=2026-10-22/` 中已生成同名副本，数据零丢失。
- 生成文件形如 `DISTRIBUTE_20261023091959139.csv`，逐列核对 31 列顺序与清单一致；混入一条纯文本报文时主文件仍为 1 行 31 列，脏数据进了 `DISTRIBUTE_reject_20261023.csv`。
- 日志滚动实测（`-DLOG_MAX_FILE_SIZE=8KB`）：产生 `kafka-file-loader.2026-10-23.0~4.log.gz`，`gunzip` 可正常解压；再以 `-DLOG_MAX_HISTORY=2` 重启，`2026-10-05/06/07` 的归档被 `cleanHistoryOnStart` 自动删除。

## 十、与旧方案的差异

| 项 | 旧 | 现 |
|---|---|---|
| 交付方式 | 直接写库 | **只生成文件并投递 FTP**，全程不碰业务库 |
| 集群配置 | Oracle `KAFKA_CLUSTER_NODE` 表 + JDBC 轮询 | **application.yml 的 `kafka.cluster.bootstrap-servers`**，零外部依赖 |
| 节点变更 | 定时重读 / 调接口热生效 | 改 yml + 重启（配置一次性绑定） |
| 失败处理 | 本地 spool + 重放 | stage → pending → backup → FTP 状态机 |
| 背压 | 无 | 磁盘水位 + 积压上限，自动限速 |
| 日志 | 单文件无限增长 | 按天 gzip 归档，保留 15 天，200MB 分片 |

## 消费组（重要）

`kafka.consumer.group-id` 现场取 **`330100`**，因为 Kafka ACL 是按「账号 `330100` + 消费组 `330100`」授权的，
组名与 ACL 不一致会直接报 `GroupAuthorizationException: Not authorized to access group: xxx`。

换组名等于换一个全新消费者，在 `auto-offset-reset: earliest` 下会把该主题历史消息全部重投一遍，
FTP 上会出现重复文件。确需改名时按下面顺序操作：

1. 先停服务，确认旧组 lag 已为 0
2. 记录各分区当前 offset：`kafka-consumer-groups.sh --bootstrap-server <节点> --describe --group <旧组>`
3. 改成新组名后，用 `--reset-offsets --to-offset <上面记录的偏移> --execute` 把新组起点对齐
4. 或者接受重复、由下游按 `md_id` 去重

> 首次切到 `330100` 组时，先用
> `kafka-consumer-groups.sh --describe --group 330100` 确认该组在目标主题上是否已有提交的 offset：
> 有则从断点续消费，无则按 `earliest` 从头消费。

## 已知限制

- 文件名不含 topic/partition/条数（按需求固定为 `系统配置名称_时间戳<扩展名>`），排查时对照日志或隔离文件里的 offset。
- 文件按字段清单不再携带 offset，**去重要靠业务主键 `md_id`**（为空时可用 `mwbs`）；同一 `md_id` 下的多条不同报文，下游去重时会被折叠成一条，需按业务确认主键唯一性。
- 消息体必须是 JSON 对象才能映射；若上游发的是 JSON 字符串套 JSON（二次转义），会整条进隔离区，需在上游修正或加一层解包。
- 每个分区一个写入器，分区多时同时打开的文件句柄较多（分区数 × 1）。
- FTP 为明文协议，跨公网请改用 SFTP/VPN；`FtpUploader` 接口已抽象，可替换实现。
- 节点/主题/消费组变更均需重启进程：配置在启动时一次性绑定，本版本不支持热重载。
- 日志保留 15 天由滚动策略保证，前提是进程持续运行或定期重启（启动时会清理超期文件）；若进程长期停机则归档文件不会被处理。

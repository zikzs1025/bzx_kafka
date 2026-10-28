# 压测说明与实测结论

本文记录 `kafka-file-loader` 的压测方案、实测数据与结论。
压测代码：`src/test/java/com/example/kafkaloader/perf/PipelineLoadTest.java`

---

## 一、压测怎么做的

### 环境（不依赖外部 Kafka / FTP）

| 组件 | 用什么 |
|---|---|
| Kafka | `spring-kafka-test` 的 `EmbeddedKafkaBroker`，JVM 内真实 broker（单节点 3 分区，带嵌入式 ZooKeeper） |
| FTP | `MockFtpServer` 起一个本地 FTP 服务端，`commons-net` 走 **真实 FTP 协议**（loopback TCP） |
| 被测应用 | 用 `SpringApplicationBuilder` 起真实 Spring 上下文，配置全部通过命令行参数注入 |

**链路一行都没绕过**：真 Kafka 消息 → `@KafkaListener` 批量消费（500 条/批）→ Jackson 流式渲染 31 列
→ 按 `(topic, partition)` 写分片 → 满 5 万行滚动 → `flush + fsync` → 原子改名进 `pending`
→ 备份到 `backup/dt=...` → `CommonsNetFtpUploader` 上传（临时名 `.uploading` → rename → 回读大小校验）
→ 删除 pending。只把「外部 Kafka 集群」「远端 FTP 服务器」换成了本地进程。

### 每个场景都做的硬校验（不是只看吞吐）

- **零丢失**：备份文件总行数 == 生产条数（逐行读盘统计）
- **列数正确**：每一行按 `\t` 切开必须恰好 **31 列**（全量校验，不抽样）
- **滚动正确**：单文件行数 <= 50000
- **上传一致**：通过 FTP 协议 `LIST` 回读远端，文件数与字节数必须等于本地备份
- **脏数据为零**：`rejectedRecords == 0`、`failed/` 目录为空、`pending` 残留为 0

### 怎么复跑

```bash
# 默认规模：20万 + 50万 + 20万对照 + 10万故障注入，约 80 秒
mvn test -Dtest=PipelineLoadTest

# 自定义规模
mvn test -Dtest=PipelineLoadTest -Dloadtest.records=1000000 -Dloadtest.sustained-records=1000000

# 只跑某一个场景
mvn test -Dtest=PipelineLoadTest#scenarioProductionSettings
```

实测数据自动写入 `target/loadtest/report.md`（每次覆盖），本文件的数据即来自该报告。

### 分离打包后怎么复跑

压测不依赖分发包：`PipelineLoadTest` 走的是测试 classpath（`target/classes` + `target/test-classes`），
不读 `target/dist`。所以 `mvn test -Dtest=PipelineLoadTest` 在任何时候都能跑。

要验证**分发包本身**的加载路径（`-Dloader.path`、外置 `config/`），用冒烟验证：

```bash
cd target/dist
./bin/start.sh start --server.port=18090
curl -s http://127.0.0.1:18090/api/admin/status      # systemName / fieldCount=31 / nodeCount=16
./bin/start.sh stop
```

想看「改配置不重新打包」：改 `config/application.yml` 的 `system.name` 后 `restart`，
状态接口的 `systemName` 与 `fileNamePattern` 会立即跟着变。

---

## 二、实测结果

环境：Windows 11 / 18 核 / JDK 1.8.0_503 / JVM 最大堆 7195 MB / 单条报文 957 字节（落盘后每行 416 字节）

### 汇总

| 场景 | 条数 | 端到端吞吐 | 落盘 | 文件数 | 丢数据 |
|---|---|---|---|---|---|
| 场景一：生产默认（fsync=true，5万行/文件） | 200,000 | 42,061 条/秒 | 16.71 MB/秒 | 6 | 0 |
| 场景二：持续压力 | 500,000 | 55,109 条/秒 | 21.90 MB/秒 | 12 | 0 |
| 场景三：对照（fsync=false） | 200,000 | 40,453 条/秒 | 16.07 MB/秒 | 6 | 0 |
| 场景四：FTP 中断 8s 后恢复 | 100,000 | 中断中 | 恢复 967ms 补齐 | 3 | 0 |

### 场景一：生产默认配置（20 万条）

| 指标 | 实测值 |
|---|---|
| 生产阶段（客户端灌数据） | 1,895 ms，105,541 条/秒 |
| 端到端（首条生产 → 最后一条提交 offset） | 2,524 ms，**42,061 条/秒** |
| 封盘 3 个 + 上传 6 个文件（79.5 MB） | 2,214 ms |
| 全链路总耗时 | 4,755 ms |
| 生成文件 | 6 个，最大 50,000 行，共 79.5 MB |
| 跑完后堆占用 | 131 MB |

6 个文件的构成正好符合设计：200,000 / 3 分区 ≈ 66,667 条每分区 → 每分区 1 个满 50,000 的文件 + 1 个 16,667 的尾文件。

### 场景二：持续压力（50 万条）

| 指标 | 实测值 |
|---|---|
| 端到端吞吐 | **55,109 条/秒**（21.90 MB/秒） |
| 生成文件 | 12 个，最大 50,000 行，共 198.7 MB |
| 封盘 + 上传 12 个文件 | 2,859 ms |
| 跑完后堆占用 | 261 MB |

吞吐比 20 万条场景高约 30%，原因是固定开销（上下文启动、首轮元数据拉取、JIT 预热）被摊薄，属于正常现象，说明**吞吐随数据量不衰减**。

### 场景三：fsync 对照（20 万条，fsync=false）

| 指标 | fsync=true | fsync=false | 差异 |
|---|---|---|---|
| 端到端吞吐 | 42,061 条/秒 | 40,453 条/秒 | 噪声范围内 |
| 落盘吞吐 | 16.71 MB/秒 | 16.07 MB/秒 | 噪声范围内 |

**结论：瓶颈不在 fsync。** 500 条/批意味着每批只 fsync 一次，本机 SSD 上这点开销被 JSON 渲染和写盘掩盖了。
所以「稳定性优先、每批强制刷盘」在当前配置下**几乎不花性能代价**，没有理由关掉它。

### 场景四：FTP 中断 8 秒再恢复（10 万条）

| 指标 | 实测值 |
|---|---|
| 故障期间已提交 offset | 100,000（数据已安全落盘，一条不少） |
| 故障期间 pending 保留文件 | 3 |
| 中断时长 | 8,000 ms |
| 恢复后补齐耗时 | 967 ms |
| 恢复后 pending 残留 / 进 failed | 0 / 0 |
| 远端文件数与字节 | 3 个 / 41,666,720 字节，与本地备份完全一致 |

这条验证的是最关键的承诺：**FTP 挂了不等于数据丢**。故障期间消费照常进行、offset 照常提交（因为数据已经 fsync 到本地盘），
文件老老实实躺在 `pending/`；FTP 一恢复，下一轮扫描就把积压补齐并核对字节数。

---

## 三、这些数字怎么用到生产

- **容量换算**：约 16–22 MB/秒落盘，相当于 **1.4–1.9 TB/天**。若上游日均 100 GB，占用不到本机 10% 的处理能力。
- **单实例结论**：3 并发 + 500 条/批的配置下，单实例处理万级条/秒没有压力；真正的上限更可能来自 FTP 网络带宽，而不是本服务。
- **分区数要对齐**：`concurrency=3` 时主题分区数应 >= 3，否则多出的线程空转。要提速先加分区分并发。
- **FTP 是外部瓶颈**：本机 loopback 上传 79.5 MB 花 2.2 秒（约 36 MB/s），真实网络（尤其跨机房）会慢一个量级。
  若 FTP 成为瓶颈，`pending` 会积压，触发 `max-pending-files` 背压自动限速——这是设计好的保护，不会把磁盘写满。

---

## 四、必须说明的局限（不要拿这些数字直接当生产 SLA）

1. **Broker、生产者、消费者在同一台机器同一个 JVM**，抢的是同一份 18 核 CPU。真实部署里 broker 在别的机器，
   本服务独占 CPU，数字会更好；但如果 broker 与消费者同机部署，会互相拖慢。
2. **FTP 是 loopback + 虚拟文件系统**（MockFtpServer 把文件存在内存里），不含真实网络延迟和远端磁盘 IO。
   上传耗时应视为**下限**，实际会更慢。
3. **Kafka 是单节点**，没有副本同步开销（`acks=1` 生产）。真实集群 `min.insync.replicas` 会影响生产侧吞吐，不影响消费侧。
4. **测试数据是固定模板生成的**：31 个字段都齐全、结构一致，Jackson 解析路径最短。真实报文若有大量清单外字段、
   超长字符串或异常结构，渲染耗时会上升。
5. **未测**：磁盘写满、pending 积压触发的背压路径、长时间（小时级）运行的内存稳定性。前三者由单元测试覆盖逻辑，
   但没在压测里跑长稳。

---

## 五、顺带验证出来的事

- `CommonsNetFtpUploaderTest` 用真实 FTP 协议验证了上传链路：登录 → 建目录 → `.uploading` 临时名 → rename →
  回读大小校验，并断言不残留临时文件；服务不可达时快速失败（不无限挂起）。
- 压测过程中发现 MockFtpServer 把 `/xxx` 解释为「相对 home」，与真实 FTP 服务器的绝对路径语义不同。
  这是**模拟服务器的差异，不是产品问题**：压测里已把远端设为根目录规避，目录创建逻辑由上面的单元用例单独覆盖。

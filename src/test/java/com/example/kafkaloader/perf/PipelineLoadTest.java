package com.example.kafkaloader.perf;

import com.example.kafkaloader.KafkaLoaderApplication;
import com.example.kafkaloader.file.FileStagingManager;
import com.example.kafkaloader.file.FileUploadService;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockftpserver.fake.filesystem.WindowsFakeFileSystem;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端压测：JVM 内嵌入式 Kafka broker + 本地真实 FTP 服务端（MockFtpServer，loopback TCP）。
 * <p>
 * 被测链路完全是生产代码：Kafka 批量监听 -> Jackson 流式渲染 31 列 -> 按 (topic,partition) 写分片
 * -> 每 5 万行滚动 -> flush+fsync -> 原子改名进 pending -> 备份到 backup -> 真实 FTP 协议上传 -> 回读大小校验。
 * 只把「外部 Kafka 集群」「远端 FTP 服务器」换成本地进程，链路本身一行都不绕过。
 * <p>
 * 规模可调：
 * <pre>
 *   -Dloadtest.records=200000            场景一总条数
 *   -Dloadtest.compare-records=100000    场景二（fsync 对照）总条数
 *   -Dloadtest.wave=20000                每波发送条数
 *   -Dloadtest.partitions=3              分区数
 *   -Dloadtest.records-per-file=50000    单文件滚动阈值
 * </pre>
 * 结果打印到控制台，并写入 target/loadtest/report.md。
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PipelineLoadTest {

    private static final String TOPIC = "DISTRIBUTE_SQAJXYRXX_TO";
    private static final String GROUP = "kafka-hive-loader";
    // MockFtpServer 把 "/x" 解释为「相对当前目录」，而真实 FTP 按绝对路径解析，
    // 因此压测把远端设为根目录（上传代码对 "/" 直接跳过建目录），
    // 目录创建逻辑另由 CommonsNetFtpUploaderTest 覆盖
    private static final String REMOTE_DIR = "/";
    private static final int PARTITIONS = Integer.getInteger("loadtest.partitions", 3);
    private static final int RECORDS_PER_FILE = Integer.getInteger("loadtest.records-per-file", 50_000);
    private static final int WAVE = Integer.getInteger("loadtest.wave", 20_000);

    private final StringBuilder report = new StringBuilder("# 端到端压测结果（本地嵌入式环境实测）\n");
    private final List<String[]> summary = new ArrayList<>();

    @org.junit.jupiter.api.BeforeAll
    void environment() {
        Runtime rt = Runtime.getRuntime();
        report.append("\n## 测试环境\n\n");
        report.append("| 项 | 值 |\n|---|---|\n");
        report.append("| 操作系统 | ").append(System.getProperty("os.name")).append(' ')
                .append(System.getProperty("os.version")).append(" (").append(System.getProperty("os.arch"))
                .append(") |\n");
        report.append("| CPU 核心 | ").append(rt.availableProcessors()).append(" |\n");
        report.append("| JDK | ").append(System.getProperty("java.version")).append(" |\n");
        report.append("| JVM 最大堆 | ").append(rt.maxMemory() / 1024 / 1024).append(" MB |\n");
        report.append("| Kafka | spring-kafka-test 嵌入式 broker（单节点，" + PARTITIONS + " 分区） |\n");
        report.append("| FTP | MockFtpServer（loopback TCP，真实 FTP 协议，虚拟文件系统） |\n");
        report.append("| 单文件滚动阈值 | ").append(RECORDS_PER_FILE).append(" 行 |\n");
        report.append("\n> 被测链路为生产代码：Kafka 批量监听 → 31 列渲染 → 分片滚动 → fsync → 备份 → FTP 上传 → 大小校验。\n");
    }

    @AfterAll
    void writeReport() throws IOException {
        if (!summary.isEmpty()) {
            report.append("\n## 汇总对照\n\n");
            report.append("| 场景 | 条数 | 端到端吞吐(条/秒) | 落盘(MB/秒) | 文件数 | 丢数据 |\n");
            report.append("|---|---|---|---|---|---|\n");
            for (String[] row : summary) {
                report.append("| ").append(String.join(" | ", row)).append(" |\n");
            }
            report.append("\n> 所有场景均校验：备份行数 == 生产条数、每行 31 列、单文件 <= 50000 行、"
                    + "FTP 远端文件数与字节数 == 本地备份。\n");
        }
        Path dir = Paths.get("target", "loadtest");
        Files.createDirectories(dir);
        Files.write(dir.resolve("report.md"), report.toString().getBytes(StandardCharsets.UTF_8));
        System.out.println("\n压测报告已写入: " + dir.resolve("report.md").toAbsolutePath());
    }

    /** 场景一：生产默认配置（fsync 开启、5 万行/文件、31 列渲染、真实 FTP 协议上传） */
    @Test
    @Order(1)
    void scenarioProductionSettings() throws Exception {
        runScenario("场景一：生产默认配置（fsync=true，50000 行/文件）",
                Integer.getInteger("loadtest.records", 200_000), true);
    }

    /** 场景二：持续压力，看吞吐是否随数据量衰减、文件滚动是否稳定 */
    @Test
    @Order(2)
    void scenarioSustainedLoad() throws Exception {
        runScenario("场景二：持续压力（50 万条）",
                Integer.getInteger("loadtest.sustained-records", 500_000), true);
    }

    /**
     * 场景四：故障注入——FTP 不可达期间照常消费落盘，恢复后自动补齐上传，全程零丢失。
     * 这验证的是「FTP 挂了不丢数据」这条最关键的稳定性承诺。
     */
    @Test
    @Order(4)
    void scenarioFtpOutageAndRecovery() throws Exception {
        int total = Integer.getInteger("loadtest.outage-records", 100_000);
        runOutageScenario("场景四：FTP 中断后恢复（" + (total / 1000) + " 千条）", total, 8_000L);
    }

    /** 场景三：关闭 fsync 做对照，量化「稳定性优先」的吞吐代价 */
    @Test
    @Order(3)
    void scenarioFsyncDisabledComparison() throws Exception {
        runScenario("场景三：对照组（fsync=false，其余相同）",
                Integer.getInteger("loadtest.compare-records", 200_000), false);
    }

    // ------------------------------------------------------------------ 单个场景

    private void runScenario(String title, int total, boolean fsync) throws Exception {
        System.out.printf("%n================ 场景: %s | %,d 条 ================%n", title, total);

        Path base = Files.createTempDirectory("kafka-loadtest-");
        Path stage = base.resolve("stage");
        Path pending = base.resolve("pending");
        Path backup = base.resolve("backup");
        Path failed = base.resolve("failed");
        Path reject = base.resolve("reject");
        Path logs = base.resolve("logs");

        FakeFtpServer ftpServer = startFtpServer();
        EmbeddedKafkaBroker broker = startBroker();
        ConfigurableApplicationContext ctx = null;
        KafkaProducer<String, String> producer = null;
        long brokerStart = System.currentTimeMillis();

        try {
            broker.addTopics(new NewTopic(TOPIC, PARTITIONS, (short) 1));
            System.out.println("嵌入式 Kafka 就绪: " + broker.getBrokersAsString()
                    + "（partitions=" + PARTITIONS + "，启动耗时 " + (System.currentTimeMillis() - brokerStart) + "ms）");

            ctx = new SpringApplicationBuilder(KafkaLoaderApplication.class).run(argsFor(broker,
                    ftpServer.getServerControlPort(), stage, pending, backup, failed, reject, logs, fsync));
            FileStagingManager staging = ctx.getBean(FileStagingManager.class);
            FileUploadService uploadService = ctx.getBean(FileUploadService.class);
            System.out.println("Spring 上下文就绪，开始灌数据...");

            producer = buildProducer(broker.getBrokersAsString());

            // ---------------- 生产阶段 ----------------
            List<String> payloads = buildPayloads(Math.min(512, total));
            int avgPayload = averagePayloadBytes(payloads);
            // 用平均字节数估算总量，避免在发送热路径里反复 getBytes 干扰生产耗时测量
            long producedBytes = (long) avgPayload * total;
            long produceStart = System.currentTimeMillis();
            long lastWaveTs = produceStart;
            List<long[]> waves = new ArrayList<>();

            int sent = 0;
            while (sent < total) {
                int size = Math.min(WAVE, total - sent);
                for (int i = 0; i < size; i++) {
                    int idx = sent + i;
                    String payload = payloads.get(idx % payloads.size());
                    producer.send(new ProducerRecord<>(TOPIC, idx % PARTITIONS, "k" + idx, payload));
                }
                producer.flush();
                sent += size;
                long now = System.currentTimeMillis();
                waves.add(new long[]{size, Math.max(1, now - lastWaveTs)});
                lastWaveTs = now;
            }
            long produceElapsed = System.currentTimeMillis() - produceStart;

            // ---------------- 等待全部消费并提交 offset ----------------
            long committed = awaitCommitted(broker.getBrokersAsString(), total, 15 * 60 * 1000L);
            long consumeElapsed = System.currentTimeMillis() - produceStart;
            if (committed < total) {
                throw new IllegalStateException("消费未在超时内完成: 已提交 " + committed + "/" + total);
            }

            // ---------------- 封盘 + 真实 FTP 上传 ----------------
            int sealed = staging.flushAll();
            long uploadStart = System.currentTimeMillis();
            drainPending(uploadService, staging, 3 * 60 * 1000L);
            long uploadElapsed = System.currentTimeMillis() - uploadStart;
            long e2eElapsed = System.currentTimeMillis() - produceStart;

            // ---------------- 校验 ----------------
            Scan backupScan = scan(backup);
            Scan failedScan = scan(failed);
            RemoteScan remote = scanRemote(ftpServer);
            long rejected = staging.getRejectedRecords();
            long pendingLeft = staging.pendingCount();

            assertEquals(total, backupScan.lines, "备份文件总行数必须等于生产条数（零丢失）");
            assertEquals(0, rejected, "本次全为合法 JSON，不应产生隔离数据");
            assertEquals(0, pendingLeft, "上传成功后 pending 必须为空");
            assertEquals(0, failedScan.files, "failed 目录必须为空（否则说明上传重试 10 次后被隔离）");
            assertEquals(backupScan.files, remote.files, "FTP 远端文件数应与备份一致");
            assertEquals(backupScan.bytes, remote.bytes, "FTP 远端字节数应与备份完全一致");
            assertEquals(31, backupScan.maxColumns, "每行必须恰好 31 列（实际最大 " + backupScan.maxColumns + " 列）");
            assertTrue(backupScan.maxLines <= RECORDS_PER_FILE,
                    "单文件行数不得超过 " + RECORDS_PER_FILE + "，实际最大 " + backupScan.maxLines);

            double produceTps = total * 1000.0 / Math.max(1, produceElapsed);
            double e2eTps = total * 1000.0 / Math.max(1, e2eElapsed);
            double writeMbPerSec = backupScan.bytes / 1024.0 / 1024.0 / Math.max(0.001, e2eElapsed / 1000.0);

            report.append("\n## ").append(title).append("\n\n");
            report.append("| 指标 | 实测值 |\n|---|---|\n");
            report.append("| 生产条数 | ").append(String.format("%,d", total)).append(" |\n");
            report.append("| 分区数 / 消费并发 | ").append(PARTITIONS).append(" / ").append(PARTITIONS).append(" |\n");
            report.append("| 单条报文平均 | ").append(String.format("%,d", avgPayload)).append(" 字节 |\n");
            report.append("| 生产总字节 | ").append(String.format("%.1f", producedBytes / 1024.0 / 1024.0))
                    .append(" MB |\n");
            report.append("| 生产阶段耗时 / 速率 | ").append(String.format("%,d", produceElapsed)).append(" ms / ")
                    .append(String.format("%,.0f", produceTps)).append(" 条/秒 |\n");
            report.append("| 端到端耗时（首条生产→最后一条提交） | ")
                    .append(String.format("%,d", consumeElapsed)).append(" ms |\n");
            report.append("| **端到端吞吐** | **").append(String.format("%,.0f", e2eTps)).append(" 条/秒** |\n");
            report.append("| 落盘吞吐 | ").append(String.format("%.2f", writeMbPerSec)).append(" MB/秒 |\n");
            report.append("| 生成文件数 / 单文件最大行数 | ").append(backupScan.files).append(" / ")
                    .append(String.format("%,d", backupScan.maxLines)).append(" |\n");
            report.append("| 生成总字节（备份侧） | ")
                    .append(String.format("%.1f", backupScan.bytes / 1024.0 / 1024.0)).append(" MB |\n");
            report.append("| 封盘数 / 上传耗时 | ").append(sealed).append(" 个 / ")
                    .append(String.format("%,d", uploadElapsed)).append(" ms |\n");
            report.append("| 全链路总耗时 | ").append(String.format("%,d", e2eElapsed)).append(" ms |\n");
            report.append("| 丢数据 / 脏数据 / 残留 pending | ").append(total - backupScan.lines).append(" / ")
                    .append(rejected).append(" / ").append(pendingLeft).append(" |\n");
            report.append("| 平均每行字节 | ").append(backupScan.bytes / Math.max(1, backupScan.lines))
                    .append(" 字节 |\n");
            report.append("| 跑完后堆占用 | ").append(usedHeapMb()).append(" MB |\n");

            summary.add(new String[]{title, String.format("%,d", total), String.format("%,.0f", e2eTps),
                    String.format("%.2f", writeMbPerSec), String.valueOf(backupScan.files), "0"});

            String block = report.substring(report.lastIndexOf("## " + title));
            System.out.println(block);
            System.out.println("每波生产速率: " + waveSummary(waves));
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (ctx != null) {
                ctx.close();
            }
            broker.destroy();
            ftpServer.stop();
        }
    }

    private String[] argsFor(EmbeddedKafkaBroker broker, int ftpPort, Path stage, Path pending,
                             Path backup, Path failed, Path reject, Path logs, boolean fsync) {
        return new String[]{
                "--spring.main.web-application-type=none",
                "--spring.main.banner-mode=off",
                "--kafka.cluster.name=LOADTEST",
                "--kafka.cluster.topic=" + TOPIC,
                "--kafka.cluster.bootstrap-servers=" + broker.getBrokersAsString(),
                "--kafka.consumer.group-id=" + GROUP,
                "--kafka.consumer.concurrency=" + PARTITIONS,
                "--kafka.consumer.max-poll-records=500",
                // 目录全部隔离到临时目录，不污染项目 data/ 与 logs/
                "--file.stage-dir=" + stage,
                "--file.pending-dir=" + pending,
                "--file.backup-dir=" + backup,
                "--file.failed-dir=" + failed,
                "--file.reject-dir=" + reject,
                "--file.system-name=LOADTEST",
                "--file.records-per-file=" + RECORDS_PER_FILE,
                "--file.fsync-per-batch=" + fsync,
                // 压测期把空闲封盘与上传调快，保证尾部落盘和上传在测试窗口内完成
                "--file.idle-flush-ms=2000",
                "--file.flush-check-interval-ms=1000",
                "--file.upload-interval-ms=2000",
                "--file.max-pending-files=5000",
                "--file.min-free-disk-mb=512",
                "--ftp.enabled=true",
                "--ftp.host=127.0.0.1",
                "--ftp.port=" + ftpPort,
                "--ftp.username=kafka",
                "--ftp.password=kafka",
                "--ftp.remote-dir=" + REMOTE_DIR,
                "--ftp.passive-mode=true",
                "--logging.file.path=" + logs,
                // 压测时压低第三方框架日志，避免嵌入式 broker 的 DEBUG 刷屏影响吞吐测量
                "--logging.level.kafka=WARN",
                "--logging.level.org.apache.zookeeper=WARN",
                "--logging.level.state.change.logger=ERROR",
                "--logging.level.com.example.kafkaloader=INFO"
        };
    }

    private long usedHeapMb() {
        Runtime rt = Runtime.getRuntime();
        System.gc();
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    }

    private String waveSummary(List<long[]> waves) {        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(5, waves.size()); i++) {
            long[] w = waves.get(i);
            sb.append(String.format("第%d波 %d 条 %.0f 条/秒; ", i + 1, w[0], w[0] * 1000.0 / w[1]));
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------ 故障注入场景

    private void runOutageScenario(String title, int total, long outageMs) throws Exception {
        System.out.printf("%n================ 场景: %s | %,d 条 | 中断 %,d ms ================%n",
                title, total, outageMs);

        Path base = Files.createTempDirectory("kafka-outage-");
        Path stage = base.resolve("stage");
        Path pending = base.resolve("pending");
        Path backup = base.resolve("backup");
        Path failed = base.resolve("failed");
        Path reject = base.resolve("reject");
        Path logs = base.resolve("logs");

        // 先占一个空闲端口，但暂不启动 FTP —— 模拟「FTP 服务不可达」
        int ftpPort = reservePort();
        EmbeddedKafkaBroker broker = startBroker();
        FakeFtpServer ftpServer = null;
        ConfigurableApplicationContext ctx = null;
        KafkaProducer<String, String> producer = null;

        try {
            broker.addTopics(new NewTopic(TOPIC, PARTITIONS, (short) 1));
            ctx = new SpringApplicationBuilder(KafkaLoaderApplication.class)
                    .run(argsFor(broker, ftpPort, stage, pending, backup, failed, reject, logs, true));
            FileStagingManager staging = ctx.getBean(FileStagingManager.class);
            FileUploadService uploadService = ctx.getBean(FileUploadService.class);
            producer = buildProducer(broker.getBrokersAsString());

            List<String> payloads = buildPayloads(Math.min(512, total));
            int sent = 0;
            while (sent < total) {
                int size = Math.min(WAVE, total - sent);
                for (int i = 0; i < size; i++) {
                    int idx = sent + i;
                    producer.send(new ProducerRecord<>(TOPIC, idx % PARTITIONS, "k" + idx,
                            payloads.get(idx % payloads.size())));
                }
                producer.flush();
                sent += size;
            }
            long committed = awaitCommitted(broker.getBrokersAsString(), total, 10 * 60 * 1000L);
            assertEquals(total, (long) committed, "FTP 故障期间也必须完成消费并提交 offset（数据已落盘）");

            staging.flushAll();
            uploadService.uploadPending();   // 必然失败
            long pendingDuringOutage = staging.pendingCount();
            assertTrue(pendingDuringOutage > 0, "FTP 不可达时文件必须保留在 pending，不能丢");
            System.out.println("FTP 中断期间：pending 保留 " + pendingDuringOutage + " 个文件");

            // 持续故障一段时间，让定时上传任务也失败若干轮
            Thread.sleep(outageMs);
            long stillPending = staging.pendingCount();
            Scan failedDuringOutage = scan(failed);
            assertEquals(0, failedDuringOutage.files, "短时中断不应把文件打入 failed（默认 10 次才隔离）");
            System.out.println("中断 " + outageMs + "ms 后：pending=" + stillPending + "，failed=0");

            // FTP 恢复
            ftpServer = startFtpServer(ftpPort);
            long recoverStart = System.currentTimeMillis();
            drainPending(uploadService, staging, 120_000);
            long recoverElapsed = System.currentTimeMillis() - recoverStart;

            Scan backupScan = scan(backup);
            RemoteScan remote = scanRemote(ftpServer);
            long pendingLeft = staging.pendingCount();

            assertEquals(total, backupScan.lines, "恢复后总行数必须等于生产条数（零丢失）");
            assertEquals(0, pendingLeft, "恢复后 pending 必须清空");
            assertEquals(backupScan.files, remote.files, "远端文件数应与备份一致");
            assertEquals(backupScan.bytes, remote.bytes, "远端字节数应与备份一致");

            report.append("\n## ").append(title).append("\n\n");
            report.append("| 指标 | 实测值 |\n|---|---|\n");
            report.append("| 生产条数 | ").append(String.format("%,d", total)).append(" |\n");
            report.append("| 故障期间已提交 offset | ").append(String.format("%,d", committed))
                    .append("（数据已安全落盘） |\n");
            report.append("| 故障期间 pending 保留文件 | ").append(pendingDuringOutage).append(" |\n");
            report.append("| 故障持续 | ").append(String.format("%,d", outageMs)).append(" ms |\n");
            report.append("| 恢复后补齐耗时 | ").append(String.format("%,d", recoverElapsed)).append(" ms |\n");
            report.append("| 恢复后 pending 残留 | ").append(pendingLeft).append(" |\n");
            report.append("| 丢数据 / 进 failed | ").append(total - backupScan.lines).append(" / ")
                    .append(failedDuringOutage.files).append(" |\n");
            report.append("| 远端文件数 / 字节 | ").append(remote.files).append(" / ")
                    .append(remote.bytes).append(" |\n");

            summary.add(new String[]{title, String.format("%,d", total), "中断中",
                    "恢复 " + recoverElapsed + "ms", String.valueOf(remote.files), "0"});

            String block = report.substring(report.lastIndexOf("## " + title));
            System.out.println(block);
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (ctx != null) {
                ctx.close();
            }
            broker.destroy();
            if (ftpServer != null) {
                ftpServer.stop();
            }
        }
    }

    // ------------------------------------------------------------------ 环境搭建

    private EmbeddedKafkaBroker startBroker() {
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, false, PARTITIONS);
        broker.afterPropertiesSet();
        return broker;
    }

    private FakeFtpServer startFtpServer() {
        return startFtpServer(0);
    }

    private FakeFtpServer startFtpServer(int port) {
        boolean windows = System.getProperty("os.name").toLowerCase().contains("win");
        // MockFtpServer 把 "/xxx" 解析为「相对 home」，所以 home 必须就是我们想要的 FTP 根：
        // home=/kafka，远端目录=/incoming，最终落到 /kafka/incoming，各次连接解析一致
        String home = windows ? "C:\\kafka" : "/kafka";

        FileSystem fs = windows ? new WindowsFakeFileSystem() : new UnixFakeFileSystem();
        if (windows) {
            fs.add(new DirectoryEntry("C:\\"));
            fs.add(new DirectoryEntry("C:\\kafka"));
            fs.add(new DirectoryEntry("C:\\kafka\\incoming"));
        } else {
            fs.add(new DirectoryEntry("/"));
            fs.add(new DirectoryEntry("/kafka"));
            fs.add(new DirectoryEntry("/kafka" + REMOTE_DIR));
        }

        FakeFtpServer server = new FakeFtpServer();
        server.setServerControlPort(port);
        server.setFileSystem(fs);
        server.addUserAccount(new UserAccount("kafka", "kafka", home));
        server.start();
        return server;
    }

    /** 预留一个当前空闲的端口，稍后再把 FTP 服务端启动在同一端口上，模拟「故障恢复」 */
    private int reservePort() throws IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private KafkaProducer<String, String> buildProducer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return new KafkaProducer<>(props);
    }

    /** 接近真实业务的报文：31 个业务字段齐全 + 清单外字段（嵌套对象/数组），约 700 字节 */
    private List<String> buildPayloads(int distinct) {
        List<String> list = new ArrayList<>(distinct);
        for (int i = 0; i < distinct; i++) {
            String payload = "{"
                    + "\"md_id\":\"MD" + pad(i, 10) + "\","
                    + "\"clue_id\":\"CLUE-20261023-" + pad(i, 10) + "\","
                    + "\"clue_src_sys\":\"SQAJ\","
                    + "\"clue_dst_sys\":\"XYRXX\","
                    + "\"capture_time\":16980123" + pad(i % 10000, 4) + ","
                    + "\"protocol\":\"HTTP\","
                    + "\"original_protocol\":\"HTTP/1.1\","
                    + "\"data_source3\":\"DS3-" + (i % 100) + "\","
                    + "\"mwbs\":\"MWBS-" + pad(i, 12) + "\","
                    + "\"sensitive_level\":\"3\","
                    + "\"saved_time\":16980124" + pad(i % 10000, 4) + ","
                    + "\"remark\":\"压测模拟数据\","
                    + "\"data_check_id\":\"CHK-" + pad(i, 10) + "\","
                    + "\"bunit\":\"某某市公安局某某分局\","
                    + "\"data_set_name\":\"重点人员数据集\","
                    + "\"dzwjwz\":\"/data/file/2026/10/23/" + i + ".bin\","
                    + "\"asjxrybh\":\"RY" + pad(i, 10) + "\","
                    + "\"asjbh\":\"AJ2026" + pad(i % 1000000, 6) + "\","
                    + "\"zjhm\":\"11010119900101" + pad(i % 10000, 4) + "\","
                    + "\"xm\":\"张三\","
                    + "\"xbdm\":\"1\","
                    + "\"xbmc\":\"男\","
                    + "\"mzdm\":\"01\","
                    + "\"mzmc\":\"汉族\","
                    + "\"csrq\":\"1990-01-01 00:00:00\","
                    + "\"xzzqhdm\":\"110101\","
                    + "\"xzzqhmc\":\"北京市东城区\","
                    + "\"criminal_time\":\"2026-10-23 09:00:00\","
                    + "\"gajgjgdm\":\"110101000000\","
                    + "\"gajgjmc\":\"某某派出所\","
                    + "\"synchro_time\":\"2026-10-23 09:30:00\","
                    + "\"ext_attachments\":[{\"type\":\"img\",\"size\":10240},{\"type\":\"doc\",\"size\":2048}],"
                    + "\"ext_meta\":{\"source\":\"loadtest\",\"batch\":" + i + ",\"operator\":\"system\"}"
                    + "}";
            list.add(payload);
        }
        return list;
    }

    private int averagePayloadBytes(List<String> payloads) {
        long sum = 0;
        for (String payload : payloads) {
            sum += payload.getBytes(StandardCharsets.UTF_8).length;
        }
        return (int) (sum / Math.max(1, payloads.size()));
    }

    private static String pad(int value, int width) {
        StringBuilder sb = new StringBuilder(String.valueOf(value));
        while (sb.length() < width) {
            sb.insert(0, '0');
        }
        return sb.toString();
    }

    /** 等到消费组已提交 offset 总和达到 total（只有落盘成功才会提交，所以这等于「已安全落盘」条数） */
    private long awaitCommitted(String brokers, int total, long timeoutMs) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        long deadline = System.currentTimeMillis() + timeoutMs;
        try (AdminClient admin = AdminClient.create(props)) {
            while (System.currentTimeMillis() < deadline) {
                long committed = committedTotal(admin);
                if (committed >= total) {
                    return committed;
                }
                Thread.sleep(200);
            }
            return committedTotal(admin);
        }
    }

    private long committedTotal(AdminClient admin) throws InterruptedException {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get();
            long sum = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                if (TOPIC.equals(entry.getKey().topic())) {
                    sum += entry.getValue().offset();
                }
            }
            return sum;
        } catch (ExecutionException e) {
            return 0;
        }
    }

    private void drainPending(FileUploadService uploadService, FileStagingManager staging, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            uploadService.uploadPending();
            if (staging.pendingCount() == 0) {
                return;
            }
            Thread.sleep(200);
        }
    }

    // ------------------------------------------------------------------ 结果校验

    private static class Scan {
        int files;
        long lines;
        long bytes;
        int maxLines = 0;
        int minLines = Integer.MAX_VALUE;
        int maxColumns = 0;
        final Set<Integer> columnCounts = new HashSet<>();
    }

    private static class RemoteScan {
        int files;
        long bytes;
        final List<String> names = new ArrayList<>();
    }

    private Scan scan(Path root) throws IOException {
        Scan scan = new Scan();
        if (!Files.isDirectory(root)) {
            return scan;
        }
        List<Path> files = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".txt"))
                    .forEach(files::add);
        }
        Collections.sort(files);
        for (Path file : files) {
            scan.files++;
            scan.bytes += Files.size(file);
            int count = 0;
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    count++;
                    int cols = line.split("\t", -1).length;
                    scan.columnCounts.add(cols);
                    scan.maxColumns = Math.max(scan.maxColumns, cols);
                }
            }
            scan.lines += count;
            scan.maxLines = Math.max(scan.maxLines, count);
            scan.minLines = Math.min(scan.minLines, count);
        }
        if (scan.files == 0) {
            scan.minLines = 0;
        }
        return scan;
    }

    /** 用 FTP 协议本身核对远端：文件数与字节数必须与本地备份完全一致 */
    private RemoteScan scanRemote(FakeFtpServer server) throws IOException {
        RemoteScan scan = new RemoteScan();
        FTPClient client = new FTPClient();
        try {
            client.connect("127.0.0.1", server.getServerControlPort());
            client.login("kafka", "kafka");
            client.setFileType(FTP.BINARY_FILE_TYPE);
            client.enterLocalPassiveMode();
            System.out.println("[DEBUG-FTP] pwd=" + client.printWorkingDirectory());
            for (FTPFile file : client.listFiles()) {
                scan.names.add(file.getName() + "(" + file.getSize() + ")");
                if (file.isFile()) {
                    scan.files++;
                    scan.bytes += file.getSize();
                }
            }
            System.out.println("[DEBUG-FTP] remote entries=" + scan.names);
        } finally {
            if (client.isConnected()) {
                client.disconnect();
            }
        }
        return scan;
    }
}

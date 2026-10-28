package com.example.kafkaloader.bench;

import com.example.kafkaloader.config.FilePipelineProperties;
import com.example.kafkaloader.config.FtpProperties;
import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.file.FileStagingManager;
import com.example.kafkaloader.file.FileUploadService;
import com.example.kafkaloader.file.UploadResult;
import com.example.kafkaloader.ftp.FtpUploader;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 压测程序（不属于单元测试，surefire 不会执行）。
 * <p>
 * 压的是真实链路的核心段：<b>31 字段渲染 + 分片写入 + fsync + 5 万条滚动 + 封盘移入 pending + 备份上传</b>。
 * 不依赖外部 Kafka / FTP：消息用 {@link ConsumerRecord} 直接构造，FTP 用"只统计不传输"的桩实现，
 * 这样测出来的数字反映的是本服务自身的处理能力，而不是网络或对端磁盘的速度。
 * <p>
 * 用法（先 {@code mvn -o test-compile}）：
 * <pre>
 * java -cp "target/classes;target/test-classes;$(cat target/cp.txt)" \
 *      com.example.kafkaloader.bench.LoadTestHarness --records=200000 --batch=500 --partitions=1
 * </pre>
 * 参数：
 * <pre>
 * --records    总条数（默认 200000）
 * --batch      单批条数，等于 Kafka max-poll-records（默认 500）
 * --partitions 分区数，模拟多分区并发写入（默认 1）
 * --per-file   单个文件行数（默认 50000）
 * --fsync      true/false，是否每批强制刷盘（默认 true）
 * --ftp        true/false，是否模拟上传阶段（默认 true）
 * --dir        工作目录（默认 target/perf/work）
 * --label      场景名，用于报告文件名
 * --keep       保留数据文件（默认 true；false 则测完删除）
 * </pre>
 */
public final class LoadTestHarness {

    private static final String TOPIC = "DISTRIBUTE_SQAJXYRXX_TO";
    private static final int FIELD_COUNT = 31;
    private static final int PAYLOAD_POOL_SIZE = 4096;

    public static void main(String[] args) throws Exception {
        Params p = Params.parse(args);
        Path workDir = Paths.get(p.dir).toAbsolutePath().normalize();
        // 每次跑都用干净目录，避免上一轮的 pending 触发背压
        deleteRecursively(workDir);
        Files.createDirectories(workDir);

        System.out.println("=== kafka-file-loader 压测 ===");
        System.out.println(p.describe());

        FilePipelineProperties fileProps = new FilePipelineProperties();
        fileProps.setStageDir(workDir.resolve("stage").toString());
        fileProps.setPendingDir(workDir.resolve("pending").toString());
        fileProps.setBackupDir(workDir.resolve("backup").toString());
        fileProps.setFailedDir(workDir.resolve("failed").toString());
        fileProps.setRejectDir(workDir.resolve("reject").toString());
        fileProps.setRecordsPerFile(p.perFile);
        fileProps.setSystemName("DISTRIBUTE");
        fileProps.setFsyncPerBatch(p.fsync);
        // 压测默认不做背压限速，阈值放大；--max-pending 可显式验证背压是否按预期触发
        fileProps.setMaxPendingFiles(p.maxPending > 0 ? p.maxPending : 10_000_000L);
        fileProps.setMinFreeDiskMb(1L);

        KafkaClusterProperties clusterProps = new KafkaClusterProperties();
        clusterProps.setName("DISTRIBUTE");
        clusterProps.setTopic(TOPIC);

        long startCpu = processCpuNanos();
        long startAt = System.currentTimeMillis();

        // ---------- 1. 预热：让 JIT 编译渲染与写入热点 ----------
        FileStagingManager warm = new FileStagingManager(fileProps, clusterProps);
        List<ConsumerRecord<String, String>> warmBatch = buildBatch(pool(), 0, p.batch, 0, 0);
        for (int i = 0; i < 20; i++) {
            warm.appendBatch(warmBatch);
        }
        warm.destroy();
        deleteRecursively(workDir.resolve("stage"));
        deleteRecursively(workDir.resolve("pending"));
        System.out.println("[预热] 完成 10000 条，已清空输出目录");

        // ---------- 2. 主体：渲染 + 写盘 ----------
        FileStagingManager manager = new FileStagingManager(fileProps, clusterProps);
        List<String> pool = pool();
        long writeStart = System.currentTimeMillis();
        long writeStartCpu = processCpuNanos();
        List<Long> batchLatency = java.util.Collections.synchronizedList(new ArrayList<>());
        long[] writtenHolder = new long[1];
        long batches = runWrite(manager, pool, p, batchLatency, writtenHolder);
        long written = writtenHolder[0];
        long writeEnd = System.currentTimeMillis();
        long writeCpu = processCpuNanos() - writeStartCpu;

        // 收尾：把未写满的分片封盘
        long sealStart = System.currentTimeMillis();
        int sealed = manager.flushAll();
        long sealElapsed = System.currentTimeMillis() - sealStart;

        long writeElapsed = writeEnd - writeStart;
        double writeTps = written * 1000.0 / Math.max(1, writeElapsed);

        // ---------- 3. 备份 + 上传（FTP 用桩，只统计不传输） ----------
        long uploadElapsed = 0;
        int uploadedFiles = 0;
        int uploadRounds = 0;
        if (p.ftp) {
            FtpProperties ftpProps = new FtpProperties();
            ftpProps.setEnabled(true);
            ftpProps.setDeleteAfterUpload(true);
            CountingUploader uploader = new CountingUploader();
            FileUploadService uploadService = new FileUploadService(
                    fileProps, ftpProps, uploader, manager, new SimpleMeterRegistry());
            long uploadStart = System.currentTimeMillis();
            while (true) {
                UploadResult result = uploadService.uploadPending();
                uploadRounds++;
                if (!result.isExecuted() || result.getScanned() == 0) {
                    break;
                }
                uploadedFiles += result.getUploaded();
                if (uploadRounds > 10000) {
                    break;
                }
            }
            uploadElapsed = System.currentTimeMillis() - uploadStart;
        }

        // ---------- 4. 校验产物 ----------
        Verification verification = verify(workDir, p.records);
        long totalBytes = verification.totalBytes;
        long endAt = System.currentTimeMillis();
        long totalCpu = processCpuNanos() - startCpu;

        // ---------- 5. 输出 ----------
        Result r = new Result();
        r.label = p.label;
        r.records = p.records;
        r.batch = p.batch;
        r.partitions = p.partitions;
        r.threads = p.threads;
        r.perFile = p.perFile;
        r.fsync = p.fsync;
        r.ftp = p.ftp;
        r.written = written;
        r.batches = batches;
        r.files = verification.files;
        r.totalBytes = totalBytes;
        r.avgLineBytes = verification.files == 0 ? 0 : (double) totalBytes / Math.max(1, p.records);
        r.writeElapsedMs = writeElapsed;
        r.writeTps = round(writeTps);
        r.writeMBps = round(totalBytes / 1024.0 / 1024.0 / Math.max(1, writeElapsed) * 1000);
        r.writeCpuPercent = writeCpu <= 0 ? 0 : round(writeCpu / 1e6 / Math.max(1, writeElapsed) * 100);
        // 均值必须对"每批延迟"求平均：多线程下用 总耗时/总批次 会把并发误算成串行
        r.avgBatchMs = round(mean(batchLatency) / 1e6);
        r.filesPerSecond = round(verification.files * 1000.0 / Math.max(1, writeElapsed));
        r.sealElapsedMs = sealElapsed;
        r.sealedFiles = sealed;
        r.p50Ms = round(percentile(batchLatency, 0.50) / 1e6);
        r.p95Ms = round(percentile(batchLatency, 0.95) / 1e6);
        r.p99Ms = round(percentile(batchLatency, 0.99) / 1e6);
        r.maxBatchMs = round(percentile(batchLatency, 1.0) / 1e6);
        r.uploadElapsedMs = uploadElapsed;
        r.uploadedFiles = uploadedFiles;
        r.uploadRounds = uploadRounds;
        r.verifyOk = verification.ok;
        r.verifyMessage = verification.message;
        r.fieldCountOk = verification.fieldCountOk;
        r.totalElapsedMs = endAt - startAt;
        r.cpuPercent = totalCpu <= 0 ? 0 : round(totalCpu / 1e6 / Math.max(1, endAt - startAt) * 100);
        r.heapUsedMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024;
        r.javaVersion = System.getProperty("java.version");
        r.cpuModel = cpuModel();

        String json = r.toJson();
        System.out.println(json);
        Path outDir = Paths.get("target", "perf");
        Files.createDirectories(outDir);
        Path outFile = outDir.resolve("run-" + p.label + ".json");
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outFile, StandardCharsets.UTF_8))) {
            writer.println(json);
        }
        System.out.println("[结果] 已写入 " + outFile);

        if (!p.keep) {
            deleteRecursively(workDir);
            System.out.println("[清理] 已删除 " + workDir);
        }

        manager.destroy();
        if (!verification.ok || !verification.fieldCountOk) {
            System.exit(2);
        }
    }

    // ==================================================================================
    // 数据构造
    // ==================================================================================

    /**
     * 写入阶段：按线程数切分记录，每个线程只负责自己的那部分分区，
     * 与真实 Kafka 消费模型一致（一个分区只被一个消费者线程处理，避免多线程抢同一文件）。
     */
    private static long runWrite(FileStagingManager manager, List<String> pool, Params p,
                                 List<Long> latencies, long[] writtenHolder) throws Exception {
        int threads = Math.max(1, Math.min(p.threads, p.partitions));
        long perThread = p.records / threads;
        long remainder = p.records % threads;
        if (threads == 1) {
            return writeRange(manager, pool, 0, p.records, p, 0, latencies, writtenHolder);
        }
        java.util.concurrent.ExecutorService poolExec =
                java.util.concurrent.Executors.newFixedThreadPool(threads);
        List<java.util.concurrent.Future<Long>> futures = new ArrayList<>();
        long start = 0;
        for (int t = 0; t < threads; t++) {
            long count = perThread + (t < remainder ? 1 : 0);
            final long base = start;
            final long items = count;
            final int threadIndex = t;
            futures.add(poolExec.submit(() ->
                    writeRange(manager, pool, base, items, p, threadIndex, latencies, writtenHolder)));
            start += count;
        }
        long batches = 0;
        try {
            for (java.util.concurrent.Future<Long> future : futures) {
                batches += future.get();
            }
        } finally {
            poolExec.shutdown();
        }
        return batches;
    }

    private static long writeRange(FileStagingManager manager, List<String> pool, long offsetBase, long count,
                                   Params p, int threadIndex, List<Long> latencies, long[] writtenHolder) {
        int threads = Math.max(1, Math.min(p.threads, p.partitions));
        long done = 0;
        long written = 0;
        while (done < count) {
            int size = (int) Math.min(p.batch, count - done);
            List<ConsumerRecord<String, String>> batch = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                long seq = offsetBase + done + i;
                // 分区归属：把分区切成 threads 份，线程只碰自己那份
                int partition = pickPartition(seq, p.partitions, threads, threadIndex);
                String payload = pool.get((int) (seq % pool.size()));
                batch.add(new ConsumerRecord<>(TOPIC, partition, seq, "k" + seq, payload));
            }
            long t0 = System.nanoTime();
            written += manager.appendBatch(batch);
            latencies.add(System.nanoTime() - t0);
            done += size;
        }
        synchronized (writtenHolder) {
            writtenHolder[0] += written;
        }
        return (count + p.batch - 1) / p.batch;
    }

    /**
     * 取属于该线程的分区：分区 p 归属 p % threads == threadIndex。
     * 用取模而不是整除，保证 partitions 不能被 threads 整除时也不会漏掉尾部若干分区。
     */
    private static int pickPartition(long seq, int partitions, int threads, int threadIndex) {
        int count = 0;
        for (int p = threadIndex; p < partitions; p += threads) {
            count++;
        }
        if (count == 0) {
            return Math.min(partitions - 1, threadIndex);
        }
        int slot = (int) (seq % count);
        return Math.min(partitions - 1, threadIndex + slot * threads);
    }

    /** 预生成一批真实形态的报文（含中文、长整型、日期），避免把字符串拼接算进耗时 */
    private static List<String> pool() {
        List<String> pool = new ArrayList<>(PAYLOAD_POOL_SIZE);
        for (int i = 0; i < PAYLOAD_POOL_SIZE; i++) {
            StringBuilder sb = new StringBuilder(768);
            sb.append('{')
                    .append("\"md_id\":\"MD").append(20261023000000L + i).append("\",")
                    .append("\"clue_id\":\"CLUE-2026-10-23-").append(String.format("%08d", i)).append("\",")
                    .append("\"clue_src_sys\":\"SRC_A_").append(i % 97).append("\",")
                    .append("\"clue_dst_sys\":\"DST_B_").append(i % 89).append("\",")
                    .append("\"capture_time\":").append(1761200000000L + i).append(',')
                    .append("\"protocol\":\"HTTP\",")
                    .append("\"original_protocol\":\"HTTP/1.1\",")
                    .append("\"data_source3\":\"DS").append(i % 13).append("\",")
                    .append("\"mwbs\":\"MWBS").append(900000000L + i).append("\",")
                    .append("\"sensitive_level\":\"").append(i % 4).append("\",")
                    .append("\"saved_time\":").append(1761200001000L + i).append(',')
                    .append("\"remark\":\"模拟数据-用于压测-第").append(i).append("条\",")
                    .append("\"data_check_id\":\"CHK").append(i % 1000).append("\",")
                    .append("\"bunit\":\"某市某某采集单位\",")
                    .append("\"data_set_name\":\"科信数据集-人员信息\",")
                    .append("\"dzwjwz\":\"/data/2026/10/23/").append(i % 500).append(".dat\",")
                    .append("\"asjxrybh\":\"RY").append(100000000L + i).append("\",")
                    .append("\"asjbh\":\"ASJ2026").append(String.format("%06d", i % 999999)).append("\",")
                    .append("\"zjhm\":\"11010119900101").append(String.format("%04d", i % 10000)).append("\",")
                    .append("\"xm\":\"张三丰").append(i % 100).append("\",")
                    .append("\"xbdm\":\"1\",")
                    .append("\"xbmc\":\"男\",")
                    .append("\"mzdm\":\"01\",")
                    .append("\"mzmc\":\"汉族\",")
                    .append("\"csrq\":\"1990-01-01 00:00:00\",")
                    .append("\"xzzqhdm\":\"110101\",")
                    .append("\"xzzqhmc\":\"北京市东城区某某街道某某社区\",")
                    .append("\"criminal_time\":\"2026-10-22 18:30:00\",")
                    .append("\"gajgjgdm\":\"110101000000\",")
                    .append("\"gajgjmc\":\"某市公安局某分局某派出所\",")
                    .append("\"synchro_time\":\"2026-10-23 09:00:00\"")
                    .append('}');
            pool.add(sb.toString());
        }
        return pool;
    }

    private static List<ConsumerRecord<String, String>> buildBatch(List<String> pool, long startIndex, int size,
                                                                   int partitions, long offsetBase) {
        List<ConsumerRecord<String, String>> batch = new ArrayList<>(size);
        int parts = Math.max(1, partitions);
        for (int i = 0; i < size; i++) {
            long global = startIndex + i;
            int partition = (int) (global % parts);
            String payload = pool.get((int) (global % pool.size()));
            batch.add(new ConsumerRecord<>(TOPIC, partition, offsetBase + i, "k" + global, payload));
        }
        return batch;
    }

    // ==================================================================================
    // 校验：行数、列数、文件名
    // ==================================================================================

    private static final class Verification {
        boolean ok;
        boolean fieldCountOk = true;
        String message = "";
        long files;
        long totalBytes;
    }

    private static Verification verify(Path workDir, long expectedRecords) throws IOException {
        Verification v = new Verification();
        // 以 backup 目录为准：pending 中的文件在上传后可能已被删除，且 backup 与 pending 是同名副本
        Path backupDir = workDir.resolve("backup");
        List<Path> all = new ArrayList<>();
        if (Files.isDirectory(backupDir)) {
            try (Stream<Path> stream = Files.walk(backupDir)) {
                all = stream.filter(Files::isRegularFile)
                        .filter(x -> x.getFileName().toString().endsWith(".txt"))
                        .sorted(Comparator.comparing(x -> x.getFileName().toString()))
                        .collect(Collectors.toList());
            }
        }
        long lines = 0;
        long badLines = 0;
        String firstLine = null;
        long sizeSum = 0;
        for (Path path : all) {
            sizeSum += Files.size(path);
            try (Stream<String> stream = Files.lines(path, StandardCharsets.UTF_8)) {
                for (String line : (Iterable<String>) stream::iterator) {
                    lines++;
                    if (firstLine == null) {
                        firstLine = line;
                    }
                    if (countTabs(line) + 1 != FIELD_COUNT) {
                        badLines++;
                    }
                }
            }
        }
        v.files = all.size();
        v.totalBytes = sizeSum;
        v.fieldCountOk = badLines == 0;
        v.ok = lines == expectedRecords && badLines == 0 && all.size() > 0;
        v.message = String.format(Locale.ROOT,
                "行数=%d(期望 %d) 文件数=%d 列数不符行数=%d 首行列数=%s",
                lines, expectedRecords, all.size(), badLines,
                firstLine == null ? "N/A" : String.valueOf(countTabs(firstLine) + 1));
        if (firstLine != null) {
            v.message = v.message + " 首行前 60 字符=" + firstLine.substring(0, Math.min(60, firstLine.length()));
        }
        return v;
    }

    private static int countTabs(String line) {
        int tabs = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '\t') {
                tabs++;
            }
        }
        return tabs;
    }

    // ==================================================================================
    // 工具
    // ==================================================================================

    /** FTP 桩：只验证文件可读，不真正传输，避免网络成为瓶颈 */
    private static final class CountingUploader implements FtpUploader {
        private long bytes;

        @Override
        public void upload(Path file) throws IOException {
            bytes += Files.size(file);
        }
    }

    private static long processCpuNanos() {
        try {
            java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
            if (bean instanceof com.sun.management.OperatingSystemMXBean) {
                long nanos = ((com.sun.management.OperatingSystemMXBean) bean).getProcessCpuTime();
                return nanos > 0 ? nanos : 0;
            }
        } catch (Throwable ignored) {
            // 非 HotSpot 实现时降级为 0
        }
        return 0;
    }

    private static String cpuModel() {
        String os = System.getenv("PROCESSOR_IDENTIFIER");
        return os == null ? System.getProperty("os.arch") : os;
    }

    private static double mean(List<Long> values) {
        if (values.isEmpty()) {
            return 0;
        }
        long sum = 0;
        for (long value : values) {
            sum += value;
        }
        return sum / (double) values.size();
    }

    private static double percentile(List<Long> values, double q) {        if (values.isEmpty()) {
            return 0;
        }
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Comparator.naturalOrder());
        int idx = (int) Math.min(sorted.size() - 1, Math.round(q * (sorted.size() - 1)));
        return sorted.get(idx);
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(dir)) {
            for (Path path : stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
        }
    }

    // ==================================================================================
    // 参数与结果
    // ==================================================================================

    private static final class Params {
        long records = 200_000;
        int batch = 500;
        int partitions = 1;
        int threads = 1;
        int perFile = 50_000;
        boolean fsync = true;
        boolean ftp = true;
        boolean keep = true;
        long maxPending = 0;
        String dir = "target/perf/work";
        String label = "default";

        static Params parse(String[] args) {
            Params p = new Params();
            for (String arg : args) {
                int eq = arg.indexOf('=');
                String key = eq < 0 ? arg : arg.substring(0, eq);
                String value = eq < 0 ? "" : arg.substring(eq + 1);
                switch (key) {
                    case "--records":
                        p.records = Long.parseLong(value);
                        break;
                    case "--batch":
                        p.batch = Integer.parseInt(value);
                        break;
                    case "--partitions":
                        p.partitions = Integer.parseInt(value);
                        break;
                    case "--threads":
                        p.threads = Integer.parseInt(value);
                        break;
                    case "--per-file":
                        p.perFile = Integer.parseInt(value);
                        break;
                    case "--fsync":
                        p.fsync = Boolean.parseBoolean(value);
                        break;
                    case "--ftp":
                        p.ftp = Boolean.parseBoolean(value);
                        break;
                    case "--keep":
                        p.keep = Boolean.parseBoolean(value);
                        break;
                    case "--max-pending":
                        p.maxPending = Long.parseLong(value);
                        break;
                    case "--dir":
                        p.dir = value;
                        break;
                    case "--label":
                        p.label = value;
                        break;
                    default:
                        throw new IllegalArgumentException("未知参数: " + arg);
                }
            }
            return p;
        }

        String describe() {
            return String.format(Locale.ROOT,
                    "条数=%d 批量=%d 分区=%d 线程=%d 单文件行数=%d fsync=%s 模拟上传=%s",
                    records, batch, partitions, threads, perFile, fsync, ftp);
        }
    }

    private static final class Result {
        String label;
        long records;
        int batch;
        int partitions;
        int threads;
        int perFile;
        boolean fsync;
        boolean ftp;
        long written;
        long batches;
        long files;
        long totalBytes;
        double avgLineBytes;
        long writeElapsedMs;
        double writeTps;
        double writeMBps;
        double writeCpuPercent;
        double avgBatchMs;
        double filesPerSecond;
        long sealElapsedMs;
        int sealedFiles;
        double p50Ms;
        double p95Ms;
        double p99Ms;
        double maxBatchMs;
        long uploadElapsedMs;
        int uploadedFiles;
        int uploadRounds;
        boolean verifyOk;
        String verifyMessage;
        boolean fieldCountOk;
        long totalElapsedMs;
        double cpuPercent;
        long heapUsedMb;
        String javaVersion;
        String cpuModel;

        String toJson() {
            StringBuilder sb = new StringBuilder(1024);
            sb.append("{\n");
            kv(sb, "label", "\"" + label + "\"", true);
            kv(sb, "records", String.valueOf(records), true);
            kv(sb, "batch", String.valueOf(batch), true);
            kv(sb, "partitions", String.valueOf(partitions), true);
            kv(sb, "threads", String.valueOf(threads), true);
            kv(sb, "perFile", String.valueOf(perFile), true);
            kv(sb, "fsync", String.valueOf(fsync), true);
            kv(sb, "ftp", String.valueOf(ftp), true);
            kv(sb, "written", String.valueOf(written), true);
            kv(sb, "batches", String.valueOf(batches), true);
            kv(sb, "files", String.valueOf(files), true);
            kv(sb, "totalBytes", String.valueOf(totalBytes), true);
            kv(sb, "avgLineBytes", String.valueOf(round(avgLineBytes)), true);
            kv(sb, "writeElapsedMs", String.valueOf(writeElapsedMs), true);
            kv(sb, "writeTps", String.valueOf(writeTps), true);
            kv(sb, "writeMBps", String.valueOf(writeMBps), true);
            kv(sb, "writeCpuPercent", String.valueOf(writeCpuPercent), true);
            kv(sb, "avgBatchMs", String.valueOf(avgBatchMs), true);
            kv(sb, "filesPerSecond", String.valueOf(filesPerSecond), true);
            kv(sb, "sealElapsedMs", String.valueOf(sealElapsedMs), true);
            kv(sb, "sealedFiles", String.valueOf(sealedFiles), true);
            kv(sb, "p50Ms", String.valueOf(p50Ms), true);
            kv(sb, "p95Ms", String.valueOf(p95Ms), true);
            kv(sb, "p99Ms", String.valueOf(p99Ms), true);
            kv(sb, "maxBatchMs", String.valueOf(maxBatchMs), true);
            kv(sb, "uploadElapsedMs", String.valueOf(uploadElapsedMs), true);
            kv(sb, "uploadedFiles", String.valueOf(uploadedFiles), true);
            kv(sb, "uploadRounds", String.valueOf(uploadRounds), true);
            kv(sb, "verifyOk", String.valueOf(verifyOk), true);
            kv(sb, "fieldCountOk", String.valueOf(fieldCountOk), true);
            kv(sb, "verifyMessage", "\"" + escape(verifyMessage) + "\"", true);
            kv(sb, "totalElapsedMs", String.valueOf(totalElapsedMs), true);
            kv(sb, "cpuPercent", String.valueOf(cpuPercent), true);
            kv(sb, "heapUsedMb", String.valueOf(heapUsedMb), true);
            kv(sb, "javaVersion", "\"" + javaVersion + "\"", true);
            kv(sb, "cpuModel", "\"" + escape(cpuModel) + "\"", false);
            sb.append("}");
            return sb.toString();
        }

        private static void kv(StringBuilder sb, String key, String value, boolean comma) {
            sb.append("  \"").append(key).append("\": ").append(value);
            if (comma) {
                sb.append(',');
            }
            sb.append('\n');
        }

        private static String escape(String text) {
            StringBuilder sb = new StringBuilder(text.length() + 16);
            for (int i = 0; i < text.length(); i++) {
                char ch = text.charAt(i);
                switch (ch) {
                    case '\\':
                        sb.append("\\\\");
                        break;
                    case '"':
                        sb.append("\\\"");
                        break;
                    case '\t':
                        sb.append("\\t");
                        break;
                    case '\n':
                        sb.append("\\n");
                        break;
                    case '\r':
                        sb.append("\\r");
                        break;
                    default:
                        if (ch < 0x20) {
                            sb.append(String.format("\\u%04x", (int) ch));
                        } else {
                            sb.append(ch);
                        }
                }
            }
            return sb.toString();
        }
    }
}

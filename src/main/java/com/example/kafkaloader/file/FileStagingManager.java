package com.example.kafkaloader.file;

import com.example.kafkaloader.config.FilePipelineProperties;
import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.model.KafkaMessageRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 分片文件写入管理：按 (topic, partition) 维护写入器，写满即滚动、封盘后移入待上传队列。
 * <p>
 * 每一行都按 {@link DataSchema} 的字段顺序渲染，列数固定 31，与《数据表字段清单》一致。
 * <p>
 * 稳定性相关：
 * <ul>
 *   <li>封盘前 flush + fsync，ack 时数据已持久化</li>
 *   <li>改名用原子 move，待上传目录不会出现半截文件</li>
 *   <li>启动时把上次进程遗留的 .writing 文件恢复进待上传队列</li>
 *   <li>无法映射到字段清单的消息转入隔离文件（同样 fsync）后再提交，不阻塞也不丢弃</li>
 *   <li>磁盘低水位 / 待上传积压时抛背压异常，调用方不 ack，Kafka 自然降速</li>
 * </ul>
 */
@Service
public class FileStagingManager implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(FileStagingManager.class);
    private static final int MAX_HEADER_LENGTH = 2000;

    private final FilePipelineProperties properties;
    private final Path stageDir;
    private final Path pendingDir;
    private final Path backupDir;
    private final Path failedDir;
    private final Path rejectDir;
    private final FileNameAllocator allocator;
    private final RecordRenderer renderer;
    private final RejectStore rejectStore;
    private final ConcurrentHashMap<String, StagingWriter> writers = new ConcurrentHashMap<>();

    private final Object pendingScanLock = new Object();
    private long lastPendingScanAt;
    private long cachedPendingCount;

    /** 进程启动以来写入主文件的记录数（一行算一条） */
    private final LongAdder writtenRecords = new LongAdder();

    public FileStagingManager(FilePipelineProperties properties, KafkaClusterProperties clusterProperties)
            throws IOException {
        this.properties = properties;
        this.stageDir = Paths.get(properties.getStageDir()).toAbsolutePath().normalize();
        this.pendingDir = Paths.get(properties.getPendingDir()).toAbsolutePath().normalize();
        this.backupDir = Paths.get(properties.getBackupDir()).toAbsolutePath().normalize();
        this.failedDir = Paths.get(properties.getFailedDir()).toAbsolutePath().normalize();
        this.rejectDir = Paths.get(properties.getRejectDir()).toAbsolutePath().normalize();
        this.allocator = new FileNameAllocator(resolveSystemName(properties, clusterProperties),
                properties.getFileExtension());
        Files.createDirectories(stageDir);
        Files.createDirectories(pendingDir);
        this.rejectStore = new RejectStore(rejectDir, allocator.getSystemName(), allocator.getExtension());
        this.renderer = new RecordRenderer(properties);
        log.info("报文解析 | mode={} | 分隔符={} | 严格字段数={}",
                properties.getParseMode(), properties.getDelimiterRegex(), properties.isStrictFieldCount());
        log.info("分片文件目录 | stage={} | pending={} | reject={} | 每文件 {} 行 | 每行 {} 列 | 文件名 {}_时间戳{}",
                stageDir, pendingDir, rejectDir, properties.getRecordsPerFile(),
                DataSchema.SIZE, allocator.getSystemName(), allocator.getExtension());
    }

    /** 日志聚合用：把带参数的原因归一成类别，如 field-count-mismatch(expected=31,actual=28) → field-count-mismatch */
    private static String failureKind(String reason) {
        if (reason == null || reason.isEmpty()) {
            return "unknown";
        }
        int idx = reason.indexOf('(');
        return idx > 0 ? reason.substring(0, idx) : reason;
    }

    /**
     * 系统配置名称：优先取 {@code file.system-name}，未配置时回退集群标识，
     * 保证文件名在任何部署下都能反查数据来源。
     */
    private static String resolveSystemName(FilePipelineProperties properties,
                                            KafkaClusterProperties clusterProperties) {
        String configured = properties.getSystemName();
        if (configured != null && !configured.trim().isEmpty()) {
            return configured;
        }
        String clusterName = clusterProperties == null ? null : clusterProperties.getName();
        if (clusterName != null && !clusterName.trim().isEmpty()) {
            log.warn("未配置 file.system-name，文件名回退使用集群标识: {}", clusterName);
            return clusterName;
        }
        log.warn("未配置 file.system-name 且集群标识为空，文件名回退使用 kafka");
        return "kafka";
    }

    /** 当前使用的系统配置名称（文件名前缀） */
    public String getSystemName() {
        return allocator.getSystemName();
    }

    /** 输出文件扩展名（含前导点），如 .txt */
    public String getFileExtension() {
        return allocator.getExtension();
    }

    /** 累计隔离（无法映射到字段清单）的记录数 */
    public long getRejectedRecords() {
        return rejectStore.getTotal();
    }

    /** 进程启动以来成功写入主文件的记录条数（含尚未封盘的分片） */
    public long getWrittenRecords() {
        return writtenRecords.sum();
    }

    /** 隔离文件目录 */
    public Path getRejectDir() {
        return rejectDir;
    }

    /**
     * 恢复上次进程异常退出时未封盘的分片，避免数据停留在 stage 目录。
     */
    @PostConstruct
    public void recoverInterruptedFiles() {
        if (!properties.isRecoverOnStart()) {
            log.warn("file.recover-on-start=false，跳过崩溃恢复：stage 目录中的 .writing 分片将保持原样");
            return;
        }
        int recovered = 0;
        try (Stream<Path> topicStream = Files.list(stageDir)) {
            List<Path> topicDirs = topicStream.filter(Files::isDirectory).collect(Collectors.toList());
            for (Path topicDir : topicDirs) {
                List<Path> leftovers;
                try (Stream<Path> fileStream = Files.list(topicDir)) {
                    leftovers = fileStream
                            .filter(p -> p.getFileName().toString().endsWith(".writing"))
                            .collect(Collectors.toList());
                }
                for (Path file : leftovers) {
                    recovered += recoverOne(topicDir, file);
                }
            }
        } catch (IOException e) {
            log.error("恢复未封盘分片失败: {}", e.toString());
        }
        if (recovered > 0) {
            log.warn("崩溃恢复完成：{} 个未封盘分片已转入待上传队列", recovered);
        }
    }

    /**
     * 消费入口：按字段清单渲染成行，按 topic/partition 分流写入，返回实际写入行数。
     * <p>
     * 渲染 + fsync 成功后才允许提交 offset；无法映射的消息转入隔离文件（同样落盘）后照常提交，
     * 既不会阻塞主链路，也不会静默丢数据。
     */
    public int appendBatch(List<ConsumerRecord<String, String>> records) {
        if (records == null || records.isEmpty()) {
            return 0;
        }
        guard();
        Map<String, List<String>> groups = new LinkedHashMap<>();
        int rejected = 0;
        int adjusted = 0;
        Map<String, Integer> rejectReasons = new LinkedHashMap<>();
        Map<String, Integer> adjustReasons = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            KafkaMessageRecord mapped = toRecord(record);
            RecordRenderer.Result parsed = renderer.parse(mapped);
            if (!parsed.isOk()) {
                rejectStore.save(mapped, parsed.getReason());
                rejected++;
                rejectReasons.merge(failureKind(parsed.getReason()), 1, Integer::sum);
                continue;
            }
            String key = record.topic() + "|" + record.partition();
            List<String> rows = groups.computeIfAbsent(key, k -> new ArrayList<>());
            // 一条消息可包含多行业务数据，全部写入同一分区文件
            rows.addAll(parsed.getRows());
            // 严格模式：字段数不符的行整行舍弃，只留存该行原文（不牵连整条报文里的合格行）
            for (RecordRenderer.BadLine bad : parsed.getBadLines()) {
                rejectStore.save(mapped, "line#" + bad.getLineNo() + ":" + bad.getReason(), bad.getContent());
                rejected++;
                rejectReasons.merge(failureKind(bad.getReason()), 1, Integer::sum);
            }
            if (parsed.isAdjusted()) {
                // 宽容模式下才会走到这里：主文件照常输出 31 列，同时把原始报文留痕
                adjusted++;
                adjustReasons.merge(failureKind(parsed.getAdjustReason()), 1, Integer::sum);
                rejectStore.save(mapped, parsed.getAdjustReason());
            }
        }
        if (rejected > 0 || adjusted > 0) {
            // 隔离/审计数据也要 fsync 后才算安全，与主数据同一标准
            rejectStore.flushAndSync();
        }
        if (rejected > 0) {
            log.warn("本批 {} 条消息无法映射到字段清单 {}（详情见隔离文件 {} 的 reason 列）",
                    rejected, rejectReasons, rejectStore.getCurrentFile());
        }
        if (adjusted > 0) {
            log.info("本批 {} 条消息列数与清单不一致，已按位置补 \\N / 截断后写入主文件 {}，原始报文同时留痕于 {}",
                    adjusted, adjustReasons, rejectStore.getCurrentFile());
        }
        int written = 0;
        for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
            int idx = entry.getKey().lastIndexOf('|');
            String topic = entry.getKey().substring(0, idx);
            int partition = Integer.parseInt(entry.getKey().substring(idx + 1));
            written += write(topic, partition, entry.getValue());
        }
        if (written > 0) {
            writtenRecords.add(written);
        }
        return written;
    }

    private int write(String topic, int partition, List<String> rows) {
        String key = topic + "|" + partition;
        StagingWriter writer = writers.get(key);
        if (writer == null) {
            synchronized (writers) {
                writer = writers.get(key);
                if (writer == null) {
                    try {
                        writer = new StagingWriter(stageDir.resolve(safeName(topic)), partition,
                                properties, allocator);
                    } catch (IOException e) {
                        throw new IllegalStateException("创建分片写入器失败: " + key, e);
                    }
                    writers.put(key, writer);
                }
            }
        }
        try {
            for (Path sealed : writer.append(rows)) {
                moveToPending(sealed);
            }
        } catch (IOException e) {
            throw new IllegalStateException("写入分片文件失败: " + key, e);
        }
        return rows.size();
    }

    /** 空闲超时封盘，返回封盘文件数 */
    public int flushIdle() {
        int sealed = 0;
        for (Map.Entry<String, StagingWriter> entry : writers.entrySet()) {
            try {
                sealed += moveAll(entry.getValue().sealIfIdle(properties.getIdleFlushMs()));
            } catch (IOException e) {
                log.error("空闲封盘失败 [{}]: {}", entry.getKey(), e.toString());
            }
        }
        if (sealed > 0) {
            log.info("空闲封盘 {} 个分片（未写满 {} 条）", sealed, properties.getRecordsPerFile());
        }
        return sealed;
    }

    /** 立即封盘全部写入器（手动刷新 / 停机） */
    public int flushAll() {
        int sealed = 0;
        for (String key : new ArrayList<>(writers.keySet())) {
            StagingWriter writer = writers.get(key);
            if (writer == null) {
                continue;
            }
            try {
                List<Path> files = writer.sealNow();
                sealed += moveAll(files);
                if (writer.getCount() == 0) {
                    writers.remove(key);
                    writer.close();
                }
            } catch (IOException e) {
                log.error("封盘失败 [{}]: {}", key, e.toString());
            }
        }
        if (sealed > 0) {
            log.info("手动封盘 {} 个分片", sealed);
        }
        return sealed;
    }

    public FileStats stats() {
        long[] stage = summarize(stageDir);
        long[] pending = summarize(pendingDir);
        long[] backup = summarize(backupDir);
        long[] failed = summarize(failedDir);
        long freeMb = stageDir.toFile().getUsableSpace() / (1024 * 1024);
        return new FileStats(stage[0], pending[0], pending[1], backup[0], backup[1], failed[0], freeMb,
                properties.getRecordsPerFile(), stageDir.toString(), pendingDir.toString(),
                backupDir.toString());
    }

    public Path getPendingDir() {
        return pendingDir;
    }

    public long pendingCount() {
        synchronized (pendingScanLock) {
            long now = System.currentTimeMillis();
            if (now - lastPendingScanAt > 1000L) {
                cachedPendingCount = countFiles(pendingDir);
                lastPendingScanAt = now;
            }
            return cachedPendingCount;
        }
    }

    public void invalidatePendingCache() {
        synchronized (pendingScanLock) {
            lastPendingScanAt = 0L;
        }
    }

    @Override
    public void destroy() {
        flushAll();
        for (StagingWriter writer : writers.values()) {
            writer.close();
        }
        writers.clear();
        rejectStore.close();
        log.info("文件管道已关闭，残留分片已封盘（累计隔离 {} 条）", rejectStore.getTotal());
    }

    private void guard() {
        long pending = pendingCount();
        if (properties.getMaxPendingFiles() > 0 && pending >= properties.getMaxPendingFiles()) {
            throw new BackpressureException("待上传文件积压 " + pending + " 个（上限 "
                    + properties.getMaxPendingFiles() + "），暂停消费等待 FTP 追平");
        }
        long minFree = properties.getMinFreeDiskMb();
        if (minFree > 0) {
            long freeMb = stageDir.toFile().getUsableSpace() / (1024 * 1024);
            if (freeMb < minFree) {
                throw new BackpressureException("磁盘剩余 " + freeMb + "MB 低于阈值 " + minFree
                        + "MB，暂停消费防止磁盘写满");
            }
        }
    }

    private int moveAll(List<Path> sealed) {
        int moved = 0;
        for (Path path : sealed) {
            moveToPending(path);
            moved++;
        }
        return moved;
    }

    private void moveToPending(Path file) {
        try {
            Path target = pendingDir.resolve(file.getFileName());
            try {
                Files.move(file, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
            }
            invalidatePendingCache();
            log.info("分片封盘待上传: {} ({} bytes)", target.getFileName(), Files.size(target));
        } catch (IOException e) {
            throw new IllegalStateException("移入待上传目录失败: " + file, e);
        }
    }

    private int recoverOne(Path topicDir, Path file) {
        try {
            long lines = countLines(file);
            if (lines == 0) {
                Files.deleteIfExists(file);
                return 0;
            }
            Path target = allocator.allocate(topicDir);
            Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
            moveToPending(target);
            log.warn("恢复未封盘分片: {} -> {} ({} 条)", file.getFileName(), target.getFileName(), lines);
            return 1;
        } catch (IOException e) {
            log.error("恢复分片失败: {} | {}", file, e.toString());
            return 0;
        }
    }

    private static long countLines(Path file) throws IOException {
        long lines = 0L;
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            while (reader.readLine() != null) {
                lines++;
            }
        }
        return lines;
    }

    private static long[] summarize(Path dir) {
        long files = 0L;
        long bytes = 0L;
        if (!Files.isDirectory(dir)) {
            return new long[]{0L, 0L};
        }
        try (Stream<Path> stream = Files.walk(dir)) {
            for (Path path : stream.filter(Files::isRegularFile).collect(Collectors.toList())) {
                files++;
                try {
                    bytes += Files.size(path);
                } catch (IOException ignored) {
                    // 统计不精确不影响主流程
                }
            }
        } catch (IOException ignored) {
            // 同上
        }
        return new long[]{files, bytes};
    }

    private static long countFiles(Path dir) {
        if (!Files.isDirectory(dir)) {
            return 0L;
        }
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.filter(Files::isRegularFile).count();
        } catch (IOException e) {
            return 0L;
        }
    }

    private static String safeName(String topic) {
        return topic.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static KafkaMessageRecord toRecord(ConsumerRecord<String, String> record) {
        return new KafkaMessageRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                renderHeaders(record),
                record.timestamp(),
                LocalDateTime.now());
    }

    private static String renderHeaders(ConsumerRecord<String, String> record) {
        if (record.headers() == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Header header : record.headers()) {
            if (sb.length() > 0) {
                sb.append('|');
            }
            sb.append(header.key()).append('=')
                    .append(header.value() == null ? "" : new String(header.value(), StandardCharsets.UTF_8));
            if (sb.length() > MAX_HEADER_LENGTH) {
                break;
            }
        }
        return sb.toString();
    }
}

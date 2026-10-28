package com.example.kafkaloader.file;

import com.example.kafkaloader.model.KafkaMessageRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 隔离区：消息体无法映射到字段清单（非 JSON 对象 / 截断 JSON / 空体）时，
 * 把「来源信息 + 原始报文」按天落盘，做到不静默丢任何一条数据。
 * <p>
 * 文件：{@code <系统配置名称>_reject_yyyyMMdd<扩展名>}（扩展名随 file.file-extension，默认 .txt），列顺序
 * {@code topic / partition / offset / record_ts / consumed_at / payload}。
 * 主数据文件因此永远只有 31 列，不会因为脏数据导致下游整行错位。
 * <p>
 * 写失败直接抛异常，调用方不会 ack，Kafka 会重投——与主链路保持同一套「落盘成功才提交」语义。
 */
class RejectStore implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RejectStore.class);
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final String HEADER = "topic\tpartition\toffset\trecord_ts\tconsumed_at\tpayload\treason";

    private final Path dir;
    private final String systemName;
    private final String extension;
    private final AtomicLong total = new AtomicLong();

    private FileOutputStream out;
    private BufferedWriter writer;
    private String currentDay;
    private Path currentFile;

    RejectStore(Path dir, String systemName) throws IOException {
        this(dir, systemName, FileNameAllocator.DEFAULT_EXTENSION);
    }

    /** 隔离文件扩展名与主数据文件保持一致（file.file-extension，默认 .txt） */
    RejectStore(Path dir, String systemName, String extension) throws IOException {
        this.dir = dir;
        this.systemName = systemName;
        this.extension = FileNameAllocator.normalizeExtension(extension);
        Files.createDirectories(dir);
    }

    /** 追加一条隔离记录（仅写缓冲，需在批次结束时 {@link #flushAndSync()} 才算落盘） */
    synchronized void save(KafkaMessageRecord record) {
        save(record, "");
    }

    /**
     * 追加一条隔离记录，并记下失败原因（empty-payload / json-invalid /
     * field-count-mismatch(expected=31,actual=N)），便于现场一次定位报文格式。
     */
    synchronized void save(KafkaMessageRecord record, String reason) {
        save(record, reason, record.getPayload());
    }

    /**
     * 追加一条隔离记录，并指定要留存的原始内容（严格模式下只舍弃某一行时，
     * 传该行原文，避免把整条多行报文写进隔离区）。
     */
    synchronized void save(KafkaMessageRecord record, String reason, String rawContent) {
        try {
            rotateIfNeeded();
            StringBuilder sb = new StringBuilder(256);
            sb.append(clean(record.getTopic())).append('\t')
                    .append(record.getPartition()).append('\t')
                    .append(record.getOffset()).append('\t')
                    .append(record.getRecordTimestamp() == null ? RecordRenderer.NULL_VALUE
                            : String.valueOf(record.getRecordTimestamp())).append('\t')
                    .append(record.getConsumedAt() == null ? RecordRenderer.NULL_VALUE
                            : record.getConsumedAt().toString()).append('\t')
                    .append(escapePayload(rawContent)).append('\t')
                    .append(clean(reason == null ? "" : reason));
            writer.write(sb.toString());
            writer.newLine();
            total.incrementAndGet();
        } catch (IOException e) {
            throw new IllegalStateException("写入隔离文件失败: " + currentFile, e);
        }
    }

    /** 批量落盘：flush + fsync，成功后调用方才允许提交 offset */
    synchronized void flushAndSync() {
        if (writer == null) {
            return;
        }
        try {
            writer.flush();
            out.getFD().sync();
        } catch (IOException e) {
            throw new IllegalStateException("刷盘隔离文件失败: " + currentFile, e);
        }
    }

    synchronized long getTotal() {
        return total.get();
    }

    synchronized Path getCurrentFile() {
        return currentFile;
    }

    @Override
    public synchronized void close() {
        try {
            if (writer != null) {
                writer.flush();
                out.getFD().sync();
                writer.close();
            }
        } catch (IOException e) {
            log.error("关闭隔离文件失败: {}", e.toString());
        } finally {
            writer = null;
            out = null;
            currentDay = null;
        }
    }

    private void rotateIfNeeded() throws IOException {
        String day = LocalDate.now().format(DAY);
        if (day.equals(currentDay) && writer != null) {
            return;
        }
        closeQuietly();
        currentFile = dir.resolve(systemName + "_reject_" + day + extension);
        boolean exists = Files.exists(currentFile) && Files.size(currentFile) > 0;
        out = new FileOutputStream(currentFile.toFile(), true);
        writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), 64 * 1024);
        if (!exists) {
            writer.write(HEADER);
            writer.newLine();
        }
        currentDay = day;
        log.info("隔离文件就绪: {}（累计隔离 {} 条）", currentFile, total.get());
    }

    private void closeQuietly() {
        if (writer == null) {
            return;
        }
        try {
            writer.flush();
            out.getFD().sync();
            writer.close();
        } catch (IOException e) {
            log.error("切换隔离文件时关闭失败: {}", e.toString());
        } finally {
            writer = null;
            out = null;
        }
    }

    private static String clean(String value) {
        if (value == null) {
            return "";
        }
        return value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    /**
     * 报文原样保存：把控制字符转成可见的 \t / \n 字面量。
     * 直接用空格替换会摧毁分隔符证据（现场报文是 TAB 分隔多行），排查时无法还原格式。
     */
    private static String escapePayload(String payload) {
        if (payload == null) {
            return "";
        }
        return payload.replace("\r\n", "\\n")
                .replace("\r", "\\n")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }
}

package com.example.kafkaloader.file;

import com.example.kafkaloader.config.FilePipelineProperties;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 单个 (topic, partition) 的分片写入器：写满 {@code recordsPerFile} 行即封盘滚动，
 * 封盘时 flush + fsync + 原子改名，保证下游永远不会看到半截文件。
 * <p>
 * 只负责「写已经渲染好的行」，不关心列的含义——列顺序由 {@link DataSchema} 决定。
 * 文件名由 {@link FileNameAllocator} 统一分配，格式 {@code 系统配置名称_时间戳.csv}，
 * 封盘后 pending / backup / FTP 全链路沿用同一个名字，便于端到端追溯。
 */
class StagingWriter implements AutoCloseable {

    private final Path topicDir;
    private final int partition;
    private final int recordsPerFile;
    private final boolean fsyncPerBatch;
    private final FileNameAllocator allocator;

    private Path workingFile;
    private FileOutputStream out;
    private BufferedWriter writer;
    private int count;
    private volatile long lastWriteAt = System.currentTimeMillis();

    StagingWriter(Path topicDir, int partition, FilePipelineProperties properties,
                  FileNameAllocator allocator) throws IOException {
        this.topicDir = topicDir;
        this.partition = partition;
        this.recordsPerFile = Math.max(1, properties.getRecordsPerFile());
        this.fsyncPerBatch = properties.isFsyncPerBatch();
        this.allocator = allocator;
        openNew();
    }

    /**
     * 追加一批已渲染的行，返回本次写满而封盘的文件。
     */
    synchronized List<Path> append(List<String> rows) throws IOException {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<Path> sealed = new ArrayList<>();
        int index = 0;
        while (index < rows.size()) {
            if (count >= recordsPerFile) {
                sealed.add(seal());
                openNew();
            }
            int room = recordsPerFile - count;
            int take = Math.min(room, rows.size() - index);
            for (int i = 0; i < take; i++) {
                writer.write(rows.get(index + i));
                writer.newLine();
            }
            count += take;
            index += take;
            lastWriteAt = System.currentTimeMillis();
            if (count >= recordsPerFile) {
                sealed.add(seal());
                openNew();
            }
        }
        writer.flush();
        if (fsyncPerBatch) {
            out.getFD().sync();
        }
        return sealed;
    }

    /**
     * 空闲超时则把未写满的文件封盘（低峰期数据不会长期滞留本地）。
     */
    synchronized List<Path> sealIfIdle(long idleMs) throws IOException {
        if (count == 0) {
            return Collections.emptyList();
        }
        if (System.currentTimeMillis() - lastWriteAt < idleMs) {
            return Collections.emptyList();
        }
        Path sealed = seal();
        openNew();
        return Collections.singletonList(sealed);
    }

    /**
     * 强制封盘（停机 / 手动刷新时调用）。无数据则不产生文件。
     */
    synchronized List<Path> sealNow() throws IOException {
        if (count == 0) {
            return Collections.emptyList();
        }
        Path sealed = seal();
        openNew();
        return Collections.singletonList(sealed);
    }

    synchronized int getCount() {
        return count;
    }

    private void openNew() throws IOException {
        Files.createDirectories(topicDir);
        // 临时名带分区号便于排障；createTempFile 保证并发下不撞名
        workingFile = Files.createTempFile(topicDir, "p" + partition + "-", ".writing");
        out = new FileOutputStream(workingFile.toFile(), false);
        writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), 128 * 1024);
        count = 0;
    }

    private Path seal() throws IOException {
        writer.flush();
        out.getFD().sync();
        writer.close();

        Path target = allocator.allocate(topicDir);
        try {
            Files.move(workingFile, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(workingFile, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }

    @Override
    public synchronized void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException ignored) {
            // 关闭失败不影响进程退出
        }
    }
}

package com.example.kafkaloader.file;

import com.example.kafkaloader.config.FilePipelineProperties;
import com.example.kafkaloader.config.FtpProperties;
import com.example.kafkaloader.ftp.FtpUploader;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 待上传文件处理：<b>先本地备份，再上传 FTP</b>，上传校验通过后才删除本地待上传文件。
 * <p>
 * 稳定性优先的取舍：
 * <ul>
 *   <li>备份失败就不上传，宁可延迟也不让数据只有一份</li>
 *   <li>上传失败保留文件，按次数重试；超过上限移入 failed 目录并告警，不静默丢弃</li>
 *   <li>上传与备份都使用临时文件 + 原子改名，避免出现半截文件</li>
 * </ul>
 */
@Service
public class FileUploadService {

    private static final Logger log = LoggerFactory.getLogger(FileUploadService.class);
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final FilePipelineProperties properties;
    private final FtpProperties ftpProperties;
    private final FtpUploader uploader;
    private final FileStagingManager stagingManager;
    private final Path pendingDir;
    private final Path backupDir;
    private final Path failedDir;
    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Counter uploadedCounter;
    private final Counter failedCounter;
    private final Counter backupCounter;

    public FileUploadService(FilePipelineProperties properties, FtpProperties ftpProperties,
                             FtpUploader uploader, FileStagingManager stagingManager,
                             MeterRegistry meterRegistry) {
        this.properties = properties;
        this.ftpProperties = ftpProperties;
        this.uploader = uploader;
        this.stagingManager = stagingManager;
        this.pendingDir = Paths.get(properties.getPendingDir()).toAbsolutePath().normalize();
        this.backupDir = Paths.get(properties.getBackupDir()).toAbsolutePath().normalize();
        this.failedDir = Paths.get(properties.getFailedDir()).toAbsolutePath().normalize();
        this.uploadedCounter = Counter.builder("file.chunks.uploaded").register(meterRegistry);
        this.failedCounter = Counter.builder("file.chunks.upload_failed").register(meterRegistry);
        this.backupCounter = Counter.builder("file.chunks.backup").register(meterRegistry);
    }

    public boolean isRunning() {
        return lock.isLocked();
    }

    /**
     * 扫描待上传目录：备份 -> 上传 -> 校验 -> 清理。
     */
    public UploadResult uploadPending() {
        if (!ftpProperties.isEnabled()) {
            return UploadResult.skipped("FTP 未启用，跳过上传");
        }
        if (!lock.tryLock()) {
            return UploadResult.skipped("已有上传任务在执行，本次跳过");
        }
        long start = System.currentTimeMillis();
        int scanned = 0;
        int uploaded = 0;
        int failed = 0;
        int backupSkipped = 0;
        try {
            List<Path> files = listPending();
            for (Path file : files) {
                scanned++;
                if (!backup(file)) {
                    backupSkipped++;
                    continue;
                }
                try {
                    uploader.upload(file);
                    uploaded++;
                    uploadedCounter.increment();
                    attempts.remove(file.getFileName().toString());
                    deletePending(file);
                } catch (Exception e) {
                    failed++;
                    handleFailure(file, e);
                }
            }
        } catch (IOException e) {
            log.error("扫描待上传目录失败: {}", e.toString());
        } finally {
            stagingManager.invalidatePendingCache();
            lock.unlock();
        }

        long elapsed = System.currentTimeMillis() - start;
        if (scanned > 0) {
            log.info("文件上传完成 | 扫描={} | 成功={} | 失败={} | 备份跳过={} | 耗时={}ms",
                    scanned, uploaded, failed, backupSkipped, elapsed);
        }
        return new UploadResult(true, scanned, uploaded, failed, backupSkipped, elapsed,
                scanned == 0 ? "没有待上传文件" : "上传完成");
    }

    /**
     * 清理超过保留天数的本地备份。
     */
    public int cleanupBackup() {
        if (!Files.isDirectory(backupDir)) {
            return 0;
        }
        LocalDate threshold = LocalDate.now().minusDays(Math.max(1, properties.getBackupRetentionDays()));
        int removed = 0;
        try (Stream<Path> days = Files.list(backupDir)) {
            List<Path> dayDirs = days.filter(Files::isDirectory).collect(Collectors.toList());
            for (Path dayDir : dayDirs) {
                String name = dayDir.getFileName().toString();
                if (!name.startsWith("dt=")) {
                    continue;
                }
                LocalDate day = LocalDate.parse(name.substring(3), DAY);
                if (!day.isBefore(threshold)) {
                    continue;
                }
                removed += deleteRecursively(dayDir);
            }
        } catch (IOException | RuntimeException e) {
            log.error("清理备份失败: {}", e.toString());
        }
        if (removed > 0) {
            log.info("已清理 {} 个过期备份文件（保留 {} 天）", removed, properties.getBackupRetentionDays());
        }
        return removed;
    }

    private boolean backup(Path file) {
        try {
            Path dayDir = backupDir.resolve("dt=" + LocalDate.now().format(DAY));
            Files.createDirectories(dayDir);
            Path target = dayDir.resolve(file.getFileName());
            if (Files.exists(target) && Files.size(target) == Files.size(file)) {
                return true;
            }
            Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
            Files.copy(file, tmp, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            backupCounter.increment();
            log.info("已本地备份: {}", target);
            return true;
        } catch (IOException e) {
            log.error("本地备份失败，暂不上传该文件: {} | {}", file.getFileName(), e.toString());
            return false;
        }
    }

    private void deletePending(Path file) {
        if (!ftpProperties.isDeleteAfterUpload()) {
            return;
        }
        try {
            Files.deleteIfExists(file);
            log.info("上传确认完成，已清理本地待上传文件（备份保留）: {}", file.getFileName());
        } catch (IOException e) {
            log.warn("删除待上传文件失败，下轮会重复上传（远端同名覆盖）: {} | {}",
                    file.getFileName(), e.toString());
        }
    }

    private void handleFailure(Path file, Exception error) {
        String name = file.getFileName().toString();
        int count = attempts.merge(name, 1, Integer::sum);
        failedCounter.increment();
        if (count >= properties.getUploadMaxAttempts()) {
            moveToFailed(file, error.getMessage());
            attempts.remove(name);
        } else {
            log.warn("上传失败({}/{}): {} | {}", count, properties.getUploadMaxAttempts(), name, error.toString());
        }
    }

    private void moveToFailed(Path file, String reason) {
        try {
            Files.createDirectories(failedDir);
            Path target = failedDir.resolve(file.getFileName());
            Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
            Files.write(target.resolveSibling(target.getFileName() + ".reason"),
                    String.valueOf(reason).getBytes(StandardCharsets.UTF_8));
            log.error("文件上传连续失败已达上限，移入隔离目录: {} | 原因: {}", target, reason);
        } catch (IOException e) {
            log.error("移入隔离目录失败: {} | {}", file.getFileName(), e.toString());
        }
    }

    private List<Path> listPending() throws IOException {
        if (!Files.isDirectory(pendingDir)) {
            return new ArrayList<>();
        }
        try (Stream<Path> stream = Files.list(pendingDir)) {
            return stream.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .collect(Collectors.toList());
        }
    }

    private static int deleteRecursively(Path dir) throws IOException {
        int removed = 0;
        try (Stream<Path> stream = Files.walk(dir)) {
            List<Path> all = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path path : all) {
                boolean regularFile = Files.isRegularFile(path);
                Files.deleteIfExists(path);
                if (regularFile) {
                    removed++;
                }
            }
        }
        return removed;
    }
}

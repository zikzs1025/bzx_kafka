package com.example.kafkaloader.file;

import com.example.kafkaloader.config.FilePipelineProperties;
import com.example.kafkaloader.config.FtpProperties;
import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.ftp.FtpUploader;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileUploadServiceTest {

    private static class StubUploader implements FtpUploader {
        private boolean failing;
        private final List<Path> uploaded = new ArrayList<>();

        @Override
        public void upload(Path file) throws IOException {
            if (failing) {
                throw new IOException("ftp unavailable");
            }
            uploaded.add(file);
        }
    }

    private FilePipelineProperties properties(Path tempDir, int maxAttempts) {
        FilePipelineProperties properties = new FilePipelineProperties();
        properties.setStageDir(tempDir.resolve("stage").toString());
        properties.setPendingDir(tempDir.resolve("pending").toString());
        properties.setBackupDir(tempDir.resolve("backup").toString());
        properties.setFailedDir(tempDir.resolve("failed").toString());
        properties.setRejectDir(tempDir.resolve("reject").toString());
        properties.setUploadMaxAttempts(maxAttempts);
        properties.setMaxPendingFiles(0);
        properties.setMinFreeDiskMb(0);
        return properties;
    }

    private FileUploadService service(FilePipelineProperties properties, StubUploader uploader,
                                      FileStagingManager manager) {
        FtpProperties ftp = new FtpProperties();
        ftp.setEnabled(true);
        ftp.setDeleteAfterUpload(true);
        return new FileUploadService(properties, ftp, uploader, manager, new SimpleMeterRegistry());
    }

    private Path createPending(Path tempDir, String name) throws IOException {
        Path pending = tempDir.resolve("pending");
        Files.createDirectories(pending);
        Path file = pending.resolve(name);
        Files.write(file, "line1\nline2\n".getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private List<Path> listDir(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return new ArrayList<>();
        }
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.collect(Collectors.toList());
        }
    }

    @Test
    void shouldBackupThenUploadThenDeletePending(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 3);
        FileStagingManager manager = new FileStagingManager(properties, new KafkaClusterProperties());
        StubUploader uploader = new StubUploader();
        FileUploadService service = service(properties, uploader, manager);

        Path pendingFile = createPending(tempDir, "DISTRIBUTE_20261022083000123.csv");
        UploadResult result = service.uploadPending();

        assertEquals(1, result.getUploaded());
        assertEquals(1, uploader.uploaded.size());
        assertFalse(Files.exists(pendingFile), "上传确认后应清理待上传文件");

        // 本地备份必须存在
        try (Stream<Path> backups = Files.walk(tempDir.resolve("backup"))) {
            List<Path> files = backups.filter(Files::isRegularFile).collect(Collectors.toList());
            assertEquals(1, files.size());
            assertEquals("DISTRIBUTE_20261022083000123.csv", files.get(0).getFileName().toString());
        }
    }

    @Test
    void shouldKeepPendingWhenUploadFails(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 3);
        FileStagingManager manager = new FileStagingManager(properties, new KafkaClusterProperties());
        StubUploader uploader = new StubUploader();
        uploader.failing = true;
        FileUploadService service = service(properties, uploader, manager);

        Path pendingFile = createPending(tempDir, "DISTRIBUTE_20261022083000456.csv");
        UploadResult result = service.uploadPending();

        assertEquals(0, result.getUploaded());
        assertEquals(1, result.getFailed());
        assertTrue(Files.exists(pendingFile), "上传失败必须保留文件等待重试");
        // 备份仍然生成，保证数据至少两份
        assertEquals(1, listDir(tempDir.resolve("backup").resolve("dt=" + java.time.LocalDate.now())).size());
    }

    @Test
    void shouldMoveToFailedAfterMaxAttempts(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 2);
        FileStagingManager manager = new FileStagingManager(properties, new KafkaClusterProperties());
        StubUploader uploader = new StubUploader();
        uploader.failing = true;
        FileUploadService service = service(properties, uploader, manager);

        createPending(tempDir, "DISTRIBUTE_20261022083000789.csv");
        service.uploadPending();
        assertEquals(1, listDir(tempDir.resolve("pending")).size());

        service.uploadPending();
        assertEquals(0, listDir(tempDir.resolve("pending")).size(), "超过重试上限应移出待上传队列");
        assertEquals(1, listDir(tempDir.resolve("failed")).stream()
                .filter(p -> p.getFileName().toString().endsWith(".csv")).count());
    }

    @Test
    void shouldSkipWhenFtpDisabled(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 3);
        FileStagingManager manager = new FileStagingManager(properties, new KafkaClusterProperties());
        FtpProperties ftp = new FtpProperties();
        ftp.setEnabled(false);
        FileUploadService service = new FileUploadService(properties, ftp, new StubUploader(), manager,
                new SimpleMeterRegistry());

        UploadResult result = service.uploadPending();
        assertFalse(result.isExecuted());
        assertTrue(result.getMessage().contains("FTP 未启用"));
    }

    @Test
    void shouldCleanupExpiredBackup(@TempDir Path tempDir) throws Exception {
        FilePipelineProperties properties = properties(tempDir, 3);
        properties.setBackupRetentionDays(1);
        FileStagingManager manager = new FileStagingManager(properties, new KafkaClusterProperties());
        FileUploadService service = service(properties, new StubUploader(), manager);

        Path oldDay = tempDir.resolve("backup").resolve("dt=2020-01-01");
        Files.createDirectories(oldDay);
        Files.write(oldDay.resolve("old.csv"), "a\n".getBytes(StandardCharsets.UTF_8));
        Path today = tempDir.resolve("backup").resolve("dt=" + java.time.LocalDate.now());
        Files.createDirectories(today);
        Files.write(today.resolve("new.csv"), "b\n".getBytes(StandardCharsets.UTF_8));

        assertEquals(1, service.cleanupBackup());
        assertFalse(Files.exists(oldDay));
        assertTrue(Files.exists(today));
    }
}

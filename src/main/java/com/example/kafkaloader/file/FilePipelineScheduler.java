package com.example.kafkaloader.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 文件管道定时任务：空闲封盘、上传待处理文件、清理过期备份。
 */
@Component
public class FilePipelineScheduler {

    private static final Logger log = LoggerFactory.getLogger(FilePipelineScheduler.class);

    private final FileStagingManager stagingManager;
    private final FileUploadService uploadService;

    public FilePipelineScheduler(FileStagingManager stagingManager, FileUploadService uploadService) {
        this.stagingManager = stagingManager;
        this.uploadService = uploadService;
    }

    /** 未写满的文件在空闲一段时间后也要封盘，避免数据长期停留在本地 */
    @Scheduled(fixedDelayString = "${file.flush-check-interval-ms:30000}")
    public void flushIdleFiles() {
        try {
            stagingManager.flushIdle();
        } catch (RuntimeException e) {
            log.error("空闲封盘异常: {}", e.toString());
        }
    }

    /** 待上传队列扫描：备份 + 上传 + 校验 + 清理 */
    @Scheduled(
            initialDelayString = "${file.upload-interval-ms:60000}",
            fixedDelayString = "${file.upload-interval-ms:60000}")
    public void uploadPendingFiles() {
        try {
            UploadResult result = uploadService.uploadPending();
            if (result.getUploaded() > 0 || result.getFailed() > 0) {
                log.info("定时上传: 成功 {} 个, 失败 {} 个", result.getUploaded(), result.getFailed());
            }
        } catch (RuntimeException e) {
            log.error("定时上传异常: {}", e.toString());
        }
    }

    /** 每天清理过期本地备份 */
    @Scheduled(cron = "${file.backup-cleanup-cron:0 30 3 * * ?}")
    public void cleanupBackup() {
        try {
            uploadService.cleanupBackup();
        } catch (RuntimeException e) {
            log.error("备份清理异常: {}", e.toString());
        }
    }
}

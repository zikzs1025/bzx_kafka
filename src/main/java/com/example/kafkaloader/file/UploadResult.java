package com.example.kafkaloader.file;

public class UploadResult {

    private final boolean executed;
    private final int scanned;
    private final int uploaded;
    private final int failed;
    private final int backupSkipped;
    private final long elapsedMs;
    private final String message;

    public UploadResult(boolean executed, int scanned, int uploaded, int failed, int backupSkipped,
                        long elapsedMs, String message) {
        this.executed = executed;
        this.scanned = scanned;
        this.uploaded = uploaded;
        this.failed = failed;
        this.backupSkipped = backupSkipped;
        this.elapsedMs = elapsedMs;
        this.message = message;
    }

    public static UploadResult skipped(String message) {
        return new UploadResult(false, 0, 0, 0, 0, 0L, message);
    }

    public boolean isExecuted() {
        return executed;
    }

    public int getScanned() {
        return scanned;
    }

    public int getUploaded() {
        return uploaded;
    }

    public int getFailed() {
        return failed;
    }

    public int getBackupSkipped() {
        return backupSkipped;
    }

    public long getElapsedMs() {
        return elapsedMs;
    }

    public String getMessage() {
        return message;
    }
}

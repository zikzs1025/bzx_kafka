package com.example.kafkaloader.file;

public class FileStats {

    private final long stageFiles;
    private final long pendingFiles;
    private final long pendingBytes;
    private final long backupFiles;
    private final long backupBytes;
    private final long failedFiles;
    private final long diskFreeMb;
    private final int recordsPerFile;
    private final String stageDir;
    private final String pendingDir;
    private final String backupDir;

    public FileStats(long stageFiles, long pendingFiles, long pendingBytes, long backupFiles, long backupBytes,
                     long failedFiles, long diskFreeMb, int recordsPerFile,
                     String stageDir, String pendingDir, String backupDir) {
        this.stageFiles = stageFiles;
        this.pendingFiles = pendingFiles;
        this.pendingBytes = pendingBytes;
        this.backupFiles = backupFiles;
        this.backupBytes = backupBytes;
        this.failedFiles = failedFiles;
        this.diskFreeMb = diskFreeMb;
        this.recordsPerFile = recordsPerFile;
        this.stageDir = stageDir;
        this.pendingDir = pendingDir;
        this.backupDir = backupDir;
    }

    public long getStageFiles() {
        return stageFiles;
    }

    public long getPendingFiles() {
        return pendingFiles;
    }

    public long getPendingBytes() {
        return pendingBytes;
    }

    public long getBackupFiles() {
        return backupFiles;
    }

    public long getBackupBytes() {
        return backupBytes;
    }

    public long getFailedFiles() {
        return failedFiles;
    }

    public long getDiskFreeMb() {
        return diskFreeMb;
    }

    public int getRecordsPerFile() {
        return recordsPerFile;
    }

    public String getStageDir() {
        return stageDir;
    }

    public String getPendingDir() {
        return pendingDir;
    }

    public String getBackupDir() {
        return backupDir;
    }
}

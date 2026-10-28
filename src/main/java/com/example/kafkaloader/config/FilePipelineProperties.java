package com.example.kafkaloader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 分片文件管道配置：Kafka 数据先落本地文件，再备份并上传 FTP。
 */
@ConfigurationProperties(prefix = "file")
public class FilePipelineProperties {

    /** 正在写入的分片目录 */
    private String stageDir = "./data/file-stage";

    /** 已封盘、待备份上传的目录 */
    private String pendingDir = "./data/file-pending";

    /** 本地备份目录（按天分目录） */
    private String backupDir = "./data/file-backup";

    /** 多次重试仍失败的文件，移入此目录等待人工处理 */
    private String failedDir = "./data/file-failed";

    /** 无法映射到字段清单的消息，按天隔离到该目录 */
    private String rejectDir = "./data/file-reject";

    /** 单文件最大记录数，达到即滚动新文件 */
    private int recordsPerFile = 50000;

    /**
     * 系统配置名称，用于拼装文件名：{@code 系统配置名称_时间戳<扩展名>}。
     * 留空时回退为 {@code kafka.cluster.name}，保证任何情况下文件名都有业务含义。
     */
    private String systemName;

    /**
     * 输出文件扩展名，默认 {@code .txt}（字段以 {@code \t} 分隔）。
     * 可写成 {@code txt} / {@code .txt} / {@code .csv} 等，程序自动补前导点；
     * 只影响主数据文件，隔离区仍为 {@code .csv}。
     */
    private String fileExtension = ".txt";

    /** 每批写入后是否强制刷盘（稳定性优先，默认开启） */
    private boolean fsyncPerBatch = true;

    /** 空闲多久（毫秒）后把未写满的文件封盘，避免低峰期数据长期滞留 */
    private long idleFlushMs = 60_000L;

    /** 空闲检查间隔 */
    private long flushCheckIntervalMs = 30_000L;

    /** 上传任务间隔 */
    private long uploadIntervalMs = 60_000L;

    /** 待上传文件积压上限，超过则暂停消费形成背压，防止磁盘被打满 */
    private long maxPendingFiles = 2000L;

    /** 磁盘剩余空间低水位（MB），低于该值暂停消费 */
    private long minFreeDiskMb = 2048L;

    /** 本地备份保留天数 */
    private int backupRetentionDays = 7;

    /** 单文件上传最大重试次数，超过移入 failed 目录 */
    private int uploadMaxAttempts = 10;

    /** 备份清理 cron */
    private String backupCleanupCron = "0 30 3 * * ?";

    /**
     * 启动时是否执行崩溃恢复：把上次进程异常退出遗留的 {@code .writing} 分片
     * 封盘转入待上传队列。除非确认遗留文件需要人工保留，否则不要关闭。
     */
    private boolean recoverOnStart = true;

    /**
     * 消息体解析模式：
     * <ul>
     *   <li>{@code AUTO}（默认）：以 <code>{</code> 开头按 JSON 对象解析，否则按分隔符切分</li>
     *   <li>{@code JSON}：只接受 JSON 对象，其余进隔离区</li>
     *   <li>{@code DELIMITER}：只按分隔符切分，不做 JSON 尝试</li>
     * </ul>
     */
    private String parseMode = "AUTO";

    /** 分隔符正则，默认制表符。现场报文是 TAB 分隔 31 列、空字段靠连续 TAB 占位，必须用精确分隔符 */
    private String delimiterRegex = "\t";

    /**
     * 期望列数：
     * <ul>
     *   <li>{@code 31}（默认）：按《字段清单》装载，配合 strict-field-count 决定补位还是拒收</li>
     *   <li>{@code 0}：自适应——不限制列数，报文行内有多少列就输出多少列（\t 分隔，空值写 \N），
     *       用于上游列数尚未定稿、需要先看真实数据的阶段</li>
     * </ul>
     */
    private int expectedColumns = 31;

    /**
     * 字段数是否必须严格等于字段清单数量（31）。仅在 expected-columns > 0 时生效。
     * true：数量不符直接进隔离区（隔离文件的 reason 列会写明实际字段数），不会写出错位数据；
     * false：少于 31 列时右侧补 {@code \N}，多于 31 列时截断（仅在现场确认规则后开启）。
     */
    private boolean strictFieldCount = true;

    /**
     * 列映射（可选）：上游报文的列顺序与《字段清单》不一致时用它转换。
     * 写法是按**报文顺序**列出每个源列要落到清单的第几列（1 基），逗号分隔。
     * <p>
     * 例：报文 4 列依次是 zjhm(清单第19) / xm(第20) / xbdm(第21) / csrq(第25)，
     * 则配置 {@code column-mapping: "19,20,21,25"}；未列出的清单列自动写 {@code \N}。
     * <p>
     * 留空表示「报文列序 == 清单列序」，按位置直接装载（要求列数正好 31）。
     */
    private String columnMapping = "";

    public String getStageDir() {
        return stageDir;
    }

    public void setStageDir(String stageDir) {
        this.stageDir = stageDir;
    }

    public String getPendingDir() {
        return pendingDir;
    }

    public void setPendingDir(String pendingDir) {
        this.pendingDir = pendingDir;
    }

    public String getBackupDir() {
        return backupDir;
    }

    public void setBackupDir(String backupDir) {
        this.backupDir = backupDir;
    }

    public String getFailedDir() {
        return failedDir;
    }

    public void setFailedDir(String failedDir) {
        this.failedDir = failedDir;
    }

    public String getRejectDir() {
        return rejectDir;
    }

    public void setRejectDir(String rejectDir) {
        this.rejectDir = rejectDir;
    }

    public int getRecordsPerFile() {
        return recordsPerFile;
    }

    public void setRecordsPerFile(int recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    public boolean isFsyncPerBatch() {
        return fsyncPerBatch;
    }

    public void setFsyncPerBatch(boolean fsyncPerBatch) {
        this.fsyncPerBatch = fsyncPerBatch;
    }

    public long getIdleFlushMs() {
        return idleFlushMs;
    }

    public void setIdleFlushMs(long idleFlushMs) {
        this.idleFlushMs = idleFlushMs;
    }

    public long getFlushCheckIntervalMs() {
        return flushCheckIntervalMs;
    }

    public void setFlushCheckIntervalMs(long flushCheckIntervalMs) {
        this.flushCheckIntervalMs = flushCheckIntervalMs;
    }

    public long getUploadIntervalMs() {
        return uploadIntervalMs;
    }

    public void setUploadIntervalMs(long uploadIntervalMs) {
        this.uploadIntervalMs = uploadIntervalMs;
    }

    public long getMaxPendingFiles() {
        return maxPendingFiles;
    }

    public void setMaxPendingFiles(long maxPendingFiles) {
        this.maxPendingFiles = maxPendingFiles;
    }

    public long getMinFreeDiskMb() {
        return minFreeDiskMb;
    }

    public void setMinFreeDiskMb(long minFreeDiskMb) {
        this.minFreeDiskMb = minFreeDiskMb;
    }

    public int getBackupRetentionDays() {
        return backupRetentionDays;
    }

    public void setBackupRetentionDays(int backupRetentionDays) {
        this.backupRetentionDays = backupRetentionDays;
    }

    public int getUploadMaxAttempts() {
        return uploadMaxAttempts;
    }

    public void setUploadMaxAttempts(int uploadMaxAttempts) {
        this.uploadMaxAttempts = uploadMaxAttempts;
    }

    public String getBackupCleanupCron() {
        return backupCleanupCron;
    }

    public void setBackupCleanupCron(String backupCleanupCron) {
        this.backupCleanupCron = backupCleanupCron;
    }

    public boolean isRecoverOnStart() {
        return recoverOnStart;
    }

    public void setRecoverOnStart(boolean recoverOnStart) {
        this.recoverOnStart = recoverOnStart;
    }

    public String getParseMode() {
        return parseMode;
    }

    public void setParseMode(String parseMode) {
        this.parseMode = parseMode;
    }

    public String getDelimiterRegex() {
        return delimiterRegex;
    }

    public void setDelimiterRegex(String delimiterRegex) {
        this.delimiterRegex = delimiterRegex;
    }

    public boolean isStrictFieldCount() {
        return strictFieldCount;
    }

    public void setStrictFieldCount(boolean strictFieldCount) {
        this.strictFieldCount = strictFieldCount;
    }

    public int getExpectedColumns() {
        return expectedColumns;
    }

    public void setExpectedColumns(int expectedColumns) {
        this.expectedColumns = expectedColumns;
    }

    public String getColumnMapping() {
        return columnMapping;
    }

    public void setColumnMapping(String columnMapping) {
        this.columnMapping = columnMapping;
    }
}

package com.example.kafkaloader.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerProperties {

    /**
     * 消费组名。必须与现场 Kafka ACL 授权登记的消费组一致，否则报
     * GroupAuthorizationException: Not authorized to access group: xxx。
     * 现场 ACL 按「账号 330100 + 消费组 330100」授权，故默认值取 330100。
     */
    private String groupId = "330100";

    /** 每个主题的并发消费线程数（不宜超过分区数） */
    private int concurrency = 3;

    /** 单次 poll 拉取的最大记录数，等同写入批量 */
    private int maxPollRecords = 500;

    private String autoOffsetReset = "earliest";

    /** 容器重试间隔（毫秒）。仅在「连暂存都失败」这类系统级故障时才会触发 */
    private long retryIntervalMs = 2000L;

    /** 容器重试次数，-1 表示无限重试（数据安全优先，宁可不提交 offset） */
    private long maxRetries = -1L;

    /** 单次 poll 等待数据的毫秒数；越小越灵敏，越大越省 CPU */
    private long pollTimeoutMs = 3000L;

    /**
     * 两次 poll 之间允许的最大间隔，超过会被踢出消费组触发 rebalance。
     * 必须大于「一批数据落盘 + fsync」的最长耗时，否则频繁重平衡。
     */
    private long maxPollIntervalMs = 300_000L;

    /** 单次 fetch 请求最大字节数，用于限制单批数据的内存峰值 */
    private int fetchMaxBytes = 50 * 1024 * 1024;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public long getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public void setRetryIntervalMs(long retryIntervalMs) {
        this.retryIntervalMs = retryIntervalMs;
    }

    public long getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(long maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public void setPollTimeoutMs(long pollTimeoutMs) {
        this.pollTimeoutMs = pollTimeoutMs;
    }

    public long getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    public void setMaxPollIntervalMs(long maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    public int getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    public void setFetchMaxBytes(int fetchMaxBytes) {
        this.fetchMaxBytes = fetchMaxBytes;
    }
}

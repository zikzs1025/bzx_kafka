package com.example.kafkaloader.model;

import java.time.LocalDateTime;

/**
 * 落库实体。offset 三元组是幂等键。
 */
public class KafkaMessageRecord {

    private String topic;
    private int partition;
    private long offset;
    private String msgKey;
    private String payload;
    private String headers;
    private Long recordTimestamp;
    private LocalDateTime consumedAt;

    public KafkaMessageRecord() {
    }

    public KafkaMessageRecord(String topic, int partition, long offset, String msgKey, String payload,
                              String headers, Long recordTimestamp, LocalDateTime consumedAt) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.msgKey = msgKey;
        this.payload = payload;
        this.headers = headers;
        this.recordTimestamp = recordTimestamp;
        this.consumedAt = consumedAt;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getHeaders() {
        return headers;
    }

    public void setHeaders(String headers) {
        this.headers = headers;
    }

    public Long getRecordTimestamp() {
        return recordTimestamp;
    }

    public void setRecordTimestamp(Long recordTimestamp) {
        this.recordTimestamp = recordTimestamp;
    }

    public LocalDateTime getConsumedAt() {
        return consumedAt;
    }

    public void setConsumedAt(LocalDateTime consumedAt) {
        this.consumedAt = consumedAt;
    }
}

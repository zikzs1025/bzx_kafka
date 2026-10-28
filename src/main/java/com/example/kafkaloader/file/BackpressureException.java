package com.example.kafkaloader.file;

/**
 * 背压信号：待上传积压过多或磁盘不足，此时必须停止写入并且不提交 offset，
 * 让 Kafka 侧自然降速，避免磁盘被写满导致整个节点故障。
 */
public class BackpressureException extends RuntimeException {

    public BackpressureException(String message) {
        super(message);
    }
}

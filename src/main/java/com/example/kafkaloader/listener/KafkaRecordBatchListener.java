package com.example.kafkaloader.listener;

import com.example.kafkaloader.file.BackpressureException;
import com.example.kafkaloader.file.FileStagingManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 批量消费入口：数据写入分片文件（含 fsync）后才提交 offset。
 * <p>
 * 写入失败或触发背压时一律不 ack，让 Kafka 重投，从而做到「数据要么落地成功，要么仍在 Kafka 中」。
 */
@Component
public class KafkaRecordBatchListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordBatchListener.class);

    private final FileStagingManager stagingManager;

    public KafkaRecordBatchListener(FileStagingManager stagingManager) {
        this.stagingManager = stagingManager;
    }

    @KafkaListener(
            id = "kafka-record-loader",
            // 必须显式指定，否则 id 会覆盖配置的消费组名
            groupId = "${kafka.consumer.group-id}",
            topics = "#{@clusterDefinition.topic}",
            containerFactory = "batchListenerContainerFactory")
    public void onBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records == null || records.isEmpty()) {
            ack.acknowledge();
            return;
        }

        int written;
        try {
            written = stagingManager.appendBatch(records);
        } catch (BackpressureException e) {
            // 磁盘不足或待上传积压：不提交 offset，Kafka 自然降速
            log.warn("背压触发，暂停消费本批 {} 条: {}", records.size(), e.getMessage());
            throw e;
        } catch (RuntimeException e) {
            log.error("写入分片文件失败，不提交 offset 等待重投: {}", e.toString());
            throw e;
        }

        ack.acknowledge();
        log.debug("本批 {} 条已写入分片文件并刷盘，提交 offset", written);
    }
}

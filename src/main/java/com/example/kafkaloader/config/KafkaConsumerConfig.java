package com.example.kafkaloader.config;

import com.example.kafkaloader.cluster.ClusterDefinitionProvider;
import com.example.kafkaloader.model.ClusterDefinition;
import com.example.kafkaloader.support.RefreshingConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 消费装配。bootstrap.servers 全部来自 application.yml 的
 * {@code kafka.cluster.bootstrap-servers}，不读取外部配置文件。
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    /**
     * 仅在启动时用于解析 @KafkaListener 的 topic；broker 地址每次建连时从 provider 取最新值。
     */
    @Bean
    public ClusterDefinition clusterDefinition(ClusterDefinitionProvider provider) {
        ClusterDefinition definition = provider.get();
        log.info("Kafka 消费初始化 | topic={} | brokers={}",
                definition.getTopic(), definition.getBootstrapServers());
        return definition;
    }

    @Bean
    public ConsumerFactory<String, String> kafkaConsumerFactory(ClusterDefinitionProvider provider,
                                                                KafkaConsumerProperties properties,
                                                                KafkaSecurityAssembler security) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, provider.getBootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getGroupId());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getAutoOffsetReset());
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getMaxPollRecords());
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) properties.getMaxPollIntervalMs());
        configs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, properties.getFetchMaxBytes());
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // 证书 / SASL 认证（Kerberos、SSL 等）；PLAINTEXT 时只有 security.protocol 一项，行为不变
        configs.putAll(security.getClientConfigs());
        log.info("Kafka 消费初始化 | topic={} | security={}",
                provider.get().getTopic(), security.getSummary());
        return new RefreshingConsumerFactory(provider::getBootstrapServers, configs);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaConsumerProperties properties,
            DefaultErrorHandler kafkaErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(properties.getConcurrency());
        factory.setCommonErrorHandler(kafkaErrorHandler);

        ContainerProperties container = factory.getContainerProperties();
        container.setAckMode(ContainerProperties.AckMode.MANUAL);
        container.setPollTimeout(properties.getPollTimeoutMs());
        container.setSyncCommits(true);
        return factory;
    }

    /**
     * 只有「连本地暂存都失败」才会走到这里，属于系统级故障。
     * 默认无限重试（max-retries=-1），宁可不提交 offset 也不丢数据。
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaConsumerProperties properties) {
        long configured = properties.getMaxRetries();
        long attempts = configured < 0 ? FixedBackOff.UNLIMITED_ATTEMPTS : configured;
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, ex) -> log.error("消息处理失败: topic={} partition={} offset={} cause={}",
                        record.topic(), record.partition(), record.offset(), ex.toString()),
                new FixedBackOff(properties.getRetryIntervalMs(), attempts));
        handler.setAckAfterHandle(true);
        return handler;
    }
}

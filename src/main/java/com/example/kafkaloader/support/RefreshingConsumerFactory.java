package com.example.kafkaloader.support;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 每次建连时从 {@link Supplier} 读取最新 bootstrap.servers。
 * <p>
 * 不继承 DefaultKafkaConsumerFactory：其 getConfigurationProperties() 返回不可变视图，
 * 无法在运行期替换 broker 地址。
 */
public class RefreshingConsumerFactory implements ConsumerFactory<String, String> {

    private final Supplier<String> bootstrapServersSupplier;
    private final Map<String, Object> baseConfigs;
    private final Deserializer<String> keyDeserializer = new StringDeserializer();
    private final Deserializer<String> valueDeserializer = new StringDeserializer();
    private final AtomicInteger clientIdSequence = new AtomicInteger();

    public RefreshingConsumerFactory(Supplier<String> bootstrapServersSupplier, Map<String, Object> baseConfigs) {
        this.bootstrapServersSupplier = bootstrapServersSupplier;
        this.baseConfigs = Collections.unmodifiableMap(new HashMap<>(baseConfigs));
    }

    @Override
    public Map<String, Object> getConfigurationProperties() {
        return baseConfigs;
    }

    @Override
    public boolean isAutoCommit() {
        Object value = baseConfigs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        return value == null || Boolean.parseBoolean(String.valueOf(value));
    }

    @Override
    public Consumer<String, String> createConsumer() {
        return createConsumer(null);
    }

    @Override
    public Consumer<String, String> createConsumer(String clientIdPrefix) {
        return createConsumer(clientIdPrefix, null, null);
    }

    @Override
    public Consumer<String, String> createConsumer(String clientIdPrefix, String groupId, String topic) {
        return new KafkaConsumer<>(resolveConfigs(clientIdPrefix, groupId), keyDeserializer, valueDeserializer);
    }

    /**
     * 合并基础配置与最新 broker 地址；反序列化器以实例传入，故移除 class 配置避免冲突。
     */
    Map<String, Object> resolveConfigs(String clientIdPrefix, String groupId) {
        Map<String, Object> configs = new HashMap<>(baseConfigs);
        configs.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        configs.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        String servers = bootstrapServersSupplier.get();
        if (StringUtils.hasText(servers)) {
            configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }
        if (StringUtils.hasText(groupId)) {
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        if (StringUtils.hasText(clientIdPrefix)) {
            configs.put(ConsumerConfig.CLIENT_ID_CONFIG,
                    clientIdPrefix + "-" + clientIdSequence.getAndIncrement());
        }
        return configs;
    }
}

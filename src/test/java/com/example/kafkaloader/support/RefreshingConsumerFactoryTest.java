package com.example.kafkaloader.support;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RefreshingConsumerFactoryTest {

    private Map<String, Object> baseConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.1:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return configs;
    }

    @Test
    void shouldApplyLatestBootstrapServersOnEachCreate() {
        AtomicReference<String> servers = new AtomicReference<>("10.0.0.1:9092");
        RefreshingConsumerFactory factory = new RefreshingConsumerFactory(servers::get, baseConfigs());

        assertEquals("10.0.0.1:9092", factory.resolveConfigs("c", null)
                .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));

        // 供应方（示例中为集群快照）变化后，下次建连必须使用新地址
        servers.set("10.100.1.180:9092,10.100.1.181:9092");
        assertEquals("10.100.1.180:9092,10.100.1.181:9092", factory.resolveConfigs("c", null)
                .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    void shouldIgnoreBlankSupplierValue() {
        RefreshingConsumerFactory factory = new RefreshingConsumerFactory(() -> "", baseConfigs());

        assertEquals("10.0.0.1:9092", factory.resolveConfigs("c", null)
                .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    void shouldKeepDeserializerInstancesNotClasses() {
        RefreshingConsumerFactory factory = new RefreshingConsumerFactory(() -> "10.0.0.1:9092", baseConfigs());
        Map<String, Object> configs = factory.resolveConfigs(null, null);

        assertFalse(configs.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertFalse(configs.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    }

    @Test
    void shouldApplyGroupIdOverrideAndGenerateClientId() {
        RefreshingConsumerFactory factory = new RefreshingConsumerFactory(() -> "10.0.0.1:9092", baseConfigs());

        Map<String, Object> first = factory.resolveConfigs("consumer-1", "group-x");
        Map<String, Object> second = factory.resolveConfigs("consumer-1", "group-x");

        assertEquals("group-x", first.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("consumer-1-0", first.get(ConsumerConfig.CLIENT_ID_CONFIG));
        assertEquals("consumer-1-1", second.get(ConsumerConfig.CLIENT_ID_CONFIG));
    }
}

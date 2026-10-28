package com.example.kafkaloader.cluster;

import com.example.kafkaloader.config.KafkaClusterProperties;
import com.example.kafkaloader.model.KafkaNode;
import com.example.kafkaloader.repository.KafkaNodeRepository;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterDefinitionProviderTest {

    private static class StubRepository implements KafkaNodeRepository {
        private List<KafkaNode> nodes = new ArrayList<>();
        private RuntimeException error;

        @Override
        public List<KafkaNode> loadNodes() {
            if (error != null) {
                throw error;
            }
            return nodes;
        }
    }

    private KafkaClusterProperties properties() {
        KafkaClusterProperties properties = new KafkaClusterProperties();
        properties.setName("DISTRIBUTE");
        properties.setTopic("DISTRIBUTE_SQAJXYRXX_TO");
        properties.setDefaultPort(9092);
        return properties;
    }

    @Test
    void shouldBuildBootstrapServersFromConfiguredNodes() {
        StubRepository repository = new StubRepository();
        repository.nodes = Arrays.asList(
                new KafkaNode("10.100.1.180", "cmkafka", 9092),
                new KafkaNode("10.100.1.181", "kafka01", 9093));

        ClusterDefinitionProvider provider = new ClusterDefinitionProvider(repository, properties());
        provider.initialize();

        assertEquals("10.100.1.180:9092,10.100.1.181:9093", provider.getBootstrapServers());
        assertEquals("DISTRIBUTE_SQAJXYRXX_TO", provider.get().getTopic());
        assertEquals(2, provider.get().getNodes().size());
    }

    @Test
    void shouldTrimTopicBeforeUse() {
        StubRepository repository = new StubRepository();
        repository.nodes = Arrays.asList(new KafkaNode("10.100.1.180", "cmkafka", 9092));
        KafkaClusterProperties properties = properties();
        properties.setTopic("  DISTRIBUTE_TO  ");

        ClusterDefinitionProvider provider = new ClusterDefinitionProvider(repository, properties);
        provider.initialize();

        assertEquals("DISTRIBUTE_TO", provider.get().getTopic());
    }

    @Test
    void shouldReturnSameSnapshotOnEveryGet() {
        StubRepository repository = new StubRepository();
        repository.nodes = Arrays.asList(new KafkaNode("10.100.1.180", "cmkafka", 9092));
        ClusterDefinitionProvider provider = new ClusterDefinitionProvider(repository, properties());
        provider.initialize();

        // 配置只读：多次读取必须是同一个不可变快照，不做任何重新加载
        assertSame(provider.get(), provider.get());
    }

    @Test
    void shouldFailWhenAccessBeforeInitialize() {
        StubRepository repository = new StubRepository();
        repository.nodes = Arrays.asList(new KafkaNode("10.100.1.180", "cmkafka", 9092));
        ClusterDefinitionProvider provider = new ClusterDefinitionProvider(repository, properties());

        IllegalStateException error = assertThrows(IllegalStateException.class, provider::get);
        assertTrue(error.getMessage().contains("尚未初始化"), error.getMessage());
    }

    @Test
    void shouldFailFastWhenNoNodeConfigured() {
        StubRepository repository = new StubRepository();
        ClusterDefinitionProvider provider = new ClusterDefinitionProvider(repository, properties());

        IllegalStateException error = assertThrows(IllegalStateException.class, provider::initialize);
        assertTrue(error.getMessage().contains("未解析出任何 broker"), error.getMessage());
    }

    @Test
    void shouldPropagateRepositoryFailure() {
        StubRepository repository = new StubRepository();
        repository.error = new IllegalStateException("kafka.cluster.bootstrap-servers 未配置");

        ClusterDefinitionProvider provider = new ClusterDefinitionProvider(repository, properties());

        IllegalStateException error = assertThrows(IllegalStateException.class, provider::initialize);
        assertTrue(error.getMessage().contains("bootstrap-servers"), error.getMessage());
    }

    @Test
    void shouldFailFastWhenTopicMissing() {
        StubRepository repository = new StubRepository();
        repository.nodes = Arrays.asList(new KafkaNode("10.100.1.180", "cmkafka", 9092));
        KafkaClusterProperties properties = properties();
        properties.setTopic("  ");

        ClusterDefinitionProvider provider = new ClusterDefinitionProvider(repository, properties);
        assertThrows(IllegalStateException.class, provider::initialize);
    }

    @Test
    void shouldFailFastWhenTopicIllegal() {
        StubRepository repository = new StubRepository();
        repository.nodes = Arrays.asList(new KafkaNode("10.100.1.180", "cmkafka", 9092));
        KafkaClusterProperties properties = properties();
        properties.setTopic("topic with space");

        ClusterDefinitionProvider provider = new ClusterDefinitionProvider(repository, properties);
        IllegalStateException error = assertThrows(IllegalStateException.class, provider::initialize);
        assertTrue(error.getMessage().contains("主题名非法"), error.getMessage());
    }
}
